"""Plan routes: mounted where the gateway looks, and refusals that keep their reason.

`/api/strategy/*` is served by the **account** app (`account/app.py` mounts
`strategies_router`); a plan router mounted anywhere else would 404 everywhere.
That is the first test here.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from bifrost_api.account.app import create_account_app
from bifrost_api.strategy.routers import plans_router
from bifrost_core.monitor.reader import strategy_plan as strategy_plan_module
from bifrost_core.monitor.reader.strategy_plan import PlanRuleError
from tests.contract.helpers import operator_server_config
from tests.reader_mock import reader_mock

PLAN_ROUTES = {
    "/strategies/plans",
    "/strategies/plans/{strategy_plan_id}",
    "/strategies/plans/{strategy_plan_id}/intend",
    "/strategies/plans/{strategy_plan_id}/link-fill",
    "/strategies/plans/{strategy_plan_id}/cancel",
}


def _paths(app: FastAPI) -> set:
    # The OpenAPI path table, not `app.routes`: from FastAPI 0.14x an included
    # router sits in `app.routes` as one wrapper with no `.path`.
    return set(app.openapi()["paths"])


def _account_client(control_via_db: Any = None) -> TestClient:
    reader = reader_mock()
    reader.config = operator_server_config()
    app = create_account_app(
        reader=reader,
        control_via_db=control_via_db,
        status_cfg_for_read=control_via_db,
        merged_config=reader.config,
    )
    return TestClient(app, raise_server_exceptions=False)


def test_every_plan_route_is_mounted_on_the_account_app() -> None:
    from bifrost_api.account import app as account_app_module

    with open(account_app_module.__file__, encoding="utf-8") as fh:
        text = fh.read()
    assert "app.include_router(plans_router)" in text, (
        "plans_router is not mounted on create_account_app — it would 404 in every environment"
    )

    bare = FastAPI()
    bare.include_router(plans_router)
    assert PLAN_ROUTES <= _paths(bare)
    assert PLAN_ROUTES <= _paths(_account_client().app)


def test_writes_without_postgres_say_so_rather_than_pretending() -> None:
    client = _account_client(control_via_db=None)
    body = {"account_id": "U1", "symbol": "NVDA", "structure_label": "Short put", "qty": 1}
    assert client.post("/strategies/plans", json=body).status_code == 503
    assert client.post("/strategies/plans/1/intend").status_code == 503
    assert client.post("/strategies/plans/1/cancel").status_code == 503


def test_reads_without_postgres_are_empty_not_broken() -> None:
    r = _account_client().get("/strategies/plans")
    assert r.status_code == 200
    assert r.json() == {"items": [], "count": 0}


def test_a_read_that_fails_is_500_not_an_empty_list(monkeypatch: pytest.MonkeyPatch) -> None:
    """An empty list is a statement about the account, never about the query.

    With `strategy_plan` missing from an environment, a swallowed read error
    returned `{"items": [], "count": 0}` and that passed as an acceptance check
    for a schema which had never been applied.
    """

    def _broken(*_a: Any, **_kw: Any) -> Any:
        raise RuntimeError('relation "strategy_plan" does not exist')

    monkeypatch.setattr(strategy_plan_module, "list_plans", _broken)
    monkeypatch.setattr(strategy_plan_module, "get_plan", _broken)
    client = _account_client(control_via_db={"sink": "postgres"})
    listed = client.get("/strategies/plans")
    assert listed.status_code == 500
    assert listed.json()["detail"] == "Failed to read strategy plans"
    assert client.get("/strategies/plans/1").status_code == 500


def test_a_rule_refusal_becomes_409_with_the_reason(monkeypatch: pytest.MonkeyPatch) -> None:
    reason = "Write a target, a stop or an exit-by date."

    def _refuse(*_a: Any, **_kw: Any) -> bool:
        raise PlanRuleError(reason)

    monkeypatch.setattr(strategy_plan_module, "intend_plan", _refuse)
    monkeypatch.setattr(strategy_plan_module, "update_plan", _refuse)
    monkeypatch.setattr(strategy_plan_module, "cancel_plan", _refuse)
    monkeypatch.setattr(strategy_plan_module, "link_fill", _refuse)
    client = _account_client(control_via_db={"sink": "postgres"})

    for response in (
        client.post("/strategies/plans/1/intend"),
        client.post("/strategies/plans/1/cancel"),
        client.post("/strategies/plans/1/link-fill", json={"trade_id": 7}),
    ):
        assert response.status_code == 409
        assert response.json()["detail"] == reason


def test_a_missing_plan_is_404_not_409(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(strategy_plan_module, "intend_plan", lambda *_a, **_kw: False)
    monkeypatch.setattr(strategy_plan_module, "get_plan", lambda *_a, **_kw: None)
    client = _account_client(control_via_db={"sink": "postgres"})
    assert client.post("/strategies/plans/404/intend").status_code == 404
    assert client.get("/strategies/plans/404").status_code == 404


def test_create_passes_the_legs_through_and_returns_the_id(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict = {}

    def _create(_cfg: Any, payload: dict) -> int:
        seen.update(payload)
        return 42

    monkeypatch.setattr(strategy_plan_module, "create_plan", _create)
    client = _account_client(control_via_db={"sink": "postgres"})
    r = client.post(
        "/strategies/plans",
        json={
            "account_id": "U1",
            "symbol": "NVDA",
            "structure_label": "Short put",
            "qty": 1,
            "legs": [
                {
                    "side": "sell",
                    "sec_type": "OPT",
                    "right": "P",
                    "strike": 180,
                    "expiry": "2026-11-20",
                }
            ],
            "source_kind": "symbol",
            "source_ref": "research/symbol NVDA",
        },
    )
    assert r.status_code == 200
    assert r.json() == {"strategy_plan_id": 42}
    assert seen["legs"][0]["ratio"] == 1
    assert seen["source_kind"] == "symbol"


def test_create_rejects_a_body_the_table_would_reject() -> None:
    client = _account_client(control_via_db={"sink": "postgres"})
    # qty must be positive, and a leg side is buy or sell — caught before the DB.
    assert client.post(
        "/strategies/plans",
        json={"account_id": "U1", "symbol": "NVDA", "structure_label": "Put", "qty": 0},
    ).status_code == 422
    assert client.post(
        "/strategies/plans",
        json={
            "account_id": "U1",
            "symbol": "NVDA",
            "structure_label": "Put",
            "qty": 1,
            "legs": [{"side": "short", "sec_type": "OPT"}],
        },
    ).status_code == 422


def test_the_limit_is_capped() -> None:
    client = _account_client()
    assert client.get("/strategies/plans?limit=501").status_code == 422
    assert client.get("/strategies/plans?limit=500").status_code == 200


def test_no_route_here_places_an_order() -> None:
    """D10: a plan is a record. There is no order path on this router."""
    import inspect

    from bifrost_api.strategy.routers import plans as plans_module

    source = inspect.getsource(plans_module)
    for forbidden in ("order_intent", "place_order", "ib:operator"):
        assert forbidden not in source, forbidden


# ── TD-178 (api 0.11.0, core 0.53.0): filter by provenance ────────────────


def _plan_row(plan_id: int, source_kind: str, source_ref: Any = None, status: str = "filled") -> dict:
    return {
        "strategy_plan_id": plan_id,
        "account_id": "U1",
        "symbol": "NVDA",
        "structure_label": "Short put",
        "strategy_structure_id": None,
        "strategy_opportunity_id": None,
        "legs_json": [],
        "qty": 1,
        "price_effect": None,
        "limit_price": None,
        "target_kind": None,
        "target_value": None,
        "stop_kind": None,
        "stop_value": None,
        "exit_by": None,
        "rationale": None,
        "source_kind": source_kind,
        "source_ref": source_ref,
        "source_json": [],
        "status": status,
        "expires_at": None,
        "intended_at": None,
        "filled_at": None,
        "cancelled_at": None,
        "trade_id": plan_id if status == "filled" else None,
        "parent_strategy_plan_id": None,
        "created_at": "2026-10-06T00:00:00+00:00",
        "updated_at": "2026-10-06T00:00:00+00:00",
    }


class _PlanTable:
    """A stand-in for Postgres that runs core's WHERE / LIMIT on rows in memory.

    It reads the ``p.<column> = %s`` conditions core writes, in order, so the route,
    core's SQL and its parameters are all exercised -- only the database is fake."""

    def __init__(self, rows: list) -> None:
        self.rows = rows
        self.executed: list = []

    def cursor(self, **_kw: Any) -> "_PlanTable":
        return self

    def __enter__(self) -> "_PlanTable":
        return self

    def __exit__(self, *_exc: Any) -> None:
        return None

    def execute(self, sql: str, params: Any = None) -> None:
        import re

        self.executed.append((sql, list(params or [])))
        where = sql.split(" WHERE ", 1)[1] if " WHERE " in sql else ""
        columns = re.findall(r"p\.(\w+) = %s", where.split("ORDER BY", 1)[0])
        values = list(params or [])
        limit = values.pop()
        assert len(columns) == len(values), (columns, values)
        out = [r for r in self.rows if all(r[c] == v for c, v in zip(columns, values))]
        out.sort(key=lambda r: -r["strategy_plan_id"])
        self._result = out[:limit]

    def fetchall(self) -> list:
        return self._result

    def close(self) -> None:
        return None


def _plans_client(monkeypatch: pytest.MonkeyPatch, rows: list) -> tuple:
    table = _PlanTable(rows)
    monkeypatch.setattr(strategy_plan_module, "_conn_from_config", lambda _cfg: table)
    return _account_client(control_via_db={"sink": "postgres"}), table


def test_source_kind_hypothesis_returns_only_those_plans(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = [_plan_row(i, "manual") for i in range(1000, 1600)]  # 600 newer manual fills
    rows += [_plan_row(i, "hypothesis", f"h-{i}") for i in range(1, 4)]
    rows += [_plan_row(9, "hypothesis", "h-9", status="cancelled")]
    client, table = _plans_client(monkeypatch, rows)

    r = client.get("/strategies/plans", params={"status": "filled", "source_kind": "hypothesis", "limit": 500})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["count"] == 3
    assert {p["strategy_plan_id"] for p in body["items"]} == {1, 2, 3}
    assert {p["source_kind"] for p in body["items"]} == {"hypothesis"}
    # The 500 cap is per filter: 600 newer manual fills do not push the hypothesis plans out.
    sql, params = table.executed[-1]
    assert "p.source_kind = %s" in sql and params == ["filled", "hypothesis", 500]

    one = client.get("/strategies/plans", params={"source_kind": "hypothesis", "source_ref": "h-2"}).json()
    assert [p["strategy_plan_id"] for p in one["items"]] == [2]

    # Without the filter the newest 500 of every kind come back -- the TD-178 failure mode.
    unfiltered = client.get("/strategies/plans", params={"status": "filled", "limit": 500}).json()
    assert unfiltered["count"] == 500
    assert not any(p["source_kind"] == "hypothesis" for p in unfiltered["items"])


def test_an_unknown_source_kind_is_422(monkeypatch: pytest.MonkeyPatch) -> None:
    client, table = _plans_client(monkeypatch, [_plan_row(1, "hypothesis", "h-1")])
    r = client.get("/strategies/plans", params={"source_kind": "Hypothesis"})
    assert r.status_code == 422
    assert r.json()["detail"][0]["loc"] == ["query", "source_kind"]
    assert table.executed == []


def test_the_source_filters_are_not_retired_names() -> None:
    from bifrost_api.common.query_vocab import RETIRED_QUERY_NAMES

    retired = {old for names in RETIRED_QUERY_NAMES.values() for old in names}
    assert not {"source_kind", "source_ref"} & retired
    assert ("GET", "/strategies/plans") not in RETIRED_QUERY_NAMES
