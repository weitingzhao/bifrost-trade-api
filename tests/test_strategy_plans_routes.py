"""Plan routes: mounted where the gateway looks, and refusals that keep their reason.

`/api/strategy/*` is served by the **account** app (`account/app.py` mounts
`strategies_router`), so a plan router added only to `strategy/app.py` would
pass its own tests and 404 everywhere. That is the first test here.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from bifrost_api.account.app import create_account_app
from bifrost_api.strategy.routers import plans_router
from bifrost_core.monitor.reader import strategy_plan as strategy_plan_module
from bifrost_core.monitor.reader.strategy_plan import PlanRuleError
from tests.contract.helpers import full_server_config

PLAN_ROUTES = {
    "/strategies/plans",
    "/strategies/plans/{strategy_plan_id}",
    "/strategies/plans/{strategy_plan_id}/intend",
    "/strategies/plans/{strategy_plan_id}/link-fill",
    "/strategies/plans/{strategy_plan_id}/cancel",
}


def _account_client(control_via_db: Any = None) -> TestClient:
    reader = MagicMock()
    reader._config = full_server_config()
    app = create_account_app(
        reader=reader,
        control_via_db=control_via_db,
        status_cfg_for_read=control_via_db,
        merged_config=reader._config,
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
    assert PLAN_ROUTES <= {route.path for route in bare.routes}
    mounted = {route.path for route in _account_client().app.routes}
    assert PLAN_ROUTES <= mounted


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
        client.put("/strategies/plans/1", json={"qty": 2}),
        client.post("/strategies/plans/1/cancel"),
        client.post("/strategies/plans/1/link-fill", json={"strategy_instance_id": 7}),
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
