"""Naming program R4 (api 0.9.0, decision pack 2026-10-03 D4-A): the old names are gone.

R1 (api 0.7.0) added ``/trades``, ``/trades/win-rate``, ``/trade-reviews``, ``/gate-sets`` and
``/preferences/saved-searches`` and kept the ``/strategies/...`` routes, the query names
``strategy_instance_id(s)``, the body names ``strategy_instance_id`` / ``instance_allocations`` /
``tags_added`` / ``tags_dropped`` and the old class names one version. R4 deletes them after a
PROD release cycle with no old-name hit in Loki for 4 days (infra scripts/release/naming_r4_gate.py).

- The old routes are not served (404); ``REPLACED_ROUTES`` is empty.
- Rows carry ``trade_id`` only (core 0.47.0); POST /trades answers ``{trade_id}``.
- The old query names are not read as ``trade_id(s)`` (and not logged as deprecated).
- Old body names: every body refuses them (422; POST / PUT bodies forbid unknown fields since TD-24).
- ``GET /data-probe`` (and ``/ops/data-probe``) on the monitor app (D8-A, from R1).

Nothing reaches a database: readers are mocks and core's writers are replaced. Fixtures are invented.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List
from unittest.mock import MagicMock

import pytest
from starlette.testclient import TestClient

from bifrost_api.account.app import create_account_app
from bifrost_api.deprecations import REPLACED_ROUTES
from bifrost_api.monitor.app import create_app as create_monitor_app
from bifrost_api.strategy import patch_bodies
from bifrost_api.strategy.schemas import responses
from bifrost_api.trading.routers import executions as executions_router
from bifrost_core.monitor.reader import strategy_instance as strategy_instance_module
from bifrost_core.monitor.reader import strategy_plan as strategy_plan_module
from bifrost_core.monitor.reader import trade_review as trade_review_module
from bifrost_core.monitor.reader.errors import ReadFailed
from tests import strategy_rows as rows
from tests.contract.helpers import full_server_config, operator_server_config
from tests.route_listing import route_paths
from tests.reader_mock import reader_mock

PG = {"sink": "postgres"}
ACC = "U0000001"
CREATE = {"strategy_opportunity_id": 5, "account_id": ACC, "opened_at": "2027-01-04T15:00:00Z"}


def _reader() -> MagicMock:
    r = MagicMock()
    r.list_trades.return_value = rows.instances()
    r.get_trade_by_id.return_value = rows.instance()
    r.get_trade_win_rate.return_value = {"items": [], "totals_all": {}}
    r.get_executions_page.return_value = {"items": [], "next_cursor": None}
    r.get_performance_stats.return_value = {}
    return r


def _client(reader: Any = None) -> TestClient:
    reader = reader or _reader()
    reader.config = operator_server_config()
    app = create_account_app(reader=reader, control_via_db=PG, status_cfg_for_read=PG, merged_config=reader.config)
    return TestClient(app, raise_server_exceptions=False)


OLD = [
    ("GET", "/strategies/instances"),
    ("GET", "/strategies/instances/41"),
    ("POST", "/strategies/instances"),
    ("PATCH", "/strategies/instances/41"),
    ("DELETE", "/strategies/instances/41"),
    ("GET", "/strategies/win-rate"),
    ("GET", "/strategies/reviews"),
    ("PATCH", "/strategies/reviews/41"),
    ("GET", "/strategies/gate-safety"),
    ("GET", "/strategies/gate-safety/defaults"),
    ("GET", "/strategies/gate-safety/2"),
    ("DELETE", "/strategies/gate-safety/2"),
    ("GET", "/strategies/saved-searches"),
    ("DELETE", "/strategies/saved-searches/3"),
]


def test_the_replaced_list_is_empty() -> None:
    assert REPLACED_ROUTES == {}


@pytest.mark.parametrize("method, path", OLD, ids=[f"{m} {p}" for m, p in OLD])
def test_an_old_route_is_not_served(method: str, path: str) -> None:
    r = _client().request(method, path, json={} if method in ("POST", "PATCH") else None)
    assert r.status_code in (404, 405), (method, path, r.status_code)
    assert "deprecation" not in r.headers


def test_the_new_routes_keep_their_order() -> None:
    paths = route_paths(_client().app)
    assert paths.index("/trades/win-rate") < paths.index("/trades/{trade_id:int}")
    assert paths.index("/gate-sets/defaults") < paths.index("/gate-sets/{gate_safety_strategy_id:int}")
    assert _client().get("/trades/win-rate").status_code == 200


def test_rows_carry_trade_id_only_and_create_answers_it(monkeypatch: pytest.MonkeyPatch) -> None:
    row = rows.instance()
    monkeypatch.setattr(strategy_instance_module, "create_instance_strict", lambda _cfg, *a, **k: {**row, "trade_id": 41})
    client = _client()
    items = client.get("/trades").json()["items"]
    assert items and all("trade_id" in i and "strategy_instance_id" not in i for i in items)
    assert client.post("/trades", json=CREATE).json() == {"trade_id": 41}
    assert "strategy_instance_id" not in responses.TradeRow.model_fields
    assert "strategy_instance_id" not in responses.PlanRow.model_fields


def test_the_old_class_names_are_gone() -> None:
    for module, name in ((responses, "InstanceRow"), (responses, "InstanceList"), (responses, "GateSafetyList"),
                         (patch_bodies, "InstancePatch"), (patch_bodies, "GateSafetyPatch")):
        assert not hasattr(module, name), name


# --- query names ------------------------------------------------------------------------------


def test_trade_ids_reach_the_reader_as_trade_ids() -> None:
    reader = _reader()
    _client(reader).get("/trades", params={"trade_ids": "41,42"})
    assert reader.list_trades.call_args.kwargs["trade_ids"] == [41, 42]


def test_strategy_instance_ids_is_not_a_filter_any_more(caplog: pytest.LogCaptureFixture) -> None:
    reader = _reader()
    with caplog.at_level(logging.WARNING):
        _client(reader).get("/trades", params={"strategy_instance_ids": "41"})
    assert reader.list_trades.call_args.kwargs["trade_ids"] is None
    assert not [r for r in caplog.records if "deprecated query params" in r.getMessage()]


@pytest.mark.parametrize("path, method", [("/executions", "get_executions_page"), ("/performance", "get_performance_stats")])
def test_executions_and_performance_filter_by_trade_id_only(path: str, method: str) -> None:
    reader = _reader()
    client = _client(reader)
    client.get(path, params={"trade_id": 41})
    assert getattr(reader, method).call_args.kwargs["trade_id"] == 41
    client.get(path, params={"strategy_instance_id": 41})
    assert getattr(reader, method).call_args.kwargs["trade_id"] is None


def test_summary_only_says_trade_id() -> None:
    r = _client().get("/performance", params={"summary_only": "true"})
    assert r.status_code == 400 and r.json()["detail"] == "summary_only requires trade_id"


def test_summary_only_reads_the_trade_summary() -> None:
    reader = _reader()
    reader.get_performance_trade_summary.return_value = {"summary": {}}
    assert _client(reader).get("/performance", params={"summary_only": "true", "trade_id": 41}).status_code == 200
    assert reader.get_performance_trade_summary.call_args.kwargs["trade_id"] == 41


# --- bodies -----------------------------------------------------------------------------------


@pytest.fixture
def review_store(monkeypatch: pytest.MonkeyPatch) -> List[Any]:
    sent: List[Any] = []
    row = {"trade_review_id": 1, "trade_id": 41, "tags_added_json": ["b"], "tags_dropped_json": [], "reviewed": False}

    def patch_review(_cfg: Any, tid: int, fields: Dict[str, Any]) -> Dict[str, Any]:
        sent.append((tid, fields))
        return row

    monkeypatch.setattr(trade_review_module, "patch_review", patch_review)
    monkeypatch.setattr(trade_review_module, "list_reviews", lambda _cfg: [row])
    return sent


def test_a_review_patch_takes_the_json_names_only(review_store: List[Any]) -> None:
    client = _client()
    assert client.patch("/trade-reviews/41", json={"tags_added_json": ["b"]}).status_code == 200
    assert review_store == [(41, {"tags_added_json": ["b"]})]
    r = client.patch("/trade-reviews/41", json={"tags_added": ["c"]})
    assert r.status_code == 422 and len(review_store) == 1
    assert client.get("/trade-reviews").json()["items"][0]["trade_id"] == 41


@pytest.mark.parametrize("old", [{"strategy_instance_id": 41}, {"instance_allocations": []}])
def test_the_attribution_patch_refuses_the_old_names(monkeypatch: pytest.MonkeyPatch, old: Dict[str, Any]) -> None:
    called: List[Any] = []
    monkeypatch.setattr(executions_router.accounts_module, "patch_execution", lambda *a: called.append(a) or {})
    r = _client().patch("/executions/77/attribution", json=old)
    assert r.status_code == 422 and called == []


def test_the_attribution_patch_hands_core_the_new_names(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: List[Any] = []

    def patch_execution(_cfg: Any, eid: int, fields: Dict[str, Any]) -> Dict[str, Any]:
        seen.append((eid, fields))
        return {"account_executions_id": eid, "trade_id": 41, "fill_splits": []}

    monkeypatch.setattr(executions_router.accounts_module, "patch_execution", patch_execution)
    r = _client().patch("/executions/77/attribution", json={"trade_id": 41, "fill_splits": []})
    assert r.status_code == 200 and r.json()["trade_id"] == 41
    assert seen == [(77, {"trade_id": 41, "fill_splits": []})]


def test_post_execution_refuses_the_old_names(monkeypatch: pytest.MonkeyPatch) -> None:
    """TD-24 (api 0.9.0): an undeclared body field is a 422, so the R1 names are too."""
    seen: List[Dict[str, Any]] = []
    monkeypatch.setattr(executions_router, "insert_one_execution", lambda _cfg, body: seen.append(body) or 5)
    body = {"account_id": ACC, "symbol": "ZZQ", "quantity": 2, "price": 1, "strategy_instance_id": 41,
            "instance_allocations": [{"strategy_instance_id": 41, "allocated_quantity": 2}]}
    r = _client().post("/executions", json=body)
    assert r.status_code == 422, r.text
    assert sorted(e["loc"][-1] for e in r.json()["detail"]) == ["instance_allocations", "strategy_instance_id"]
    assert {e["type"] for e in r.json()["detail"]} == {"extra_forbidden"}
    assert seen == []


def test_link_fill_takes_trade_id_only(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: List[Any] = []
    monkeypatch.setattr(strategy_plan_module, "link_fill", lambda _cfg, pid, tid: seen.append((pid, tid)) or True)
    client = _client()  # one app per test: a second one would register its metrics twice
    r = client.post("/strategies/plans/7/link-fill", json={"trade_id": 41})
    assert r.status_code == 200 and seen == [(7, 41)]
    assert r.json() == {"ok": True, "strategy_plan_id": 7, "trade_id": 41, "status": "filled"}
    assert client.post("/strategies/plans/7/link-fill", json={"strategy_instance_id": 42}).status_code == 422
    assert seen == [(7, 41)]


def test_the_trade_writes_reach_core_with_the_trade_id(monkeypatch: pytest.MonkeyPatch) -> None:
    sent: List[Any] = []
    monkeypatch.setattr(strategy_instance_module, "patch_instance", lambda _c, tid, f: sent.append(("p", tid, f)) or rows.instance())
    monkeypatch.setattr(
        strategy_instance_module, "delete_instance_strict", lambda _c, tid: sent.append(("d", tid)) or {"deleted": "hard", "trade_id": tid}
    )
    client = _client()
    assert client.patch("/trades/41", json={"label": "x"}).status_code == 200
    assert client.delete("/trades/41").json() == {"deleted": "hard", "trade_id": 41, "ok": True}
    assert sent == [("p", 41, {"label": "x"}), ("d", 41)]


# --- data probe (D8-A) ------------------------------------------------------------------------

PROBE = {
    "generated_at": "2031-03-04T14:30:00Z",
    "activity": [{"source": "trades", "last_ts": "2031-03-04T14:00:00Z"}],
    "sample": {"label": "trades", "rows": 12},
    "clone_groups": [{"name": "trades", "tables": ["trade", "trade_review"], "note": "…"}],
    "watchlist": {"label": "optionable_stocks", "symbols": ["QZAA", "QZFF"], "count": 2},
}


def _monitor(reader: MagicMock) -> TestClient:
    reader._config = {**full_server_config(), "redis": {"enabled": False}, "ops": {"auth": {"default_role": "viewer"}}}
    app = create_monitor_app(reader=reader, control_via_db=None, data_lag_threshold_ms=5000, merged_config=reader._config)
    return TestClient(app, raise_server_exceptions=False)


@pytest.mark.parametrize("path", ["/data-probe", "/ops/data-probe"])
def test_the_data_probe_answers_what_core_reads(path: str) -> None:
    reader = reader_mock()
    reader.get_data_probe.return_value = PROBE
    r = _monitor(reader).get(path)
    assert r.status_code == 200 and r.json() == PROBE


def test_a_probe_that_cannot_read_is_503_not_an_empty_answer() -> None:
    reader = reader_mock()
    reader.get_data_probe.side_effect = ReadFailed("data_probe: database unavailable")
    r = _monitor(reader).get("/data-probe")
    assert r.status_code == 503 and r.json() == {"detail": "data_probe: database unavailable", "reason": "read_failed"}
