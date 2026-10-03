"""Naming program R1 (api 0.7.0, decision pack 2026-10-03): new names added, nothing removed.

- ``/trades``, ``/trades/win-rate``, ``/trade-reviews``, ``/gate-sets`` and
  ``/preferences/saved-searches`` answer exactly what the old ``/strategies/...`` routes
  answer; the old ones say ``Deprecation: true`` with a ``Link`` to the new one.
- ``trade_id`` / ``trade_ids`` are the query names; ``strategy_instance_id(s)`` still work.
- Bodies take ``trade_id`` / ``fill_splits`` / ``tags_*_json``; the new name wins.
- ``GET /data-probe`` (and ``/ops/data-probe``) on the monitor app, for the Ops platform (D8-A).

Nothing reaches a database: readers are mocks and core's writers are replaced. Fixtures are invented.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Tuple
from unittest.mock import MagicMock

import pytest
from starlette.testclient import TestClient

from bifrost_api.account.app import create_account_app
from bifrost_api.deprecations import REPLACED_ROUTES, replaced_route
from bifrost_api.monitor.app import create_app as create_monitor_app
from bifrost_api.strategy.patch_bodies import ReviewPatch
from bifrost_api.trading.routers import executions as executions_router
from bifrost_api.trading.routers.executions import ExecutionAttributionPatch
from bifrost_core.monitor.reader import saved_search as saved_search_module
from bifrost_core.monitor.reader import strategy_instance as strategy_instance_module
from bifrost_core.monitor.reader import strategy_plan as strategy_plan_module
from bifrost_core.monitor.reader import trade_review as trade_review_module
from bifrost_core.monitor.reader.errors import ReadFailed
from tests import strategy_rows as rows
from tests.contract.helpers import full_server_config, operator_server_config
from tests.route_listing import route_paths

PG = {"sink": "postgres"}
ACC = "U0000001"
PREFIX = {"X-Forwarded-Prefix": "/api/strategy"}


def _reader() -> MagicMock:
    r = MagicMock()
    r.list_strategy_instances.return_value = rows.instances()
    r.get_strategy_instance_by_id.return_value = rows.instance()
    r.get_strategy_win_rate.return_value = {"items": [{"structure_name": "Short put", "total_trades": 2}], "totals_all": {}}
    r.list_gate_safety_sets.return_value = rows.gate_sets()
    r.get_gate_safety_full_by_id.return_value = rows.gate_set()
    r.create_strategy_instance.return_value = 41
    return r


def _client(reader: Any = None) -> TestClient:
    reader = reader or _reader()
    reader.config = operator_server_config()
    app = create_account_app(reader=reader, control_via_db=PG, status_cfg_for_read=PG, merged_config=reader.config)
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture
def stores(monkeypatch: pytest.MonkeyPatch) -> Dict[str, List[Any]]:
    """Core's review and saved-search stores, and the writers, recording what they were sent."""
    sent: Dict[str, List[Any]] = {"review": [], "patch": [], "delete": [], "saved": []}
    review = {"strategy_instance_id": 41, "trade_id": 41, "tags_added": ["a"], "tags_added_json": ["a"], "reviewed": True}
    monkeypatch.setattr(trade_review_module, "list_reviews", lambda _cfg: [review])

    def patch_review(_cfg: Any, tid: int, fields: Dict[str, Any]) -> Dict[str, Any]:
        sent["review"].append((tid, fields))
        return review

    monkeypatch.setattr(trade_review_module, "patch_review", patch_review)

    def patch_instance(_cfg: Any, tid: int, fields: Dict[str, Any]) -> Dict[str, Any]:
        sent["patch"].append((tid, fields))
        return rows.instance()

    monkeypatch.setattr(strategy_instance_module, "patch_instance", patch_instance)

    def delete_instance_strict(_cfg: Any, tid: int) -> Dict[str, Any]:
        sent["delete"].append(tid)
        return {"deleted": "hard", "strategy_instance_id": tid, "trade_id": tid}

    monkeypatch.setattr(strategy_instance_module, "delete_instance_strict", delete_instance_strict)
    saved = [{"preference_saved_search_id": 3, "route": "/trade/plans", "label": "Mine", "state_json": {}}]
    monkeypatch.setattr(saved_search_module, "list_saved_searches", lambda _cfg: saved)
    return sent


# --- the same answer under both names ------------------------------------------------------

# (method, old path, new path, body)
PAIRS: List[Tuple[str, str, str, Any]] = [
    ("GET", "/strategies/instances", "/trades", None),
    ("GET", "/strategies/instances/41", "/trades/41", None),
    ("POST", "/strategies/instances", "/trades", {"strategy_opportunity_id": 5, "account_id": ACC, "opened_at": "2027-01-04T15:00:00Z"}),
    ("PATCH", "/strategies/instances/41", "/trades/41", {"label": "#041"}),
    ("DELETE", "/strategies/instances/41", "/trades/41", None),
    ("GET", "/strategies/win-rate", "/trades/win-rate", None),
    ("GET", "/strategies/reviews", "/trade-reviews", None),
    ("PATCH", "/strategies/reviews/41", "/trade-reviews/41", {"reviewed": True}),
    ("GET", "/strategies/gate-safety", "/gate-sets", None),
    ("GET", "/strategies/gate-safety/defaults", "/gate-sets/defaults", None),
    ("GET", "/strategies/gate-safety/2", "/gate-sets/2", None),
    ("GET", "/strategies/saved-searches", "/preferences/saved-searches", None),
]


@pytest.mark.parametrize("method, old, new, body", PAIRS, ids=[f"{m} {o}" for m, o, _, _ in PAIRS])
def test_the_new_route_answers_what_the_old_one_does(stores: Any, method: str, old: str, new: str, body: Any) -> None:
    client = _client()
    r_old = client.request(method, old, json=body, headers=PREFIX)
    r_new = client.request(method, new, json=body, headers=PREFIX)
    assert r_old.status_code == r_new.status_code == 200, (r_old.text, r_new.text)
    assert r_old.json() == r_new.json()
    assert r_old.headers.get("deprecation") == "true"
    assert r_old.headers.get("link") == f'</api/strategy{new}>; rel="successor-version"'
    assert "deprecation" not in r_new.headers and "link" not in r_new.headers


def test_every_old_route_listed_is_covered_here() -> None:
    covered = {(m, o) for m, o, _, _ in PAIRS}
    tested = {replaced_route(m, o)[0] for m, o in covered}
    untested = {t for (_, t) in REPLACED_ROUTES} - tested
    # Writes whose answer needs a database of their own: covered by the route listing tests.
    assert untested <= {
        "/strategies/gate-safety",
        "/strategies/gate-safety/{gate_safety_strategy_id}",
        "/strategies/saved-searches",
        "/strategies/saved-searches/{preference_saved_search_id}",
    }


def test_rows_carry_trade_id_and_create_answers_it(stores: Any) -> None:
    client = _client()
    items = client.get("/trades").json()["items"]
    assert items and all(i["trade_id"] == i["strategy_instance_id"] for i in items)
    assert client.post("/trades", json={"strategy_opportunity_id": 5, "account_id": ACC, "opened_at": "2027-01-04T15:00:00Z"}).json() == {
        "trade_id": 41,
        "strategy_instance_id": 41,
    }


def test_the_writes_reach_core_with_the_trade_id(stores: Dict[str, List[Any]]) -> None:
    client = _client()
    client.patch("/trades/41", json={"label": "x"})
    client.delete("/trades/41")
    client.patch("/trade-reviews/41", json={"tags_added_json": ["b"], "tags_added": ["c"]})
    assert stores["patch"] == [(41, {"label": "x"})]
    assert stores["delete"] == [41]
    # the new name wins and reaches core as the name it takes
    assert stores["review"] == [(41, {"tags_added": ["b"]})]


def test_win_rate_is_not_read_as_a_trade_id() -> None:
    paths = route_paths(_client().app)
    assert paths.index("/trades/win-rate") < paths.index("/trades/{trade_id:int}")
    assert paths.index("/gate-sets/defaults") < paths.index("/gate-sets/{gate_safety_strategy_id:int}")
    assert _client().get("/trades/win-rate").status_code == 200


def test_a_replaced_hit_is_logged_with_its_successor(stores: Any, caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger="bifrost_api.deprecations"):
        _client().get("/strategies/instances/41", headers={"User-Agent": "r1-test"})
    line = next(rec.getMessage() for rec in caplog.records if "replaced route hit" in rec.getMessage())
    assert "use GET /trades/{trade_id:int}" in line and "r1-test" in line


def test_open_option_legs_stays_deprecated_without_a_successor() -> None:
    r = _client().get("/strategies/instances/41/open-option-legs")
    assert r.headers.get("deprecation") == "true" and "link" not in r.headers


# --- query names --------------------------------------------------------------------------


@pytest.mark.parametrize("path", ["/trades", "/strategies/instances"])
@pytest.mark.parametrize("param", ["trade_ids", "strategy_instance_ids"])
def test_trade_ids_filter_under_either_name(path: str, param: str) -> None:
    reader = _reader()
    _client(reader).get(path, params={param: "41,42"})
    assert reader.list_strategy_instances.call_args.kwargs["strategy_instance_ids"] == [41, 42]


@pytest.mark.parametrize("param", ["trade_id", "strategy_instance_id"])
def test_executions_and_performance_take_trade_id(param: str, caplog: pytest.LogCaptureFixture) -> None:
    reader = _reader()
    reader.get_executions_page.return_value = {"items": [], "next_cursor": None}
    reader.get_performance_stats.return_value = {}
    client = _client(reader)
    with caplog.at_level(logging.WARNING):
        client.get("/executions", params={param: 41})
        client.get("/performance", params={param: 41})
    assert reader.get_executions_page.call_args.kwargs["strategy_instance_id"] == 41
    assert reader.get_performance_stats.call_args.kwargs["strategy_instance_id"] == 41
    deprecated = [r for r in caplog.records if "deprecated query params" in r.getMessage()]
    assert bool(deprecated) == (param == "strategy_instance_id")


def test_summary_only_says_trade_id() -> None:
    r = _client().get("/performance", params={"summary_only": "true"})
    assert r.status_code == 400 and r.json()["detail"] == "summary_only requires trade_id"


# --- bodies ---------------------------------------------------------------------------------


def test_attribution_patch_maps_the_new_names_and_the_new_one_wins() -> None:
    body = ExecutionAttributionPatch(trade_id=41, strategy_instance_id=9, fill_splits=[{"trade_id": 41, "quantity": 1}])
    assert body.patch_fields() == {"strategy_instance_id": 41, "instance_allocations": [{"trade_id": 41, "quantity": 1}]}
    assert ReviewPatch(tags_dropped_json=[]).patch_fields() == {"tags_dropped": []}


def test_attribution_patch_route_hands_core_the_mapped_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: List[Any] = []

    def patch_execution(_cfg: Any, eid: int, fields: Dict[str, Any]) -> Dict[str, Any]:
        seen.append((eid, fields))
        return {"account_executions_id": eid, "trade_id": 41, "strategy_instance_id": 41}

    monkeypatch.setattr(executions_router.accounts_module, "patch_execution", patch_execution)
    r = _client().patch("/executions/77/attribution", json={"trade_id": 41})
    assert r.status_code == 200 and r.json()["trade_id"] == 41
    assert seen == [(77, {"strategy_instance_id": 41})]


def test_post_execution_hands_core_the_new_names(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: List[Dict[str, Any]] = []
    monkeypatch.setattr(executions_router, "insert_one_execution", lambda _cfg, body: seen.append(body) or 5)
    body = {"account_id": ACC, "symbol": "ZZQ", "quantity": 2, "price": 1, "fill_splits": [{"trade_id": 41, "quantity": 2}]}
    assert _client().post("/executions", json=body).status_code == 200
    assert seen[0]["fill_splits"] == [{"trade_id": 41, "quantity": 2}]


def test_link_fill_takes_trade_id_and_answers_both(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: List[Any] = []
    monkeypatch.setattr(strategy_plan_module, "link_fill", lambda _cfg, pid, tid: seen.append((pid, tid)) or True)
    client = _client()  # one app per test: a second one would register its metrics twice
    r = client.post("/strategies/plans/7/link-fill", json={"trade_id": 41})
    assert r.status_code == 200 and seen == [(7, 41)]
    assert r.json()["trade_id"] == r.json()["strategy_instance_id"] == 41
    assert client.post("/strategies/plans/7/link-fill", json={"strategy_instance_id": 42}).status_code == 200
    assert seen[-1] == (7, 42)
    assert client.post("/strategies/plans/7/link-fill", json={}).status_code == 422


# --- data probe (D8-A) ------------------------------------------------------------------------

PROBE = {
    "generated_at": "2031-03-04T14:30:00Z",
    "activity": [{"source": "trades", "last_ts": "2031-03-04T14:00:00Z"}],
    "sample": {"label": "trades", "rows": 12},
    "clone_groups": [{"name": "trades", "tables": ["strategy_instance", "trade_review"], "note": "…"}],
    "watchlist": {"label": "optionable_stocks", "symbols": ["QZAA", "QZFF"], "count": 2},
}


def _monitor(reader: MagicMock) -> TestClient:
    reader._config = {**full_server_config(), "redis": {"enabled": False}, "ops": {"auth": {"default_role": "viewer"}}}
    app = create_monitor_app(reader=reader, control_via_db=None, data_lag_threshold_ms=5000, merged_config=reader._config)
    return TestClient(app, raise_server_exceptions=False)


@pytest.mark.parametrize("path", ["/data-probe", "/ops/data-probe"])
def test_the_data_probe_answers_what_core_reads(path: str) -> None:
    reader = MagicMock()
    reader.get_data_probe.return_value = PROBE
    r = _monitor(reader).get(path)
    assert r.status_code == 200 and r.json() == PROBE


def test_a_probe_that_cannot_read_is_503_not_an_empty_answer() -> None:
    reader = MagicMock()
    reader.get_data_probe.side_effect = ReadFailed("data_probe: database unavailable")
    r = _monitor(reader).get("/data-probe")
    assert r.status_code == 503 and r.json() == {"detail": "data_probe: database unavailable", "reason": "read_failed"}
