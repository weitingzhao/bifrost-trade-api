"""Ids and fields are named one way between paths, bodies and answers (debt TD-57, api 0.6.7).

- Path parameters are named for the table id they take; the URLs did not change.
- A client can send back what it read: plans take ``legs_json`` / ``source_json`` and
  saved searches ``state_json`` (the old write names still work for one release).
- Answers gain the table's name where they used another: ``category_id`` beside a
  category's ``id``, ``account_execution_option_stock_link_id`` beside ``link_id``,
  ``from_date`` / ``to_date`` beside the candidates' ``trade_date_from`` / ``_to``.
- ``GET /strategies/dims`` keys the same lists ``by_column`` (``dim_direction``) as well.

Fixtures are invented.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Set
from unittest.mock import MagicMock

import pytest
from starlette.testclient import TestClient

from bifrost_api.account.app import create_account_app
from bifrost_core.monitor.reader import saved_search as saved_search_module
from bifrost_core.monitor.reader import strategy_plan as strategy_plan_module
from bifrost_core.monitor.reader.errors import WriteNotFound
from tests.contract.helpers import operator_server_config
from tests.route_listing import served_routes
from tests.test_deprecations import _apps

ACC = "U0000001"

# Every id a path takes, by name. ``category_id`` is the exception the table forces: its key
# column is ``id`` and every referencing column is ``category_id`` (a rename is DDL, Owner's).
PATH_IDS = {
    "strategy_template_id",
    "strategy_structure_id",
    "strategy_opportunity_id",
    "strategy_allocation_id",
    "gate_safety_strategy_id",
    "strategy_instance_id",
    "strategy_plan_id",
    "preference_saved_search_id",
    "account_executions_id",
    "account_execution_option_stock_link_id",
    "category_id",
    "report_id",
}
OLD_SHORT_NAMES = {
    "template_id",
    "structure_id",
    "opportunity_id",
    "allocation_id",
    "gate_safety_id",
    "saved_search_id",
    "execution_id",
    "link_id",
}


def _path_params() -> Set[str]:
    names: Set[str] = set()
    for app in _apps().values():
        for _method, path in served_routes(app):
            names |= {m.split(":")[0] for m in re.findall(r"\{([^}]+)\}", path)}
    return names


def test_path_ids_are_named_for_their_table() -> None:
    ids = {n for n in _path_params() if n.endswith("_id")}
    assert ids == PATH_IDS
    assert not ids & OLD_SHORT_NAMES


def _client(reader: Any = None) -> TestClient:
    reader = reader or MagicMock()
    reader.config = operator_server_config()
    pg = {"sink": "postgres"}
    app = create_account_app(reader=reader, control_via_db=pg, status_cfg_for_read=pg, merged_config=reader.config)
    return TestClient(app, raise_server_exceptions=False)


LEG = {"side": "sell", "sec_type": "OPT", "right": "P", "strike": 10.0, "expiry": "2026-11-20", "ratio": 1}
PLAN = {"account_id": ACC, "symbol": "ZZQ", "structure_label": "Short put", "qty": 1}


@pytest.mark.parametrize(
    "sent, legs, source",
    [
        ({"legs_json": [LEG], "source_json": [{"kind": "manual", "text": "x"}]}, [LEG], [{"kind": "manual", "text": "x"}]),
        ({"legs": [LEG], "source": [{"kind": "manual", "text": "x"}]}, [LEG], [{"kind": "manual", "text": "x"}]),
        ({"legs": [], "legs_json": [LEG]}, [LEG], []),
    ],
)
def test_plan_create_takes_the_read_names(monkeypatch: pytest.MonkeyPatch, sent: Dict[str, Any], legs: list, source: list) -> None:
    seen: Dict[str, Any] = {}

    def _create(_cfg: Any, payload: dict) -> int:
        seen.update(payload)
        return 42

    monkeypatch.setattr(strategy_plan_module, "create_plan", _create)
    r = _client().post("/strategies/plans", json={**PLAN, **sent})
    assert r.status_code == 200 and r.json() == {"strategy_plan_id": 42}, r.text
    assert [{k: v for k, v in leg.items() if k in LEG} for leg in seen["legs"]] == legs
    assert seen["source"] == source
    assert "legs_json" not in seen and "source_json" not in seen


def test_plan_patch_takes_the_read_names(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: Dict[str, Any] = {}

    def _patch(_cfg: Any, plan_id: int, fields: dict) -> dict:
        seen.update(fields)
        raise WriteNotFound("No strategy plan 9.")  # what core got is the point, not the row

    monkeypatch.setattr(strategy_plan_module, "patch_plan", _patch)
    client = _client()
    assert client.patch("/strategies/plans/9", json={"legs_json": [LEG], "source_json": []}).status_code == 404
    assert seen == {"legs": [LEG], "source": []}
    seen.clear()
    assert client.patch("/strategies/plans/9", json={"legs": [], "legs_json": [LEG]}).status_code == 404
    assert seen == {"legs": [LEG]}


@pytest.mark.parametrize(
    "sent, state",
    [
        ({"state_json": {"search": "?sym=ZZQ"}}, {"search": "?sym=ZZQ"}),
        ({"state": {"search": "?sym=ZZQ"}}, {"search": "?sym=ZZQ"}),
        ({"state": {"search": "old"}, "state_json": {"search": "new"}}, {"search": "new"}),
    ],
)
def test_saved_search_takes_state_json(monkeypatch: pytest.MonkeyPatch, sent: Dict[str, Any], state: dict) -> None:
    seen: Dict[str, Any] = {}

    def _create(_cfg: Any, route: str, label: str, st: Any) -> int:
        seen["state"] = st
        return 7

    monkeypatch.setattr(saved_search_module, "create_saved_search", _create)
    r = _client().post("/strategies/saved-searches", json={"route": "/trade/plans", "label": "ZZQ", **sent})
    assert r.status_code == 200, r.text
    assert seen["state"] == state


def test_dims_by_column_is_by_type_keyed_by_the_column() -> None:
    reader = MagicMock()
    reader.list_dims_grouped.return_value = {"direction": [{"code": "bull"}], "time": [{"code": "short"}]}
    body = _client(reader).get("/strategies/dims").json()
    assert body["by_type"] == {"direction": [{"code": "bull"}], "time": [{"code": "short"}]}
    assert body["by_column"] == {"dim_direction": [{"code": "bull"}], "dim_time": [{"code": "short"}]}


def test_link_rows_carry_the_table_id() -> None:
    reader = MagicMock()
    reader.get_option_stock_links.return_value = {"links": [{"link_id": 3}], "slippage_total": None}
    reader.get_option_stock_links_bulk.return_value = {"by_option_id": {"4": {"links": [{"link_id": 3}], "slippage_total": None}}}
    client = _client(reader)
    rows = client.get(f"/executions/option-stock-links?account_id={ACC}&option_account_executions_id=4").json()["items"]
    assert rows == [{"link_id": 3, "account_execution_option_stock_link_id": 3}]
    bulk = client.post(
        "/executions/option-stock-links/query",
        json={"batches": [{"account_id": ACC, "option_account_executions_ids": [4]}]},
    ).json()
    assert bulk["by_option_id"]["4"]["links"] == [{"link_id": 3, "account_execution_option_stock_link_id": 3}]


def test_stock_link_candidates_echo_the_window_under_the_query_names() -> None:
    reader = MagicMock()
    reader.get_stock_link_candidates.return_value = {
        "executions": [],
        "underlying_symbol": "ZZQ",
        "trade_date_from": "2026-01-02",
        "trade_date_to": "2026-01-16",
    }
    body = _client(reader).get(
        f"/executions/stock-link-candidates?account_id={ACC}&option_account_executions_id=4"
    ).json()
    assert (body["from_date"], body["to_date"]) == ("2026-01-02", "2026-01-16")
    assert (body["trade_date_from"], body["trade_date_to"]) == ("2026-01-02", "2026-01-16")


def test_category_patch_answers_category_id(monkeypatch: pytest.MonkeyPatch) -> None:
    from bifrost_core.portfolio.reader import position_categories

    monkeypatch.setattr(
        position_categories, "patch_position_category", lambda _c, cid, f: {"id": cid, "name": "Core"}
    )
    body = _client().patch("/position-categories/5", json={"name": "Core"}).json()
    # `id` dropped in api 0.6.12 (TD-56): one name for the key.
    assert body == {"category_id": 5, "name": "Core", "ok": True}
