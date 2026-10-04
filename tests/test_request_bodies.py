"""Typed POST / PUT bodies (TD-24, batch 3c-1; Owner decision B).

Each write route replays the payload the frontend sends today (the shape of the
call site named beside it; values invented) and must pass, handing core exactly the
declared fields. Then: wrong types are 422 and write nothing -- the tag bug first --
and unknown fields are ignored and logged by name, never by value.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock

import pytest
from starlette.testclient import TestClient

import bifrost_api.trading.routers.executions as ex
from bifrost_api.account.app import create_account_app
from bifrost_api.common.request_bodies import LenientBody, LenientItem, unknown_field_names
from bifrost_api.market.app import create_market_app
from bifrost_core.monitor.reader import gate_safety_write
from bifrost_core.monitor.reader import saved_search
from bifrost_core.monitor.reader import strategy_structure_write
from bifrost_core.monitor.reader import template_config_write
from bifrost_core.monitor.reader import watchlist
from bifrost_core.monitor.schemas.gate_params import default_gates
from tests.contract.helpers import operator_server_config

PG = {"sink": "postgres"}
ACC = "U0000001"


def _account(reader: Optional[MagicMock] = None) -> Tuple[TestClient, MagicMock]:
    reader = reader or MagicMock()
    reader.config = operator_server_config()
    app = create_account_app(reader=reader, control_via_db=PG, status_cfg_for_read=PG, merged_config=reader.config)
    return TestClient(app, raise_server_exceptions=False), reader


def _market() -> TestClient:
    reader = MagicMock()
    reader.config = {**operator_server_config(), "redis": {"enabled": False}}
    app = create_market_app(reader=reader, control_via_db=PG, merged_config=reader.config)
    return TestClient(app, raise_server_exceptions=False)


# --- the frontend's payloads --------------------------------------------------------------

STRUCTURE = {  # structureEditPayload (StructureInspector / useDeskEditing)
    "name": "Short put 30d",
    "strategy_template_id": 4,
    "structure_type": "short_put",
    "legs": [{"role": "put", "direction": "sell", "option_right": "P", "quantity": 1, "strike": None,
              "expiration": ""}],
    "version": 1,
    "is_active": True,
    "meta": [{"meta_key": "dte_target", "meta_value_text": "30"}, {"meta_key": "otm_pct", "meta_value_text": None}],
}
GATE = {  # gateFormToPayload (GateSafetyFormSheet / GateInspector)
    "name": "Calm tape",
    "version": 2,
    "dim_direction": None,
    "dim_structure": None,
    "dim_coverage": None,
    "dim_risk": "defined",
    "dim_volatility": None,
    "dim_time": None,
    "is_active": False,
    "gates": default_gates(),
    "earnings_dates": ["2031-04-22", ""],
}
EXECUTION_CREATE = {  # ExecutionFormModal (create), splits variant below
    "account_id": ACC, "time": 1930487400, "symbol": "ZZQ", "sec_type": "OPT", "side": "SELL",
    "quantity": -2, "price": 1.15, "source": "manual", "expiry": "20310417", "strike": 40,
    "option_right": "P", "contract_key": "ZZQ|OPT|20310417|40|P", "commission": 1.3, "currency": "USD",
    "strategy_opportunity_id": 5, "strategy_instance_id": 41,
}
QUICK_CLOSE = {  # quickCloseBody (QuickCloseModal)
    "account_id": ACC, "time": 1930487400, "symbol": "ZZQ", "sec_type": "OPT", "side": "BUY", "quantity": 2,
    "price": 0.4, "source": "journal_closed", "expiry": "20310417", "strike": 40.0, "option_right": "P",
    "contract_key": "ZZQ|OPT|20310417|40.0|P", "commission": 1.1, "currency": "USD",
    "strategy_instance_id": 41, "strategy_opportunity_id": 5,
}
EXECUTION_UPDATE = {  # ExecutionFormModal (edit)
    "account_id": ACC, "exec_time": 1930487400.5, "symbol": "ZZQ", "sec_type": "OPT", "side": "SELL",
    "quantity": -2, "price": 1.15, "strike": 40, "option_right": "P", "contract_key": "ZZQ|OPT|20310417|40|P",
    "instance_allocations": [{"strategy_instance_id": 41, "allocated_quantity": -1.5},
                             {"strategy_instance_id": 42, "allocated_quantity": -0.5}],
    "expiry": "20310417",
}

# (method, path, body, (core module | None for the reader, function), its answer)
ACCOUNT_CASES: List[Tuple[str, str, Dict[str, Any], Tuple[Any, str], Any]] = [
    ("POST", "/strategies/templates", {"template_code": "zz_put", "display_name": "ZZ put", "sort_order": 100},
     (template_config_write, "create_template"), 9),  # TemplateCatalogControls
    ("PUT", "/strategies/templates/9/legs",
     {"legs": [{"role": "put", "direction": "sell", "option_right": "P", "quantity_default": 1, "sort_order": 0},
               {"role": "underlying", "direction": "buy", "option_right": "", "quantity_default": 100,
                "sort_order": 1}]},
     (template_config_write, "replace_template_legs"), None),  # TemplateEditor.saveLegs
    ("PUT", "/strategies/templates/9/params",
     {"items": [{"meta_key": "otm_pct", "display_label": "OTM %", "default_value_text": None, "param_kind": "percent",
                 "sort_order": 0}]},
     (template_config_write, "replace_template_params"), None),  # TemplateEditor.saveParams
    ("PUT", "/strategies/templates/9/characteristics", {"items": ["Collects premium", "Assignment risk"]},
     (template_config_write, "replace_template_characteristics"), None),
    ("POST", "/strategies/structures", STRUCTURE, (strategy_structure_write, "create_structure"), 11),
    ("PUT", "/strategies/structures/11", STRUCTURE, (strategy_structure_write, "update_structure"), True),
    ("POST", "/strategies/gate-safety", GATE, (gate_safety_write, "create_gate_safety"), 2),
    ("PUT", "/strategies/gate-safety/2", GATE, (gate_safety_write, "update_gate_safety"), True),
    ("POST", "/strategies/saved-searches", {"route": "/trade/plans", "label": "ZZQ only", "state": {"search": "ZZQ"}},
     (saved_search, "create_saved_search"), 3),  # TradePlansPage
    ("POST", "/position-categories", {"name": "Watching", "sort_order": 2},
     (None, "create_position_category"), (7, None)),  # useEnsureWatchlistCategories
    ("PUT", "/position-categories/tag", {"account_id": ACC, "contract_key": "ZZQ|STK|||", "category_id": 3},
     (None, "set_position_category_tag"), True),  # SharesBand.retag
    ("PUT", "/position-categories/tag", {"account_id": ACC, "contract_key": "ZZQ|STK|||", "category_id": None},
     (None, "set_position_category_tag"), True),  # SharesBand.retag to None
    ("PUT", "/position-categories/symbol-order", {"category_name": "Watching", "symbols": ["ZZQ", "ZZR"]},
     (None, "set_market_streams_symbol_order"), True),  # useMarketStreamsSymbolOrder
    ("PUT", "/instrument-classes/ZZFI%7CSTK%7C%7C%7C", {"instrument_class": "fixed_income"},
     (None, "set_instrument_class"), (True, None)),  # SharesBand.register
    ("POST", "/executions", EXECUTION_CREATE, (ex, "insert_one_execution"), -101),
    ("POST", "/executions", QUICK_CLOSE, (ex, "insert_one_execution"), -1000000101),
    ("POST", "/executions", {k: v for k, v in EXECUTION_UPDATE.items() if k != "exec_time"} | {"time": 1930487400},
     (ex, "insert_one_execution"), -102),  # ExecutionFormModal (create, with splits)
    ("PUT", "/executions/-101", EXECUTION_UPDATE, (ex, "update_one_execution"), True),
    ("POST", "/executions/option-stock-links",
     {"account_id": ACC, "option_account_executions_id": 31, "stock_account_executions_id": 32, "role": "assignment"},
     (ex, "insert_option_stock_link"), (True, 5, None, None)),  # LinkOptionStockModal / LedgerLinksFace
    ("POST", "/executions/option-stock-links/query",
     {"batches": [{"account_id": ACC, "option_account_executions_ids": [31, 33]}]},
     (None, "get_option_stock_links_bulk"), {"by_option_id": {}}),  # fetchOptionStockLinkMap
]


@pytest.mark.parametrize("method,path,body,target,answer", ACCOUNT_CASES, ids=[f"{c[0]} {c[1]}" for c in ACCOUNT_CASES])
def test_the_frontends_payload_passes(
    monkeypatch: pytest.MonkeyPatch, method: str, path: str, body: Dict[str, Any], target: Tuple[Any, str], answer: Any
) -> None:
    module, fn = target
    writer = MagicMock(return_value=answer)
    reader = MagicMock()
    if module is None:
        getattr(reader, fn).return_value = answer
    else:
        monkeypatch.setattr(module, fn, writer)
    c, reader = _account(reader)
    r = c.request(method, path, json=body)
    assert r.status_code == 200, r.text
    called = getattr(reader, fn) if module is None else writer
    assert called.call_count == 1


def test_the_payload_reaches_core_as_sent(monkeypatch: pytest.MonkeyPatch) -> None:
    """The declared fields the client sent, nested ones too; nothing else."""
    create = MagicMock(return_value=11)
    monkeypatch.setattr(strategy_structure_write, "create_structure", create)
    c, _ = _account()
    assert c.post("/strategies/structures", json=STRUCTURE).status_code == 200
    assert create.call_args.args[1] == STRUCTURE

    update = MagicMock(return_value=True)
    monkeypatch.setattr(ex, "update_one_execution", update)
    assert c.put("/executions/-101", json=EXECUTION_UPDATE).status_code == 200
    sent = update.call_args.args[2]
    assert sent == EXECUTION_UPDATE and "time" not in sent  # PUT changes only what was sent


WATCHLIST_PAYLOADS = [
    {"contract_key": "ZZQ|STK|||", "symbol": "ZZQ", "sec_type": "STK", "source": "omnibar"},  # Omnibar / Dock / drop
    {"contract_key": "ZZQ|STK|||", "symbol": "ZZQ", "sec_type": "STK", "source": "research_hypothesis",
     "optionable": True},  # PromoteToWatchlistButton
    {"contract_key": "ZZQ|STK|||", "symbol": "ZZQ", "sec_type": "STK", "source": "manual", "category_id": 4},
    {"contract_key": "ZZQ|OPT|20310417|40.5|P", "symbol": "ZZQ", "sec_type": "OPT", "expiry": "20310417",
     "strike": 40.5, "option_right": "P", "source": "symbol_chain"},  # SymbolChainParts
    {"contract_key": "ZZQ|OPT|20310417|40|P", "symbol": "ZZQ", "sec_type": "OPT", "expiry": "20310417",
     "strike": 40, "option_right": "P", "source": "position", "category_id": 4},  # StockWatchlistPage
]


@pytest.mark.parametrize("body", WATCHLIST_PAYLOADS)
def test_the_frontends_watchlist_payloads_pass(monkeypatch: pytest.MonkeyPatch, body: Dict[str, Any]) -> None:
    upsert = MagicMock(return_value={"contract_key": body["contract_key"]})
    monkeypatch.setattr(watchlist, "upsert_watchlist", upsert)
    r = _market().post("/watchlist", json=body)
    assert r.status_code == 200, r.text
    assert upsert.call_args.args[2] == {k: v for k, v in body.items() if k != "contract_key"}


# --- the tag bug ---------------------------------------------------------------------------


@pytest.mark.parametrize("category_id", ["abc", "3", 3.5, True, [3], {"id": 3}])
def test_a_malformed_category_id_is_422_and_never_clears_the_tag(category_id: Any) -> None:
    c, reader = _account()
    r = c.put("/position-categories/tag", json={"account_id": ACC, "contract_key": "ZZQ|STK|||",
                                                "category_id": category_id})
    assert r.status_code == 422, r.text
    reader.set_position_category_tag.assert_not_called()


def test_null_clears_the_tag_and_an_id_sets_it() -> None:
    c, reader = _account()
    reader.set_position_category_tag.return_value = True
    assert c.put("/position-categories/tag", json={"account_id": ACC, "contract_key": "ZZQ|STK|||",
                                                   "category_id": None}).status_code == 200
    assert c.put("/position-categories/tag", json={"account_id": ACC, "contract_key": "ZZQ|STK|||",
                                                   "category_id": 3}).status_code == 200
    assert [call.args for call in reader.set_position_category_tag.call_args_list] == [
        (ACC, "ZZQ|STK|||", None),
        (ACC, "ZZQ|STK|||", 3),
    ]


def test_a_left_out_category_id_is_400_not_a_delete() -> None:
    c, reader = _account()
    r = c.put("/position-categories/tag", json={"account_id": ACC, "contract_key": "ZZQ|STK|||"})
    assert r.status_code == 400 and "category_id is required" in r.json()["detail"]
    reader.set_position_category_tag.assert_not_called()


# --- other wrong types ---------------------------------------------------------------------

WRONG_TYPES = [
    ("POST", "/strategies/templates", {"template_code": "zz_put", "sort_order": "100"}),
    ("PUT", "/strategies/templates/9/legs", {"legs": [{"role": "put", "quantity_default": 1.5}]}),
    ("POST", "/strategies/structures", {**STRUCTURE, "strategy_template_id": "4"}),
    ("PUT", "/strategies/gate-safety/2", {**GATE, "version": "2"}),
    ("PUT", "/strategies/gate-safety/2", {**GATE, "is_active": 1}),
    ("POST", "/strategies/saved-searches", {"route": "/trade/plans", "label": "x", "state": "ZZQ"}),
    ("POST", "/position-categories", {"name": "Watching", "sort_order": "2"}),
    ("PUT", "/position-categories/symbol-order", {"category_name": "Core", "symbols": "ZZQ"}),
    ("POST", "/executions", {**EXECUTION_CREATE, "strategy_instance_id": "x"}),
    ("POST", "/executions", {**EXECUTION_CREATE, "price": "1.15"}),
    ("PUT", "/executions/-101", {"strategy_opportunity_id": "abc"}),
    ("PUT", "/executions/-101", {"time": "yesterday"}),
    ("POST", "/executions/option-stock-links", {"account_id": ACC, "option_account_executions_id": "31",
                                                 "stock_account_executions_id": 32}),
    ("POST", "/executions/option-stock-links/query", {"batches": [{"account_id": ACC,
                                                                   "option_account_executions_ids": ["31"]}]}),
]


@pytest.mark.parametrize("method,path,body", WRONG_TYPES, ids=[f"{m} {p} {i}" for i, (m, p, _) in enumerate(WRONG_TYPES)])
def test_a_wrong_type_is_422_and_writes_nothing(
    monkeypatch: pytest.MonkeyPatch, method: str, path: str, body: Dict[str, Any]
) -> None:
    writers = [MagicMock() for _ in range(9)]
    for w, (module, fn) in zip(writers, [
        (template_config_write, "create_template"), (template_config_write, "replace_template_legs"),
        (strategy_structure_write, "create_structure"), (gate_safety_write, "update_gate_safety"),
        (saved_search, "create_saved_search"), (ex, "insert_one_execution"), (ex, "update_one_execution"),
        (ex, "insert_option_stock_link"), (template_config_write, "update_template"),
    ]):
        monkeypatch.setattr(module, fn, w)
    c, reader = _account()
    r = c.request(method, path, json=body)
    assert r.status_code == 422, r.text
    for w in writers:
        w.assert_not_called()
    for fn in ("create_position_category", "set_market_streams_symbol_order", "batch_update_execution_strategy",
               "get_option_stock_links_bulk"):
        getattr(reader, fn).assert_not_called()


def test_a_wrong_watchlist_type_is_422(monkeypatch: pytest.MonkeyPatch) -> None:
    upsert = MagicMock()
    monkeypatch.setattr(watchlist, "upsert_watchlist", upsert)
    c = _market()  # one market app per test: a second one re-registers its metrics
    assert c.post("/watchlist", json={"contract_key": "ZZQ|OPT|20310417|40|P", "strike": "40"}).status_code == 422
    assert c.post("/watchlist", json={"contract_key": "ZZQ|STK|||", "category_id": "4"}).status_code == 422
    upsert.assert_not_called()


# --- unknown fields ------------------------------------------------------------------------


def test_unknown_fields_are_ignored_and_logged_by_name_only(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    create = MagicMock(return_value=2)
    monkeypatch.setattr(gate_safety_write, "create_gate_safety", create)
    c, _ = _account()
    with caplog.at_level(logging.WARNING, logger="bifrost_api.common.request_bodies"):
        r = c.post("/strategies/gate-safety", json={**GATE, "structure_type": "SECRET-VALUE", "colour": "teal"})
    assert r.status_code == 200, r.text
    assert create.call_args.args[1] == GATE  # neither unknown field reaches core
    lines = [rec.getMessage() for rec in caplog.records if "unknown request fields" in rec.getMessage()]
    assert lines == ["unknown request fields: POST /strategies/gate-safety ignored ['colour', 'structure_type']"]
    assert "SECRET-VALUE" not in caplog.text and "teal" not in caplog.text


def test_nested_unknown_fields_are_named_with_their_path(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    replace = MagicMock()
    monkeypatch.setattr(template_config_write, "replace_template_legs", replace)
    c, _ = _account()
    legs = [{"role": "put", "direction": "sell", "option_right": "P", "quantity_default": 1, "leg_uid": "a"},
            {"role": "call", "direction": "sell", "option_right": "C", "quantity_default": 1, "leg_uid": "b"}]
    with caplog.at_level(logging.WARNING, logger="bifrost_api.common.request_bodies"):
        assert c.put("/strategies/templates/9/legs", json={"legs": legs}).status_code == 200
    assert replace.call_args.args[2] == [{k: v for k, v in leg.items() if k != "leg_uid"} for leg in legs]
    assert "PUT /strategies/templates/{strategy_template_id}/legs ignored ['legs[].leg_uid']" in caplog.text


def test_a_body_without_unknown_fields_logs_nothing(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(gate_safety_write, "create_gate_safety", MagicMock(return_value=2))
    c, _ = _account()
    with caplog.at_level(logging.WARNING, logger="bifrost_api.common.request_bodies"):
        assert c.post("/strategies/gate-safety", json=GATE).status_code == 200
    assert "unknown request fields" not in caplog.text


class _Item(LenientItem):
    a: Optional[int] = None


class _Body(LenientBody):
    x: Optional[int] = None
    items: Optional[List[_Item]] = None


def test_declared_drops_unknown_fields_at_every_level() -> None:
    body = _Body.model_validate({"x": 1, "y": 2, "items": [{"a": 1, "b": 2}, {"c": 3}]})
    assert unknown_field_names(body) == ["items[].b", "items[].c", "y"]
    assert body.declared() == {"x": 1, "items": [{"a": 1}, {"a": None}]}
    assert body.declared(exclude_unset=True) == {"x": 1, "items": [{"a": 1}, {}]}


def test_outside_a_request_the_log_still_names_the_fields(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger="bifrost_api.common.request_bodies"):
        _Body.model_validate({"z": 1})
    assert "unknown request fields: ? ? ignored ['z']" in caplog.text


@pytest.mark.parametrize(
    "factory",
    [lambda: _account()[0].app, lambda: _market().app],
    ids=["account", "market"],
)
def test_the_apps_with_typed_bodies_install_the_field_log(factory: Any) -> None:
    from bifrost_api.common.request_bodies import _RequestScope

    assert any(m.cls is _RequestScope for m in factory().user_middleware)
