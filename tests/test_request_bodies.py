"""Typed POST / PUT bodies (TD-24, batch 3c-1; Owner decision B).

Each write route replays the payload the frontend sends today (the shape of the
call site named beside it; values invented) and must pass, handing core exactly the
declared fields. Then: wrong types are 422 and write nothing -- the tag bug first --
and so are unknown fields (``extra="forbid"`` since api 0.9.0; they were ignored and
logged by name from 0.3.1 until a week of logs showed no caller sending one).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel
from starlette.testclient import TestClient

import bifrost_api.trading.routers.executions as ex
from bifrost_api.account.app import create_account_app
from bifrost_api.common.request_bodies import StrictBody, StrictItem
from bifrost_api.market.app import create_market_app
from bifrost_core.monitor.reader import gate_safety_write
from bifrost_core.monitor.reader import saved_search
from bifrost_core.monitor.reader import strategy_structure_write
from bifrost_core.monitor.reader import template_config_write
from bifrost_core.monitor.reader import watchlist
from bifrost_core.monitor.schemas.gate_params import default_gates
from bifrost_core.portfolio.reader import instrument_class
from bifrost_core.portfolio.reader import position_categories
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
    "strategy_opportunity_id": 5, "trade_id": 41,
}
QUICK_CLOSE = {  # quickCloseBody (QuickCloseModal)
    "account_id": ACC, "time": 1930487400, "symbol": "ZZQ", "sec_type": "OPT", "side": "BUY", "quantity": 2,
    "price": 0.4, "source": "journal_closed", "expiry": "20310417", "strike": 40.0, "option_right": "P",
    "contract_key": "ZZQ|OPT|20310417|40.0|P", "commission": 1.1, "currency": "USD",
    "trade_id": 41, "strategy_opportunity_id": 5,
}
EXECUTION_UPDATE = {  # ExecutionFormModal (edit)
    "account_id": ACC, "exec_time": 1930487400.5, "symbol": "ZZQ", "sec_type": "OPT", "side": "SELL",
    "quantity": -2, "price": 1.15, "strike": 40, "option_right": "P", "contract_key": "ZZQ|OPT|20310417|40|P",
    "fill_splits": [{"trade_id": 41, "quantity": -1.5},
                             {"trade_id": 42, "quantity": -0.5}],
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
    ("POST", "/gate-sets", GATE, (gate_safety_write, "create_gate_safety"), 2),
    ("PUT", "/gate-sets/2", GATE, (gate_safety_write, "update_gate_safety"), True),
    ("POST", "/preferences/saved-searches", {"route": "/trade/plans", "label": "ZZQ only", "state": {"search": "ZZQ"}},
     (saved_search, "create_saved_search"), 3),  # TradePlansPage
    ("POST", "/position-categories", {"name": "Watching", "sort_order": 2},
     (position_categories, "create_position_category_strict"),
     {"id": 7, "name": "Watching", "sort_order": 2}),  # useEnsureWatchlistCategories
    ("PUT", "/position-categories/tag", {"account_id": ACC, "contract_key": "ZZQ|STK|||", "category_id": 3},
     (position_categories, "set_position_category_tag_strict"),
     {"account_id": ACC, "contract_key": "ZZQ|STK|||", "category_id": 3, "cleared": False}),  # SharesBand.retag
    ("PUT", "/position-categories/tag", {"account_id": ACC, "contract_key": "ZZQ|STK|||", "category_id": None},
     (position_categories, "set_position_category_tag_strict"),
     {"account_id": ACC, "contract_key": "ZZQ|STK|||", "category_id": None, "cleared": True}),  # retag to None
    ("PUT", "/position-categories/symbol-order", {"category_name": "Watching", "symbols": ["ZZQ", "ZZR"]},
     (position_categories, "set_market_streams_symbol_order_strict"),
     {"category_name": "Watching", "symbols": ["ZZQ", "ZZR"]}),  # useMarketStreamsSymbolOrder
    ("PUT", "/instrument-classes/ZZFI%7CSTK%7C%7C%7C", {"instrument_class": "fixed_income"},
     (instrument_class, "set_instrument_class_strict"),
     {"contract_key": "ZZFI|STK|||", "instrument_class": "fixed_income", "note": None}),  # SharesBand.register
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


def _tag_writer(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    tag = MagicMock(return_value={"account_id": ACC, "contract_key": "ZZQ|STK|||", "category_id": None, "cleared": True})
    monkeypatch.setattr(position_categories, "set_position_category_tag_strict", tag)
    return tag


@pytest.mark.parametrize("category_id", ["abc", "3", 3.5, True, [3], {"id": 3}])
def test_a_malformed_category_id_is_422_and_never_clears_the_tag(
    monkeypatch: pytest.MonkeyPatch, category_id: Any
) -> None:
    tag = _tag_writer(monkeypatch)
    c, _ = _account()
    r = c.put("/position-categories/tag", json={"account_id": ACC, "contract_key": "ZZQ|STK|||",
                                                "category_id": category_id})
    assert r.status_code == 422, r.text
    tag.assert_not_called()


def test_null_clears_the_tag_and_an_id_sets_it(monkeypatch: pytest.MonkeyPatch) -> None:
    tag = _tag_writer(monkeypatch)
    c, _ = _account()
    assert c.put("/position-categories/tag", json={"account_id": ACC, "contract_key": "ZZQ|STK|||",
                                                   "category_id": None}).status_code == 200
    assert c.put("/position-categories/tag", json={"account_id": ACC, "contract_key": "ZZQ|STK|||",
                                                   "category_id": 3}).status_code == 200
    assert [call.args for call in tag.call_args_list] == [
        (PG, ACC, "ZZQ|STK|||", None),
        (PG, ACC, "ZZQ|STK|||", 3),
    ]


def test_a_left_out_category_id_is_400_not_a_delete(monkeypatch: pytest.MonkeyPatch) -> None:
    tag = _tag_writer(monkeypatch)
    c, _ = _account()
    r = c.put("/position-categories/tag", json={"account_id": ACC, "contract_key": "ZZQ|STK|||"})
    assert r.status_code == 400 and "category_id is required" in r.json()["detail"]
    tag.assert_not_called()


# --- other wrong types ---------------------------------------------------------------------

WRONG_TYPES = [
    ("POST", "/strategies/templates", {"template_code": "zz_put", "sort_order": "100"}),
    ("PUT", "/strategies/templates/9/legs", {"legs": [{"role": "put", "quantity_default": 1.5}]}),
    ("POST", "/strategies/structures", {**STRUCTURE, "strategy_template_id": "4"}),
    ("PUT", "/gate-sets/2", {**GATE, "version": "2"}),
    ("PUT", "/gate-sets/2", {**GATE, "is_active": 1}),
    ("POST", "/preferences/saved-searches", {"route": "/trade/plans", "label": "x", "state": "ZZQ"}),
    ("POST", "/position-categories", {"name": "Watching", "sort_order": "2"}),
    ("PUT", "/position-categories/symbol-order", {"category_name": "Core", "symbols": "ZZQ"}),
    ("POST", "/executions", {**EXECUTION_CREATE, "trade_id": "x"}),
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
    writers = [MagicMock() for _ in range(11)]
    for w, (module, fn) in zip(writers, [
        (template_config_write, "create_template"), (template_config_write, "replace_template_legs"),
        (strategy_structure_write, "create_structure"), (gate_safety_write, "update_gate_safety"),
        (saved_search, "create_saved_search"), (ex, "insert_one_execution"), (ex, "update_one_execution"),
        (ex, "insert_option_stock_link"), (template_config_write, "update_template"),
        (position_categories, "create_position_category_strict"),
        (position_categories, "set_market_streams_symbol_order_strict"),
    ]):
        monkeypatch.setattr(module, fn, w)
    c, reader = _account()
    r = c.request(method, path, json=body)
    assert r.status_code == 422, r.text
    for w in writers:
        w.assert_not_called()
    reader.get_option_stock_links_bulk.assert_not_called()


def test_a_wrong_watchlist_type_is_422(monkeypatch: pytest.MonkeyPatch) -> None:
    upsert = MagicMock()
    monkeypatch.setattr(watchlist, "upsert_watchlist", upsert)
    c = _market()  # one market app per test: a second one re-registers its metrics
    assert c.post("/watchlist", json={"contract_key": "ZZQ|OPT|20310417|40|P", "strike": "40"}).status_code == 422
    assert c.post("/watchlist", json={"contract_key": "ZZQ|STK|||", "category_id": "4"}).status_code == 422
    upsert.assert_not_called()


# --- unknown fields ------------------------------------------------------------------------


@pytest.mark.parametrize(
    "method,path,body,extra",
    [
        ("POST", "/gate-sets", {**GATE, "colour": "teal"}, "colour"),
        ("POST", "/strategies/structures", {**STRUCTURE, "subtype": "x"}, "subtype"),
        ("POST", "/strategies/templates", {"template_code": "zz_put", "colour": "teal"}, "colour"),
        ("POST", "/preferences/saved-searches", {"route": "/trade/plans", "label": "x", "owner": "y"}, "owner"),
        ("POST", "/position-categories", {"name": "Watching", "color": "teal"}, "color"),
        ("PUT", "/position-categories/tag", {"account_id": ACC, "contract_key": "ZZQ|STK|||", "category_id": 3,
                                             "category_name": "Core"}, "category_name"),
        ("PUT", "/position-categories/symbol-order", {"category_name": "Core", "symbols": ["ZZQ"], "x": 1}, "x"),
        ("PUT", "/instrument-classes/ZZFI%7CSTK%7C%7C%7C", {"instrument_class": "fixed_income", "x": 1}, "x"),
        ("POST", "/executions", {**EXECUTION_CREATE, "exec_time": 1930487400}, "exec_time"),
        ("PUT", "/executions/-101", {**EXECUTION_UPDATE, "account_executions_id": -101}, "account_executions_id"),
        ("POST", "/executions/option-stock-links", {"account_id": ACC, "option_account_executions_id": 31,
                                                    "stock_account_executions_id": 32, "x": 1}, "x"),
        ("POST", "/executions/option-stock-links/query", {"batches": [], "x": 1}, "x"),
    ],
    ids=lambda v: v if isinstance(v, str) and v.startswith("/") else None,
)
def test_an_unknown_field_is_422_named_and_writes_nothing(
    monkeypatch: pytest.MonkeyPatch, method: str, path: str, body: Dict[str, Any], extra: str
) -> None:
    writers = [MagicMock() for _ in range(6)]
    for w, (module, fn) in zip(writers, [
        (gate_safety_write, "create_gate_safety"), (strategy_structure_write, "create_structure"),
        (template_config_write, "create_template"), (saved_search, "create_saved_search"),
        (ex, "insert_one_execution"), (ex, "update_one_execution"),
    ]):
        monkeypatch.setattr(module, fn, w)
    monkeypatch.setattr(ex, "insert_option_stock_link", MagicMock())
    c, reader = _account()
    r = c.request(method, path, json=body)
    assert r.status_code == 422, r.text
    errors = r.json()["detail"]
    assert [e["type"] for e in errors] == ["extra_forbidden"]
    assert errors[0]["loc"] == ["body", extra]
    for w in writers:
        w.assert_not_called()
    ex.insert_option_stock_link.assert_not_called()
    for fn in ("create_position_category", "set_position_category_tag", "set_market_streams_symbol_order",
               "set_instrument_class", "get_option_stock_links_bulk"):
        getattr(reader, fn).assert_not_called()


def test_a_nested_unknown_field_is_422_with_its_path(monkeypatch: pytest.MonkeyPatch) -> None:
    replace = MagicMock()
    monkeypatch.setattr(template_config_write, "replace_template_legs", replace)
    c, _ = _account()
    legs = [{"role": "put", "direction": "sell", "option_right": "P", "quantity_default": 1},
            {"role": "call", "direction": "sell", "option_right": "C", "quantity_default": 1, "leg_uid": "b"}]
    r = c.put("/strategies/templates/9/legs", json={"legs": legs})
    assert r.status_code == 422, r.text
    assert [(e["type"], e["loc"]) for e in r.json()["detail"]] == [
        ("extra_forbidden", ["body", "legs", 1, "leg_uid"])
    ]
    replace.assert_not_called()


def test_an_unknown_watchlist_field_is_422(monkeypatch: pytest.MonkeyPatch) -> None:
    upsert = MagicMock()
    monkeypatch.setattr(watchlist, "upsert_watchlist", upsert)
    r = _market().post("/watchlist", json={**WATCHLIST_PAYLOADS[0], "colour": "teal"})
    assert r.status_code == 422, r.text
    assert r.json()["detail"][0]["loc"] == ["body", "colour"]
    upsert.assert_not_called()


class _Item(StrictItem):
    a: Optional[int] = None


class _Body(StrictBody):
    x: Optional[int] = None
    items: Optional[List[_Item]] = None


def test_declared_is_the_body_as_plain_data() -> None:
    body = _Body.model_validate({"x": 1, "items": [{"a": 1}, {}]})
    assert body.declared() == {"x": 1, "items": [{"a": 1}, {"a": None}]}
    assert body.declared(exclude_unset=True) == {"x": 1, "items": [{"a": 1}, {}]}


@pytest.mark.parametrize("data", [{"y": 2}, {"items": [{"a": 1, "b": 2}]}])
def test_every_body_and_item_refuses_undeclared_keys(data: Dict[str, Any]) -> None:
    from pydantic import ValidationError

    with pytest.raises(ValidationError) as e:
        _Body.model_validate(data)
    assert [err["type"] for err in e.value.errors()] == ["extra_forbidden"]


def test_every_request_model_forbids_extra() -> None:
    """Every POST / PUT body and item in the four schema modules, aliases included."""
    from bifrost_api.market.schemas import requests as market
    from bifrost_api.portfolio.schemas import requests as portfolio
    from bifrost_api.strategy.schemas import requests as strategy
    from bifrost_api.trading.schemas import requests as trading

    seen = 0
    for module in (market, portfolio, strategy, trading):
        for obj in vars(module).values():
            if isinstance(obj, type) and issubclass(obj, BaseModel) and obj.__module__ == module.__name__:
                assert obj.model_config.get("extra") == "forbid", obj.__name__
                seen += 1
    assert seen >= 20
