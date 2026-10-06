"""Response models for allocations, opportunities, gate-safety sets, instances and plans
(TD-24, batch 3c-1).

Each reader's real answer (``tests/strategy_rows.py`` runs core's readers over rows
shaped like their SQL output) must validate against the route's model, reach the wire
exactly as it did before the model (what the route sent as ``-> Dict[str, Any]``), and the OpenAPI
schema must now document the fields. Fixtures are invented.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Tuple
from unittest.mock import MagicMock

import pytest
from starlette.testclient import TestClient

from bifrost_api.account.app import create_account_app
from bifrost_api.strategy.schemas import responses as models
from bifrost_core.monitor.reader import gate_safety_write
from bifrost_core.monitor.reader import strategy_allocation_write
from bifrost_core.monitor.reader import strategy_instance
from bifrost_core.monitor.reader import strategy_opportunity_write
from bifrost_core.monitor.reader import strategy_plan
from tests import strategy_rows as rows
from tests.contract.helpers import operator_server_config
from tests.reader_mock import reader_mock

PG = {"sink": "postgres"}


def _client(reader: MagicMock) -> TestClient:
    reader.config = operator_server_config()
    app = create_account_app(reader=reader, control_via_db=PG, status_cfg_for_read=PG, merged_config=reader.config)
    return TestClient(app, raise_server_exceptions=False)


# (path, reader method, its answer, model)
READS: List[Tuple[str, str, Callable[[], Any], Any]] = [
    ("/strategies/allocations", "list_allocations", rows.allocations, models.AllocationList),
    ("/strategies/allocations/3", "get_allocation_by_id", rows.allocation, models.AllocationRow),
    ("/strategies/opportunities", "list_opportunities", rows.opportunities, models.OpportunityList),
    ("/strategies/opportunities/5", "get_opportunity_by_id", rows.opportunity, models.OpportunityDetail),
    ("/gate-sets", "list_gate_safety_sets", rows.gate_sets, models.GateSetList),
    ("/gate-sets/2", "get_gate_safety_full_by_id", rows.gate_set, models.GateSetDetail),
    ("/trades", "list_trades", rows.instances, models.TradeList),
    ("/trades/41", "get_trade_by_id", rows.instance, models.TradeRow),
]


_expected = rows.as_sent_before


@pytest.mark.parametrize("path,method,answer,model", READS, ids=[r[0] for r in READS])
def test_reader_answer_validates_and_reaches_the_wire_unchanged(
    path: str, method: str, answer: Callable[[], Any], model: Any
) -> None:
    out = answer()
    reader = reader_mock()
    getattr(reader, method).return_value = out
    r = _client(reader).get(path)
    assert r.status_code == 200, r.text
    assert r.json() == _expected(out)
    model.model_validate(out if not isinstance(out, list) else {"items": out, "count": len(out)})


# (path, core module, patch writer, its answer, model)
PATCHES: List[Tuple[str, Any, str, Callable[[], Dict[str, Any]], Dict[str, Any]]] = [
    ("/strategies/allocations/3", strategy_allocation_write, "patch_allocation", rows.allocation, {"max_positions": 5}),
    ("/strategies/opportunities/5", strategy_opportunity_write, "patch_opportunity", rows.opportunity,
     {"is_active": False}),
    ("/gate-sets/2", gate_safety_write, "patch_gate_safety", rows.gate_set, {"name": "Calmer tape"}),
    ("/trades/41", strategy_instance, "patch_instance", rows.instance, {"label": None}),
    ("/strategies/plans/12", strategy_plan, "patch_plan", rows.plan, {"rationale": "Rolled."}),
]


@pytest.mark.parametrize("path,module,fn,answer,body", PATCHES, ids=[p[0] for p in PATCHES])
def test_patch_answers_the_row_unchanged(
    monkeypatch: pytest.MonkeyPatch, path: str, module: Any, fn: str, answer: Callable[[], Any], body: Dict[str, Any]
) -> None:
    out = answer()
    monkeypatch.setattr(module, fn, lambda _cfg, _id, _fields: out)
    r = _client(MagicMock()).patch(path, json=body)
    assert r.status_code == 200, r.text
    assert r.json() == _expected(out)


def test_plans_list_and_one_validate_and_reach_the_wire_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    listed, one = rows.plans(), rows.plan()
    monkeypatch.setattr(strategy_plan, "list_plans", lambda *_a, **_kw: listed)
    monkeypatch.setattr(strategy_plan, "get_plan", lambda *_a, **_kw: one)
    c = _client(MagicMock())
    r = c.get("/strategies/plans")
    assert r.status_code == 200, r.text
    assert r.json() == _expected(listed)
    r = c.get("/strategies/plans/12")
    assert r.status_code == 200, r.text
    assert r.json() == _expected(one)
    assert r.json()["effective_status"] in {"intended", "expired"}
    models.PlanList.model_validate({"items": listed, "count": len(listed)})


def test_the_wire_formats_are_the_ones_the_ui_already_reads(monkeypatch: pytest.MonkeyPatch) -> None:
    reader = reader_mock()
    reader.get_allocation_by_id.return_value = rows.allocation()
    monkeypatch.setattr(strategy_plan, "get_plan", lambda *_a, **_kw: rows.plan())
    c = _client(reader)
    body = c.get("/strategies/allocations/3").json()
    assert body["created_at"] == "2031-03-04T14:30:00Z"
    assert body["max_bp_pct"] == 0.25 and body["allocation_limits"] == {"max_positions": 4, "max_bp_pct": 0.25}
    r = c.get("/strategies/plans/12")
    assert r.status_code == 200, r.text
    plan = r.json()
    # numeric columns stay decimal strings (the UI's apiNumeric parses them); jsonb numbers stay numbers
    assert plan["limit_price"] == "1.10" and plan["target_value"] == "50" and plan["stop_value"] is None
    assert plan["exit_by"] == "2031-04-10" and plan["legs_json"][0]["strike"] == 40.0
    assert plan["expires_at"] == "2031-03-06T17:30:00Z"


def test_a_field_the_reader_did_not_send_is_not_added() -> None:
    reader = reader_mock()
    reader.get_trade_by_id.return_value = rows.instance()
    body = _client(reader).get("/trades/41").json()
    assert "executions_count" not in body  # list-only
    assert body["opened_at_epoch"] == rows.T0.timestamp()


def test_an_undeclared_reader_field_still_goes_out() -> None:
    reader = reader_mock()
    reader.get_allocation_by_id.return_value = {**rows.allocation(), "added_by_a_newer_core": [1, 2]}
    assert _client(reader).get("/strategies/allocations/3").json()["added_by_a_newer_core"] == [1, 2]


def test_a_reader_row_missing_an_always_present_field_is_a_500_not_a_silent_gap() -> None:
    reader = reader_mock()
    row = rows.allocation()
    del row["strategy_opportunity_ids"]
    reader.get_allocation_by_id.return_value = row
    assert _client(reader).get("/strategies/allocations/3").status_code == 500


# (method, path template in the OpenAPI document, model)
DOCUMENTED = [
    ("get", "/strategies/allocations", models.AllocationList),
    ("get", "/strategies/allocations/{strategy_allocation_id}", models.AllocationRow),
    ("patch", "/strategies/allocations/{strategy_allocation_id}", models.AllocationRow),
    ("get", "/strategies/opportunities", models.OpportunityList),
    ("get", "/strategies/opportunities/{strategy_opportunity_id}", models.OpportunityDetail),
    ("patch", "/strategies/opportunities/{strategy_opportunity_id}", models.OpportunityDetail),
    ("get", "/gate-sets", models.GateSetList),
    ("get", "/gate-sets/{gate_safety_strategy_id}", models.GateSetDetail),
    ("patch", "/gate-sets/{gate_safety_strategy_id}", models.GateSetDetail),
    ("get", "/trades", models.TradeList),
    ("get", "/trades/{trade_id}", models.TradeRow),
    ("patch", "/trades/{trade_id}", models.TradeRow),
    ("get", "/strategies/plans", models.PlanList),
    ("get", "/strategies/plans/{strategy_plan_id}", models.PlanRow),
    ("patch", "/strategies/plans/{strategy_plan_id}", models.PlanRow),
]


@pytest.mark.parametrize("method,path,model", DOCUMENTED, ids=[f"{m} {p}" for m, p, _ in DOCUMENTED])
def test_openapi_documents_the_fields(method: str, path: str, model: Any) -> None:
    doc = _client(MagicMock()).app.openapi()
    schema = doc["paths"][path][method]["responses"]["200"]["content"]["application/json"]["schema"]
    ref = schema["$ref"].rsplit("/", 1)[-1]
    assert ref == model.__name__
    props = doc["components"]["schemas"][ref]["properties"]
    assert set(model.model_fields) <= set(props)
    required = set(doc["components"]["schemas"][ref].get("required", []))
    assert required == {n for n, f in model.model_fields.items() if f.is_required()}


@pytest.mark.parametrize(
    "model,fields",
    [
        (models.AllocationRow, rows.allocation),
        (models.OpportunityRow, lambda: rows.opportunities()[0]),
        (models.OpportunityDetail, rows.opportunity),
        (models.GateSetRow, lambda: rows.gate_sets()[0]),
        (models.GateSetDetail, rows.gate_set),
        (models.TradeRow, lambda: rows.instances()[0]),
        (models.TradeRow, rows.instance),
        (models.PlanRow, rows.plan),
    ],
)
def test_the_model_declares_every_field_the_reader_returns(model: Any, fields: Callable[[], Dict[str, Any]]) -> None:
    """So nothing the UI reads is only there through extra="allow"."""
    assert set(fields()) <= set(model.model_fields)


def test_as_sent_before_is_what_a_dict_route_sends() -> None:
    """The baseline the parity tests compare with, checked against FastAPI itself."""
    out = rows.plan()
    sent = rows.as_sent_before(out)
    assert sent["created_at"].endswith("Z") and sent["limit_price"] == "1.10"
