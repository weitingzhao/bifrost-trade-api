"""Strategy rows as core's readers return them -- the readers run over rows shaped like
their SQL output (psycopg2 types: datetime, date, Decimal, parsed jsonb).

The tests that need a reader's answer (the response models, the list counts, the
PATCH answers) take it from here, so a reader that changes its shape fails them
rather than the UI. Every value is invented.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

from fastapi import FastAPI
from starlette.testclient import TestClient

from bifrost_core.monitor.reader import gate_safety, strategy, strategy_instance, strategy_plan
from bifrost_core.monitor.schemas.gate_params import GateParams

T0 = datetime(2031, 3, 4, 14, 30, tzinfo=timezone.utc)
T1 = T0 + timedelta(days=2, hours=3)


class _Cursor:
    def __init__(self, rows: List[Dict[str, Any]]) -> None:
        self._rows = rows

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_a: Any) -> None:
        return None

    def execute(self, _sql: str, _params: Any = None) -> None:
        return None

    def fetchall(self) -> List[Dict[str, Any]]:
        return [dict(r) for r in self._rows]

    def fetchone(self) -> Optional[Dict[str, Any]]:
        return dict(self._rows[0]) if self._rows else None


class FakeConn:
    """Answers every SELECT with the given rows, as a RealDictCursor would."""

    def __init__(self, rows: List[Dict[str, Any]]) -> None:
        self._rows = rows

    def cursor(self, **_kw: Any) -> _Cursor:
        return _Cursor(self._rows)

    def close(self) -> None:
        return None


# --- SQL-shaped rows ----------------------------------------------------------------------

ALLOCATION_SQL = [
    {
        "strategy_allocation_id": 3, "name": "Wheel book", "gate_safety_strategy_id": 2,
        "max_positions": 4, "max_bp_pct": Decimal("0.25"), "is_active": True,
        "created_at": T0, "updated_at": T1, "gate_safety_name": "Calm tape",
        "strategy_opportunity_ids": [5, 6],
    },
    {
        "strategy_allocation_id": 4, "name": "Parked", "gate_safety_strategy_id": None,
        "max_positions": None, "max_bp_pct": None, "is_active": False,
        "created_at": T0, "updated_at": T0, "gate_safety_name": None,
        "strategy_opportunity_ids": None,
    },
]

OPPORTUNITY_LIST_SQL = [
    {
        "strategy_opportunity_id": 5, "name": "ZZQ puts", "strategy_structure_id": 9,
        "default_gate_safety_strategy_id": 2, "scope_type": "explicit_symbols",
        "is_active": True, "created_at": T0, "updated_at": T1,
        "structure_name": "Short put 30d", "gate_safety_name": "Calm tape", "symbols": ["ZZQ", "ZZR"],
    },
    {
        "strategy_opportunity_id": 6, "name": "Watchlist CCs", "strategy_structure_id": 10,
        "default_gate_safety_strategy_id": None, "scope_type": None,
        "is_active": True, "created_at": T0, "updated_at": T0,
        "structure_name": None, "gate_safety_name": None, "symbols": None,
    },
]

OPPORTUNITY_DETAIL_SQL = {
    "strategy_opportunity_id": 5, "name": "ZZQ puts", "strategy_structure_id": 9,
    "default_gate_safety_strategy_id": 2, "scope_type": "explicit_symbols",
    "is_active": True, "created_at": T0, "updated_at": T1,
    "symbols_json": ["ZZQ", "ZZR"],
    "entry_conditions_json": [
        {"condition_type": "iv_min", "value_numeric": 31},
        {"condition_type": "earnings_blackout_days", "value_text": "5", "value_numeric": None},
    ],
    "structure_name": "Short put 30d", "gate_safety_name": "Calm tape",
}

GATE_LIST_SQL = [
    {
        "gate_safety_strategy_id": 2, "name": "Calm tape", "version": 3,
        "dim_direction": "neutral", "dim_structure": None, "dim_coverage": None,
        "dim_risk": "defined", "dim_volatility": None, "dim_time": None, "is_active": True,
    },
]

GATE_DETAIL_SQL = {
    **GATE_LIST_SQL[0],
    "params_json": {
        **GateParams().model_dump(),
        "strategy": {
            **GateParams().model_dump()["strategy"],
            "earnings": {"blackout_days_before": 2, "blackout_days_after": 1, "dates": ["2031-04-22"]},
        },
    },
}

INSTANCE_LIST_SQL = [
    {
        "strategy_instance_id": 41, "strategy_opportunity_id": 5, "account_id": "U0000001",
        "opened_at": T0, "label": "ZZQ Apr 40P", "notes": None, "created_at": T0, "updated_at": T1,
        "strategy_opportunity_name": "ZZQ puts", "strategy_structure_id": 9,
        "strategy_structure_name": "Short put 30d", "executions_count": 3,
    },
]

INSTANCE_DETAIL_SQL = {k: v for k, v in INSTANCE_LIST_SQL[0].items() if k != "executions_count"}

PLAN_SQL = {
    "strategy_plan_id": 12, "account_id": "U0000001", "symbol": "ZZQ", "structure_label": "Short put",
    "strategy_structure_id": 9, "strategy_opportunity_id": 5,
    "legs_json": [
        {
            "side": "sell", "sec_type": "OPT", "right": "P", "strike": 40.0, "expiry": "2031-04-17",
            "ratio": 1, "contract_key": "ZZQ|OPT|20310417|40|P", "mid_at_plan": 1.15,
            "quote_asof": "2031-03-04T14:29:00+00:00",
        }
    ],
    "qty": 2, "price_effect": "credit", "limit_price": Decimal("1.10"),
    "target_kind": "credit_pct", "target_value": Decimal("50"), "stop_kind": None, "stop_value": None,
    "exit_by": date(2031, 4, 10), "rationale": "IV rank high into a quiet tape.",
    "source_kind": "symbol", "source_ref": "ZZQ", "source_json": [{"kind": "symbol", "ref": "ZZQ"}],
    "status": "intended", "expires_at": T1, "intended_at": T0, "filled_at": None, "cancelled_at": None,
    "strategy_instance_id": None, "parent_strategy_plan_id": None, "created_at": T0, "updated_at": T1,
}


# --- what the readers answer --------------------------------------------------------------


def allocations() -> List[Dict[str, Any]]:
    return strategy.list_allocations(FakeConn(ALLOCATION_SQL), active_only=False)


def allocation() -> Dict[str, Any]:
    row = strategy.get_allocation_by_id(FakeConn(ALLOCATION_SQL[:1]), 3)
    assert row is not None
    return row


def opportunities() -> List[Dict[str, Any]]:
    return strategy.list_opportunities(FakeConn(OPPORTUNITY_LIST_SQL), active_only=False)


def opportunity() -> Dict[str, Any]:
    row = strategy.get_opportunity_by_id(FakeConn([OPPORTUNITY_DETAIL_SQL]), 5)
    assert row is not None
    return row


def gate_sets() -> List[Dict[str, Any]]:
    return gate_safety.list_gate_safety_sets(FakeConn(GATE_LIST_SQL))


def gate_set() -> Dict[str, Any]:
    row = gate_safety.get_gate_safety_full_by_id(FakeConn([GATE_DETAIL_SQL]), 2)
    assert row is not None
    return row


def instances() -> List[Dict[str, Any]]:
    return strategy_instance.list_instances(FakeConn(INSTANCE_LIST_SQL))


def instance() -> Dict[str, Any]:
    row = strategy_instance.get_instance_by_id(FakeConn([INSTANCE_DETAIL_SQL]), 41)
    assert row is not None
    return row


def plan(**changes: Any) -> Dict[str, Any]:
    row = strategy_plan._get_plan_on(FakeConn([{**PLAN_SQL, **changes}]), 12)
    assert row is not None
    return row


def plans() -> List[Dict[str, Any]]:
    draft = {**PLAN_SQL, "strategy_plan_id": 13, "status": "draft", "expires_at": None, "intended_at": None,
             "legs_json": "[]", "source_json": None, "limit_price": None, "target_kind": None,
             "target_value": None, "exit_by": None}
    return [strategy_plan._row_out(dict(r)) for r in (PLAN_SQL, draft)]


# --- what the routes sent before they had a response model --------------------------------

_BEFORE = FastAPI()
_BEFORE_ANSWER: List[Any] = []


@_BEFORE.get("/row")
def _row() -> Dict[str, Any]:
    return _BEFORE_ANSWER[-1]


def as_sent_before(answer: Any) -> Any:
    """``answer`` as a route annotated ``-> Dict[str, Any]`` sent it (FastAPI serialises
    that through pydantic: datetimes with Z, Decimal as a string). A list is wrapped in
    the list envelope, as the list routes do."""
    body = {"items": answer, "count": len(answer)} if isinstance(answer, list) else answer
    _BEFORE_ANSWER.append(body)
    try:
        return TestClient(_BEFORE).get("/row").json()
    finally:
        _BEFORE_ANSWER.pop()
