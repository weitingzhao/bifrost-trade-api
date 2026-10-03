"""Response models for allocations, opportunities, gate sets, trades and plans
(TD-24, batch 3c-1; Owner decision B).

Each model declares the fields core's reader returns -- the reader's SELECT list and
what it adds (``bifrost_core.monitor.reader.strategy`` / ``gate_safety`` /
``strategy_instance`` / ``strategy_plan``) -- so the OpenAPI schema documents them and
the UI can mirror them. A field with no default is always in the answer (``Optional``
there means it can be null); a field with a default is sent only when the reader has it.

What reaches the wire does not change (``tests/test_strategy_response_models.py``
compares each answer with what the route sent as ``-> Dict[str, Any]``):

- ``extra="allow"`` this release, so a field the reader adds before this file does
  still goes out;
- the routes set ``response_model_exclude_unset=True``, so a declared field the
  reader did not send is not added as null;
- each field is typed as the reader's Python value, so pydantic serialises it as it
  did through ``Dict[str, Any]``: a timestamp (``datetime``) is ISO 8601 with ``Z``
  for UTC, a date is ``YYYY-MM-DD``, and a PostgreSQL ``numeric`` the reader passes
  through (``Decimal``: the plan's ``limit_price`` / ``target_value`` /
  ``stop_value``) is a **decimal string** such as ``"1.10"`` -- the UI parses those
  (``apiNumeric``). A numeric the reader converts (``max_bp_pct``) is a number.

A reader row that does not fit (a missing always-present field, a wrong type) is a
500 -- ``tests/test_strategy_response_models.py`` runs each reader over rows shaped
like its SQL output so that shows up in CI, not in the UI.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict

# On the wire: ISO 8601 (2031-03-04T14:30:00Z).
Timestamp = datetime
# A number from jsonb: an integer stays an integer on the wire.
Number = Union[int, float]
# PostgreSQL numeric passed through: a decimal string on the wire ("1.10").
Numeric = Decimal


class ResponseRow(BaseModel):
    """A response object: declared fields documented, undeclared ones passed through."""

    model_config = ConfigDict(extra="allow")


# --- allocations ------------------------------------------------------------------------


class AllocationLimits(ResponseRow):
    """The limits that are set; a key is absent when its column is null."""

    max_positions: Optional[int] = None
    max_bp_pct: Optional[float] = None


class AllocationRow(ResponseRow):
    """GET /strategies/allocations item, GET /allocations/{id}, PATCH /allocations/{id}."""

    strategy_allocation_id: int
    name: str
    gate_safety_strategy_id: Optional[int]
    gate_safety_name: Optional[str]
    max_positions: Optional[int]
    max_bp_pct: Optional[float]
    # {max_positions?, max_bp_pct?}; null when neither is set.
    allocation_limits: Optional[AllocationLimits]
    # In the allocation's order; [] when it has none.
    strategy_opportunity_ids: List[int]
    is_active: bool
    created_at: Timestamp
    updated_at: Timestamp


class AllocationList(ResponseRow):
    items: List[AllocationRow]
    count: int


# --- opportunities ----------------------------------------------------------------------


class EntryCondition(ResponseRow):
    condition_type: Optional[str]
    value_text: Optional[str]
    value_numeric: Optional[float]


class OpportunityRow(ResponseRow):
    """GET /strategies/opportunities item."""

    strategy_opportunity_id: int
    name: str
    strategy_structure_id: int
    structure_name: Optional[str]
    default_gate_safety_strategy_id: Optional[int]
    gate_safety_name: Optional[str]
    scope_type: Optional[str]
    # The list sends null for an opportunity without symbols; the detail sends [].
    symbols: Optional[List[str]]
    is_active: bool
    created_at: Timestamp
    updated_at: Timestamp


class OpportunityDetail(OpportunityRow):
    """GET /strategies/opportunities/{id}, PATCH /opportunities/{id}."""

    entry_conditions: List[EntryCondition]


class OpportunityList(ResponseRow):
    items: List[OpportunityRow]
    count: int


# --- gate sets (table gate_safety_strategy, a legacy name: D6-A) ------------------------


class GateSetRow(ResponseRow):
    """GET /gate-sets item (and GET /strategies/gate-safety until R4)."""

    gate_safety_strategy_id: int
    name: str
    version: int
    dim_direction: Optional[str]
    dim_structure: Optional[str]
    dim_coverage: Optional[str]
    dim_risk: Optional[str]
    dim_volatility: Optional[str]
    dim_time: Optional[str]
    is_active: bool
    # Always null (the gate set has no structure type of its own); kept for old readers.
    structure_type: Optional[str]


class GateSetDetail(GateSetRow):
    """GET / PATCH /gate-sets/{id} (and /strategies/gate-safety/{id} until R4)."""

    # Core's GateParams as nested objects (strategy / state / intent / guard), the shape of
    # GET /gate-safety/defaults; without strategy.earnings.dates (see earnings_dates).
    gates: Dict[str, Any]
    # YYYY-MM-DD.
    earnings_dates: List[str]


class GateSetList(ResponseRow):
    items: List[GateSetRow]
    count: int


# The old class names (naming R1); they go in R4.
GateSafetyRow = GateSetRow
GateSafetyDetail = GateSetDetail
GateSafetyList = GateSetList


# --- trades (table strategy_instance until R3) ------------------------------------------


class TradeRow(ResponseRow):
    """GET /trades item, GET / PATCH /trades/{trade_id} (and /strategies/instances… until R4)."""

    # The trade's id under its name (core 0.42.0); strategy_instance_id is the same value
    # until R4.
    trade_id: int
    strategy_instance_id: int
    strategy_opportunity_id: int
    strategy_opportunity_name: Optional[str]
    strategy_structure_id: Optional[int]
    strategy_structure_name: Optional[str]
    account_id: str
    opened_at: Timestamp
    label: Optional[str]
    notes: Optional[str]
    created_at: Timestamp
    updated_at: Timestamp
    # Unix seconds of opened_at / created_at (sent whenever those are).
    opened_at_epoch: Optional[float] = None
    created_at_epoch: Optional[float] = None
    # The list only: fills attributed or split-allocated to the instance.
    executions_count: Optional[int] = None
    # The list only (core 0.41.0, TD-43): where the instance stands by its option fills --
    # no_fills / open / expired (every open leg past expiry, no closing fill; counts as
    # closed) / closed -- and the day it closed (YYYY-MM-DD; null unless expired or closed).
    state: Optional[Literal["no_fills", "open", "expired", "closed"]] = None
    closed_on: Optional[str] = None


class TradeList(ResponseRow):
    items: List[TradeRow]
    count: int


# The old class names (naming R1); they go in R4.
InstanceRow = TradeRow
InstanceList = TradeList


# --- plans ------------------------------------------------------------------------------


class PlanLegRow(ResponseRow):
    """One leg as core normalised it when the plan was written."""

    side: Optional[str] = None  # buy | sell
    sec_type: Optional[str] = None  # OPT | STK
    right: Optional[str] = None  # C | P | null
    strike: Optional[Number] = None
    expiry: Optional[str] = None  # YYYY-MM-DD
    ratio: Optional[int] = None
    contract_key: Optional[str] = None
    mid_at_plan: Optional[Number] = None
    quote_asof: Optional[str] = None


class PlanRow(ResponseRow):
    """GET /strategies/plans item, GET /plans/{id}, PATCH /plans/{id}."""

    strategy_plan_id: int
    account_id: str
    symbol: str
    structure_label: str
    strategy_structure_id: Optional[int]
    strategy_opportunity_id: Optional[int]
    legs_json: List[PlanLegRow]
    qty: int
    price_effect: Optional[str]  # credit | debit
    limit_price: Optional[Numeric]
    target_kind: Optional[str]  # credit_pct | option_price | underlying_price
    target_value: Optional[Numeric]
    stop_kind: Optional[str]  # credit_multiple | option_price | underlying_price
    stop_value: Optional[Numeric]
    exit_by: Optional[date]
    rationale: Optional[str]
    source_kind: str  # manual | symbol | hypothesis | inbox_draft | roll
    source_ref: Optional[str]
    # The provenance chain as written with the plan.
    source_json: List[Dict[str, Any]]
    status: str  # draft | intended | filled | cancelled
    # `status`, except an intended plan past expires_at reads expired.
    effective_status: str
    expires_at: Optional[Timestamp]
    intended_at: Optional[Timestamp]
    filled_at: Optional[Timestamp]
    cancelled_at: Optional[Timestamp]
    strategy_instance_id: Optional[int]
    # The same id under the trade's name (core 0.42.0, naming R1).
    trade_id: Optional[int] = None
    parent_strategy_plan_id: Optional[int]
    created_at: Timestamp
    updated_at: Timestamp


class PlanList(ResponseRow):
    items: List[PlanRow]
    count: int
