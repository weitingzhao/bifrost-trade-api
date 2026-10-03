"""PATCH bodies for the strategy routers (TD-15, batch 3b-2).

Every field is optional and only the fields sent change; an explicit ``null``
clears a nullable column (core refuses it on a required one, 400). Unknown
fields and an empty body are 422 (:class:`~bifrost_api.common.write_errors.PatchBody`).
Types are strict -- ``"5"`` is not a number and ``1`` is not ``true`` -- and the
domain rules (lengths, enums, catalog codes, ranges) are core's, answered as 400
with core's reason. The patchable sets mirror core's ``*_PATCHABLE`` tuples.
"""

from __future__ import annotations

from typing import Any, ClassVar, Dict, List, Optional, Union

from pydantic import StrictBool, StrictFloat, StrictInt, StrictStr

from bifrost_api.common.write_errors import PatchBody, RetiredFields
from bifrost_core.monitor.schemas.strategies import StrategyInstanceCreateBody

# Core reads a timestamp as Unix seconds or ISO 8601 (naive = UTC).
Timestamp = Union[StrictFloat, StrictStr]


class TemplatePatch(PatchBody):
    template_code: Optional[StrictStr] = None
    display_name: Optional[StrictStr] = None
    dim_direction: Optional[StrictStr] = None
    dim_structure: Optional[StrictStr] = None
    dim_coverage: Optional[StrictStr] = None
    dim_risk: Optional[StrictStr] = None
    dim_volatility: Optional[StrictStr] = None
    dim_time: Optional[StrictStr] = None
    explanation: Optional[StrictStr] = None
    typical_use: Optional[StrictStr] = None
    example: Optional[StrictStr] = None
    nature: Optional[StrictStr] = None
    sort_order: Optional[StrictInt] = None
    is_active: Optional[StrictBool] = None


class StructurePatch(PatchBody):
    name: Optional[StrictStr] = None
    version: Optional[StrictInt] = None
    is_active: Optional[StrictBool] = None
    notes: Optional[StrictStr] = None
    # [{meta_key, meta_value_text}], replaces the structure's meta whole.
    meta: Optional[List[Dict[str, Any]]] = None


class OpportunityPatch(PatchBody):
    name: Optional[StrictStr] = None
    strategy_structure_id: Optional[StrictInt] = None
    default_gate_safety_strategy_id: Optional[StrictInt] = None
    scope_type: Optional[StrictStr] = None
    is_active: Optional[StrictBool] = None
    symbols: Optional[List[StrictStr]] = None
    # [{condition_type, value_text?, value_numeric?}], replaces the conditions whole.
    entry_conditions: Optional[List[Dict[str, Any]]] = None


class AllocationPatch(PatchBody):
    name: Optional[StrictStr] = None
    gate_safety_strategy_id: Optional[StrictInt] = None
    max_positions: Optional[StrictInt] = None
    max_bp_pct: Optional[StrictFloat] = None
    # The PUT body's {max_positions, max_bp_pct}: keys patched one by one, null clears both.
    allocation_limits: Optional[Dict[str, Any]] = None
    is_active: Optional[StrictBool] = None
    strategy_opportunity_ids: Optional[List[StrictInt]] = None


class GateSetPatch(PatchBody):
    name: Optional[StrictStr] = None
    version: Optional[StrictInt] = None
    dim_direction: Optional[StrictStr] = None
    dim_structure: Optional[StrictStr] = None
    dim_coverage: Optional[StrictStr] = None
    dim_risk: Optional[StrictStr] = None
    dim_volatility: Optional[StrictStr] = None
    dim_time: Optional[StrictStr] = None
    is_active: Optional[StrictBool] = None
    # A partial gates object, deep-merged into the stored gates (core validates the result).
    gates: Optional[Dict[str, Any]] = None
    earnings_dates: Optional[List[StrictStr]] = None


# TD-73: a trade's notes live in the journal only (Research `journal.note`). `notes` on a
# trade and `note` on a review were deprecated in api 0.7.0 and removed in 0.7.1 (core
# 0.43.0 stops naming the columns; an Owner db-step drops them). Sending one is a 422 that
# says where notes go, never an ok that wrote nothing.
def journal_only(field: str) -> str:
    return (
        f"`{field}` was removed in api 0.7.1 (TD-73): a trade's notes live in the Research "
        "journal (research-api POST /research/journal/notes, with a ref of type 'inst'). "
        "Nothing was written; send the request without it."
    )



# The gate set's old class name (D6-A, naming R1); goes in R4.
GateSafetyPatch = GateSetPatch


class TradePatch(PatchBody):
    RETIRED_FIELDS: ClassVar[Dict[str, str]] = {"notes": journal_only("notes")}

    label: Optional[StrictStr] = None
    opened_at: Optional[Timestamp] = None
    created_at: Optional[Timestamp] = None


# The trade's old class name (naming R1); goes in R4.
InstancePatch = TradePatch


class TradeCreate(StrategyInstanceCreateBody, RetiredFields):
    """POST /trades (and /strategies/instances until R4): core's create body, refusing ``notes``."""

    RETIRED_FIELDS: ClassVar[Dict[str, str]] = {"notes": journal_only("notes")}


class PlanPatch(PatchBody):
    """A draft takes any field; an intended plan only ``expires_at`` (409 otherwise).

    ``legs_json`` / ``source_json`` are the names a plan is read with (TD-57, api 0.6.7);
    ``legs`` / ``source`` still work for one release and lose when both are sent."""

    account_id: Optional[StrictStr] = None
    symbol: Optional[StrictStr] = None
    structure_label: Optional[StrictStr] = None
    strategy_structure_id: Optional[StrictInt] = None
    strategy_opportunity_id: Optional[StrictInt] = None
    qty: Optional[StrictInt] = None
    price_effect: Optional[StrictStr] = None
    limit_price: Optional[StrictFloat] = None
    target_kind: Optional[StrictStr] = None
    target_value: Optional[StrictFloat] = None
    stop_kind: Optional[StrictStr] = None
    stop_value: Optional[StrictFloat] = None
    exit_by: Optional[StrictStr] = None
    rationale: Optional[StrictStr] = None
    source_kind: Optional[StrictStr] = None
    source_ref: Optional[StrictStr] = None
    expires_at: Optional[Timestamp] = None
    legs: Optional[List[Dict[str, Any]]] = None
    source: Optional[List[Dict[str, Any]]] = None
    legs_json: Optional[List[Dict[str, Any]]] = None
    source_json: Optional[List[Dict[str, Any]]] = None

    # read name -> the name core's patch_plan takes (PatchBody.patch_fields maps them)
    READ_NAMES: ClassVar[Dict[str, str]] = {"legs_json": "legs", "source_json": "source"}


class ReviewPatch(PatchBody):
    """``tags_added_json`` / ``tags_dropped_json`` are the names a review is read with from
    api 0.7.0 (naming R1; the columns take them in R3); ``tags_added`` / ``tags_dropped``
    still work until R4 and lose when both are sent."""

    tags_added: Optional[List[StrictStr]] = None
    tags_dropped: Optional[List[StrictStr]] = None
    tags_added_json: Optional[List[StrictStr]] = None
    tags_dropped_json: Optional[List[StrictStr]] = None
    reviewed: Optional[StrictBool] = None

    READ_NAMES: ClassVar[Dict[str, str]] = {"tags_added_json": "tags_added", "tags_dropped_json": "tags_dropped"}
    RETIRED_FIELDS: ClassVar[Dict[str, str]] = {"note": journal_only("note")}
