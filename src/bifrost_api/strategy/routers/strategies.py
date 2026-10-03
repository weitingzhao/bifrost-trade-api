"""Phase A: Strategy structures API for management and monitoring."""

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request

from bifrost_api.common.envelopes import list_body
from bifrost_api.common.query_vocab import from_ts_query, to_ts_query
from bifrost_api.common.write_errors import deleted_body, write_target
from bifrost_api.strategy.deps import write_config
from bifrost_api.strategy.patch_bodies import (
    AllocationPatch,
    GateSafetyPatch,
    InstancePatch,
    OpportunityPatch,
    StructurePatch,
    TemplatePatch,
)
from bifrost_api.strategy.schemas.requests import (
    GateSafetyBody,
    StructureBody,
    TemplateBody,
    TemplateCharacteristicsBody,
    TemplateLegsBody,
    TemplateParamsBody,
)
from bifrost_api.strategy.schemas.responses import (
    AllocationList,
    AllocationRow,
    GateSafetyDetail,
    GateSafetyList,
    InstanceList,
    InstanceRow,
    OpportunityDetail,
    OpportunityList,
)
from bifrost_core.monitor.reader import gate_safety_write as gate_safety_write_module
from bifrost_core.monitor.reader.errors import WriteError
from bifrost_core.monitor.reader import strategy_allocation_write as strategy_allocation_write_module
from bifrost_core.monitor.reader import strategy_opportunity_write as strategy_opportunity_write_module
from bifrost_core.monitor.reader import strategy_structure_write as strategy_structure_write_module
from bifrost_core.monitor.reader import strategy_rules_delete as strategy_rules_delete_module
from bifrost_core.monitor.reader import strategy_instance as strategy_instance_module
from bifrost_core.monitor.reader import template_config_write as template_config_write_module
from bifrost_core.monitor.schemas.gate_params import default_gates
from bifrost_core.monitor.schemas.strategies import (
    AllocationBody,
    OpportunityBody,
    StrategyInstanceCreateBody,
)
from bifrost_core.monitor.services import option_strategy_templates
from bifrost_core.monitor.services.strategy_parsing import (
    parse_opened_at_to_unix,
    parse_strategy_instance_ids_csv,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/strategies", tags=["strategies"])

# Bodies and answers (TD-24, batch 3c-1): POST / PUT bodies are typed
# (bifrost_api.strategy.schemas.requests -- strict types, unknown fields ignored and
# logged this release); allocations, opportunities, gate-safety sets and instances
# answer through the response models in bifrost_api.strategy.schemas.responses.
#
# Writes (TD-15, batch 3b-2): PATCH changes only the fields sent and answers the
# row as GET-by-id does; DELETE is strict and answers {"deleted": "hard"|"soft",
# <id>, ..., "ok": true}. Their failures are core's Write* outcomes, mapped once
# in bifrost_api.common.write_errors (404 / 409 / 400 / 503 / 500). The merge-style
# PUTs on templates, opportunities and allocations went in api 0.6.0 (TD-15); the
# PUTs left (structures, gate-safety, template legs / params / characteristics)
# replace on purpose.


@router.get("/dims")
def list_dims_grouped_endpoint(request: Request) -> Dict[str, Any]:
    """Dimension rows grouped by dim type, from core's dim catalog. Read-only: the
    dims POST / PUT / DELETE routes only ever raised and went with core's writers (TD-58)."""
    reader = request.app.state.reader
    by_type = reader.list_dims_grouped()
    # by_column (TD-57, api 0.6.7): the same lists keyed by the column a code is written to
    # (dim_direction ...), the names the template / gate bodies use; by_type keys the
    # bare dim type (direction ...), which a gate form once read with the column name.
    return {"by_type": by_type, "by_column": {f"dim_{t}": rows for t, rows in (by_type or {}).items()}}


@router.get("/templates/options/param-kind")
def template_param_kind_options() -> Dict[str, Any]:
    return option_strategy_templates.param_kind_options_payload()


@router.get("/templates/options/leg-role")
def template_leg_role_options() -> Dict[str, Any]:
    return option_strategy_templates.leg_role_options_payload()


@router.get("/templates/options/leg-direction")
def template_leg_direction_options() -> Dict[str, Any]:
    return option_strategy_templates.leg_direction_options_payload()


@router.get("/templates/options/leg-option-right")
def template_leg_option_right_options() -> Dict[str, Any]:
    return option_strategy_templates.leg_option_right_options_payload()


@router.get("/templates/options/meta-keys")
def template_meta_key_options() -> Dict[str, Any]:
    return option_strategy_templates.meta_key_options_payload("covered_call")


@router.get("/templates/options/meta-values")
def template_meta_value_options(
    meta_key: str = Query(..., description="meta_key"),
) -> Dict[str, Any]:
    return option_strategy_templates.meta_value_options_payload("covered_call", meta_key)


@router.get("/templates")
def list_templates_endpoint(
    request: Request,
    active_only: bool = Query(True),
) -> Dict[str, Any]:
    reader = request.app.state.reader
    return list_body(reader.list_templates(active_only=active_only))


@router.get("/templates/{strategy_template_id}")
def get_template_detail_endpoint(request: Request, strategy_template_id: int) -> Dict[str, Any]:
    reader = request.app.state.reader
    row = reader.get_template_detail(strategy_template_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Template not found")
    return row


@router.post("/templates")
def create_template_endpoint(request: Request, body: TemplateBody) -> Dict[str, Any]:
    config = write_config(request)
    try:
        tid = template_config_write_module.create_template(config, body.declared(exclude_unset=True))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return {"strategy_template_id": tid}


@router.patch("/templates/{strategy_template_id}")
def patch_template_endpoint(request: Request, strategy_template_id: int, body: TemplatePatch) -> Dict[str, Any]:
    """Change the fields sent; answer the template as GET /templates/{id} does.
    Legs, params and characteristics keep their own PUT routes."""
    config = write_target(request, f"template {strategy_template_id}")
    return template_config_write_module.patch_template(config, strategy_template_id, body.patch_fields())


@router.delete("/templates/{strategy_template_id}")
def delete_template_endpoint(request: Request, strategy_template_id: int) -> Dict[str, Any]:
    """Hard delete. 409 naming the structures (deactivated ones too) that still use it."""
    config = write_target(request, f"template {strategy_template_id}")
    return deleted_body(template_config_write_module.delete_template_strict(config, strategy_template_id))


@router.put("/templates/{strategy_template_id}/legs")
def replace_template_legs_endpoint(
    request: Request, strategy_template_id: int, body: TemplateLegsBody
) -> Dict[str, Any]:
    config = write_config(request)
    if body.legs is None:
        raise HTTPException(status_code=400, detail="legs array is required")
    legs = body.declared(exclude_unset=True)["legs"]
    try:
        template_config_write_module.replace_template_legs(config, strategy_template_id, legs)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return {"ok": True}


@router.put("/templates/{strategy_template_id}/params")
def replace_template_params_endpoint(
    request: Request, strategy_template_id: int, body: TemplateParamsBody
) -> Dict[str, Any]:
    config = write_config(request)
    if body.items is None:
        raise HTTPException(status_code=400, detail="items must be an array")
    items = body.declared(exclude_unset=True)["items"]
    try:
        template_config_write_module.replace_template_params(config, strategy_template_id, items)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return {"ok": True}


@router.put("/templates/{strategy_template_id}/characteristics")
def replace_template_characteristics_endpoint(
    request: Request, strategy_template_id: int, body: TemplateCharacteristicsBody
) -> Dict[str, Any]:
    config = write_config(request)
    try:
        template_config_write_module.replace_template_characteristics(config, strategy_template_id, list(body.items or []))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return {"ok": True}


@router.get("/structures")
def list_structures(
    request: Request,
    active_only: bool = Query(True, description="If true, return only active structures"),
) -> Dict[str, Any]:
    """Return list of strategy_structure rows for management dropdown."""
    reader = request.app.state.reader
    items: List[Dict[str, Any]] = reader.list_structures(active_only=active_only)
    return list_body(items)


@router.get("/structures/{strategy_structure_id}")
def get_structure(request: Request, strategy_structure_id: int) -> Dict[str, Any]:
    """Return one strategy_structure row by id. 404 if not found."""
    reader = request.app.state.reader
    row = reader.get_structure_by_id(strategy_structure_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Structure not found")
    return row


@router.post("/structures")
def create_structure_endpoint(request: Request, body: StructureBody) -> Dict[str, Any]:
    """Create a new strategy structure.

    Body: name, structure_type, legs (array), optional version, is_active, meta (array of {meta_key, meta_value_text}).
    Per leg: quantity = ratio per leg (structural); strike and expiration are optional presets
    (null/blank = resolve when structure is applied, e.g. ATM or DTE).
    """
    control_via_db = write_config(request)
    try:
        sid = strategy_structure_write_module.create_structure(control_via_db, body.declared(exclude_unset=True))
    except (ValueError, TypeError) as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    if sid is None:
        raise HTTPException(status_code=500, detail="Failed to create structure")
    return {"strategy_structure_id": sid}


@router.put("/structures/{strategy_structure_id}")
def update_structure_endpoint(request: Request, strategy_structure_id: int, body: StructureBody) -> Dict[str, Any]:
    """Update an existing strategy structure. Body same as POST (legs: quantity=ratio, strike/expiration=optional preset)."""
    control_via_db = write_config(request)
    try:
        ok = strategy_structure_write_module.update_structure(
            control_via_db, strategy_structure_id, body.declared(exclude_unset=True)
        )
    except (ValueError, TypeError) as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    if not ok:
        raise HTTPException(status_code=404, detail="Structure not found or update failed")
    return {"ok": True}


@router.patch("/structures/{strategy_structure_id}")
def patch_structure_endpoint(request: Request, strategy_structure_id: int, body: StructurePatch) -> Dict[str, Any]:
    """Change name / version / is_active / notes / meta; answer the structure as GET does.
    The template and legs are not patchable. `is_active: false` only flips the column."""
    config = write_target(request, f"structure {strategy_structure_id}")
    return strategy_structure_write_module.patch_structure(config, strategy_structure_id, body.patch_fields())


@router.delete("/structures/{strategy_structure_id}")
def delete_structure_endpoint(request: Request, strategy_structure_id: int) -> Dict[str, Any]:
    """Soft delete (is_active = false); clears the daemon's active-structure setting if it
    pointed here. Answers deleted "soft", was_active, cleared_daemon_setting."""
    config = write_target(request, f"structure {strategy_structure_id}")
    return deleted_body(strategy_structure_write_module.delete_structure_strict(config, strategy_structure_id))


@router.get("/opportunities", response_model=OpportunityList, response_model_exclude_unset=True)
def list_opportunities(
    request: Request,
    active_only: bool = Query(True, description="If true, return only active opportunities"),
) -> Dict[str, Any]:
    """Return list of strategy_opportunity rows for management."""
    reader = request.app.state.reader
    items: List[Dict[str, Any]] = reader.list_opportunities(active_only=active_only)
    return list_body(items)


@router.get("/opportunities/{strategy_opportunity_id}", response_model=OpportunityDetail, response_model_exclude_unset=True)
def get_opportunity(request: Request, strategy_opportunity_id: int) -> Dict[str, Any]:
    """Return one strategy_opportunity row by id. 404 if not found."""
    reader = request.app.state.reader
    row = reader.get_opportunity_by_id(strategy_opportunity_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Opportunity not found")
    return row


@router.post("/opportunities")
def create_opportunity_endpoint(request: Request, body: OpportunityBody) -> Dict[str, Any]:
    """Create a new strategy opportunity. Body: name (required), strategy_structure_id (required), optional default_gate_safety_strategy_id, scope_type (e.g. watchlist_stk | explicit_symbols), symbols (array of strings), entry_conditions (array of { condition_type, value_text?, value_numeric? }), is_active."""
    control_via_db = write_config(request)
    payload = body.model_dump()
    payload["entry_conditions"] = [c.model_dump() for c in (body.entry_conditions or [])]
    try:
        oid = strategy_opportunity_write_module.create_opportunity(control_via_db, payload)
    except WriteError:
        raise  # WriteInvalid (a bad limit / gate id, TD-48) -> 400 via common.write_errors
    except (ValueError, TypeError) as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    if oid is None:
        raise HTTPException(status_code=500, detail="Failed to create opportunity")
    return {"strategy_opportunity_id": oid}


@router.patch("/opportunities/{strategy_opportunity_id}", response_model=OpportunityDetail, response_model_exclude_unset=True)
def patch_opportunity_endpoint(request: Request, strategy_opportunity_id: int, body: OpportunityPatch) -> Dict[str, Any]:
    """Change the fields sent (a field left out keeps its value); answer the
    opportunity as GET does. `symbols` / `entry_conditions` replace the list whole."""
    config = write_target(request, f"opportunity {strategy_opportunity_id}")
    return strategy_opportunity_write_module.patch_opportunity(config, strategy_opportunity_id, body.patch_fields())


@router.get("/win-rate")
def get_strategy_win_rate(
    request: Request,
    from_ts: Optional[float] = from_ts_query("the time of the fills counted toward each instance"),
    to_ts: Optional[float] = to_ts_query("the time of the fills counted toward each instance"),
) -> Dict[str, Any]:
    """Return per-structure win-rate rows and ``totals_all`` (all instances combined).

    ``total_profit`` = sum of execution-derived Net PnL for instances with strictly positive net (same rule for every structure);
    ``total_loss`` = sum of those nets for instances with net &lt; 0 only (same Net PnL as Instance Detail;
    omitted when no such instance).
    """
    reader = request.app.state.reader
    return reader.get_strategy_win_rate(since_ts=from_ts, until_ts=to_ts)


@router.get("/instances", response_model=InstanceList, response_model_exclude_unset=True)
def list_strategy_instances(
    request: Request,
    account_id: Optional[str] = Query(None, description="Filter by account ID"),
    strategy_opportunity_id: Optional[int] = Query(None, description="Filter by strategy opportunity ID"),
    strategy_instance_ids: Optional[str] = Query(
        None,
        description="Comma-separated strategy instance IDs (e.g. 1,2,3)",
    ),
    from_ts: Optional[float] = from_ts_query("the instance's opened_at"),
    to_ts: Optional[float] = to_ts_query("the instance's opened_at"),
) -> Dict[str, Any]:
    """Return list of strategy_instance rows (SI.2). Optional filters: account_id, strategy_opportunity_id, strategy_instance_ids, opened_at range."""
    reader = request.app.state.reader
    try:
        ids = parse_strategy_instance_ids_csv(strategy_instance_ids)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    items: List[Dict[str, Any]] = reader.list_strategy_instances(
        account_id=account_id,
        strategy_opportunity_id=strategy_opportunity_id,
        strategy_instance_ids=ids,
        opened_at_from=from_ts,
        opened_at_until=to_ts,
    )
    return list_body(items)


@router.get("/instances/{strategy_instance_id}", response_model=InstanceRow, response_model_exclude_unset=True)
def get_strategy_instance(request: Request, strategy_instance_id: int) -> Dict[str, Any]:
    """Return one strategy_instance by id. 404 if not found."""
    reader = request.app.state.reader
    row = reader.get_strategy_instance_by_id(strategy_instance_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Strategy instance not found")
    return row


@router.post("/instances")
def create_strategy_instance_endpoint(request: Request, body: StrategyInstanceCreateBody) -> Dict[str, Any]:
    """Create a new strategy instance. Body: strategy_opportunity_id, account_id, opened_at (required), label?, notes?. opened_at: ISO 8601 or Unix seconds."""
    reader = request.app.state.reader
    write_config(request)  # 503 without Postgres; the reader does the write
    try:
        opened_at_val = parse_opened_at_to_unix(body.opened_at)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    sid = reader.create_strategy_instance(
        strategy_opportunity_id=body.strategy_opportunity_id,
        account_id=body.account_id.strip(),
        opened_at=opened_at_val,
        label=body.label.strip() if body.label else None,
        notes=body.notes.strip() if body.notes else None,
    )
    if sid is None:
        raise HTTPException(status_code=500, detail="Failed to create strategy instance")
    return {"strategy_instance_id": sid}


@router.get("/instances/{strategy_instance_id}/open-option-legs")
def get_instance_open_option_legs(request: Request, strategy_instance_id: int) -> Dict[str, Any]:
    """Return current open OPT positions linked to this instance (derived from executions intersected with positions)."""
    reader = request.app.state.reader
    legs = reader.get_instance_open_option_legs(strategy_instance_id)
    return list_body(legs, strategy_instance_id=strategy_instance_id)


@router.delete("/instances/{strategy_instance_id}")
def delete_strategy_instance_endpoint(request: Request, strategy_instance_id: int) -> Dict[str, Any]:
    """Hard delete. 409 while executions are split-allocated to it or attributed to it on
    Golden Source; 503 when Golden Source is unreachable, 500 when its check fails --
    nothing is deleted blind.
    Its review goes with it; a plan that pointed at it keeps its text."""
    config = write_target(request, f"strategy instance {strategy_instance_id}")
    return deleted_body(strategy_instance_module.delete_instance_strict(config, strategy_instance_id))


@router.patch("/instances/{strategy_instance_id}", response_model=InstanceRow, response_model_exclude_unset=True)
def update_strategy_instance_endpoint(
    request: Request, strategy_instance_id: int, body: InstancePatch
) -> Dict[str, Any]:
    """Change label / notes / opened_at / created_at; `null` clears label or notes.
    Answers the instance as GET /instances/{id} does."""
    config = write_target(request, f"strategy instance {strategy_instance_id}")
    return strategy_instance_module.patch_instance(config, strategy_instance_id, body.patch_fields())


@router.get("/allocations", response_model=AllocationList, response_model_exclude_unset=True)
def list_allocations(
    request: Request,
    active_only: bool = Query(True, description="If true, return only active allocations"),
) -> Dict[str, Any]:
    """Return list of strategy_allocation rows for management."""
    reader = request.app.state.reader
    items: List[Dict[str, Any]] = reader.list_allocations(active_only=active_only)
    return list_body(items)


@router.get("/allocations/{strategy_allocation_id}", response_model=AllocationRow, response_model_exclude_unset=True)
def get_allocation(request: Request, strategy_allocation_id: int) -> Dict[str, Any]:
    """Return one strategy_allocation row by id. 404 if not found."""
    reader = request.app.state.reader
    row = reader.get_allocation_by_id(strategy_allocation_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Allocation not found")
    return row


@router.post("/allocations")
def create_allocation_endpoint(request: Request, body: AllocationBody) -> Dict[str, Any]:
    """Create a new strategy allocation. Body: name, strategy_opportunity_ids, optional gate_safety_strategy_id, allocation_limits, is_active."""
    control_via_db = write_config(request)
    payload = body.model_dump()
    try:
        aid = strategy_allocation_write_module.create_allocation(control_via_db, payload)
    except WriteError:
        raise  # WriteInvalid (a bad limit / gate id, TD-48) -> 400 via common.write_errors
    except (ValueError, TypeError) as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    if aid is None:
        raise HTTPException(status_code=500, detail="Failed to create allocation")
    return {"strategy_allocation_id": aid}


@router.patch("/allocations/{strategy_allocation_id}", response_model=AllocationRow, response_model_exclude_unset=True)
def patch_allocation_endpoint(request: Request, strategy_allocation_id: int, body: AllocationPatch) -> Dict[str, Any]:
    """Change the fields sent; answer the allocation as GET does. `allocation_limits` keys
    are patched one by one (null clears both); `strategy_opportunity_ids` replaces the membership."""
    config = write_target(request, f"allocation {strategy_allocation_id}")
    return strategy_allocation_write_module.patch_allocation(config, strategy_allocation_id, body.patch_fields())


@router.get("/gate-safety", response_model=GateSafetyList, response_model_exclude_unset=True)
def list_gate_safety(request: Request) -> Dict[str, Any]:
    """Return list of gate_safety_strategy rows for management dropdown."""
    reader = request.app.state.reader
    items = reader.list_gate_safety_sets()
    return list_body(items)


@router.get("/gate-safety/defaults")
def get_gate_safety_defaults() -> Dict[str, Any]:
    """Core's default gates -- what a new gate set starts from (TD-72), so the UI
    keeps no copy of them. Same shape as a gate set's `gates`, without earnings dates.
    Declared before `/gate-safety/{gate_safety_strategy_id}` so "defaults" is not read as an id."""
    return {"gates": default_gates()}


@router.get("/gate-safety/{gate_safety_strategy_id}", response_model=GateSafetyDetail, response_model_exclude_unset=True)
def get_gate_safety_by_id(request: Request, gate_safety_strategy_id: int) -> Dict[str, Any]:
    """Return full gate set for UI edit: metadata + gates + earnings_dates. 404 if not found."""
    reader = request.app.state.reader
    full = reader.get_gate_safety_full_by_id(gate_safety_strategy_id)
    if full is None:
        raise HTTPException(status_code=404, detail="Gate safety set not found")
    return full


@router.post("/gate-safety")
def create_gate_safety_endpoint(request: Request, body: GateSafetyBody) -> Dict[str, Any]:
    """Create a new gate safety set. Body: name, optional version / six dims / is_active, gates, optional earnings_dates."""
    control_via_db = write_config(request)
    if not (body.name or "").strip():
        raise HTTPException(status_code=400, detail="name is required")
    try:
        gid = gate_safety_write_module.create_gate_safety(control_via_db, body.declared(exclude_unset=True))
    except (ValueError, TypeError) as e:
        raise HTTPException(status_code=400, detail=f"Invalid payload: {e}") from e
    if gid is None:
        raise HTTPException(status_code=500, detail="Failed to create gate safety set")
    return {"gate_safety_strategy_id": gid}


@router.put("/gate-safety/{gate_safety_strategy_id}")
def update_gate_safety_endpoint(request: Request, gate_safety_strategy_id: int, body: GateSafetyBody) -> Dict[str, Any]:
    """Update an existing gate safety set. Body same as POST."""
    control_via_db = write_config(request)
    if not (body.name or "").strip():
        raise HTTPException(status_code=400, detail="name is required")
    try:
        ok = gate_safety_write_module.update_gate_safety(
            control_via_db, gate_safety_strategy_id, body.declared(exclude_unset=True)
        )
    except (ValueError, TypeError) as e:
        raise HTTPException(status_code=400, detail=f"Invalid payload: {e}") from e
    if not ok:
        raise HTTPException(status_code=404, detail="Gate safety set not found or update failed")
    return {"ok": True}


@router.patch("/gate-safety/{gate_safety_strategy_id}", response_model=GateSafetyDetail, response_model_exclude_unset=True)
def patch_gate_safety_endpoint(request: Request, gate_safety_strategy_id: int, body: GateSafetyPatch) -> Dict[str, Any]:
    """Change the fields sent; answer the set as GET /gate-safety/{id} does. `gates` is a
    partial object deep-merged into the stored gates; `earnings_dates` replaces the list."""
    config = write_target(request, f"gate safety set {gate_safety_strategy_id}")
    return gate_safety_write_module.patch_gate_safety(config, gate_safety_strategy_id, body.patch_fields())


@router.delete("/opportunities/{strategy_opportunity_id}")
def delete_opportunity_endpoint(request: Request, strategy_opportunity_id: int) -> Dict[str, Any]:
    """Hard delete; its allocation memberships go with it. 409 while it has trades.
    The Desk calls this only once its Undo toast has closed (design Rev .140)."""
    config = write_target(request, f"opportunity {strategy_opportunity_id}")
    return deleted_body(strategy_rules_delete_module.delete_opportunity_strict(config, strategy_opportunity_id))


@router.delete("/allocations/{strategy_allocation_id}")
def delete_allocation_endpoint(request: Request, strategy_allocation_id: int) -> Dict[str, Any]:
    """Hard delete. 409 while the daemon runs it."""
    config = write_target(request, f"allocation {strategy_allocation_id}")
    return deleted_body(strategy_rules_delete_module.delete_allocation_strict(config, strategy_allocation_id))


@router.delete("/gate-safety/{gate_safety_strategy_id}")
def delete_gate_safety_endpoint(request: Request, gate_safety_strategy_id: int) -> Dict[str, Any]:
    """Hard delete. 409 while an opportunity, an allocation or the daemon's settings use it."""
    config = write_target(request, f"gate safety set {gate_safety_strategy_id}")
    return deleted_body(strategy_rules_delete_module.delete_gate_safety_strict(config, gate_safety_strategy_id))
