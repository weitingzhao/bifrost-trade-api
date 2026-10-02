"""Phase A: Strategy structures API for management and monitoring."""

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request

from bifrost_api.strategy.deps import write_config
from bifrost_core.monitor.reader import gate_safety_write as gate_safety_write_module
from bifrost_core.monitor.reader import strategy_allocation_write as strategy_allocation_write_module
from bifrost_core.monitor.reader import strategy_opportunity_write as strategy_opportunity_write_module
from bifrost_core.monitor.reader import strategy_structure_write as strategy_structure_write_module
from bifrost_core.monitor.reader import strategy_rules_delete as strategy_rules_delete_module
from bifrost_core.monitor.reader.strategy_rules_delete import RuleInUseError
from bifrost_core.monitor.reader import template_config_write as template_config_write_module
from bifrost_core.monitor.schemas.gate_params import default_gates
from bifrost_core.monitor.schemas.strategies import (
    AllocationBody,
    AllocationUpdateBody,
    OpportunityBody,
    OpportunityUpdateBody,
    StrategyInstanceCreateBody,
    StrategyInstanceUpdateBody,
)
from bifrost_core.monitor.services import option_strategy_templates
from bifrost_core.monitor.services.strategy_parsing import (
    parse_opened_at_to_unix,
    parse_optional_timestamp,
    parse_strategy_instance_ids_csv,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/strategies", tags=["strategies"])


@router.get("/dims")
def list_dims_grouped_endpoint(request: Request) -> Dict[str, Any]:
    """Dimension rows grouped by dim type, from core's dim catalog. Read-only: the
    dims POST / PUT / DELETE routes only ever raised and went with core's writers (TD-58)."""
    reader = request.app.state.reader
    return {"by_type": reader.list_dims_grouped()}


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
    return {"items": reader.list_templates(active_only=active_only)}


@router.get("/templates/{template_id}")
def get_template_detail_endpoint(request: Request, template_id: int) -> Dict[str, Any]:
    reader = request.app.state.reader
    row = reader.get_template_detail(template_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Template not found")
    return row


@router.post("/templates")
def create_template_endpoint(request: Request, body: Dict[str, Any]) -> Dict[str, Any]:
    config = write_config(request)
    try:
        tid = template_config_write_module.create_template(config, body)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return {"strategy_template_id": tid}


@router.put("/templates/{template_id}")
def update_template_endpoint(
    request: Request, template_id: int, body: Dict[str, Any]
) -> Dict[str, Any]:
    config = write_config(request)
    try:
        ok = template_config_write_module.update_template(config, template_id, body)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    if not ok:
        raise HTTPException(status_code=404, detail="Template not found")
    return {"ok": True}


@router.delete("/templates/{template_id}")
def delete_template_endpoint(request: Request, template_id: int) -> Dict[str, Any]:
    config = write_config(request)
    try:
        template_config_write_module.delete_template(config, template_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return {"ok": True}


@router.put("/templates/{template_id}/legs")
def replace_template_legs_endpoint(
    request: Request, template_id: int, body: Dict[str, Any]
) -> Dict[str, Any]:
    config = write_config(request)
    legs = body.get("legs")
    if not isinstance(legs, list):
        raise HTTPException(status_code=400, detail="legs array is required")
    try:
        template_config_write_module.replace_template_legs(config, template_id, legs)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return {"ok": True}


@router.put("/templates/{template_id}/params")
def replace_template_params_endpoint(
    request: Request, template_id: int, body: Dict[str, Any]
) -> Dict[str, Any]:
    config = write_config(request)
    items = body.get("items")
    if not isinstance(items, list):
        raise HTTPException(status_code=400, detail="items must be an array")
    try:
        template_config_write_module.replace_template_params(config, template_id, items)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return {"ok": True}


@router.put("/templates/{template_id}/characteristics")
def replace_template_characteristics_endpoint(
    request: Request, template_id: int, body: Dict[str, Any]
) -> Dict[str, Any]:
    config = write_config(request)
    items = body.get("items")
    if items is not None and not isinstance(items, list):
        raise HTTPException(status_code=400, detail="items must be an array of strings")
    try:
        template_config_write_module.replace_template_characteristics(
            config, template_id, [str(x) for x in (items or [])]
        )
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
    return {"items": items}


@router.get("/structures/{structure_id}")
def get_structure(request: Request, structure_id: int) -> Dict[str, Any]:
    """Return one strategy_structure row by id. 404 if not found."""
    reader = request.app.state.reader
    row = reader.get_structure_by_id(structure_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Structure not found")
    return row


@router.post("/structures")
def create_structure_endpoint(request: Request, body: Dict[str, Any]) -> Dict[str, Any]:
    """Create a new strategy structure.

    Body: name, structure_type, legs (array), optional version, is_active, meta (array of {meta_key, meta_value_text}).
    Per leg: quantity = ratio per leg (structural); strike and expiration are optional presets
    (null/blank = resolve when structure is applied, e.g. ATM or DTE).
    """
    control_via_db = write_config(request)
    try:
        sid = strategy_structure_write_module.create_structure(control_via_db, body)
    except (ValueError, TypeError) as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    if sid is None:
        raise HTTPException(status_code=500, detail="Failed to create structure")
    return {"strategy_structure_id": sid}


@router.put("/structures/{structure_id}")
def update_structure_endpoint(request: Request, structure_id: int, body: Dict[str, Any]) -> Dict[str, Any]:
    """Update an existing strategy structure. Body same as POST (legs: quantity=ratio, strike/expiration=optional preset)."""
    control_via_db = write_config(request)
    try:
        ok = strategy_structure_write_module.update_structure(control_via_db, structure_id, body)
    except (ValueError, TypeError) as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    if not ok:
        raise HTTPException(status_code=404, detail="Structure not found or update failed")
    return {"ok": True}


@router.delete("/structures/{structure_id}")
def delete_structure_endpoint(request: Request, structure_id: int) -> Dict[str, Any]:
    """Soft-delete a strategy structure (set is_active = false). Clears settings.active_strategy_structure_id if it pointed to this structure."""
    control_via_db = write_config(request)
    try:
        ok = strategy_structure_write_module.deactivate_structure(control_via_db, structure_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    if not ok:
        raise HTTPException(status_code=404, detail="Structure not found")
    return {"ok": True}


@router.get("/opportunities")
def list_opportunities(
    request: Request,
    active_only: bool = Query(True, description="If true, return only active opportunities"),
) -> Dict[str, Any]:
    """Return list of strategy_opportunity rows for management."""
    reader = request.app.state.reader
    items: List[Dict[str, Any]] = reader.list_opportunities(active_only=active_only)
    return {"items": items}


@router.get("/opportunities/{opportunity_id}")
def get_opportunity(request: Request, opportunity_id: int) -> Dict[str, Any]:
    """Return one strategy_opportunity row by id. 404 if not found."""
    reader = request.app.state.reader
    row = reader.get_opportunity_by_id(opportunity_id)
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
    except (ValueError, TypeError) as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    if oid is None:
        raise HTTPException(status_code=500, detail="Failed to create opportunity")
    return {"strategy_opportunity_id": oid}


@router.put("/opportunities/{opportunity_id}")
def update_opportunity_endpoint(request: Request, opportunity_id: int, body: OpportunityUpdateBody) -> Dict[str, Any]:
    """Update an existing strategy opportunity. Body same as POST: name, strategy_structure_id, optional default_gate_safety_strategy_id, scope_type, symbols, entry_conditions, is_active (partial update supported)."""
    control_via_db = write_config(request)
    payload = body.model_dump(exclude_unset=True)
    if "entry_conditions" in payload:
        payload["entry_conditions"] = [c.model_dump() for c in (body.entry_conditions or [])]
    try:
        ok = strategy_opportunity_write_module.update_opportunity(control_via_db, opportunity_id, payload)
    except (ValueError, TypeError) as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    if not ok:
        raise HTTPException(status_code=404, detail="Opportunity not found or update failed")
    return {"ok": True}


@router.get("/win-rate")
def get_strategy_win_rate(
    request: Request,
    since_ts: Optional[float] = Query(None, description="Filter: since Unix timestamp"),
    until_ts: Optional[float] = Query(None, description="Filter: until Unix timestamp"),
) -> Dict[str, Any]:
    """Return per-structure win-rate rows and ``totals_all`` (all instances combined).

    ``total_profit`` = sum of execution-derived Net PnL for instances with strictly positive net (same rule for every structure);
    ``total_loss`` = sum of those nets for instances with net &lt; 0 only (same Net PnL as Instance Detail;
    omitted when no such instance).
    """
    reader = request.app.state.reader
    return reader.get_strategy_win_rate(since_ts=since_ts, until_ts=until_ts)


@router.get("/instances")
def list_strategy_instances(
    request: Request,
    account_id: Optional[str] = Query(None, description="Filter by account ID"),
    strategy_opportunity_id: Optional[int] = Query(None, description="Filter by strategy opportunity ID"),
    strategy_instance_ids: Optional[str] = Query(
        None,
        description="Comma-separated strategy instance IDs (e.g. 1,2,3)",
    ),
    opened_at_from: Optional[float] = Query(None, description="Filter: opened_at >= (Unix seconds)"),
    opened_at_until: Optional[float] = Query(None, description="Filter: opened_at <= (Unix seconds)"),
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
        opened_at_from=opened_at_from,
        opened_at_until=opened_at_until,
    )
    return {"items": items}


@router.get("/instances/{strategy_instance_id}")
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
    return {"items": legs, "strategy_instance_id": strategy_instance_id}


@router.delete("/instances/{strategy_instance_id}")
def delete_strategy_instance_endpoint(request: Request, strategy_instance_id: int) -> Dict[str, Any]:
    """Delete a strategy instance by id. Fails with 409 if the instance has linked executions."""
    reader = request.app.state.reader
    write_config(request)  # 503 without Postgres; the reader does the write
    row = reader.get_strategy_instance_by_id(strategy_instance_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Strategy instance not found")
    ok = reader.delete_strategy_instance(strategy_instance_id)
    if not ok:
        raise HTTPException(status_code=409, detail="Cannot delete: instance has linked executions or delete failed")
    return {"ok": True}


@router.patch("/instances/{strategy_instance_id}")
def update_strategy_instance_endpoint(
    request: Request, strategy_instance_id: int, body: StrategyInstanceUpdateBody
) -> Dict[str, Any]:
    """Update strategy instance label/notes/created_at/opened_at. Partial update supported."""
    reader = request.app.state.reader
    payload = body.model_dump(exclude_unset=True)
    if not payload:
        return {"ok": True}
    if "created_at" in payload:
        try:
            created_at_val = parse_optional_timestamp(payload.get("created_at"), "created_at")
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
    else:
        created_at_val = payload.get("created_at")
    if "opened_at" in payload:
        try:
            opened_at_val = parse_optional_timestamp(payload.get("opened_at"), "opened_at")
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
    else:
        opened_at_val = payload.get("opened_at")
    ok = reader.update_strategy_instance(
        strategy_instance_id,
        label=payload.get("label"),
        notes=payload.get("notes"),
        created_at=created_at_val,
        opened_at=opened_at_val,
    )
    if not ok:
        raise HTTPException(status_code=404, detail="Strategy instance not found or update failed")
    return {"ok": True}


@router.get("/allocations")
def list_allocations(
    request: Request,
    active_only: bool = Query(True, description="If true, return only active allocations"),
) -> Dict[str, Any]:
    """Return list of strategy_allocation rows for management."""
    reader = request.app.state.reader
    items: List[Dict[str, Any]] = reader.list_allocations(active_only=active_only)
    return {"items": items}


@router.get("/allocations/{allocation_id}")
def get_allocation(request: Request, allocation_id: int) -> Dict[str, Any]:
    """Return one strategy_allocation row by id. 404 if not found."""
    reader = request.app.state.reader
    row = reader.get_allocation_by_id(allocation_id)
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
    except (ValueError, TypeError) as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    if aid is None:
        raise HTTPException(status_code=500, detail="Failed to create allocation")
    return {"strategy_allocation_id": aid}


@router.put("/allocations/{allocation_id}")
def update_allocation_endpoint(request: Request, allocation_id: int, body: AllocationUpdateBody) -> Dict[str, Any]:
    """Update an existing strategy allocation. Partial update supported."""
    control_via_db = write_config(request)
    payload = body.model_dump(exclude_unset=True)
    try:
        ok = strategy_allocation_write_module.update_allocation(control_via_db, allocation_id, payload)
    except (ValueError, TypeError) as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    if not ok:
        raise HTTPException(status_code=404, detail="Allocation not found or update failed")
    return {"ok": True}


@router.get("/gate-safety")
def list_gate_safety(request: Request) -> Dict[str, Any]:
    """Return list of gate_safety_strategy rows for management dropdown."""
    reader = request.app.state.reader
    items = reader.list_gate_safety_sets()
    return {"items": items}


@router.get("/gate-safety/defaults")
def get_gate_safety_defaults() -> Dict[str, Any]:
    """Core's default gates -- what a new gate set starts from (TD-72), so the UI
    keeps no copy of them. Same shape as a gate set's `gates`, without earnings dates.
    Declared before `/gate-safety/{gate_safety_id}` so "defaults" is not read as an id."""
    return {"gates": default_gates()}


@router.get("/gate-safety/{gate_safety_id}")
def get_gate_safety_by_id(request: Request, gate_safety_id: int) -> Dict[str, Any]:
    """Return full gate set for UI edit: metadata + gates + earnings_dates. 404 if not found."""
    reader = request.app.state.reader
    full = reader.get_gate_safety_full_by_id(gate_safety_id)
    if full is None:
        raise HTTPException(status_code=404, detail="Gate safety set not found")
    return full


@router.post("/gate-safety")
def create_gate_safety_endpoint(request: Request, body: Dict[str, Any]) -> Dict[str, Any]:
    """Create a new gate safety set. Body: name, optional version/structure_type/is_active, gates, optional earnings_dates."""
    control_via_db = write_config(request)
    name = (body.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name is required")
    try:
        gid = gate_safety_write_module.create_gate_safety(control_via_db, body)
    except (ValueError, TypeError) as e:
        raise HTTPException(status_code=400, detail=f"Invalid payload: {e}") from e
    if gid is None:
        raise HTTPException(status_code=500, detail="Failed to create gate safety set")
    return {"gate_safety_strategy_id": gid}


@router.put("/gate-safety/{gate_safety_id}")
def update_gate_safety_endpoint(request: Request, gate_safety_id: int, body: Dict[str, Any]) -> Dict[str, Any]:
    """Update an existing gate safety set. Body same as POST."""
    control_via_db = write_config(request)
    name = (body.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name is required")
    try:
        ok = gate_safety_write_module.update_gate_safety(control_via_db, gate_safety_id, body)
    except (ValueError, TypeError) as e:
        raise HTTPException(status_code=400, detail=f"Invalid payload: {e}") from e
    if not ok:
        raise HTTPException(status_code=404, detail="Gate safety set not found or update failed")
    return {"ok": True}


def _delete_rule(request: Request, fn, row_id: int, what: str) -> Dict[str, Any]:
    """Delete one Desk rule object: 409 with the reason while it is in use, 404
    when absent. The Desk calls this only once its Undo toast has closed (design
    Rev .140), so it is final."""
    control_via_db = write_config(request)
    try:
        ok = fn(control_via_db, row_id)
    except RuleInUseError as e:
        raise HTTPException(status_code=409, detail=e.reason) from e
    except Exception as e:
        logger.warning("delete %s failed: %s", what, e)
        raise HTTPException(status_code=500, detail=f"Failed to delete {what}") from e
    if not ok:
        raise HTTPException(status_code=404, detail=f"{what[:1].upper()}{what[1:]} not found")
    return {"ok": True}


@router.delete("/opportunities/{opportunity_id}")
def delete_opportunity_endpoint(request: Request, opportunity_id: int) -> Dict[str, Any]:
    """Delete an opportunity with no trades; its allocation memberships go with it."""
    return _delete_rule(request, strategy_rules_delete_module.delete_opportunity, opportunity_id, "opportunity")


@router.delete("/allocations/{allocation_id}")
def delete_allocation_endpoint(request: Request, allocation_id: int) -> Dict[str, Any]:
    """Delete an allocation that is not the active one."""
    return _delete_rule(request, strategy_rules_delete_module.delete_allocation, allocation_id, "allocation")


@router.delete("/gate-safety/{gate_safety_id}")
def delete_gate_safety_endpoint(request: Request, gate_safety_id: int) -> Dict[str, Any]:
    """Delete a gate set nothing points at."""
    return _delete_rule(request, strategy_rules_delete_module.delete_gate_safety, gate_safety_id, "gate safety set")
