"""`/gate-sets` -- the gate set under its own name (naming program R1, api 0.7.0; D5-A, D6-A).

A gate set is one set of limit parameters a daemon allocation runs under. Its table keeps
the legacy name ``gate_safety_strategy`` (D6-A: renaming it would touch the daemon's
start-up read during the D10 freeze), so the id keeps the table's name too:
``gate_safety_strategy_id``.

    GET    /gate-sets                              every set
    GET    /gate-sets/defaults                     core's default gates (TD-72)
    GET    /gate-sets/{gate_safety_strategy_id}    one set with gates + earnings_dates (404)
    POST   /gate-sets                              create
    PUT    /gate-sets/{gate_safety_strategy_id}    replace
    PATCH  /gate-sets/{gate_safety_strategy_id}    change the fields sent
    DELETE /gate-sets/{gate_safety_strategy_id}    strict delete (409 while in use)

The old ``/strategies/gate-safety…`` routes answered the same from api 0.7.0 and went in
naming R4 (api 0.9.0); the handlers moved here from ``routers.strategies``.
"""

from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request

from bifrost_api.common.envelopes import list_body
from bifrost_api.common.write_errors import deleted_body, write_target
from bifrost_api.strategy.deps import write_config
from bifrost_api.strategy.patch_bodies import GateSetPatch
from bifrost_api.strategy.schemas.requests import GateSetBody
from bifrost_api.strategy.schemas.responses import GateSetDetail, GateSetList
from bifrost_core.monitor.reader import gate_safety_write as gate_safety_write_module
from bifrost_core.monitor.reader import strategy_rules_delete as strategy_rules_delete_module
from bifrost_core.monitor.schemas.gate_params import default_gates

router = APIRouter(tags=["gate-sets"])

_ID = "/gate-sets/{gate_safety_strategy_id:int}"


@router.get("/gate-sets", response_model=GateSetList, response_model_exclude_unset=True)
def list_gate_safety(request: Request) -> Dict[str, Any]:
    """Return list of gate_safety_strategy rows for management dropdown."""
    reader = request.app.state.reader
    items = reader.list_gate_safety_sets()
    return list_body(items)


# Before the id route: "defaults" is not an id.
@router.get("/gate-sets/defaults")
def get_gate_safety_defaults() -> Dict[str, Any]:
    """Core's default gates -- what a new gate set starts from (TD-72), so the UI
    keeps no copy of them. Same shape as a gate set's `gates`, without earnings dates."""
    return {"gates": default_gates()}


@router.get(_ID, response_model=GateSetDetail, response_model_exclude_unset=True)
def get_gate_safety_by_id(request: Request, gate_safety_strategy_id: int) -> Dict[str, Any]:
    """Return full gate set for UI edit: metadata + gates + earnings_dates. 404 if not found."""
    reader = request.app.state.reader
    full = reader.get_gate_safety_full_by_id(gate_safety_strategy_id)
    if full is None:
        raise HTTPException(status_code=404, detail="Gate safety set not found")
    return full


@router.post("/gate-sets")
def create_gate_safety_endpoint(request: Request, body: GateSetBody) -> Dict[str, Any]:
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


@router.put(_ID)
def update_gate_safety_endpoint(request: Request, gate_safety_strategy_id: int, body: GateSetBody) -> Dict[str, Any]:
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


@router.patch(_ID, response_model=GateSetDetail, response_model_exclude_unset=True)
def patch_gate_safety_endpoint(request: Request, gate_safety_strategy_id: int, body: GateSetPatch) -> Dict[str, Any]:
    """Change the fields sent; answer the set as GET /gate-sets/{id} does. `gates` is a
    partial object deep-merged into the stored gates; `earnings_dates` replaces the list."""
    config = write_target(request, f"gate safety set {gate_safety_strategy_id}")
    return gate_safety_write_module.patch_gate_safety(config, gate_safety_strategy_id, body.patch_fields())


@router.delete(_ID)
def delete_gate_safety_endpoint(request: Request, gate_safety_strategy_id: int) -> Dict[str, Any]:
    """Hard delete. 409 while an opportunity, an allocation or the daemon's settings use it."""
    config = write_target(request, f"gate safety set {gate_safety_strategy_id}")
    return deleted_body(strategy_rules_delete_module.delete_gate_safety_strict(config, gate_safety_strategy_id))
