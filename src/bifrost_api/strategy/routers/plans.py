"""`/strategies/plans` — structured trade plans.

A plan is a record of intent, never an instruction: nothing downstream reads
this table to act (D10). Orders are placed in TWS.

The rules are the core module's, not this router's — it maps them to status
codes and passes the reason through, so a refusal reads the same in the UI as
it does in a script:

    404  no such plan
    409  the plan's state says no (with the reason)
    503  Postgres is not configured for writes
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query, Request

from bifrost_core.monitor.reader import strategy_plan as strategy_plan_module
from bifrost_core.monitor.reader.strategy_plan import PlanRuleError
from bifrost_core.monitor.schemas.strategy_plans import (
    PlanCreateBody,
    PlanLinkFillBody,
    PlanUpdateBody,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/strategies", tags=["strategy-plans"])

PLANS_LIMIT_DEFAULT = 200
PLANS_LIMIT_MAX = 500


def _read_config(request: Request) -> Optional[dict]:
    return getattr(request.app.state, "status_cfg_for_read", None)


def _write_config(request: Request) -> dict:
    """The write path needs Postgres; say so plainly when it is absent."""
    control_via_db = getattr(request.app.state, "control_via_db", None)
    if not control_via_db:
        raise HTTPException(status_code=503, detail="Database control not configured")
    return control_via_db


@router.get("/plans")
def list_plans_endpoint(
    request: Request,
    status: Optional[str] = Query(None, description="draft, intended, filled or cancelled"),
    symbol: Optional[str] = Query(None, description="Filter by underlying symbol"),
    account_id: Optional[str] = Query(None, description="Filter by account ID"),
    limit: int = Query(PLANS_LIMIT_DEFAULT, ge=1, le=PLANS_LIMIT_MAX),
) -> Dict[str, Any]:
    """Plans, newest first. `status` filters the stored status; each row also
    carries `effective_status`, where an intent past its expiry reads expired.

    A read that fails is a 500. An empty list means the account has no plans and
    nothing else -- it must never stand in for a query that did not run."""
    try:
        items = strategy_plan_module.list_plans(
            _read_config(request),
            status=status,
            symbol=symbol,
            account_id=account_id,
            limit=limit,
        )
    except Exception as e:
        logger.warning("list_plans failed: %s", e)
        raise HTTPException(status_code=500, detail="Failed to read strategy plans") from e
    return {"items": items, "count": len(items)}


@router.get("/plans/{strategy_plan_id}")
def get_plan_endpoint(request: Request, strategy_plan_id: int) -> Dict[str, Any]:
    """One plan by id. 404 when there is no such row, 500 when the read fails."""
    try:
        row = strategy_plan_module.get_plan(_read_config(request), strategy_plan_id)
    except Exception as e:
        logger.warning("get_plan failed: %s", e)
        raise HTTPException(status_code=500, detail="Failed to read strategy plan") from e
    if row is None:
        raise HTTPException(status_code=404, detail="Strategy plan not found")
    return row


@router.post("/plans")
def create_plan_endpoint(request: Request, body: PlanCreateBody) -> Dict[str, Any]:
    """Write one draft. `intend` is a separate step, on purpose."""
    config = _write_config(request)
    payload = body.model_dump()
    try:
        plan_id = strategy_plan_module.create_plan(config, payload)
    except PlanRuleError as e:
        raise HTTPException(status_code=400, detail=e.reason) from e
    except Exception as e:
        logger.warning("create_plan failed: %s", e)
        raise HTTPException(status_code=500, detail="Failed to create strategy plan") from e
    if plan_id is None:
        raise HTTPException(status_code=503, detail="Database control not configured")
    return {"strategy_plan_id": plan_id}


@router.put("/plans/{strategy_plan_id}")
def update_plan_endpoint(
    request: Request, strategy_plan_id: int, body: PlanUpdateBody
) -> Dict[str, Any]:
    """Edit a draft. 409 once the plan has been marked intended."""
    config = _write_config(request)
    payload = body.model_dump(exclude_unset=True)
    try:
        updated = strategy_plan_module.update_plan(config, strategy_plan_id, payload)
    except PlanRuleError as e:
        raise HTTPException(status_code=409, detail=e.reason) from e
    except Exception as e:
        logger.warning("update_plan failed: %s", e)
        raise HTTPException(status_code=500, detail="Failed to update strategy plan") from e
    if not updated:
        raise HTTPException(status_code=404, detail="Strategy plan not found")
    return {"ok": True, "strategy_plan_id": strategy_plan_id}


@router.post("/plans/{strategy_plan_id}/intend")
def intend_plan_endpoint(request: Request, strategy_plan_id: int) -> Dict[str, Any]:
    """Mark a draft intended. 409 carries what the plan is still missing."""
    config = _write_config(request)
    try:
        moved = strategy_plan_module.intend_plan(config, strategy_plan_id)
    except PlanRuleError as e:
        raise HTTPException(status_code=409, detail=e.reason) from e
    except Exception as e:
        logger.warning("intend_plan failed: %s", e)
        raise HTTPException(status_code=500, detail="Failed to mark plan intended") from e
    if not moved:
        raise HTTPException(status_code=404, detail="Strategy plan not found")
    return {"ok": True, "strategy_plan_id": strategy_plan_id, "status": "intended"}


@router.post("/plans/{strategy_plan_id}/link-fill")
def link_fill_endpoint(
    request: Request, strategy_plan_id: int, body: PlanLinkFillBody
) -> Dict[str, Any]:
    """Say which instance the plan turned into. The fill itself happened in TWS."""
    config = _write_config(request)
    try:
        linked = strategy_plan_module.link_fill(
            config, strategy_plan_id, body.strategy_instance_id
        )
    except PlanRuleError as e:
        raise HTTPException(status_code=409, detail=e.reason) from e
    except Exception as e:
        logger.warning("link_fill failed: %s", e)
        raise HTTPException(status_code=500, detail="Failed to link fill") from e
    if not linked:
        raise HTTPException(status_code=404, detail="Strategy plan not found")
    return {
        "ok": True,
        "strategy_plan_id": strategy_plan_id,
        "strategy_instance_id": body.strategy_instance_id,
        "status": "filled",
    }


@router.post("/plans/{strategy_plan_id}/cancel")
def cancel_plan_endpoint(request: Request, strategy_plan_id: int) -> Dict[str, Any]:
    """Drop a plan that will not be taken. A filled plan stays as it is."""
    config = _write_config(request)
    try:
        cancelled = strategy_plan_module.cancel_plan(config, strategy_plan_id)
    except PlanRuleError as e:
        raise HTTPException(status_code=409, detail=e.reason) from e
    except Exception as e:
        logger.warning("cancel_plan failed: %s", e)
        raise HTTPException(status_code=500, detail="Failed to cancel strategy plan") from e
    if not cancelled:
        raise HTTPException(status_code=404, detail="Strategy plan not found")
    return {"ok": True, "strategy_plan_id": strategy_plan_id, "status": "cancelled"}
