"""`/strategies/plans` — structured trade plans.

A plan is a record of intent, never an instruction: nothing downstream reads
this table to act (D10). Orders are placed in TWS.

The rules are the core module's, not this router's — it maps them to status
codes and passes the reason through, so a refusal reads the same in the UI as
it does in a script:

    404  no such plan
    409  the plan's state says no (with the reason)
    503  Postgres is not configured for writes

PATCH and DELETE (TD-15, batch 3b-2) raise core's Write* outcomes, mapped in
``bifrost_api.common.write_errors``: input errors are 400 there, the body is
``{detail}``. The merge-style PUT went in api 0.6.0 (TD-15).

A plan is read with ``legs_json`` and ``source_json``, its columns' names. Since api
0.6.7 (TD-57) POST and PATCH take those names too, so a client sends back what it
read; ``legs`` / ``source`` still work for one release and lose to ``legs_json`` /
``source_json`` when a body sends both. Before 0.6.7 a POST with ``legs_json`` wrote a
plan with no legs: the create body ignored unknown fields.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request

from bifrost_api.common.write_errors import deleted_body, write_target
from bifrost_api.strategy.deps import db_not_configured, read_config, write_config
from bifrost_api.strategy.patch_bodies import PlanPatch
from bifrost_api.strategy.schemas.responses import PlanList, PlanRow
from bifrost_core.monitor.reader import strategy_plan as strategy_plan_module
from bifrost_core.monitor.reader.strategy_plan import PlanRuleError
from bifrost_core.monitor.schemas.strategy_plans import (
    PlanCreateBody,
    PlanLeg,
    PlanLinkFillBody,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/strategies", tags=["strategy-plans"])

PLANS_LIMIT_DEFAULT = 200
PLANS_LIMIT_MAX = 500


class PlanCreateRequest(PlanCreateBody):
    """Core's create body, plus the read names ``legs_json`` / ``source_json`` (TD-57)."""

    legs_json: Optional[List[PlanLeg]] = None
    source_json: Optional[List[Dict[str, Any]]] = None

    def payload(self) -> Dict[str, Any]:
        """What core's create_plan takes: ``legs`` / ``source``, the read names folded in."""
        data = self.model_dump(exclude={"legs_json", "source_json"})
        sent = self.model_dump(include={"legs_json", "source_json"}, exclude_none=True)
        if "legs_json" in sent:
            data["legs"] = sent["legs_json"]
        if "source_json" in sent:
            data["source"] = sent["source_json"]
        return data


@router.get("/plans", response_model=PlanList, response_model_exclude_unset=True)
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
            read_config(request),
            status=status,
            symbol=symbol,
            account_id=account_id,
            limit=limit,
        )
    except Exception as e:
        logger.warning("list_plans failed: %s", e)
        raise HTTPException(status_code=500, detail="Failed to read strategy plans") from e
    return {"items": items, "count": len(items)}


@router.get("/plans/{strategy_plan_id}", response_model=PlanRow, response_model_exclude_unset=True)
def get_plan_endpoint(request: Request, strategy_plan_id: int) -> Dict[str, Any]:
    """One plan by id. 404 when there is no such row, 500 when the read fails."""
    try:
        row = strategy_plan_module.get_plan(read_config(request), strategy_plan_id)
    except Exception as e:
        logger.warning("get_plan failed: %s", e)
        raise HTTPException(status_code=500, detail="Failed to read strategy plan") from e
    if row is None:
        raise HTTPException(status_code=404, detail="Strategy plan not found")
    return row


@router.post("/plans")
def create_plan_endpoint(request: Request, body: PlanCreateRequest) -> Dict[str, Any]:
    """Write one draft. `intend` is a separate step, on purpose. The legs and the source
    chain may be sent as `legs_json` / `source_json` (the names a plan is read with)."""
    config = write_config(request)
    payload = body.payload()
    try:
        plan_id = strategy_plan_module.create_plan(config, payload)
    except PlanRuleError as e:
        raise HTTPException(status_code=400, detail=e.reason) from e
    except Exception as e:
        logger.warning("create_plan failed: %s", e)
        raise HTTPException(status_code=500, detail="Failed to create strategy plan") from e
    if plan_id is None:
        raise db_not_configured()
    return {"strategy_plan_id": plan_id}


@router.patch("/plans/{strategy_plan_id}", response_model=PlanRow, response_model_exclude_unset=True)
def patch_plan_endpoint(request: Request, strategy_plan_id: int, body: PlanPatch) -> Dict[str, Any]:
    """Change the fields sent; answer the plan as GET /plans/{id} does.

    A draft takes any field. An intended plan takes `expires_at` only -- the plan
    card's "Extend 7 days" and "Re-issue intent" -- and any other field is 409 with
    the reason; a filled or cancelled plan is 409 for everything."""
    config = write_target(request, f"strategy plan {strategy_plan_id}")
    return strategy_plan_module.patch_plan(config, strategy_plan_id, body.patch_fields())


@router.post("/plans/{strategy_plan_id}/intend")
def intend_plan_endpoint(request: Request, strategy_plan_id: int) -> Dict[str, Any]:
    """Mark a draft intended. 409 carries what the plan is still missing."""
    config = write_config(request)
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
    """Say which trade the plan turned into: body ``{trade_id}`` (``strategy_instance_id`` was
    read the same until naming R4, api 0.9.0). The fill itself happened in TWS."""
    config = write_config(request)
    try:
        linked = strategy_plan_module.link_fill(
            config, strategy_plan_id, body.trade_id
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
        "trade_id": body.trade_id,
        "status": "filled",
    }


@router.post("/plans/{strategy_plan_id}/cancel")
def cancel_plan_endpoint(request: Request, strategy_plan_id: int) -> Dict[str, Any]:
    """Drop a plan that will not be taken. A filled plan stays as it is."""
    config = write_config(request)
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


@router.delete("/plans/{strategy_plan_id}")
def delete_plan_endpoint(request: Request, strategy_plan_id: int) -> Dict[str, Any]:
    """Hard-delete a draft; 409 for anything past draft. The UI calls this only once
    its Undo toast has closed (design Rev .138), so it is final."""
    config = write_target(request, f"strategy plan {strategy_plan_id}")
    return deleted_body(strategy_plan_module.delete_plan_strict(config, strategy_plan_id))
