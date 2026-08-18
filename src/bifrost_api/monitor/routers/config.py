"""Config: IB, active-strategy (Flex config write lives in Flex Query Plugin)."""

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Body, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict

from bifrost_core.monitor.reader import (
    write_ib_config,
)
from bifrost_core.monitor.reader.ib_config_public import ib_client_for_api
from bifrost_core.monitor.reader.settings import write_active_strategy_and_gates

logger = logging.getLogger(__name__)

router = APIRouter(tags=["config"])


class IbConfigBody(BaseModel):
    """POST /config/ib body: account/stream IDs only. IB host, port, client IDs live in config.yaml."""

    model_config = ConfigDict(extra="ignore")

    ib_host_account_id: Optional[str] = None
    stream_host_account_id: Optional[str] = None
    stream_secondary_account_id: Optional[str] = None


class ActiveStrategyBody(BaseModel):
    """POST /config/active-strategy body: active_strategy_structure_id, active_gate_safety_strategy_id, active_strategy_allocation_id (null to clear)."""
    active_strategy_structure_id: Optional[int] = None
    active_gate_safety_strategy_id: Optional[int] = None
    active_strategy_allocation_id: Optional[int] = None

    class Config:
        extra = "ignore"


def _optional_account_field(
    body: IbConfigBody,
    field: str,
    current: Dict[str, Any],
) -> Optional[str]:
    fs = getattr(body, "model_fields_set", None) or getattr(body, "__fields_set__", None) or set()
    if field not in fs:
        v = current.get(field)
        if v is None:
            return None
        s = str(v).strip()
        return s or None
    v = getattr(body, field, None)
    if v is None:
        return None
    s = str(v).strip()
    return s or None


@router.post("/config/ib")
def post_config_ib(request: Request, body: IbConfigBody = Body(...)) -> JSONResponse:
    """Persist ib_host_account_id and stream account IDs. IB host/port/client IDs come from config.yaml only."""
    control_via_db = request.app.state.control_via_db
    reader = request.app.state.reader
    if not control_via_db:
        return JSONResponse(status_code=503, content={"error": "control via DB not available (postgres required)"})
    current = reader.get_ib_config() or {}

    host_id = _optional_account_field(body, "ib_host_account_id", current)
    stream_host_id = _optional_account_field(body, "stream_host_account_id", current)
    stream_secondary_id = _optional_account_field(body, "stream_secondary_account_id", current)

    logger.info("[config/ib] writing settings: host_account_id=%r stream_host=%r stream_secondary=%r", host_id, stream_host_id, stream_secondary_id)
    if write_ib_config(control_via_db, host_id, stream_host_id, stream_secondary_id):
        merged = reader.get_ib_config() or {}
        out: Dict[str, Any] = {"ok": True, **ib_client_for_api(merged)}
        return JSONResponse(status_code=200, content=out)
    return JSONResponse(status_code=500, content={"error": "failed to write settings"})


@router.post("/config/active-strategy")
def post_config_active_strategy(request: Request, body: ActiveStrategyBody = Body(...)) -> JSONResponse:
    """Update settings: active_strategy_structure_id, active_gate_safety_strategy_id, active_strategy_allocation_id (null to clear). Daemon uses these on next start when loading gates from DB."""
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return JSONResponse(status_code=503, content={"error": "control via DB not available (postgres required)"})
    if write_active_strategy_and_gates(
        control_via_db,
        active_strategy_structure_id=body.active_strategy_structure_id,
        active_gate_safety_strategy_id=body.active_gate_safety_strategy_id,
        active_strategy_allocation_id=body.active_strategy_allocation_id,
    ):
        return JSONResponse(
            status_code=200,
            content={
                "ok": True,
                "active_strategy_structure_id": body.active_strategy_structure_id,
                "active_gate_safety_strategy_id": body.active_gate_safety_strategy_id,
                "active_strategy_allocation_id": body.active_strategy_allocation_id,
            },
        )
    return JSONResponse(status_code=500, content={"error": "failed to write active strategy and gates"})
