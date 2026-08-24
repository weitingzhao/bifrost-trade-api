"""Ops API routes for authentication, shutdown, and audit."""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any, Dict, Optional

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from bifrost_api.ops.models.schemas import AuditEntry

logger = logging.getLogger(__name__)

router = APIRouter(tags=["ops"])

OPS_SHUTDOWN_EXIT_DELAY_SEC = 2.5


def _audit_log(request: Request) -> list:
    return getattr(request.app.state, "audit_log", [])


def _ops_auth(request: Request):
    from bifrost_api.ops.auth import OpsAuth

    return getattr(request.app.state, "ops_auth", OpsAuth.__new__(OpsAuth))


def _identity(request: Request):
    return _ops_auth(request).resolve(request)


def _role(request: Request) -> str:
    return _identity(request).role


def _audit(
    request: Request,
    action: str,
    target: str,
    outcome: str,
    command_id: Optional[str] = None,
    detail: Optional[str] = None,
) -> None:
    ident = _identity(request)
    entry = AuditEntry(
        operator=ident.name,
        source_ip=request.client.host if request.client else None,
        action=action,
        target=target,
        command_id=command_id,
        outcome=outcome,
        detail=detail,
    )
    audit_store = getattr(request.app.state, "audit_store", None)
    if audit_store is not None:
        audit_store.append(entry)
    else:
        _audit_log(request).append(entry)
    logger.info(
        "AUDIT: %s %s -> %s by %s from %s",
        action,
        target,
        outcome,
        ident.name,
        entry.source_ip,
    )


def _require_role(request: Request, minimum: str) -> Optional[JSONResponse]:
    """Return a 403 response when the caller lacks the required role."""
    _, denied = _ops_auth(request).require_role(request, minimum)
    return denied


@router.get("/ops/auth/capabilities")
def auth_capabilities(request: Request) -> Dict[str, Any]:
    return _ops_auth(request).capabilities(request)


@router.post("/ops/shutdown")
def post_ops_shutdown(request: Request) -> Any:
    denied = _require_role(request, "operator")
    if denied:
        _audit(request, "ops_shutdown", "process", "denied", detail=f"role={_role(request)}")
        return denied
    _audit(request, "ops_shutdown", "process", "scheduled", detail="process exit")

    def _exit_after_send() -> None:
        time.sleep(OPS_SHUTDOWN_EXIT_DELAY_SEC)
        logger.info("Ops API shutdown: exiting process.")
        os._exit(0)

    threading.Thread(target=_exit_after_send, daemon=True).start()
    return {"ok": True}
