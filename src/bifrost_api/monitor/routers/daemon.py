"""The desk's daemon controls: suspend, resume, flatten, refresh accounts.

The other control routes (monitor stop / connect / release IB, daemon stop, retry and
release IB, replay and ticker-subscription refreshes, heartbeat interval) had no caller
and no traffic in the release they were marked deprecated, and are gone (TD-40, api 0.8.0).
Imports name core's canonical modules, not the ``monitor.reader`` / ``config.startup``
re-exports (TD-80 C1-a).
"""

import logging
from typing import Optional

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from bifrost_api.common.envelopes import error_response
from bifrost_core.ib_operator.client import IbOperatorClient
from bifrost_core.monitor.reader.status import write_control_command, write_run_status
from bifrost_core.portfolio.reader.accounts import sync_accounts_snapshot_to_db

logger = logging.getLogger(__name__)

router = APIRouter(tags=["daemon"])

# The desk calls the gateway's "primary" slot "Host".
_SLOT_LABELS = {"primary": "host", "secondary": "secondary"}
# What the IB Gateway answers when a slot has no TWS session (plugin ib_gateway/live.py).
_NOT_CONNECTED = frozenset({"slot_not_connected", "host_not_connected"})


def _gateway_failure(slot: str, error: str) -> str:
    """The desk's reason for a fetch the IB Gateway could not serve; the gateway's code stays at the end."""
    label = _SLOT_LABELS.get(slot, slot)
    if error in _NOT_CONNECTED:
        return (
            f"IB is not connected on the {label} slot: TWS is not accepting API connections. "
            f"Check that TWS is running and logged in, then refresh again. ({error})"
        )
    if error == "timeout_waiting_for_operator":
        return f"IB Gateway did not answer for the {label} slot in time. ({error})"
    if error.startswith(("redis_xadd:", "redis_get:")):
        return f"IB Gateway request queue is not reachable. ({error})"
    return f"IB Gateway could not fetch accounts for the {label} slot. ({error})"


@router.post("/control/flatten")
def post_control_flatten(request: Request) -> JSONResponse:
    """Publish 'flatten' to Redis control stream. R-C3 not implemented in daemon yet; daemon logs and continues."""
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return JSONResponse(status_code=503, content={"error": "control not available (config required)"})
    if write_control_command(control_via_db, "flatten"):
        return JSONResponse(status_code=200, content={"ok": True, "message": "flatten written to Redis control stream (daemon may not implement yet)"})
    return JSONResponse(status_code=500, content={"error": "failed to write control command"})


@router.post("/control/suspend")
def post_control_suspend(request: Request) -> JSONResponse:
    """Set Redis trading state suspended=true; daemon will pause hedging until resume (R-C2-style)."""
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return JSONResponse(status_code=503, content={"error": "control not available (config required)"})
    if write_run_status(control_via_db, suspended=True):
        return JSONResponse(status_code=200, content={"ok": True, "message": "trading suspended (daemon will not hedge until resume)"})
    return JSONResponse(status_code=500, content={"error": "failed to set run status"})


@router.post("/control/resume")
def post_control_resume(request: Request) -> JSONResponse:
    """Set Redis trading state suspended=false; daemon will resume hedging on next heartbeat."""
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return JSONResponse(status_code=503, content={"error": "control not available (config required)"})
    if not write_run_status(control_via_db, suspended=False):
        return JSONResponse(status_code=500, content={"error": "failed to set run status"})
    return JSONResponse(
        status_code=200,
        content={"ok": True, "message": "trading resumed; daemon will leave RUNNING_SUSPENDED on next heartbeat"},
    )


@router.post("/control/refresh_accounts")
async def post_control_refresh_accounts(request: Request) -> JSONResponse:
    """Fetch accounts/positions from IB via the IB Gateway (operator stream) and write to DB.

    503 when control or the gateway client is not configured, or the gateway could not
    serve the primary slot (no TWS session, no answer in time, queue unreachable); the
    ``detail`` says which, with the gateway's code at the end. 500 when the write failed.
    A secondary slot that fails is logged and left out; the primary's accounts are written.
    """
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return error_response(503, "Control is not available (PostgreSQL / Redis config required).")
    op_client: Optional[IbOperatorClient] = getattr(request.app.state, "ib_operator_client", None)
    if op_client is None:
        return error_response(503, "IB Gateway client is not configured (ib_operator or Redis is off).")
    try:
        env = await op_client.request_async(
            "fetch_accounts_snapshot",
            {"account_slot": "primary"},
            caller="refresh_accounts",
        )
        if not env.get("ok"):
            err = str(env.get("error") or "fetch_accounts_snapshot failed")
            logger.warning("refresh_accounts primary: %s", err)
            return error_response(503, _gateway_failure("primary", err))
        data = env.get("data") or {}
        accounts_list = list(data.get("accounts") or [])
        from bifrost_core.config.yaml_config import get_effective_ib_config

        try:
            ibc = get_effective_ib_config(request.app.state.reader.config)
            if (ibc.get("ib2_host") or "").strip():
                env2 = await op_client.request_async(
                    "fetch_accounts_snapshot",
                    {"account_slot": "secondary"},
                    caller="refresh_accounts",
                )
                if env2.get("ok"):
                    d2 = env2.get("data") or {}
                    al2 = list(d2.get("accounts") or [])
                    if al2:
                        accounts_list = (accounts_list or []) + al2
                else:
                    logger.warning("refresh_accounts secondary: %s", env2.get("error"))
        except Exception as e2:
            logger.warning("refresh_accounts secondary check failed: %s", e2)
        if not accounts_list:
            return JSONResponse(
                status_code=200,
                content={"ok": True, "message": "No account data received (IB may not have returned managed accounts)."},
            )
        if not sync_accounts_snapshot_to_db(control_via_db, accounts_list):
            return error_response(500, "Failed to write account data to DB; try again later.")
        return JSONResponse(
            status_code=200,
            content={"ok": True, "message": "Accounts/positions fetched from IB via monitor and written to DB."},
        )
    except Exception as e:
        logger.warning("refresh_accounts via IB Operator failed: %s", e, exc_info=True)
        return error_response(500, f"Refresh accounts failed: {e}")
