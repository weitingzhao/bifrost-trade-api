"""Daemon control: POST /control/* (stop, flatten, suspend, resume, retry_ib, release_ib, refresh_*, set_heartbeat_interval, monitor_stop, monitor_release_ib, celery_stop, monitor_connect). Engine has no IB socket; retry_ib/release_ib are legacy queue drains for the daemon."""

import asyncio
import json
import logging
import os
import threading
import time
from typing import Any, Dict, Optional

from fastapi import APIRouter, Body, Request
from fastapi.responses import JSONResponse

from bifrost_core.ib_operator.client import IbOperatorClient
from bifrost_core.monitor.reader import (
    sync_accounts_snapshot_to_db,
    write_account_sync_control,
    write_account_sync_heartbeat_interval,
    write_account_sync_run_status,
    write_control_command,
    write_heartbeat_interval,
    write_run_status,
)

MONITOR_STOP_DISCONNECT_TIMEOUT = 2.5  # seconds; avoid hang if IB disconnect blocks
MONITOR_STOP_EXIT_DELAY = 3.0  # seconds; give client time to receive 200 before process exits

logger = logging.getLogger(__name__)

router = APIRouter(tags=["daemon"])


def _exit_after_send() -> None:
    time.sleep(MONITOR_STOP_EXIT_DELAY)  # give client time to receive response before exit
    logger.info("Monitor stop: exiting process.")
    os._exit(0)


@router.post("/control/monitor_stop")
async def post_monitor_stop(request: Request) -> JSONResponse:
    """Stop monitor-side IB activity AND terminate the monitor process itself.
    Disconnects are wrapped in a short timeout so a stuck IB API cannot block the response;
    disconnects run concurrently so total wait is one timeout, not three; exit is delayed
    so the client can receive 200 before the process exits."""
    app = request.app
    app.state.monitor_enabled = False

    op_client: Optional[IbOperatorClient] = getattr(app.state, "ib_operator_client", None)
    if op_client is not None:
        try:
            await asyncio.wait_for(
                op_client.request_async("disconnect_all", {}, caller="monitor_stop"),
                timeout=MONITOR_STOP_DISCONNECT_TIMEOUT,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "monitor_stop: IB Operator disconnect_all timed out after %.1fs; process will exit anyway.",
                MONITOR_STOP_DISCONNECT_TIMEOUT,
            )
        except Exception as e:
            logger.debug("monitor_stop: operator disconnect_all error (ignored): %s", e)

    threading.Thread(target=_exit_after_send, daemon=True).start()
    return JSONResponse(status_code=200, content={"ok": True, "monitor_enabled": False})


@router.post("/control/monitor_release_ib")
async def post_monitor_release_ib(request: Request) -> JSONResponse:
    """Release IB Operator TWS connections (Host operator + Secondary account if configured). Monitor process keeps running."""
    app = request.app
    op_client: Optional[IbOperatorClient] = getattr(app.state, "ib_operator_client", None)
    if op_client is None:
        return JSONResponse(
            status_code=503,
            content={"ok": False, "error": "IB Operator client not configured (Redis / ib_operator)."},
        )
    try:
        env = await op_client.request_async("disconnect_all", {}, caller="monitor_release_ib")
        if not env.get("ok"):
            return JSONResponse(
                status_code=500,
                content={"ok": False, "error": env.get("error") or "disconnect_all failed"},
            )
    except Exception as e:
        logger.warning("monitor_release_ib operator: %s", e)
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})
    return JSONResponse(status_code=200, content={"ok": True, "message": "IB Operator connections released."})


CELERY_STOP_REDIS_TIMEOUT = 5  # seconds; avoid hang if Redis is unreachable

@router.post("/control/celery_stop")
def post_celery_stop() -> JSONResponse:
    """Set Redis key so Celery worker exits. Worker polls every 2s; process will terminate shortly after.
    Uses a short Redis timeout so the request does not hang if the broker is unreachable."""
    try:
        import redis
        from bifrost_worker.celery.celery_app import (
            WORKER_IB_STATUS_KEY,
            WORKER_IB_STATUS_TTL_SEC,
            WORKER_STOP_REQUESTED_KEY,
            broker_url,
        )
        r = redis.from_url(
            broker_url,
            socket_connect_timeout=CELERY_STOP_REDIS_TIMEOUT,
            socket_timeout=CELERY_STOP_REDIS_TIMEOUT,
        )
        r.set(WORKER_STOP_REQUESTED_KEY, "1")
        r.setex(
            WORKER_IB_STATUS_KEY,
            WORKER_IB_STATUS_TTL_SEC,
            json.dumps({"connected": False, "client_id": 0}),
        )
        return JSONResponse(
            status_code=200,
            content={"ok": True, "message": "Celery worker stop requested; process will exit within a few seconds."},
        )
    except Exception as e:
        logger.warning("celery_stop failed: %s", e)
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@router.post("/control/monitor_connect")
async def post_monitor_connect(request: Request) -> JSONResponse:
    """Reconnect IB Operator (host operator + secondary account if configured). On success, /status socket.ib_operator reflects health."""
    app = request.app
    if not getattr(app.state, "monitor_enabled", True):
        return JSONResponse(status_code=400, content={"ok": False, "error": "Monitor stopped; cannot connect IB."})
    op_client: Optional[IbOperatorClient] = getattr(app.state, "ib_operator_client", None)
    if op_client is None:
        return JSONResponse(
            status_code=503,
            content={"ok": False, "error": "IB Operator client not configured (Redis / ib_operator)."},
        )
    try:
        env = await op_client.request_async("reconnect_all", {}, caller="monitor_connect")
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})
    if not env.get("ok"):
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": env.get("error") or "reconnect_all failed"},
        )
    h = env.get("data") or {}
    host_slot = h.get("host") if isinstance(h.get("host"), dict) else {}
    if not host_slot and isinstance(h.get("operator"), dict):
        host_slot = h["operator"]
    sec = h.get("secondary")
    if sec is None:
        sec = h.get("account2")
    host_ok = bool(host_slot.get("connected"))
    if sec is None:
        secondary_requested = False
        secondary_ok = None
        secondary_err = None
    else:
        secondary_requested = True
        secondary_ok = bool(sec.get("connected")) if isinstance(sec, dict) else False
        secondary_err = sec.get("last_error") if isinstance(sec, dict) else None
    ok = host_ok and (not secondary_requested or bool(secondary_ok))
    status_code = 200 if ok else 500
    return JSONResponse(
        status_code=status_code,
        content={
            "ok": ok,
            "host": {"requested": True, "success": host_ok, "error": host_slot.get("last_error")},
            "secondary": {
                "requested": secondary_requested,
                "success": secondary_ok,
                "error": secondary_err,
            },
        },
    )


@router.post("/control/stop")
def post_control_stop(request: Request) -> JSONResponse:
    """Publish 'stop' to Redis control stream; daemon will request_stop() on next poll (R-C1b)."""
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return JSONResponse(status_code=503, content={"error": "control not available (config required)"})
    if write_control_command(control_via_db, "stop"):
        return JSONResponse(status_code=200, content={"ok": True, "message": "stop written to Redis control stream"})
    return JSONResponse(status_code=500, content={"error": "failed to write control command"})


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


@router.post("/control/retry_ib")
def post_control_retry_ib(request: Request) -> JSONResponse:
    """Publish 'retry_ib' to Redis control stream; daemon consumes it as a legacy no-op (no in-process IB)."""
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return JSONResponse(status_code=503, content={"error": "control not available (config required)"})
    if write_control_command(control_via_db, "retry_ib"):
        return JSONResponse(
            status_code=200,
            content={"ok": True, "message": "retry_ib written (daemon clears queue; no IB reconnect)"},
        )
    return JSONResponse(status_code=500, content={"error": "failed to write control command"})


@router.post("/control/release_ib")
def post_control_release_ib(request: Request) -> JSONResponse:
    """Publish 'release_ib' to Redis control stream; daemon consumes it as a legacy no-op (no IB in daemon)."""
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return JSONResponse(status_code=503, content={"error": "control not available (config required)"})
    if write_control_command(control_via_db, "release_ib"):
        return JSONResponse(
            status_code=200,
            content={"ok": True, "message": "release_ib written (daemon clears queue; use monitor_release_ib for Operator)"},
        )
    return JSONResponse(status_code=500, content={"error": "failed to write control command"})


@router.post("/control/refresh_accounts")
async def post_control_refresh_accounts(request: Request) -> JSONResponse:
    """Fetch accounts/positions from IB via monitor AccountIbClient(s) and write to DB."""
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return JSONResponse(status_code=503, content={"error": "control not available (config required)"})
    op_client: Optional[IbOperatorClient] = getattr(request.app.state, "ib_operator_client", None)
    if op_client is None:
        return JSONResponse(
            status_code=503,
            content={"error": "IB Operator client not configured; start scripts/systemd/run_ib_operator.py and enable Redis / ib_operator."},
        )
    try:
        env = await op_client.request_async(
            "fetch_accounts_snapshot",
            {"account_slot": "primary"},
            caller="refresh_accounts",
        )
        if not env.get("ok"):
            return JSONResponse(
                status_code=500,
                content={"ok": False, "error": env.get("error") or "fetch_accounts_snapshot failed"},
            )
        data = env.get("data") or {}
        accounts_list = list(data.get("accounts") or [])
        from bifrost_core.config.startup import get_effective_ib_config

        try:
            ibc = get_effective_ib_config(request.app.state.reader._config)
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
            return JSONResponse(status_code=500, content={"error": "Failed to write account data to DB; try again later."})
        return JSONResponse(
            status_code=200,
            content={"ok": True, "message": "Accounts/positions fetched from IB via monitor and written to DB."},
        )
    except Exception as e:
        logger.warning("refresh_accounts via IB Operator failed: %s", e, exc_info=True)
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@router.post("/control/refresh_replay")
def post_control_refresh_replay(request: Request) -> JSONResponse:
    """Publish 'refresh_replay' to Redis control stream; daemon will sync executions from IB on next poll."""
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return JSONResponse(status_code=503, content={"error": "control not available (config required)"})
    if write_control_command(control_via_db, "refresh_replay"):
        return JSONResponse(status_code=200, content={"ok": True, "message": "refresh_replay written to Redis control stream"})
    return JSONResponse(status_code=500, content={"error": "failed to write control command"})


@router.post("/control/refresh_ticker_subscriptions")
def post_control_refresh_ticker_subscriptions(request: Request) -> JSONResponse:
    """Publish 'refresh_ticker_subscriptions' to Redis control stream; daemon will Release then Init on next poll."""
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return JSONResponse(status_code=503, content={"error": "control not available (config required)"})
    if write_control_command(control_via_db, "refresh_ticker_subscriptions"):
        return JSONResponse(status_code=200, content={"ok": True, "message": "refresh_ticker_subscriptions written to Redis control stream"})
    return JSONResponse(status_code=500, content={"error": "failed to write control command"})


@router.post("/control/release_ticker_subscriptions")
def post_control_release_ticker_subscriptions(request: Request) -> JSONResponse:
    """Publish 'release_ticker_subscriptions' to Redis control stream; daemon will unsubscribe all tickers on next poll."""
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return JSONResponse(status_code=503, content={"error": "control not available (config required)"})
    if write_control_command(control_via_db, "release_ticker_subscriptions"):
        return JSONResponse(status_code=200, content={"ok": True, "message": "release_ticker_subscriptions written to Redis control stream"})
    return JSONResponse(status_code=500, content={"error": "failed to write control command"})


@router.post("/control/init_ticker_subscriptions")
def post_control_init_ticker_subscriptions(request: Request) -> JSONResponse:
    """Publish 'init_ticker_subscriptions' to Redis control stream; daemon will subscribe to watchlist+positions if none subscribed, else set last_control_message."""
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return JSONResponse(status_code=503, content={"error": "control not available (config required)"})
    if write_control_command(control_via_db, "init_ticker_subscriptions"):
        return JSONResponse(status_code=200, content={"ok": True, "message": "init_ticker_subscriptions written to Redis control stream"})
    return JSONResponse(status_code=500, content={"error": "failed to write control command"})


@router.post("/control/set_heartbeat_interval")
def post_set_heartbeat_interval(request: Request, body: Dict[str, Any] = Body(...)) -> JSONResponse:
    """Set Redis trading state heartbeat_interval_sec (5–120). Daemon polls and uses this on next heartbeat."""
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return JSONResponse(status_code=503, content={"error": "control not available (config required)"})
    sec = body.get("heartbeat_interval_sec")
    if sec is None:
        return JSONResponse(status_code=400, content={"error": "heartbeat_interval_sec required (5–120)"})
    try:
        sec = int(sec)
    except (TypeError, ValueError):
        return JSONResponse(status_code=400, content={"error": "heartbeat_interval_sec must be an integer"})
    if write_heartbeat_interval(control_via_db, sec):
        return JSONResponse(status_code=200, content={"ok": True, "heartbeat_interval_sec": max(5, min(120, sec))})
    return JSONResponse(status_code=500, content={"error": "failed to set heartbeat interval"})


# ---------------------------------------------------------------------------
# Account Sync Daemon control
# ---------------------------------------------------------------------------


@router.post("/account-sync/control/suspend")
def post_account_sync_suspend(request: Request) -> JSONResponse:
    """Suspend Account Sync Daemon (Redis account_sync state suspended=true)."""
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return JSONResponse(status_code=503, content={"error": "control not available (config required)"})
    if write_account_sync_run_status(control_via_db, suspended=True):
        return JSONResponse(status_code=200, content={"ok": True, "message": "Account Sync Daemon suspended"})
    return JSONResponse(status_code=500, content={"error": "failed to set account sync run status"})


@router.post("/account-sync/control/resume")
def post_account_sync_resume(request: Request) -> JSONResponse:
    """Resume Account Sync Daemon (Redis account_sync state suspended=false)."""
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return JSONResponse(status_code=503, content={"error": "control not available (config required)"})
    if write_account_sync_run_status(control_via_db, suspended=False):
        return JSONResponse(status_code=200, content={"ok": True, "message": "Account Sync Daemon resumed"})
    return JSONResponse(status_code=500, content={"error": "failed to set account sync run status"})


@router.post("/account-sync/control/stop")
def post_account_sync_stop(request: Request) -> JSONResponse:
    """Publish 'stop' to Redis account-sync control stream."""
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return JSONResponse(status_code=503, content={"error": "control not available (config required)"})
    if write_account_sync_control(control_via_db, "stop"):
        return JSONResponse(status_code=200, content={"ok": True, "message": "stop written to Redis account-sync control stream"})
    return JSONResponse(status_code=500, content={"error": "failed to write account sync control command"})


@router.post("/account-sync/control/force-sync")
def post_account_sync_force_sync(request: Request) -> JSONResponse:
    """Publish 'force_sync' to Redis account-sync control stream (clears diff cache, forces full write)."""
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return JSONResponse(status_code=503, content={"error": "control not available (config required)"})
    if write_account_sync_control(control_via_db, "force_sync"):
        return JSONResponse(status_code=200, content={"ok": True, "message": "force_sync written to Redis account-sync control stream"})
    return JSONResponse(status_code=500, content={"error": "failed to write account sync control command"})


@router.post("/account-sync/control/set_heartbeat_interval")
def post_account_sync_set_heartbeat_interval(request: Request, body: Dict[str, Any] = Body(...)) -> JSONResponse:
    """Set Redis account_sync heartbeat_interval_sec (2–60)."""
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return JSONResponse(status_code=503, content={"error": "control not available (config required)"})
    sec = body.get("heartbeat_interval_sec")
    if sec is None:
        return JSONResponse(status_code=400, content={"error": "heartbeat_interval_sec required (2–60)"})
    try:
        sec = float(sec)
    except (TypeError, ValueError):
        return JSONResponse(status_code=400, content={"error": "heartbeat_interval_sec must be a number"})
    if write_account_sync_heartbeat_interval(control_via_db, sec):
        return JSONResponse(status_code=200, content={"ok": True, "heartbeat_interval_sec": max(2.0, min(60.0, sec))})
    return JSONResponse(status_code=500, content={"error": "failed to set account sync heartbeat interval"})
