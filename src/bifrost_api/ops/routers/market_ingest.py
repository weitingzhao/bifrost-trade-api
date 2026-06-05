"""Market data ingest: systemd status + start/stop/restart/reset (whitelisted units)."""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, Body, Request
from fastapi.responses import JSONResponse

from bifrost_api.ops.ib_operator_rpc import ib_operator_disconnect_all_sync
from bifrost_api.ops.market_ingest_config import market_ingest_service_by_id, market_ingest_services_from_config
from bifrost_api.ops.market_ingest_control_env import (
    clear_control_env,
    meta_redis_url_from_ops_config,
    normalize_control_profile,
    read_control_env,
    read_control_host,
    read_control_updated_at,
    write_control_env,
    write_trading_engine_ops_lease,
)
from bifrost_api.ops.market_ingest_health_clear import (
    clear_ingest_health_after_stop,
    ingest_redis_health_looks_live,
)
from bifrost_api.ops.models.schemas import MarketIngestAction, MarketIngestControlRequest
from bifrost_api.ops.routers.workers import _audit, _require_role

logger = logging.getLogger(__name__)

router = APIRouter(tags=["ops-market-ingest"])

_ENSURE_START_STOP_TIMEOUT_SEC = 30
_ENSURE_START_START_TIMEOUT_SEC = 45
_RECENT_CONTROL_WRITE_GRACE_SEC = 120.0


async def _ensure_stop_background(exc: Any, unit: str) -> None:
    """Run ``systemctl stop`` in the background; Redis was already cleaned up by the HTTP handler."""
    try:
        await exc._systemctl("stop", unit, timeout=_ENSURE_START_STOP_TIMEOUT_SEC)  # noqa: SLF001
        logger.info("ensure_stop: %s stopped", unit)
    except Exception as stop_err:
        logger.warning("ensure_stop: stop %s failed: %s", unit, stop_err)


async def _ensure_start_background(
    exc: Any,
    unit: str,
    sid: str,
    rurl: Optional[str],
    meta_key: str,
    ops_profile: Optional[str],
) -> None:
    """Stop any running instance of ``unit`` then start fresh.

    Redis lease is written by the HTTP handler before this task is dispatched, so HOST updates
    on the first frontend poll without waiting for systemctl to complete.
    """
    # Step 1: stop if currently running (best-effort)
    try:
        if hasattr(exc, "systemctl_is_active"):
            active = await exc.systemctl_is_active(unit)
            if active == "active":
                logger.info("ensure_start: %s is active — stopping before re-start", unit)
                try:
                    await exc._systemctl("stop", unit, timeout=_ENSURE_START_STOP_TIMEOUT_SEC)  # noqa: SLF001
                    await asyncio.sleep(1)
                except Exception as stop_err:
                    logger.warning(
                        "ensure_start: stop %s failed (%s); proceeding to start anyway", unit, stop_err
                    )
    except Exception as check_err:
        logger.warning("ensure_start: is-active check failed for %s: %s", unit, check_err)

    # Step 2: start
    start_ok = False
    try:
        await exc._systemctl("start", unit, timeout=_ENSURE_START_START_TIMEOUT_SEC)  # noqa: SLF001
        logger.info("ensure_start: %s started successfully", unit)
        start_ok = True
    except Exception as start_err:
        logger.warning("ensure_start: start %s failed: %s", unit, start_err)

    # Step 3: trading_engine active marker is written after confirmed start.
    # Socket-service Dev/Prod HOST was already written to its health hash in the HTTP handler.
    if sid == "trading_engine" and start_ok and rurl and ops_profile and meta_key:
        try:
            await asyncio.to_thread(write_trading_engine_ops_lease, rurl, meta_key, ops_profile)
        except Exception as redis_err:
            logger.warning("ensure_start: trading_engine lease write failed: %s", redis_err)


def _executor(request: Request):
    return request.app.state.executor


def _config(request: Request) -> dict:
    return getattr(request.app.state, "bifrost_config", {}) or {}


def _ops_control_profile(request: Request) -> Optional[str]:
    raw = getattr(request.app.state, "bifrost_config_profile", None)
    return normalize_control_profile(raw if isinstance(raw, str) else None)


def _effective_ops_control_profile(request: Request) -> Optional[str]:
    """dev|prod for Redis lease + 409 guard: filename profile, then ``ops.control_profile`` YAML, then env."""
    p = _ops_control_profile(request)
    if p:
        return p
    cfg = _config(request)
    ops_cfg = cfg.get("ops") if isinstance(cfg.get("ops"), dict) else {}
    raw = ops_cfg.get("control_profile")
    if isinstance(raw, str):
        n = normalize_control_profile(raw)
        if n:
            return n
    return normalize_control_profile(os.environ.get("BIFROST_OPS_CONTROL_PROFILE"))


@router.get("/ops/market-ingest/services")
async def market_ingest_services(request: Request) -> Dict[str, Any]:
    """List configured ingest services with current systemd ``is-active`` state."""
    cfg = _config(request)
    rows = market_ingest_services_from_config(cfg)
    exc = _executor(request)
    rurl = meta_redis_url_from_ops_config(cfg)
    out: List[Dict[str, Any]] = []
    for row in rows:
        unit = row["systemd_unit"]
        try:
            active = await exc.systemctl_is_active(unit)
        except Exception as e:
            active = "unknown"
            logger.debug("systemctl_is_active %s: %s", unit, e)
        meta_key = (row.get("redis_meta_key") or "").strip()
        redis_control_env: Optional[str] = None
        redis_control_host: Optional[str] = None
        redis_control_updated_at: Optional[float] = None
        if rurl:
            row_sid = (row.get("id") or "").strip()
            # Dev/Prod HOST is stored on the service health hash so Prod can use the same
            # Redis node it already updates (bifrost:health:*), avoiding bifrost:ops:lease:*.
            lk = meta_key
            if lk:
                redis_control_env = await asyncio.to_thread(read_control_env, rurl, lk)
                redis_control_host = await asyncio.to_thread(read_control_host, rurl, lk)
                redis_control_updated_at = await asyncio.to_thread(read_control_updated_at, rurl, lk)
            # Orphan detection: lease present but health gone → service died, clear stale lease.
            # Requires meta_key to check health hash; skip if meta_key empty.
            if redis_control_env is not None and row_sid != "trading_engine" and meta_key:
                is_live = await asyncio.to_thread(
                    ingest_redis_health_looks_live, rurl, meta_key, row_sid
                )
                control_age = (
                    time.time() - redis_control_updated_at
                    if redis_control_updated_at is not None
                    else None
                )
                recent_control_write = (
                    control_age is not None and control_age <= _RECENT_CONTROL_WRITE_GRACE_SEC
                )
                if not is_live and not recent_control_write:
                    try:
                        await asyncio.to_thread(clear_control_env, rurl, lk)
                    except Exception as _ce:
                        logger.debug("GET /services: clear orphaned lease %s: %s", row_sid, _ce)
                    redis_control_env = None
                    redis_control_host = None
                    redis_control_updated_at = None
        out.append({
            **row,
            "process_active": active,
            "redis_control_env": redis_control_env,
            "redis_control_host": redis_control_host,
            "redis_control_updated_at": redis_control_updated_at,
        })
    return {"ok": True, "services": out}


@router.post("/ops/market-ingest/control")
async def market_ingest_control(
    request: Request,
    background_tasks: BackgroundTasks,
    body: MarketIngestControlRequest = Body(...),
) -> Any:
    from bifrost_api.ops.routers.workers import _role

    denied = _require_role(request, "operator")
    if denied:
        _audit(
            request,
            f"market_ingest_{body.action.value}",
            body.service_id,
            "denied",
            detail=f"role={_role(request)}",
        )
        return denied
    cfg = _config(request)
    svc = market_ingest_service_by_id(cfg, body.service_id)
    if not svc:
        _audit(
            request,
            f"market_ingest_{body.action.value}",
            body.service_id,
            "rejected",
            detail="unknown service_id",
        )
        return JSONResponse(
            status_code=400,
            content={"ok": False, "error": f"Unknown service_id: {body.service_id!r}"},
        )
    unit = svc["systemd_unit"]
    exc = _executor(request)
    sid = svc["id"]
    action = body.action
    meta_key = (svc.get("redis_meta_key") or "").strip()
    rurl = meta_redis_url_from_ops_config(cfg)
    ops_profile = _effective_ops_control_profile(request)
    # Dev/Prod HOST lives in each service health hash. Linux Prod has proven writes to
    # bifrost:health:* work, while bifrost:ops:lease:* may be unavailable/filtered.
    lease_key = meta_key
    claimed: Optional[str] = None
    if rurl and lease_key:
        claimed = await asyncio.to_thread(read_control_env, rurl, lease_key)
    if ops_profile and claimed and claimed != ops_profile:
        # Orphan detection: if meta_key available, check whether health is still live.
        # If health hash is gone/stale the service died and the lease is orphaned → auto-clear.
        if rurl and meta_key:
            looks_live = await asyncio.to_thread(ingest_redis_health_looks_live, rurl, meta_key, sid)
            if not looks_live:
                logger.info(
                    "market_ingest: clearing orphaned %s lease (health hash gone/stale) for %s",
                    claimed,
                    sid,
                )
                try:
                    await asyncio.to_thread(clear_control_env, rurl, lease_key)
                    claimed = None
                except Exception as ce:
                    logger.warning("market_ingest: failed to clear orphaned lease for %s: %s", sid, ce)

    # 409 conflict: lease held by the other stack and service is still alive.
    if ops_profile and claimed and claimed != ops_profile:
        msg = (
            f"Ingest control is held by the {claimed.upper()} stack (Redis). "
            "Stop the service from that Ops host first."
        )
        _audit(
            request,
            f"market_ingest_{body.action.value}",
            f"{body.service_id}:{unit}",
            "rejected",
            detail=f"redis_control_env={claimed} ops_profile={ops_profile}",
        )
        return JSONResponse(
            status_code=409,
            content={"ok": False, "error": msg},
        )

    # Exclusive writer: no lease but Redis health still shows a fresh connected snapshot (other stack).
    # RESET intentionally excluded — it is a force-restart that should succeed even when stale
    # health data is present (e.g. previous run died without clearing its Redis health key).
    if (
        action
        in (
            MarketIngestAction.START,
            MarketIngestAction.RESTART,
        )
        and rurl
        and meta_key
        and ops_profile
        and claimed is None
    ):
        looks_live = await asyncio.to_thread(
            ingest_redis_health_looks_live,
            rurl,
            meta_key,
            sid,
        )
        if looks_live:
            msg = (
                "Redis health still shows an active Socket Services writer (no control lease). "
                "Only one of Dev or Prod may run this service against this Redis. "
                "Stop the other stack's process or wait for health to go stale, then retry."
            )
            _audit(
                request,
                f"market_ingest_{body.action.value}",
                f"{body.service_id}:{unit}",
                "rejected",
                detail="redis_health_looks_live without bifrost_ops_control_env",
            )
            return JSONResponse(
                status_code=409,
                content={"ok": False, "error": msg},
            )

    # START: write Redis lease immediately so HOST column shows on first frontend poll,
    # then dispatch systemctl start to background (avoids nginx proxy_read_timeout).
    if action == MarketIngestAction.START:
        if sid != "trading_engine" and rurl and ops_profile:
            _lk = lease_key
            try:
                await asyncio.to_thread(write_control_env, rurl, _lk, ops_profile)
            except Exception as _le:
                logger.warning("market_ingest start: pre-write lease failed for %s: %s", sid, _le)
        background_tasks.add_task(
            _ensure_start_background, exc, unit, sid, rurl, meta_key, ops_profile
        )
        _audit(
            request,
            "market_ingest_start",
            f"{body.service_id}:{unit}",
            "queued",
            detail="lease written; stop-if-running + start dispatched to background task",
        )
        return JSONResponse(
            content={"ok": True, "queued": True, "service_id": body.service_id, "action": "start"},
            headers={"X-Accel-Buffering": "no"},
        )

    # STOP: clear Redis lease + health immediately so HOST clears on first frontend poll,
    # then dispatch systemctl stop to background (avoids nginx proxy_read_timeout).
    if action == MarketIngestAction.STOP:
        if rurl:
            try:
                if ops_profile:
                    await asyncio.to_thread(clear_control_env, rurl, lease_key)
                if meta_key:
                    await asyncio.to_thread(clear_ingest_health_after_stop, rurl, meta_key, sid)
            except Exception as _ce:
                logger.warning("market_ingest stop: pre-clear redis failed for %s: %s", sid, _ce)
        background_tasks.add_task(_ensure_stop_background, exc, unit)
        _audit(
            request,
            "market_ingest_stop",
            f"{body.service_id}:{unit}",
            "queued",
            detail="redis cleared; systemctl stop dispatched to background task",
        )
        return JSONResponse(
            content={"ok": True, "queued": True, "service_id": body.service_id, "action": "stop"},
            headers={"X-Accel-Buffering": "no"},
        )

    try:
        if action == MarketIngestAction.RESET:
            extra: Dict[str, Any] = {}
            if sid == "ib_operator":
                ok_rpc, rpc_err, rpc_data = await asyncio.to_thread(
                    ib_operator_disconnect_all_sync,
                    cfg,
                )
                extra["disconnect_all_rpc"] = {
                    "ok": ok_rpc,
                    "error": rpc_err,
                    "result": rpc_data,
                }
                if not ok_rpc:
                    logger.warning(
                        "ib_operator reset: disconnect_all RPC failed (%s); continuing with restart",
                        rpc_err,
                    )
            # massive_ws / ib_ingestor / ib_operator: ordered release + restart via systemd.
            result = await exc._systemctl("restart", unit)  # noqa: SLF001
            if extra:
                result = {**result, **extra} if isinstance(result, dict) else {"result": result, **extra}
        else:
            result = await exc._systemctl(action.value, unit)  # noqa: SLF001
    except Exception as e:
        _audit(
            request,
            f"market_ingest_{body.action.value}",
            f"{body.service_id}:{unit}",
            "failed",
            detail=str(e),
        )
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": str(e)},
            headers={"X-Accel-Buffering": "no"},
        )

    if rurl and meta_key:
        try:
            if (
                ops_profile
                and action
                in (
                    MarketIngestAction.RESTART,
                    MarketIngestAction.RESET,
                )
            ):
                if sid == "trading_engine":
                    await asyncio.to_thread(write_trading_engine_ops_lease, rurl, meta_key, ops_profile)
                else:
                    await asyncio.to_thread(write_control_env, rurl, lease_key, ops_profile)
        except Exception as e:
            logger.warning(
                "market_ingest redis post-action failed: %s %s %s",
                body.service_id,
                action.value,
                e,
            )
            _audit(
                request,
                f"market_ingest_{body.action.value}",
                f"{body.service_id}:{unit}",
                "failed",
                detail=f"redis_control_env_or_health: {e}",
            )
            return JSONResponse(
                status_code=500,
                content={
                    "ok": False,
                    "error": (
                        "systemd action succeeded but updating Redis (lease or health) failed: "
                        f"{e}"
                    ),
                },
                headers={"X-Accel-Buffering": "no"},
            )

    _audit(
        request,
        f"market_ingest_{body.action.value}",
        f"{body.service_id}:{unit}",
        "success",
    )
    return JSONResponse(
        content={"ok": True, "service_id": body.service_id, "action": body.action.value, "result": result},
        headers={"X-Accel-Buffering": "no"},
    )


@router.post("/ops/market-ingest/clear-conflict-leases")
async def market_ingest_clear_conflict_leases(request: Request) -> Any:
    """Clear all Redis control leases (bifrost_ops_control_env/host) across all configured services.

    Resolves the dev/prod stack conflict so either stack can regain control. Does not stop
    any running processes — it only removes the Ops ownership fields from each service health hash.
    Requires operator role.
    """
    from bifrost_api.ops.routers.workers import _role

    denied = _require_role(request, "operator")
    if denied:
        _audit(request, "market_ingest_clear_conflict_leases", "*", "denied", detail=f"role={_role(request)}")
        return denied

    cfg = _config(request)
    rows = market_ingest_services_from_config(cfg)
    rurl = meta_redis_url_from_ops_config(cfg)
    if not rurl:
        return JSONResponse(status_code=503, content={"ok": False, "error": "Redis URL not configured for Ops."})

    cleared: List[str] = []
    errors: List[str] = []
    for row in rows:
        row_sid = (row.get("id") or "").strip()
        meta_key = (row.get("redis_meta_key") or "").strip()
        if not meta_key:
            continue
        lk = meta_key
        try:
            await asyncio.to_thread(clear_control_env, rurl, lk)
            cleared.append(row_sid)
        except Exception as e:
            errors.append(f"{row_sid}: {e}")
            logger.warning("clear_conflict_leases %s: %s", lk, e)

    _audit(request, "market_ingest_clear_conflict_leases", "*", "success" if not errors else "partial",
           detail=f"cleared={cleared} errors={errors}")
    return {"ok": True, "cleared": cleared, "errors": errors}
