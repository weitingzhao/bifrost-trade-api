"""Massive option feed: SSE stream, WS ingest status, reconciliation readout (FASTAPI_PLAN FA-1 SSE)."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse

from bifrost_core.monitor.redis_url import redis_url_from_config
from bifrost_api.massive.deps import db_config as _db_config

logger = logging.getLogger(__name__)

router = APIRouter(tags=["massive-stream"])


@router.get("/research/massive/stream")
async def get_massive_option_stream(request: Request):
    """SSE: subscribe to Redis ``massive:channel`` via server-side subscriber; payloads are contract summaries."""
    queues = getattr(request.app.state, "massive_sse_queues", None)
    lock = getattr(request.app.state, "massive_sse_lock", None)
    if queues is None or lock is None:
        return JSONResponse(
            status_code=503,
            content={"detail": "Massive SSE not initialised"},
        )
    redis_url = redis_url_from_config(getattr(request.app.state.reader, "_config", {}) or {})
    if not redis_url:
        return JSONResponse(
            status_code=503,
            content={"detail": "Redis disabled in config; Massive SSE unavailable"},
        )

    queue: asyncio.Queue = asyncio.Queue(maxsize=256)
    with lock:
        queues.append(queue)

    async def event_gen():
        try:
            while True:
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=25.0)
                    yield f"data: {json.dumps(data, default=str)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            with lock:
                if queue in queues:
                    queues.remove(queue)

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-store",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/research/massive/ws-status")
def get_massive_ws_status(request: Request) -> Dict[str, Any]:
    """Redis ingest health for Massive WS (``bifrost:health:ws_massive_option``, older ``bifrost:health:massive_ws``, ``massive:meta:status``)."""
    redis_url = redis_url_from_config(getattr(request.app.state.reader, "_config", {}) or {})
    if not redis_url:
        return {"ok": False, "error": "Redis not configured", "connected": None}
    try:
        import redis

        from bifrost_core.core.redis_health_keys import hgetall_massive_ws_status

        r = redis.from_url(redis_url, decode_responses=True)
        raw = hgetall_massive_ws_status(r)
        subs = r.smembers("massive:meta:subscriptions")
        return {
            "ok": True,
            "status": dict(raw) if raw else {},
            "subscriptions_sample": sorted(list(subs))[:40] if subs else [],
        }
    except Exception as e:
        logger.warning("get_massive_ws_status: %s", e)
        return {"ok": False, "error": str(e), "status": {}}


@router.get("/research/massive/reconciliation")
def get_massive_reconciliation(
    request: Request,
    symbol: Optional[str] = Query(None, description="Filter result row by symbol"),
    trade_date: Optional[str] = Query(None, description="Match payload trade_date (YYYY-MM-DD)"),
) -> Dict[str, Any]:
    """Latest ``reconcile`` job result from job_massive_backfill."""
    from bifrost_worker.data.massive.vendor.reader import get_latest_massive_job_by_kind

    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}
    job = get_latest_massive_job_by_kind(db, "reconcile")
    if not job:
        return {"ok": True, "job": None, "message": "No reconcile job yet"}
    res = job.get("result")
    if isinstance(res, str):
        try:
            res = json.loads(res)
        except json.JSONDecodeError:
            res = {"raw": res}
    sym = (symbol or "").strip().upper()
    td = (trade_date or "").strip()
    if isinstance(res, dict):
        if td and str(res.get("trade_date") or "")[:10] != td[:10]:
            res = {**res, "results": [], "filter_note": "trade_date mismatch"}
        rows = res.get("results")
        if sym and isinstance(rows, list):
            res = dict(res)
            res["results"] = [
                x for x in rows
                if isinstance(x, dict) and str(x.get("symbol") or "").upper() == sym
            ]
            res["filtered"] = True
    return {
        "ok": True,
        "job_id": str(job.get("job_massive_backfill_id", "")),
        "status": job.get("status"),
        "updated_ts": job.get("updated_at"),
        "result": res,
    }
