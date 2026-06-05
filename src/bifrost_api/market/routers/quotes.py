"""Quotes endpoints: Redis cache and SSE stream."""

import asyncio
import json
import math
from typing import Any, Dict, Optional

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse

import logging

logger = logging.getLogger(__name__)

router = APIRouter(tags=["quotes"])


def _sanitize_for_sse_json(obj: Any) -> Any:
    """Replace NaN/Inf so ``json.dumps`` emits RFC-compliant JSON (``JSON.parse`` in browsers rejects NaN)."""
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, dict):
        return {k: _sanitize_for_sse_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_for_sse_json(x) for x in obj]
    return obj


@router.get("/quotes")
def get_quotes(
    request: Request,
    symbols: Optional[str] = Query(None, description="Comma-separated symbols; if omitted, use focus list (positions + watchlist)"),
    contract_keys: Optional[str] = Query(
        None,
        description="Comma-separated OPT contract_key values; merged with watchlist OPT keys when symbols omitted",
    ),
) -> Dict[str, Any]:
    """STK from Redis ``ib:ingester:tick:{symbol}|STK|||`` (IB Ingestor); OPT from contract_quote_live. Combined list."""
    app = request.app
    reader = app.state.reader
    rq = getattr(app.state, "redis_quotes", None)
    symbol_list: list = []
    contract_keys_opt: list = []
    ck_param = (contract_keys or "").strip() if contract_keys else ""
    if symbols and symbols.strip():
        symbol_list = [s.strip() for s in symbols.split(",") if s.strip()]
    elif not ck_param:
        accounts = reader.get_accounts_from_tables() or []
        for acc in accounts:
            for pos in (acc.get("positions") or []):
                sym = (pos.get("symbol") or "").strip()
                if sym and sym not in symbol_list:
                    symbol_list.append(sym)
        for w in reader.get_watchlist():
            sec_type = (w.get("sec_type") or "").strip().upper()
            if sec_type == "OPT":
                ck = (w.get("contract_key") or "").strip()
                if ck and ck not in contract_keys_opt:
                    contract_keys_opt.append(ck)
            else:
                sym = (w.get("symbol") or "").strip()
                if sym and sym not in symbol_list:
                    symbol_list.append(sym)
    if ck_param:
        for ck in ck_param.split(","):
            c = ck.strip()
            if c and c not in contract_keys_opt:
                contract_keys_opt.append(c)
    quotes: list = []
    if symbol_list and rq and getattr(rq, "available", False):
        try:
            for sym in symbol_list:
                s = (sym or "").strip()
                if not s:
                    continue
                q = rq.get_ingester_tick(f"{s}|STK|||")
                if q is not None:
                    quotes.append(q)
        except Exception as e:
            logger.warning("GET /quotes Redis failed: %s", e)
    if contract_keys_opt:
        try:
            opt_quotes = reader.get_contract_quotes(contract_keys_opt)
            for q in opt_quotes or []:
                quotes.append(q)
        except Exception as e:
            logger.warning("GET /quotes contract_quote_live failed: %s", e)
    if not symbol_list and not contract_keys_opt:
        return {"quotes": [], "message": "No symbols in watchlist"}
    if not quotes and not symbol_list and contract_keys_opt:
        return {"quotes": [], "message": "No option quotes"}
    if not quotes and symbol_list and not (rq and getattr(rq, "available", False)):
        return {"quotes": [], "message": "Real-time quotes disabled or Redis unavailable"}
    return {"quotes": quotes}


@router.get("/quotes/stream")
async def get_quotes_stream(request: Request):
    """R-RM* SSE: Subscribe to Redis ``ib:ingester:channel`` (config ``redis.subscribe_channel``); load full tick from ``ib:ingester:tick:*``. Returns 503 when Redis unavailable."""
    app = request.app
    rq = getattr(app.state, "redis_quotes", None)
    if rq is None or not getattr(rq, "available", False):
        return JSONResponse(
            status_code=503,
            content={"detail": "Real-time quotes disabled or Redis unavailable"},
        )
    queue: asyncio.Queue = asyncio.Queue(maxsize=256)

    with app.state.sse_lock:
        app.state.sse_queues.append(queue)

    async def event_gen():
        try:
            while True:
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=25.0)
                    safe = _sanitize_for_sse_json(data)
                    try:
                        line = json.dumps(safe, default=str, allow_nan=False)
                    except (ValueError, TypeError) as ex:
                        logger.warning("SSE quote JSON skip (non-encodable): %s", ex)
                        continue
                    yield f"data: {line}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            with app.state.sse_lock:
                if queue in app.state.sse_queues:
                    app.state.sse_queues.remove(queue)

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-store",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
