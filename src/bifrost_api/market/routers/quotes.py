"""Quotes endpoints: Redis cache and SSE stream."""

import asyncio
import json
import math
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

import logging

logger = logging.getLogger(__name__)

router = APIRouter(tags=["quotes"])


class QuotesCleanupBody(BaseModel):
    keep_symbols: List[str] = Field(default_factory=list)


class QuotesRefreshOptionsBody(BaseModel):
    contract_keys: List[str] = Field(default_factory=list)


def _sanitize_for_sse_json(obj: Any) -> Any:
    """Replace NaN/Inf so ``json.dumps`` emits RFC-compliant JSON (``JSON.parse`` in browsers rejects NaN)."""
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, dict):
        return {k: _sanitize_for_sse_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_for_sse_json(x) for x in obj]
    return obj


def _ib_redis_client(rq: Any) -> Any:
    if rq is None:
        return None
    return getattr(rq, "ib_redis_client", None) or getattr(rq, "redis_client", None)


@router.get("/quotes")
def get_quotes(
    request: Request,
    symbols: Optional[str] = Query(None, description="Comma-separated symbols; if omitted, use focus list (positions + watchlist)"),
    contract_keys: Optional[str] = Query(
        None,
        description="Comma-separated OPT contract_key values; merged with watchlist OPT keys when symbols omitted",
    ),
) -> Dict[str, Any]:
    """STK from Redis tick keys; OPT from ``ib:option:cache:*`` (primary) with contract_quote_live fallback."""
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
            if not c or c in contract_keys_opt:
                continue
            # STK live quotes come from Redis only; contract_quote_live rows are stale snapshots.
            if "|STK|" in c.upper():
                continue
            contract_keys_opt.append(c)

    # Ask IB Gateway / Ingestor to stream STK ticks for Live focus symbols (D10-safe; no orders).
    if symbol_list and rq and getattr(rq, "available", False):
        try:
            from bifrost_core.core.realtime.on_demand_stk import ensure_on_demand_stk

            ensure_on_demand_stk(_ib_redis_client(rq), symbol_list)
        except Exception as e:
            logger.warning("GET /quotes on-demand STK register failed: %s", e)

    # Register OPT contract_keys for Gateway one-shot cache refresh (D10-safe).
    if contract_keys_opt and rq and getattr(rq, "available", False):
        try:
            from bifrost_core.core.realtime.on_demand_opt import ensure_on_demand_opt

            ensure_on_demand_opt(_ib_redis_client(rq), contract_keys_opt)
        except Exception as e:
            logger.warning("GET /quotes on-demand OPT register failed: %s", e)

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
        missing: List[str] = []
        if rq and getattr(rq, "available", False) and hasattr(rq, "get_option_cache"):
            try:
                for ck in contract_keys_opt:
                    q = rq.get_option_cache(ck)
                    if q is not None:
                        quotes.append(q)
                    else:
                        missing.append(ck)
            except Exception as e:
                logger.warning("GET /quotes OPT Redis cache failed: %s", e)
                missing = list(contract_keys_opt)
        else:
            missing = list(contract_keys_opt)
        if missing:
            try:
                opt_quotes = reader.get_contract_quotes(missing)
                for q in opt_quotes or []:
                    quotes.append(q)
            except Exception as e:
                logger.warning("GET /quotes contract_quote_live fallback failed: %s", e)

    if not symbol_list and not contract_keys_opt:
        return {"quotes": [], "message": "No symbols in watchlist"}
    if not quotes and not symbol_list and contract_keys_opt:
        return {"quotes": [], "message": "No option quotes"}
    if not quotes and symbol_list and not (rq and getattr(rq, "available", False)):
        return {"quotes": [], "message": "Real-time quotes disabled or Redis unavailable"}
    return {"quotes": quotes}


@router.post("/quotes/refresh-options")
def post_quotes_refresh_options(request: Request, body: QuotesRefreshOptionsBody) -> Any:
    """Register OPT contract_keys for on-demand cache (soft FE Refresh path)."""
    app = request.app
    rq = getattr(app.state, "redis_quotes", None)
    if rq is None or not getattr(rq, "available", False):
        return JSONResponse(
            status_code=503,
            content={"detail": "Real-time quotes disabled or Redis unavailable"},
        )
    ib_client = _ib_redis_client(rq)
    if ib_client is None:
        return JSONResponse(
            status_code=503,
            content={"detail": "IB Redis client unavailable"},
        )
    try:
        from bifrost_core.core.realtime.on_demand_opt import ensure_on_demand_opt

        registered = ensure_on_demand_opt(ib_client, body.contract_keys or [])
    except Exception as e:
        logger.warning("POST /quotes/refresh-options failed: %s", e)
        return JSONResponse(
            status_code=503,
            content={"detail": f"Redis register failed: {e}"},
        )
    return {"registered": len(registered), "contract_keys": registered}


@router.post("/quotes/cleanup")
def post_quotes_cleanup(request: Request, body: QuotesCleanupBody) -> Any:
    """Remove on-demand STK symbols not in ``keep_symbols`` (SREM + HDEL + DEL tick keys)."""
    app = request.app
    rq = getattr(app.state, "redis_quotes", None)
    if rq is None or not getattr(rq, "available", False):
        return JSONResponse(
            status_code=503,
            content={"detail": "Real-time quotes disabled or Redis unavailable"},
        )
    ib_client = _ib_redis_client(rq)
    if ib_client is None:
        return JSONResponse(
            status_code=503,
            content={"detail": "IB Redis client unavailable"},
        )

    from bifrost_core.core.realtime.ib_ingestor_keys import IB_INGESTER_ON_DEMAND_STK
    from bifrost_core.core.realtime.on_demand_stk import (
        normalize_stk_symbols,
        remove_on_demand_stk,
    )

    keep = normalize_stk_symbols(body.keep_symbols or [])
    keep_set = set(keep)
    try:
        members_raw = ib_client.smembers(IB_INGESTER_ON_DEMAND_STK) or set()
    except Exception as e:
        logger.warning("POST /quotes/cleanup SMEMBERS failed: %s", e)
        return JSONResponse(
            status_code=503,
            content={"detail": f"Redis unavailable: {e}"},
        )

    current: List[str] = []
    for raw in members_raw:
        if isinstance(raw, (bytes, bytearray)):
            sym = raw.decode("utf-8", errors="replace").strip().upper()
        else:
            sym = str(raw or "").strip().upper()
        if sym:
            current.append(sym)

    to_remove = sorted({s for s in current if s not in keep_set})
    kept = sorted({s for s in current if s in keep_set})
    if to_remove:
        try:
            remove_on_demand_stk(ib_client, to_remove)
        except Exception as e:
            logger.warning("POST /quotes/cleanup remove failed: %s", e)
            return JSONResponse(
                status_code=503,
                content={"detail": f"Redis remove failed: {e}"},
            )
    return {"removed": to_remove, "kept": kept}


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
