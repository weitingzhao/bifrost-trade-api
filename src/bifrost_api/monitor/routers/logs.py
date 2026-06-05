"""Log endpoints: daemon, Monitor API, Massive, sidecars, and Celery console logs (fetch, clear, trim, stream)."""

import asyncio
import json
import logging
import re
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Body, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse

from bifrost_api.monitor.routers.deps import (
    ACCOUNT_SYNC_DAEMON_LOG_STREAM_KEY,
    IB_ACCOUNT_AGENT_LOG_STREAM_KEY,
    IB_OPERATOR_LOG_STREAM_KEY,
    IB_INGESTOR_LOG_STREAM_KEY,
    MASSIVE_LOG_STREAM_KEY,
    MASSIVE_WS_LOG_STREAM_KEY,
    daemon_log_redis_url,
)
from bifrost_core.core.redis_console_streams import BIFROST_CONSOLE_DAEMON_TRADING
from bifrost_core.config.yaml_config import daemon_trading_console_stream_key
from bifrost_core.sse.queue_utils import put_nowait_drop_oldest

logger = logging.getLogger(__name__)

router = APIRouter(tags=["logs"])


def _redis_stream_line(fields: Dict[Any, Any]) -> str:
    """Decode log line from Redis Stream fields (bytes or str, with or without decode_responses)."""
    raw = fields.get(b"line")
    if raw is None:
        raw = fields.get("line")
    if raw is None:
        return ""
    if isinstance(raw, bytes):
        return raw.decode("utf-8", errors="replace")
    return str(raw)


def _trading_console_stream_keys_dual() -> List[str]:
    """Dev + prod stream keys so Monitor and sidecars can disagree on config profile."""
    from bifrost_core.config.yaml_config import trading_api_console_stream_key

    d = trading_api_console_stream_key(None)
    p = trading_api_console_stream_key("prod")
    return list(dict.fromkeys([d, p]))


def _portfolio_console_stream_keys_dual() -> List[str]:
    from bifrost_core.config.yaml_config import portfolio_api_console_stream_key

    d = portfolio_api_console_stream_key(None)
    p = portfolio_api_console_stream_key("prod")
    return list(dict.fromkeys([d, p]))


def _research_console_stream_keys_dual() -> List[str]:
    from bifrost_core.config.yaml_config import research_api_console_stream_key

    d = research_api_console_stream_key(None)
    p = research_api_console_stream_key("prod")
    return list(dict.fromkeys([d, p]))


def _strategy_console_stream_keys_dual() -> List[str]:
    from bifrost_core.config.yaml_config import strategy_api_console_stream_key

    d = strategy_api_console_stream_key(None)
    p = strategy_api_console_stream_key("prod")
    return list(dict.fromkeys([d, p]))


def _market_console_stream_keys_dual() -> List[str]:
    from bifrost_core.config.yaml_config import market_api_console_stream_key

    d = market_api_console_stream_key(None)
    p = market_api_console_stream_key("prod")
    return list(dict.fromkeys([d, p]))


def _daemon_console_stream_keys_for_read() -> List[str]:
    """Legacy + dev + prod daemon streams (fixed names; same Redis, no extra config hash)."""
    dev_default = daemon_trading_console_stream_key(None)
    prod_default = daemon_trading_console_stream_key("prod")
    legacy = BIFROST_CONSOLE_DAEMON_TRADING
    return list(dict.fromkeys([legacy, dev_default, prod_default]))


_TIME_PREFIX_RE_STREAM = re.compile(
    r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:,\d+)?)",
)


def _stream_line_sort_key(line: str) -> str:
    m = _TIME_PREFIX_RE_STREAM.match(line)
    return m.group(1) if m else "\uffff"


def _merged_console_tail_from_keys(r: Any, keys: List[str], tail: int) -> Tuple[List[str], Optional[str]]:
    """Merge newest `tail` lines per key, sort by leading timestamp, return global last `tail` lines."""
    scored: List[Tuple[str, str]] = []
    err_parts: List[str] = []
    for key in keys:
        try:
            raw = r.xrevrange(key, count=tail)
            for _eid, fields in reversed(raw):
                line = _redis_stream_line(fields)
                if not line:
                    continue
                scored.append((_stream_line_sort_key(line), line))
        except Exception as e:
            err_parts.append(f"{key}: {e}")
    scored.sort(key=lambda x: x[0])
    merged = [ln for _, ln in scored[-tail:]]
    err = "; ".join(err_parts) if err_parts else None
    return merged, err


def _daemon_log_reader_loop(app_ref) -> None:
    """Background thread: XREAD legacy + dev + prod daemon console streams."""
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url(), decode_responses=True)
        keys = _daemon_console_stream_keys_for_read()
        last_ids: Dict[str, str] = {k: "$" for k in keys}
        while True:
            try:
                result = r.xread(block=5000, streams=last_ids, count=100)
                if not result:
                    continue
                for stream_name, entries in result:
                    for eid, fields in entries:
                        last_ids[stream_name] = eid
                        line = _redis_stream_line(fields)
                        with app_ref.state.daemon_log_lock:
                            queues = list(app_ref.state.daemon_log_queues)
                        loop = getattr(app_ref.state, "_daemon_log_loop", None)
                        for q in queues:
                            if loop and not loop.is_closed():
                                loop.call_soon_threadsafe(put_nowait_drop_oldest, q, line)
            except redis.ConnectionError:
                time.sleep(2)
            except Exception as e:
                logger.debug("daemon_log_reader_loop: %s", e)
    except Exception as e:
        logger.warning("daemon_log_reader_loop exited: %s", e)


def _monitor_log_reader_loop(app_ref) -> None:
    """Background thread: XREAD Redis stream bifrost:console:{dev|prod}:api_monitor, push each line to SSE queues."""
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        last_id = "$"
        stream_key = getattr(app_ref.state, "monitor_log_stream_key", None) or "bifrost:console:dev:api_monitor"
        while True:
            try:
                result = r.xread(block=5000, streams={stream_key: last_id}, count=100)
                if not result:
                    continue
                for _stream_name, entries in result:
                    for eid, fields in entries:
                        last_id = eid
                        line = _redis_stream_line(fields)
                        with app_ref.state.monitor_log_lock:
                            queues = list(app_ref.state.monitor_log_queues)
                        loop = getattr(app_ref.state, "_monitor_log_loop", None)
                        for q in queues:
                            if loop and not loop.is_closed():
                                loop.call_soon_threadsafe(put_nowait_drop_oldest, q, line)
            except redis.ConnectionError:
                time.sleep(2)
            except Exception as e:
                logger.debug("monitor_log_reader_loop: %s", e)
    except Exception as e:
        logger.warning("monitor_log_reader_loop exited: %s", e)


def _massive_log_reader_loop(app_ref) -> None:
    """Background thread: XREAD Redis stream bifrost:massive_console, push each line to all SSE queues."""
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        last_id = "$"
        while True:
            try:
                result = r.xread(block=5000, streams={MASSIVE_LOG_STREAM_KEY: last_id}, count=100)
                if not result:
                    continue
                for _stream_name, entries in result:
                    for eid, fields in entries:
                        last_id = eid
                        line = _redis_stream_line(fields)
                        with app_ref.state.massive_log_lock:
                            queues = list(app_ref.state.massive_log_queues)
                        loop = getattr(app_ref.state, "_massive_log_loop", None)
                        for q in queues:
                            if loop and not loop.is_closed():
                                loop.call_soon_threadsafe(put_nowait_drop_oldest, q, line)
            except redis.ConnectionError:
                time.sleep(2)
            except Exception as e:
                logger.debug("massive_log_reader_loop: %s", e)
    except Exception as e:
        logger.warning("massive_log_reader_loop exited: %s", e)


def _massive_ws_log_reader_loop(app_ref) -> None:
    """Background thread: XREAD Redis stream bifrost:console:ws_massive_option (scripts/systemd/run_massive_ws.py)."""
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        last_id = "$"
        while True:
            try:
                result = r.xread(block=5000, streams={MASSIVE_WS_LOG_STREAM_KEY: last_id}, count=100)
                if not result:
                    continue
                for _stream_name, entries in result:
                    for eid, fields in entries:
                        last_id = eid
                        line = _redis_stream_line(fields)
                        with app_ref.state.massive_ws_log_lock:
                            queues = list(app_ref.state.massive_ws_log_queues)
                        loop = getattr(app_ref.state, "_massive_ws_log_loop", None)
                        for q in queues:
                            if loop and not loop.is_closed():
                                loop.call_soon_threadsafe(put_nowait_drop_oldest, q, line)
            except redis.ConnectionError:
                time.sleep(2)
            except Exception as e:
                logger.debug("massive_ws_log_reader_loop: %s", e)
    except Exception as e:
        logger.warning("massive_ws_log_reader_loop exited: %s", e)


def _ib_operator_log_reader_loop(app_ref) -> None:
    """Background thread: XREAD Redis stream bifrost:console:ws_ib_operator (run_ib_operator.py)."""
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        last_id = "$"
        while True:
            try:
                result = r.xread(block=5000, streams={IB_OPERATOR_LOG_STREAM_KEY: last_id}, count=100)
                if not result:
                    continue
                for _stream_name, entries in result:
                    for eid, fields in entries:
                        last_id = eid
                        line = _redis_stream_line(fields)
                        with app_ref.state.ib_operator_log_lock:
                            queues = list(app_ref.state.ib_operator_log_queues)
                        loop = getattr(app_ref.state, "_ib_operator_log_loop", None)
                        for q in queues:
                            if loop and not loop.is_closed():
                                loop.call_soon_threadsafe(put_nowait_drop_oldest, q, line)
            except redis.ConnectionError:
                time.sleep(2)
            except Exception as e:
                logger.debug("ib_operator_log_reader_loop: %s", e)
    except Exception as e:
        logger.warning("ib_operator_log_reader_loop exited: %s", e)


def _ib_ingestor_log_reader_loop(app_ref) -> None:
    """Background thread: XREAD Redis stream bifrost:console:ws_ib_ingestor (run_ib_ingestor.py)."""
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        last_id = "$"
        while True:
            try:
                result = r.xread(block=5000, streams={IB_INGESTOR_LOG_STREAM_KEY: last_id}, count=100)
                if not result:
                    continue
                for _stream_name, entries in result:
                    for eid, fields in entries:
                        last_id = eid
                        line = _redis_stream_line(fields)
                        with app_ref.state.ib_ingestor_log_lock:
                            queues = list(app_ref.state.ib_ingestor_log_queues)
                        loop = getattr(app_ref.state, "_ib_ingestor_log_loop", None)
                        for q in queues:
                            if loop and not loop.is_closed():
                                loop.call_soon_threadsafe(put_nowait_drop_oldest, q, line)
            except redis.ConnectionError:
                time.sleep(2)
            except Exception as e:
                logger.debug("ib_ingestor_log_reader_loop: %s", e)
    except Exception as e:
        logger.warning("ib_ingestor_log_reader_loop exited: %s", e)


def _ib_account_agent_log_reader_loop(app_ref) -> None:
    """Background thread: XREAD Redis stream bifrost:console:ws_ib_account_agent (run_ib_account_agent.py)."""
    try:
        import redis

        r = redis.from_url(daemon_log_redis_url())
        last_id = "$"
        while True:
            try:
                result = r.xread(
                    block=5000,
                    streams={IB_ACCOUNT_AGENT_LOG_STREAM_KEY: last_id},
                    count=100,
                )
                if not result:
                    continue
                for _stream_name, entries in result:
                    for eid, fields in entries:
                        last_id = eid
                        line = _redis_stream_line(fields)
                        with app_ref.state.ib_account_agent_log_lock:
                            queues = list(app_ref.state.ib_account_agent_log_queues)
                        loop = getattr(app_ref.state, "_ib_account_agent_log_loop", None)
                        for q in queues:
                            if loop and not loop.is_closed():
                                loop.call_soon_threadsafe(put_nowait_drop_oldest, q, line)
            except redis.ConnectionError:
                time.sleep(2)
            except Exception as e:
                logger.debug("ib_account_agent_log_reader_loop: %s", e)
    except Exception as e:
        logger.warning("ib_account_agent_log_reader_loop exited: %s", e)


def _docs_log_reader_loop(app_ref) -> None:
    """Background thread: XREAD Redis stream bifrost:console:{dev|prod}:api_docs, push each line to SSE queues."""
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        last_id = "$"
        stream_key = getattr(app_ref.state, "docs_log_stream_key", None) or "bifrost:console:dev:api_docs"
        while True:
            try:
                result = r.xread(block=5000, streams={stream_key: last_id}, count=100)
                if not result:
                    continue
                for _stream_name, entries in result:
                    for eid, fields in entries:
                        last_id = eid
                        line = _redis_stream_line(fields)
                        with app_ref.state.docs_log_lock:
                            queues = list(app_ref.state.docs_log_queues)
                        loop = getattr(app_ref.state, "_docs_log_loop", None)
                        for q in queues:
                            if loop and not loop.is_closed():
                                loop.call_soon_threadsafe(put_nowait_drop_oldest, q, line)
            except redis.ConnectionError:
                time.sleep(2)
            except Exception as e:
                logger.debug("docs_log_reader_loop: %s", e)
    except Exception as e:
        logger.warning("docs_log_reader_loop exited: %s", e)


def _ops_log_reader_loop(app_ref) -> None:
    """Background thread: XREAD Redis stream bifrost:console:{dev|prod}:api_ops, push each line to SSE queues."""
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        last_id = "$"
        stream_key = getattr(app_ref.state, "ops_log_stream_key", None) or "bifrost:console:dev:api_ops"
        while True:
            try:
                result = r.xread(block=5000, streams={stream_key: last_id}, count=100)
                if not result:
                    continue
                for _stream_name, entries in result:
                    for eid, fields in entries:
                        last_id = eid
                        line = _redis_stream_line(fields)
                        with app_ref.state.ops_log_lock:
                            queues = list(app_ref.state.ops_log_queues)
                        loop = getattr(app_ref.state, "_ops_log_loop", None)
                        for q in queues:
                            if loop and not loop.is_closed():
                                loop.call_soon_threadsafe(put_nowait_drop_oldest, q, line)
            except redis.ConnectionError:
                time.sleep(2)
            except Exception as e:
                logger.debug("ops_log_reader_loop: %s", e)
    except Exception as e:
        logger.warning("ops_log_reader_loop exited: %s", e)


def _trading_log_reader_loop(app_ref) -> None:
    """Background thread: XREAD dev+prod Trading API console streams (sidecar vs Monitor profile mismatch safe)."""
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url(), decode_responses=True)
        keys = _trading_console_stream_keys_dual()
        last_ids = {k: "$" for k in keys}
        while True:
            try:
                result = r.xread(block=5000, streams=last_ids, count=100)
                if not result:
                    continue
                for stream_name, entries in result:
                    for eid, fields in entries:
                        last_ids[stream_name] = eid
                        line = _redis_stream_line(fields)
                        with app_ref.state.trading_log_lock:
                            queues = list(app_ref.state.trading_log_queues)
                        loop = getattr(app_ref.state, "_trading_log_loop", None)
                        for q in queues:
                            if loop and not loop.is_closed():
                                loop.call_soon_threadsafe(put_nowait_drop_oldest, q, line)
            except redis.ConnectionError:
                time.sleep(2)
            except Exception as e:
                logger.debug("trading_log_reader_loop: %s", e)
    except Exception as e:
        logger.warning("trading_log_reader_loop exited: %s", e)


def _portfolio_log_reader_loop(app_ref) -> None:
    """Background thread: XREAD dev+prod Portfolio API console streams."""
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url(), decode_responses=True)
        keys = _portfolio_console_stream_keys_dual()
        last_ids = {k: "$" for k in keys}
        while True:
            try:
                result = r.xread(block=5000, streams=last_ids, count=100)
                if not result:
                    continue
                for stream_name, entries in result:
                    for eid, fields in entries:
                        last_ids[stream_name] = eid
                        line = _redis_stream_line(fields)
                        with app_ref.state.portfolio_log_lock:
                            queues = list(app_ref.state.portfolio_log_queues)
                        loop = getattr(app_ref.state, "_portfolio_log_loop", None)
                        for q in queues:
                            if loop and not loop.is_closed():
                                loop.call_soon_threadsafe(put_nowait_drop_oldest, q, line)
            except redis.ConnectionError:
                time.sleep(2)
            except Exception as e:
                logger.debug("portfolio_log_reader_loop: %s", e)
    except Exception as e:
        logger.warning("portfolio_log_reader_loop exited: %s", e)


def _research_log_reader_loop(app_ref) -> None:
    """Background thread: XREAD dev+prod Research API console streams."""
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url(), decode_responses=True)
        keys = _research_console_stream_keys_dual()
        last_ids = {k: "$" for k in keys}
        while True:
            try:
                result = r.xread(block=5000, streams=last_ids, count=100)
                if not result:
                    continue
                for stream_name, entries in result:
                    for eid, fields in entries:
                        last_ids[stream_name] = eid
                        line = _redis_stream_line(fields)
                        with app_ref.state.research_log_lock:
                            queues = list(app_ref.state.research_log_queues)
                        loop = getattr(app_ref.state, "_research_log_loop", None)
                        for q in queues:
                            if loop and not loop.is_closed():
                                loop.call_soon_threadsafe(put_nowait_drop_oldest, q, line)
            except redis.ConnectionError:
                time.sleep(2)
            except Exception as e:
                logger.debug("research_log_reader_loop: %s", e)
    except Exception as e:
        logger.warning("research_log_reader_loop exited: %s", e)


def _strategy_log_reader_loop(app_ref) -> None:
    """Background thread: XREAD dev+prod Strategy API console streams."""
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url(), decode_responses=True)
        keys = _strategy_console_stream_keys_dual()
        last_ids = {k: "$" for k in keys}
        while True:
            try:
                result = r.xread(block=5000, streams=last_ids, count=100)
                if not result:
                    continue
                for stream_name, entries in result:
                    for eid, fields in entries:
                        last_ids[stream_name] = eid
                        line = _redis_stream_line(fields)
                        with app_ref.state.strategy_log_lock:
                            queues = list(app_ref.state.strategy_log_queues)
                        loop = getattr(app_ref.state, "_strategy_log_loop", None)
                        for q in queues:
                            if loop and not loop.is_closed():
                                loop.call_soon_threadsafe(put_nowait_drop_oldest, q, line)
            except redis.ConnectionError:
                time.sleep(2)
            except Exception as e:
                logger.debug("strategy_log_reader_loop: %s", e)
    except Exception as e:
        logger.warning("strategy_log_reader_loop exited: %s", e)


def _market_log_reader_loop(app_ref) -> None:
    """Background thread: XREAD dev+prod Market API console streams."""
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url(), decode_responses=True)
        keys = _market_console_stream_keys_dual()
        last_ids = {k: "$" for k in keys}
        while True:
            try:
                result = r.xread(block=5000, streams=last_ids, count=100)
                if not result:
                    continue
                for stream_name, entries in result:
                    for eid, fields in entries:
                        last_ids[stream_name] = eid
                        line = _redis_stream_line(fields)
                        with app_ref.state.market_log_lock:
                            queues = list(app_ref.state.market_log_queues)
                        loop = getattr(app_ref.state, "_market_log_loop", None)
                        for q in queues:
                            if loop and not loop.is_closed():
                                loop.call_soon_threadsafe(put_nowait_drop_oldest, q, line)
            except redis.ConnectionError:
                time.sleep(2)
            except Exception as e:
                logger.debug("market_log_reader_loop: %s", e)
    except Exception as e:
        logger.warning("market_log_reader_loop exited: %s", e)


# --- Daemon logs ---

@router.get("/api/daemon/logs")
def get_daemon_logs(
    request: Request,
    tail: int = Query(1000, ge=1, le=5000, description="Number of latest lines (oldest-first in response)"),
) -> Dict[str, Any]:
    """Return last N lines from Strategy Trading Daemon console streams (dev + prod + legacy, merged by time)."""
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url(), decode_responses=True)
        keys = _daemon_console_stream_keys_for_read()
        lines, partial_err = _merged_console_tail_from_keys(r, keys, tail)
        out: Dict[str, Any] = {"lines": lines}
        if partial_err:
            out["error"] = partial_err
        return out
    except Exception as e:
        logger.warning("get_daemon_logs failed: %s", e)
        return {"lines": [], "error": str(e)}


@router.delete("/api/daemon/logs")
def clear_daemon_logs(request: Request) -> Dict[str, Any]:
    """Delete Strategy Trading Daemon console Redis streams (dev + prod + legacy). UI Clear uses this."""
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url(), decode_responses=True)
        for key in _daemon_console_stream_keys_for_read():
            r.delete(key)
        return {"ok": True}
    except Exception as e:
        logger.warning("clear_daemon_logs failed: %s", e)
        return {"ok": False, "error": str(e)}


@router.post("/api/daemon/logs/trim")
def trim_daemon_logs(request: Request, body: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    """Trim each Strategy Trading Daemon console stream to at most max_lines (keep newest)."""
    try:
        max_lines = body.get("max_lines")
        if max_lines is None:
            return {"ok": False, "error": "max_lines required"}
        max_lines = int(max_lines)
        if max_lines < 1 or max_lines > 10000:
            return {"ok": False, "error": "max_lines must be between 1 and 10000"}
        import redis
        r = redis.from_url(daemon_log_redis_url(), decode_responses=True)
        for key in _daemon_console_stream_keys_for_read():
            r.xtrim(key, maxlen=max_lines, approximate=True)
        return {"ok": True}
    except Exception as e:
        logger.warning("trim_daemon_logs failed: %s", e)
        return {"ok": False, "error": str(e)}


@router.get("/api/daemon/logs/stream")
async def get_daemon_logs_stream(request: Request):
    """SSE: stream new daemon console lines in real time (Redis XREAD)."""
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        r.ping()
    except Exception as e:
        logger.warning("daemon_logs_stream check failed: %s", e)
        return JSONResponse(status_code=503, content={"detail": str(e)})

    app = request.app
    queue: asyncio.Queue = asyncio.Queue(maxsize=512)
    with app.state.daemon_log_lock:
        app.state.daemon_log_queues.append(queue)
        if app.state._daemon_log_loop is None:
            app.state._daemon_log_loop = asyncio.get_running_loop()
        if app.state._daemon_log_thread is None or not app.state._daemon_log_thread.is_alive():
            app.state._daemon_log_thread = threading.Thread(
                target=_daemon_log_reader_loop,
                args=(app,),
                name="daemon-log-reader",
                daemon=True,
            )
            app.state._daemon_log_thread.start()

    async def event_gen():
        try:
            while True:
                try:
                    line = await asyncio.wait_for(queue.get(), timeout=25.0)
                    yield f"data: {json.dumps({'line': line})}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            with app.state.daemon_log_lock:
                if queue in app.state.daemon_log_queues:
                    app.state.daemon_log_queues.remove(queue)

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-store",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# --- Monitor API server logs (run_server.py → Redis stream bifrost:console:{dev|prod}:api_monitor) ---


@router.get("/api/monitor/logs")
def get_monitor_logs(
    request: Request,
    tail: int = Query(1000, ge=1, le=5000, description="Number of latest lines (oldest-first in response)"),
) -> Dict[str, Any]:
    """Return last N lines from Monitor API console Redis stream."""
    try:
        import redis
        key = getattr(request.app.state, "monitor_log_stream_key", None) or "bifrost:console:dev:api_monitor"
        r = redis.from_url(daemon_log_redis_url())
        raw = r.xrevrange(key, count=tail)
        lines = []
        for _eid, fields in reversed(raw):
            line = _redis_stream_line(fields)
            lines.append(line)
        return {"lines": lines}
    except Exception as e:
        logger.warning("get_monitor_logs failed: %s", e)
        return {"lines": [], "error": str(e)}


@router.delete("/api/monitor/logs")
def clear_monitor_logs(request: Request) -> Dict[str, Any]:
    """Delete the Monitor API console Redis stream so next fetch is empty."""
    try:
        import redis
        key = getattr(request.app.state, "monitor_log_stream_key", None) or "bifrost:console:dev:api_monitor"
        r = redis.from_url(daemon_log_redis_url())
        r.delete(key)
        return {"ok": True}
    except Exception as e:
        logger.warning("clear_monitor_logs failed: %s", e)
        return {"ok": False, "error": str(e)}


@router.post("/api/monitor/logs/trim")
def trim_monitor_logs(request: Request, body: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    """Trim Monitor API console Redis stream to at most max_lines (keep newest)."""
    try:
        max_lines = body.get("max_lines")
        if max_lines is None:
            return {"ok": False, "error": "max_lines required"}
        max_lines = int(max_lines)
        if max_lines < 1 or max_lines > 10000:
            return {"ok": False, "error": "max_lines must be between 1 and 10000"}
        import redis
        key = getattr(request.app.state, "monitor_log_stream_key", None) or "bifrost:console:dev:api_monitor"
        r = redis.from_url(daemon_log_redis_url())
        r.xtrim(key, maxlen=max_lines, approximate=True)
        return {"ok": True}
    except Exception as e:
        logger.warning("trim_monitor_logs failed: %s", e)
        return {"ok": False, "error": str(e)}


@router.get("/api/monitor/logs/stream")
async def get_monitor_logs_stream(request: Request):
    """SSE: stream new Monitor API console lines in real time (Redis XREAD)."""
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        r.ping()
    except Exception as e:
        logger.warning("monitor_logs_stream check failed: %s", e)
        return JSONResponse(status_code=503, content={"detail": str(e)})

    app = request.app
    queue: asyncio.Queue = asyncio.Queue(maxsize=512)
    with app.state.monitor_log_lock:
        app.state.monitor_log_queues.append(queue)
        if app.state._monitor_log_loop is None:
            app.state._monitor_log_loop = asyncio.get_running_loop()
        if app.state._monitor_log_thread is None or not app.state._monitor_log_thread.is_alive():
            app.state._monitor_log_thread = threading.Thread(
                target=_monitor_log_reader_loop,
                args=(app,),
                name="monitor-log-reader",
                daemon=True,
            )
            app.state._monitor_log_thread.start()

    async def event_gen():
        try:
            while True:
                try:
                    line = await asyncio.wait_for(queue.get(), timeout=25.0)
                    yield f"data: {json.dumps({'line': line})}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            with app.state.monitor_log_lock:
                if queue in app.state.monitor_log_queues:
                    app.state.monitor_log_queues.remove(queue)

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-store",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# --- Massive API server logs (run_server_massive.py → Redis stream bifrost:massive_console) ---


@router.get("/api/massive/logs")
def get_massive_logs(
    request: Request,
    tail: int = Query(1000, ge=1, le=5000, description="Number of latest lines (oldest-first in response)"),
) -> Dict[str, Any]:
    """Return last N lines from Massive API console Redis stream."""
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        raw = r.xrevrange(MASSIVE_LOG_STREAM_KEY, count=tail)
        lines = []
        for _eid, fields in reversed(raw):
            line = _redis_stream_line(fields)
            lines.append(line)
        return {"lines": lines}
    except Exception as e:
        logger.warning("get_massive_logs failed: %s", e)
        return {"lines": [], "error": str(e)}


@router.delete("/api/massive/logs")
def clear_massive_logs(request: Request) -> Dict[str, Any]:
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        r.delete(MASSIVE_LOG_STREAM_KEY)
        return {"ok": True}
    except Exception as e:
        logger.warning("clear_massive_logs failed: %s", e)
        return {"ok": False, "error": str(e)}


@router.post("/api/massive/logs/trim")
def trim_massive_logs(request: Request, body: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    try:
        max_lines = body.get("max_lines")
        if max_lines is None:
            return {"ok": False, "error": "max_lines required"}
        max_lines = int(max_lines)
        if max_lines < 1 or max_lines > 10000:
            return {"ok": False, "error": "max_lines must be between 1 and 10000"}
        import redis
        r = redis.from_url(daemon_log_redis_url())
        r.xtrim(MASSIVE_LOG_STREAM_KEY, maxlen=max_lines, approximate=True)
        return {"ok": True}
    except Exception as e:
        logger.warning("trim_massive_logs failed: %s", e)
        return {"ok": False, "error": str(e)}


@router.get("/api/massive/logs/stream")
async def get_massive_logs_stream(request: Request):
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        r.ping()
    except Exception as e:
        logger.warning("massive_logs_stream check failed: %s", e)
        return JSONResponse(status_code=503, content={"detail": str(e)})

    app = request.app
    queue: asyncio.Queue = asyncio.Queue(maxsize=512)
    with app.state.massive_log_lock:
        app.state.massive_log_queues.append(queue)
        if app.state._massive_log_loop is None:
            app.state._massive_log_loop = asyncio.get_running_loop()
        if app.state._massive_log_thread is None or not app.state._massive_log_thread.is_alive():
            app.state._massive_log_thread = threading.Thread(
                target=_massive_log_reader_loop,
                args=(app,),
                name="massive-log-reader",
                daemon=True,
            )
            app.state._massive_log_thread.start()

    async def event_gen():
        try:
            while True:
                try:
                    line = await asyncio.wait_for(queue.get(), timeout=25.0)
                    yield f"data: {json.dumps({'line': line})}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            with app.state.massive_log_lock:
                if queue in app.state.massive_log_queues:
                    app.state.massive_log_queues.remove(queue)

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-store",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# --- Massive WS ingest logs (scripts/systemd/run_massive_ws.py → bifrost:console:ws_massive_option) ---


@router.get("/api/massive-ws/logs")
def get_massive_ws_logs(
    request: Request,
    tail: int = Query(1000, ge=1, le=5000, description="Number of latest lines (oldest-first in response)"),
) -> Dict[str, Any]:
    """Return last N lines from Massive WS ingest Redis stream."""
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        raw = r.xrevrange(MASSIVE_WS_LOG_STREAM_KEY, count=tail)
        lines = []
        for _eid, fields in reversed(raw):
            line = _redis_stream_line(fields)
            lines.append(line)
        return {"lines": lines}
    except Exception as e:
        logger.warning("get_massive_ws_logs failed: %s", e)
        return {"lines": [], "error": str(e)}


@router.delete("/api/massive-ws/logs")
def clear_massive_ws_logs(request: Request) -> Dict[str, Any]:
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        r.delete(MASSIVE_WS_LOG_STREAM_KEY)
        return {"ok": True}
    except Exception as e:
        logger.warning("clear_massive_ws_logs failed: %s", e)
        return {"ok": False, "error": str(e)}


@router.post("/api/massive-ws/logs/trim")
def trim_massive_ws_logs(request: Request, body: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    try:
        max_lines = body.get("max_lines")
        if max_lines is None:
            return {"ok": False, "error": "max_lines required"}
        max_lines = int(max_lines)
        if max_lines < 1 or max_lines > 10000:
            return {"ok": False, "error": "max_lines must be between 1 and 10000"}
        import redis
        r = redis.from_url(daemon_log_redis_url())
        r.xtrim(MASSIVE_WS_LOG_STREAM_KEY, maxlen=max_lines, approximate=True)
        return {"ok": True}
    except Exception as e:
        logger.warning("trim_massive_ws_logs failed: %s", e)
        return {"ok": False, "error": str(e)}


@router.get("/api/massive-ws/logs/stream")
async def get_massive_ws_logs_stream(request: Request):
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        r.ping()
    except Exception as e:
        logger.warning("massive_ws_logs_stream check failed: %s", e)
        return JSONResponse(status_code=503, content={"detail": str(e)})

    app = request.app
    queue: asyncio.Queue = asyncio.Queue(maxsize=512)
    with app.state.massive_ws_log_lock:
        app.state.massive_ws_log_queues.append(queue)
        if app.state._massive_ws_log_loop is None:
            app.state._massive_ws_log_loop = asyncio.get_running_loop()
        if app.state._massive_ws_log_thread is None or not app.state._massive_ws_log_thread.is_alive():
            app.state._massive_ws_log_thread = threading.Thread(
                target=_massive_ws_log_reader_loop,
                args=(app,),
                name="massive-ws-log-reader",
                daemon=True,
            )
            app.state._massive_ws_log_thread.start()

    async def event_gen():
        try:
            while True:
                try:
                    line = await asyncio.wait_for(queue.get(), timeout=25.0)
                    yield f"data: {json.dumps({'line': line})}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            with app.state.massive_ws_log_lock:
                if queue in app.state.massive_ws_log_queues:
                    app.state.massive_ws_log_queues.remove(queue)

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-store",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# --- IB Operator logs (scripts/systemd/run_ib_operator.py → bifrost:console:ws_ib_operator) ---


@router.get("/api/ib-operator/logs")
def get_ib_operator_logs(
    request: Request,
    tail: int = Query(1000, ge=1, le=5000, description="Number of latest lines (oldest-first in response)"),
) -> Dict[str, Any]:
    """Return last N lines from IB Operator Redis stream."""
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        raw = r.xrevrange(IB_OPERATOR_LOG_STREAM_KEY, count=tail)
        lines = []
        for _eid, fields in reversed(raw):
            line = _redis_stream_line(fields)
            lines.append(line)
        return {"lines": lines}
    except Exception as e:
        logger.warning("get_ib_operator_logs failed: %s", e)
        return {"lines": [], "error": str(e)}


@router.delete("/api/ib-operator/logs")
def clear_ib_operator_logs(request: Request) -> Dict[str, Any]:
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        r.delete(IB_OPERATOR_LOG_STREAM_KEY)
        return {"ok": True}
    except Exception as e:
        logger.warning("clear_ib_operator_logs failed: %s", e)
        return {"ok": False, "error": str(e)}


@router.post("/api/ib-operator/logs/trim")
def trim_ib_operator_logs(request: Request, body: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    try:
        max_lines = body.get("max_lines")
        if max_lines is None:
            return {"ok": False, "error": "max_lines required"}
        max_lines = int(max_lines)
        if max_lines < 1 or max_lines > 10000:
            return {"ok": False, "error": "max_lines must be between 1 and 10000"}
        import redis
        r = redis.from_url(daemon_log_redis_url())
        r.xtrim(IB_OPERATOR_LOG_STREAM_KEY, maxlen=max_lines, approximate=True)
        return {"ok": True}
    except Exception as e:
        logger.warning("trim_ib_operator_logs failed: %s", e)
        return {"ok": False, "error": str(e)}


@router.get("/api/ib-operator/logs/stream")
async def get_ib_operator_logs_stream(request: Request):
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        r.ping()
    except Exception as e:
        logger.warning("ib_operator_logs_stream check failed: %s", e)
        return JSONResponse(status_code=503, content={"detail": str(e)})

    app = request.app
    queue: asyncio.Queue = asyncio.Queue(maxsize=512)
    with app.state.ib_operator_log_lock:
        app.state.ib_operator_log_queues.append(queue)
        if app.state._ib_operator_log_loop is None:
            app.state._ib_operator_log_loop = asyncio.get_running_loop()
        if app.state._ib_operator_log_thread is None or not app.state._ib_operator_log_thread.is_alive():
            app.state._ib_operator_log_thread = threading.Thread(
                target=_ib_operator_log_reader_loop,
                args=(app,),
                name="ib-operator-log-reader",
                daemon=True,
            )
            app.state._ib_operator_log_thread.start()

    async def event_gen():
        try:
            while True:
                try:
                    line = await asyncio.wait_for(queue.get(), timeout=25.0)
                    yield f"data: {json.dumps({'line': line})}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            with app.state.ib_operator_log_lock:
                if queue in app.state.ib_operator_log_queues:
                    app.state.ib_operator_log_queues.remove(queue)

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-store",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# --- IB ingestor logs (scripts/systemd/run_ib_ingestor.py → bifrost:console:ws_ib_ingestor) ---


@router.get("/api/ib-ingestor/logs")
def get_ib_ingestor_logs(
    request: Request,
    tail: int = Query(1000, ge=1, le=5000, description="Number of latest lines (oldest-first in response)"),
) -> Dict[str, Any]:
    """Return last N lines from IB ingestor Redis stream."""
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        raw = r.xrevrange(IB_INGESTOR_LOG_STREAM_KEY, count=tail)
        lines = []
        for _eid, fields in reversed(raw):
            line = _redis_stream_line(fields)
            lines.append(line)
        return {"lines": lines}
    except Exception as e:
        logger.warning("get_ib_ingestor_logs failed: %s", e)
        return {"lines": [], "error": str(e)}


@router.delete("/api/ib-ingestor/logs")
def clear_ib_ingestor_logs(request: Request) -> Dict[str, Any]:
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        r.delete(IB_INGESTOR_LOG_STREAM_KEY)
        return {"ok": True}
    except Exception as e:
        logger.warning("clear_ib_ingestor_logs failed: %s", e)
        return {"ok": False, "error": str(e)}


@router.post("/api/ib-ingestor/logs/trim")
def trim_ib_ingestor_logs(request: Request, body: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    try:
        max_lines = body.get("max_lines")
        if max_lines is None:
            return {"ok": False, "error": "max_lines required"}
        max_lines = int(max_lines)
        if max_lines < 1 or max_lines > 10000:
            return {"ok": False, "error": "max_lines must be between 1 and 10000"}
        import redis
        r = redis.from_url(daemon_log_redis_url())
        r.xtrim(IB_INGESTOR_LOG_STREAM_KEY, maxlen=max_lines, approximate=True)
        return {"ok": True}
    except Exception as e:
        logger.warning("trim_ib_ingestor_logs failed: %s", e)
        return {"ok": False, "error": str(e)}


@router.get("/api/ib-ingestor/logs/stream")
async def get_ib_ingestor_logs_stream(request: Request):
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        r.ping()
    except Exception as e:
        logger.warning("ib_ingestor_logs_stream check failed: %s", e)
        return JSONResponse(status_code=503, content={"detail": str(e)})

    app = request.app
    queue: asyncio.Queue = asyncio.Queue(maxsize=512)
    with app.state.ib_ingestor_log_lock:
        app.state.ib_ingestor_log_queues.append(queue)
        if app.state._ib_ingestor_log_loop is None:
            app.state._ib_ingestor_log_loop = asyncio.get_running_loop()
        if app.state._ib_ingestor_log_thread is None or not app.state._ib_ingestor_log_thread.is_alive():
            app.state._ib_ingestor_log_thread = threading.Thread(
                target=_ib_ingestor_log_reader_loop,
                args=(app,),
                name="ib-ingestor-log-reader",
                daemon=True,
            )
            app.state._ib_ingestor_log_thread.start()

    async def event_gen():
        try:
            while True:
                try:
                    line = await asyncio.wait_for(queue.get(), timeout=25.0)
                    yield f"data: {json.dumps({'line': line})}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            with app.state.ib_ingestor_log_lock:
                if queue in app.state.ib_ingestor_log_queues:
                    app.state.ib_ingestor_log_queues.remove(queue)

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-store",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# --- IB Account Agent logs (scripts/systemd/run_ib_account_agent.py → bifrost:console:ws_ib_account_agent) ---


@router.get("/api/ib-account-agent/logs")
def get_ib_account_agent_logs(
    request: Request,
    tail: int = Query(1000, ge=1, le=5000, description="Number of latest lines (oldest-first in response)"),
) -> Dict[str, Any]:
    """Return last N lines from IB Account Agent Redis stream."""
    try:
        import redis

        r = redis.from_url(daemon_log_redis_url())
        raw = r.xrevrange(IB_ACCOUNT_AGENT_LOG_STREAM_KEY, count=tail)
        lines = []
        for _eid, fields in reversed(raw):
            line = _redis_stream_line(fields)
            lines.append(line)
        return {"lines": lines}
    except Exception as e:
        logger.warning("get_ib_account_agent_logs failed: %s", e)
        return {"lines": [], "error": str(e)}


@router.delete("/api/ib-account-agent/logs")
def clear_ib_account_agent_logs(request: Request) -> Dict[str, Any]:
    try:
        import redis

        r = redis.from_url(daemon_log_redis_url())
        r.delete(IB_ACCOUNT_AGENT_LOG_STREAM_KEY)
        return {"ok": True}
    except Exception as e:
        logger.warning("clear_ib_account_agent_logs failed: %s", e)
        return {"ok": False, "error": str(e)}


@router.post("/api/ib-account-agent/logs/trim")
def trim_ib_account_agent_logs(request: Request, body: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    try:
        max_lines = body.get("max_lines")
        if max_lines is None:
            return {"ok": False, "error": "max_lines required"}
        max_lines = int(max_lines)
        if max_lines < 1 or max_lines > 10000:
            return {"ok": False, "error": "max_lines must be between 1 and 10000"}
        import redis

        r = redis.from_url(daemon_log_redis_url())
        r.xtrim(IB_ACCOUNT_AGENT_LOG_STREAM_KEY, maxlen=max_lines, approximate=True)
        return {"ok": True}
    except Exception as e:
        logger.warning("trim_ib_account_agent_logs failed: %s", e)
        return {"ok": False, "error": str(e)}


@router.get("/api/ib-account-agent/logs/stream")
async def get_ib_account_agent_logs_stream(request: Request):
    try:
        import redis

        r = redis.from_url(daemon_log_redis_url())
        r.ping()
    except Exception as e:
        logger.warning("ib_account_agent_logs_stream check failed: %s", e)
        return JSONResponse(status_code=503, content={"detail": str(e)})

    app = request.app
    queue: asyncio.Queue = asyncio.Queue(maxsize=512)
    with app.state.ib_account_agent_log_lock:
        app.state.ib_account_agent_log_queues.append(queue)
        if app.state._ib_account_agent_log_loop is None:
            app.state._ib_account_agent_log_loop = asyncio.get_running_loop()
        if app.state._ib_account_agent_log_thread is None or not app.state._ib_account_agent_log_thread.is_alive():
            app.state._ib_account_agent_log_thread = threading.Thread(
                target=_ib_account_agent_log_reader_loop,
                args=(app,),
                name="ib-account-agent-log-reader",
                daemon=True,
            )
            app.state._ib_account_agent_log_thread.start()

    async def event_gen():
        try:
            while True:
                try:
                    line = await asyncio.wait_for(queue.get(), timeout=25.0)
                    yield f"data: {json.dumps({'line': line})}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            with app.state.ib_account_agent_log_lock:
                if queue in app.state.ib_account_agent_log_queues:
                    app.state.ib_account_agent_log_queues.remove(queue)

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-store",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# --- Account Sync Daemon logs (scripts/systemd/run_account_sync_daemon.py → Redis stream bifrost:console:account_sync_daemon) ---


@router.get("/api/account-sync-daemon/logs")
def get_account_sync_daemon_logs(
    request: Request,
    tail: int = Query(1000, ge=1, le=5000, description="Number of latest lines (oldest-first in response)"),
) -> Dict[str, Any]:
    """Return last N lines from Account Sync Daemon console Redis stream."""
    try:
        import redis

        r = redis.from_url(daemon_log_redis_url(), decode_responses=True)
        raw = r.xrevrange(ACCOUNT_SYNC_DAEMON_LOG_STREAM_KEY, max="+", min="-", count=tail)
        lines: List[str] = []
        for _eid, fields in reversed(raw):
            lines.append(_redis_stream_line(fields))
        return {"lines": lines}
    except Exception as e:
        logger.warning("get_account_sync_daemon_logs failed: %s", e)
        return {"lines": [], "error": str(e)}


@router.delete("/api/account-sync-daemon/logs")
def clear_account_sync_daemon_logs(request: Request) -> Dict[str, Any]:
    """Delete Account Sync Daemon console Redis stream (UI Clear)."""
    try:
        import redis

        r = redis.from_url(daemon_log_redis_url(), decode_responses=True)
        r.delete(ACCOUNT_SYNC_DAEMON_LOG_STREAM_KEY)
        return {"ok": True}
    except Exception as e:
        logger.warning("clear_account_sync_daemon_logs failed: %s", e)
        return {"ok": False, "error": str(e)}


def _account_sync_daemon_log_reader_loop(app_ref) -> None:
    """Background thread: XREAD Redis stream for Account Sync Daemon console."""
    try:
        import redis

        r = redis.from_url(daemon_log_redis_url())
        last_id = "$"
        while True:
            try:
                result = r.xread(
                    block=5000,
                    streams={ACCOUNT_SYNC_DAEMON_LOG_STREAM_KEY: last_id},
                    count=100,
                )
                if not result:
                    continue
                for _stream_name, entries in result:
                    for eid, fields in entries:
                        last_id = eid
                        line = _redis_stream_line(fields)
                        with app_ref.state.account_sync_daemon_log_lock:
                            queues = list(app_ref.state.account_sync_daemon_log_queues)
                        loop = getattr(app_ref.state, "_account_sync_daemon_log_loop", None)
                        for q in queues:
                            if loop and not loop.is_closed():
                                loop.call_soon_threadsafe(put_nowait_drop_oldest, q, line)
            except redis.ConnectionError:
                time.sleep(2)
            except Exception as e:
                logger.debug("account_sync_daemon_log_reader_loop: %s", e)
    except Exception as e:
        logger.warning("account_sync_daemon_log_reader_loop fatal: %s", e)


@router.get("/api/account-sync-daemon/logs/stream")
async def get_account_sync_daemon_logs_stream(request: Request):
    try:
        import redis

        r = redis.from_url(daemon_log_redis_url())
        r.ping()
    except Exception as e:
        logger.warning("account_sync_daemon_logs_stream check failed: %s", e)
        return JSONResponse(status_code=503, content={"detail": str(e)})

    app = request.app
    queue: asyncio.Queue = asyncio.Queue(maxsize=512)
    with app.state.account_sync_daemon_log_lock:
        app.state.account_sync_daemon_log_queues.append(queue)
        if app.state._account_sync_daemon_log_loop is None:
            app.state._account_sync_daemon_log_loop = asyncio.get_running_loop()
        if app.state._account_sync_daemon_log_thread is None or not app.state._account_sync_daemon_log_thread.is_alive():
            app.state._account_sync_daemon_log_thread = threading.Thread(
                target=_account_sync_daemon_log_reader_loop,
                args=(app,),
                name="account-sync-daemon-log-reader",
                daemon=True,
            )
            app.state._account_sync_daemon_log_thread.start()

    async def event_gen():
        try:
            while True:
                try:
                    line = await asyncio.wait_for(queue.get(), timeout=25.0)
                    yield f"data: {json.dumps({'line': line})}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            with app.state.account_sync_daemon_log_lock:
                if queue in app.state.account_sync_daemon_log_queues:
                    app.state.account_sync_daemon_log_queues.remove(queue)

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-store",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# --- Docs API server logs (run_server_docs.py → Redis stream bifrost:console:{dev|prod}:api_docs) ---


@router.get("/api/docs/logs")
def get_docs_logs(
    request: Request,
    tail: int = Query(1000, ge=1, le=5000, description="Number of latest lines (oldest-first in response)"),
) -> Dict[str, Any]:
    """Return last N lines from Docs API console Redis stream."""
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        key = getattr(request.app.state, "docs_log_stream_key", None) or "bifrost:console:dev:api_docs"
        raw = r.xrevrange(key, count=tail)
        lines = []
        for _eid, fields in reversed(raw):
            line = _redis_stream_line(fields)
            lines.append(line)
        return {"lines": lines}
    except Exception as e:
        logger.warning("get_docs_logs failed: %s", e)
        return {"lines": [], "error": str(e)}


@router.delete("/api/docs/logs")
def clear_docs_logs(request: Request) -> Dict[str, Any]:
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        key = getattr(request.app.state, "docs_log_stream_key", None) or "bifrost:console:dev:api_docs"
        r.delete(key)
        return {"ok": True}
    except Exception as e:
        logger.warning("clear_docs_logs failed: %s", e)
        return {"ok": False, "error": str(e)}


@router.post("/api/docs/logs/trim")
def trim_docs_logs(request: Request, body: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    try:
        max_lines = body.get("max_lines")
        if max_lines is None:
            return {"ok": False, "error": "max_lines required"}
        max_lines = int(max_lines)
        if max_lines < 1 or max_lines > 10000:
            return {"ok": False, "error": "max_lines must be between 1 and 10000"}
        import redis
        r = redis.from_url(daemon_log_redis_url())
        key = getattr(request.app.state, "docs_log_stream_key", None) or "bifrost:console:dev:api_docs"
        r.xtrim(key, maxlen=max_lines, approximate=True)
        return {"ok": True}
    except Exception as e:
        logger.warning("trim_docs_logs failed: %s", e)
        return {"ok": False, "error": str(e)}


@router.get("/api/docs/logs/stream")
async def get_docs_logs_stream(request: Request):
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        r.ping()
    except Exception as e:
        logger.warning("docs_logs_stream check failed: %s", e)
        return JSONResponse(status_code=503, content={"detail": str(e)})

    app = request.app
    queue: asyncio.Queue = asyncio.Queue(maxsize=512)
    with app.state.docs_log_lock:
        app.state.docs_log_queues.append(queue)
        if app.state._docs_log_loop is None:
            app.state._docs_log_loop = asyncio.get_running_loop()
        if app.state._docs_log_thread is None or not app.state._docs_log_thread.is_alive():
            app.state._docs_log_thread = threading.Thread(
                target=_docs_log_reader_loop,
                args=(app,),
                name="docs-log-reader",
                daemon=True,
            )
            app.state._docs_log_thread.start()

    async def event_gen():
        try:
            while True:
                try:
                    line = await asyncio.wait_for(queue.get(), timeout=25.0)
                    yield f"data: {json.dumps({'line': line})}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            with app.state.docs_log_lock:
                if queue in app.state.docs_log_queues:
                    app.state.docs_log_queues.remove(queue)

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-store",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# --- Celery logs ---


# --- Ops API server logs (run_server_ops.py -> Redis stream bifrost:console:{dev|prod}:api_ops) ---


@router.get("/api/ops/logs")
def get_ops_logs(
    request: Request,
    tail: int = Query(1000, ge=1, le=5000, description="Number of latest lines (oldest-first in response)"),
) -> Dict[str, Any]:
    """Return last N lines from Ops API console Redis stream."""
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        key = getattr(request.app.state, "ops_log_stream_key", None) or "bifrost:console:dev:api_ops"
        raw = r.xrevrange(key, count=tail)
        lines = []
        for _eid, fields in reversed(raw):
            line = _redis_stream_line(fields)
            lines.append(line)
        return {"lines": lines}
    except Exception as e:
        logger.warning("get_ops_logs failed: %s", e)
        return {"lines": [], "error": str(e)}


@router.delete("/api/ops/logs")
def clear_ops_logs(request: Request) -> Dict[str, Any]:
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        key = getattr(request.app.state, "ops_log_stream_key", None) or "bifrost:console:dev:api_ops"
        r.delete(key)
        return {"ok": True}
    except Exception as e:
        logger.warning("clear_ops_logs failed: %s", e)
        return {"ok": False, "error": str(e)}


@router.post("/api/ops/logs/trim")
def trim_ops_logs(request: Request, body: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    try:
        max_lines = body.get("max_lines")
        if max_lines is None:
            return {"ok": False, "error": "max_lines required"}
        max_lines = int(max_lines)
        if max_lines < 1 or max_lines > 10000:
            return {"ok": False, "error": "max_lines must be between 1 and 10000"}
        import redis
        r = redis.from_url(daemon_log_redis_url())
        key = getattr(request.app.state, "ops_log_stream_key", None) or "bifrost:console:dev:api_ops"
        r.xtrim(key, maxlen=max_lines, approximate=True)
        return {"ok": True}
    except Exception as e:
        logger.warning("trim_ops_logs failed: %s", e)
        return {"ok": False, "error": str(e)}


@router.get("/api/ops/logs/stream")
async def get_ops_logs_stream(request: Request):
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        r.ping()
    except Exception as e:
        logger.warning("ops_logs_stream check failed: %s", e)
        return JSONResponse(status_code=503, content={"detail": str(e)})

    app = request.app
    queue: asyncio.Queue = asyncio.Queue(maxsize=512)
    with app.state.ops_log_lock:
        app.state.ops_log_queues.append(queue)
        if app.state._ops_log_loop is None:
            app.state._ops_log_loop = asyncio.get_running_loop()
        if app.state._ops_log_thread is None or not app.state._ops_log_thread.is_alive():
            app.state._ops_log_thread = threading.Thread(
                target=_ops_log_reader_loop,
                args=(app,),
                name="ops-log-reader",
                daemon=True,
            )
            app.state._ops_log_thread.start()

    async def event_gen():
        try:
            while True:
                try:
                    line = await asyncio.wait_for(queue.get(), timeout=25.0)
                    yield f"data: {json.dumps({'line': line})}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            with app.state.ops_log_lock:
                if queue in app.state.ops_log_queues:
                    app.state.ops_log_queues.remove(queue)

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-store",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# --- Trading API server logs (run_server_trading.py → bifrost:console:{dev|prod}:api_trading) ---


@router.get("/api/trading/logs")
def get_trading_logs(
    tail: int = Query(1000, ge=1, le=5000, description="Number of latest lines (oldest-first in response)"),
) -> Dict[str, Any]:
    """Return last N lines from Trading API console Redis streams (dev + prod keys merged)."""
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        keys = _trading_console_stream_keys_dual()
        lines, partial_err = _merged_console_tail_from_keys(r, keys, tail)
        out: Dict[str, Any] = {"lines": lines}
        if partial_err:
            out["error"] = partial_err
        return out
    except Exception as e:
        logger.warning("get_trading_logs failed: %s", e)
        return {"lines": [], "error": str(e)}


@router.delete("/api/trading/logs")
def clear_trading_logs(request: Request) -> Dict[str, Any]:
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        for key in _trading_console_stream_keys_dual():
            r.delete(key)
        return {"ok": True}
    except Exception as e:
        logger.warning("clear_trading_logs failed: %s", e)
        return {"ok": False, "error": str(e)}


@router.post("/api/trading/logs/trim")
def trim_trading_logs(request: Request, body: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    try:
        max_lines = body.get("max_lines")
        if max_lines is None:
            return {"ok": False, "error": "max_lines required"}
        max_lines = int(max_lines)
        if max_lines < 1 or max_lines > 10000:
            return {"ok": False, "error": "max_lines must be between 1 and 10000"}
        import redis
        r = redis.from_url(daemon_log_redis_url())
        for key in _trading_console_stream_keys_dual():
            r.xtrim(key, maxlen=max_lines, approximate=True)
        return {"ok": True}
    except Exception as e:
        logger.warning("trim_trading_logs failed: %s", e)
        return {"ok": False, "error": str(e)}


@router.get("/api/trading/logs/stream")
async def get_trading_logs_stream(request: Request):
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        r.ping()
    except Exception as e:
        logger.warning("trading_logs_stream check failed: %s", e)
        return JSONResponse(status_code=503, content={"detail": str(e)})

    app = request.app
    queue: asyncio.Queue = asyncio.Queue(maxsize=512)
    with app.state.trading_log_lock:
        app.state.trading_log_queues.append(queue)
        if app.state._trading_log_loop is None:
            app.state._trading_log_loop = asyncio.get_running_loop()
        if app.state._trading_log_thread is None or not app.state._trading_log_thread.is_alive():
            app.state._trading_log_thread = threading.Thread(
                target=_trading_log_reader_loop,
                args=(app,),
                name="trading-log-reader",
                daemon=True,
            )
            app.state._trading_log_thread.start()

    async def event_gen():
        try:
            while True:
                try:
                    line = await asyncio.wait_for(queue.get(), timeout=25.0)
                    yield f"data: {json.dumps({'line': line})}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            with app.state.trading_log_lock:
                if queue in app.state.trading_log_queues:
                    app.state.trading_log_queues.remove(queue)

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-store",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# --- Portfolio API server logs (run_server_portfolio.py → bifrost:console:{dev|prod}:api_portfolio) ---


@router.get("/api/portfolio/logs")
def get_portfolio_logs(
    tail: int = Query(1000, ge=1, le=5000, description="Number of latest lines (oldest-first in response)"),
) -> Dict[str, Any]:
    """Return last N lines from Portfolio API console Redis streams (dev + prod keys merged)."""
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        keys = _portfolio_console_stream_keys_dual()
        lines, partial_err = _merged_console_tail_from_keys(r, keys, tail)
        out: Dict[str, Any] = {"lines": lines}
        if partial_err:
            out["error"] = partial_err
        return out
    except Exception as e:
        logger.warning("get_portfolio_logs failed: %s", e)
        return {"lines": [], "error": str(e)}


@router.delete("/api/portfolio/logs")
def clear_portfolio_logs(request: Request) -> Dict[str, Any]:
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        for key in _portfolio_console_stream_keys_dual():
            r.delete(key)
        return {"ok": True}
    except Exception as e:
        logger.warning("clear_portfolio_logs failed: %s", e)
        return {"ok": False, "error": str(e)}


@router.post("/api/portfolio/logs/trim")
def trim_portfolio_logs(request: Request, body: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    try:
        max_lines = body.get("max_lines")
        if max_lines is None:
            return {"ok": False, "error": "max_lines required"}
        max_lines = int(max_lines)
        if max_lines < 1 or max_lines > 10000:
            return {"ok": False, "error": "max_lines must be between 1 and 10000"}
        import redis
        r = redis.from_url(daemon_log_redis_url())
        for key in _portfolio_console_stream_keys_dual():
            r.xtrim(key, maxlen=max_lines, approximate=True)
        return {"ok": True}
    except Exception as e:
        logger.warning("trim_portfolio_logs failed: %s", e)
        return {"ok": False, "error": str(e)}


@router.get("/api/portfolio/logs/stream")
async def get_portfolio_logs_stream(request: Request):
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        r.ping()
    except Exception as e:
        logger.warning("portfolio_logs_stream check failed: %s", e)
        return JSONResponse(status_code=503, content={"detail": str(e)})

    app = request.app
    queue: asyncio.Queue = asyncio.Queue(maxsize=512)
    with app.state.portfolio_log_lock:
        app.state.portfolio_log_queues.append(queue)
        if app.state._portfolio_log_loop is None:
            app.state._portfolio_log_loop = asyncio.get_running_loop()
        if app.state._portfolio_log_thread is None or not app.state._portfolio_log_thread.is_alive():
            app.state._portfolio_log_thread = threading.Thread(
                target=_portfolio_log_reader_loop,
                args=(app,),
                name="portfolio-log-reader",
                daemon=True,
            )
            app.state._portfolio_log_thread.start()

    async def event_gen():
        try:
            while True:
                try:
                    line = await asyncio.wait_for(queue.get(), timeout=25.0)
                    yield f"data: {json.dumps({'line': line})}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            with app.state.portfolio_log_lock:
                if queue in app.state.portfolio_log_queues:
                    app.state.portfolio_log_queues.remove(queue)

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-store",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# --- Research API server logs (run_server_research.py → bifrost:console:{dev|prod}:api_research) ---


@router.get("/api/research/logs")
def get_research_logs(
    tail: int = Query(1000, ge=1, le=5000, description="Number of latest lines (oldest-first in response)"),
) -> Dict[str, Any]:
    """Return last N lines from Research API console Redis streams (dev + prod keys merged)."""
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        keys = _research_console_stream_keys_dual()
        lines, partial_err = _merged_console_tail_from_keys(r, keys, tail)
        out: Dict[str, Any] = {"lines": lines}
        if partial_err:
            out["error"] = partial_err
        return out
    except Exception as e:
        logger.warning("get_research_logs failed: %s", e)
        return {"lines": [], "error": str(e)}


@router.delete("/api/research/logs")
def clear_research_logs(request: Request) -> Dict[str, Any]:
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        for key in _research_console_stream_keys_dual():
            r.delete(key)
        return {"ok": True}
    except Exception as e:
        logger.warning("clear_research_logs failed: %s", e)
        return {"ok": False, "error": str(e)}


@router.post("/api/research/logs/trim")
def trim_research_logs(request: Request, body: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    try:
        max_lines = body.get("max_lines")
        if max_lines is None:
            return {"ok": False, "error": "max_lines required"}
        max_lines = int(max_lines)
        if max_lines < 1 or max_lines > 10000:
            return {"ok": False, "error": "max_lines must be between 1 and 10000"}
        import redis
        r = redis.from_url(daemon_log_redis_url())
        for key in _research_console_stream_keys_dual():
            r.xtrim(key, maxlen=max_lines, approximate=True)
        return {"ok": True}
    except Exception as e:
        logger.warning("trim_research_logs failed: %s", e)
        return {"ok": False, "error": str(e)}


@router.get("/api/research/logs/stream")
async def get_research_logs_stream(request: Request):
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        r.ping()
    except Exception as e:
        logger.warning("research_logs_stream check failed: %s", e)
        return JSONResponse(status_code=503, content={"detail": str(e)})

    app = request.app
    queue: asyncio.Queue = asyncio.Queue(maxsize=512)
    with app.state.research_log_lock:
        app.state.research_log_queues.append(queue)
        if app.state._research_log_loop is None:
            app.state._research_log_loop = asyncio.get_running_loop()
        if app.state._research_log_thread is None or not app.state._research_log_thread.is_alive():
            app.state._research_log_thread = threading.Thread(
                target=_research_log_reader_loop,
                args=(app,),
                name="research-log-reader",
                daemon=True,
            )
            app.state._research_log_thread.start()

    async def event_gen():
        try:
            while True:
                try:
                    line = await asyncio.wait_for(queue.get(), timeout=25.0)
                    yield f"data: {json.dumps({'line': line})}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            with app.state.research_log_lock:
                if queue in app.state.research_log_queues:
                    app.state.research_log_queues.remove(queue)

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-store",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# --- Strategy API server logs (run_server_strategy.py → bifrost:console:{dev|prod}:api_strategy) ---


@router.get("/api/strategy/logs")
def get_strategy_logs(
    tail: int = Query(1000, ge=1, le=5000, description="Number of latest lines (oldest-first in response)"),
) -> Dict[str, Any]:
    """Return last N lines from Strategy API console Redis streams (dev + prod keys merged)."""
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        keys = _strategy_console_stream_keys_dual()
        lines, partial_err = _merged_console_tail_from_keys(r, keys, tail)
        out: Dict[str, Any] = {"lines": lines}
        if partial_err:
            out["error"] = partial_err
        return out
    except Exception as e:
        logger.warning("get_strategy_logs failed: %s", e)
        return {"lines": [], "error": str(e)}


@router.delete("/api/strategy/logs")
def clear_strategy_logs(request: Request) -> Dict[str, Any]:
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        for key in _strategy_console_stream_keys_dual():
            r.delete(key)
        return {"ok": True}
    except Exception as e:
        logger.warning("clear_strategy_logs failed: %s", e)
        return {"ok": False, "error": str(e)}


@router.post("/api/strategy/logs/trim")
def trim_strategy_logs(request: Request, body: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    try:
        max_lines = body.get("max_lines")
        if max_lines is None:
            return {"ok": False, "error": "max_lines required"}
        max_lines = int(max_lines)
        if max_lines < 1 or max_lines > 10000:
            return {"ok": False, "error": "max_lines must be between 1 and 10000"}
        import redis
        r = redis.from_url(daemon_log_redis_url())
        for key in _strategy_console_stream_keys_dual():
            r.xtrim(key, maxlen=max_lines, approximate=True)
        return {"ok": True}
    except Exception as e:
        logger.warning("trim_strategy_logs failed: %s", e)
        return {"ok": False, "error": str(e)}


@router.get("/api/strategy/logs/stream")
async def get_strategy_logs_stream(request: Request):
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        r.ping()
    except Exception as e:
        logger.warning("strategy_logs_stream check failed: %s", e)
        return JSONResponse(status_code=503, content={"detail": str(e)})

    app = request.app
    queue: asyncio.Queue = asyncio.Queue(maxsize=512)
    with app.state.strategy_log_lock:
        app.state.strategy_log_queues.append(queue)
        if app.state._strategy_log_loop is None:
            app.state._strategy_log_loop = asyncio.get_running_loop()
        if app.state._strategy_log_thread is None or not app.state._strategy_log_thread.is_alive():
            app.state._strategy_log_thread = threading.Thread(
                target=_strategy_log_reader_loop,
                args=(app,),
                name="strategy-log-reader",
                daemon=True,
            )
            app.state._strategy_log_thread.start()

    async def event_gen():
        try:
            while True:
                try:
                    line = await asyncio.wait_for(queue.get(), timeout=25.0)
                    yield f"data: {json.dumps({'line': line})}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            with app.state.strategy_log_lock:
                if queue in app.state.strategy_log_queues:
                    app.state.strategy_log_queues.remove(queue)

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-store",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# --- Market API server logs (run_server_market.py → bifrost:console:{dev|prod}:api_market) ---


@router.get("/api/market/logs")
def get_market_logs(
    tail: int = Query(1000, ge=1, le=5000, description="Number of latest lines (oldest-first in response)"),
) -> Dict[str, Any]:
    """Return last N lines from Market API console Redis streams (dev + prod keys merged)."""
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        keys = _market_console_stream_keys_dual()
        lines, partial_err = _merged_console_tail_from_keys(r, keys, tail)
        out: Dict[str, Any] = {"lines": lines}
        if partial_err:
            out["error"] = partial_err
        return out
    except Exception as e:
        logger.warning("get_market_logs failed: %s", e)
        return {"lines": [], "error": str(e)}


@router.delete("/api/market/logs")
def clear_market_logs(request: Request) -> Dict[str, Any]:
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        for key in _market_console_stream_keys_dual():
            r.delete(key)
        return {"ok": True}
    except Exception as e:
        logger.warning("clear_market_logs failed: %s", e)
        return {"ok": False, "error": str(e)}


@router.post("/api/market/logs/trim")
def trim_market_logs(request: Request, body: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    try:
        max_lines = body.get("max_lines")
        if max_lines is None:
            return {"ok": False, "error": "max_lines required"}
        max_lines = int(max_lines)
        if max_lines < 1 or max_lines > 10000:
            return {"ok": False, "error": "max_lines must be between 1 and 10000"}
        import redis
        r = redis.from_url(daemon_log_redis_url())
        for key in _market_console_stream_keys_dual():
            r.xtrim(key, maxlen=max_lines, approximate=True)
        return {"ok": True}
    except Exception as e:
        logger.warning("trim_market_logs failed: %s", e)
        return {"ok": False, "error": str(e)}


@router.get("/api/market/logs/stream")
async def get_market_logs_stream(request: Request):
    try:
        import redis
        r = redis.from_url(daemon_log_redis_url())
        r.ping()
    except Exception as e:
        logger.warning("market_logs_stream check failed: %s", e)
        return JSONResponse(status_code=503, content={"detail": str(e)})

    app = request.app
    queue: asyncio.Queue = asyncio.Queue(maxsize=512)
    with app.state.market_log_lock:
        app.state.market_log_queues.append(queue)
        if app.state._market_log_loop is None:
            app.state._market_log_loop = asyncio.get_running_loop()
        if app.state._market_log_thread is None or not app.state._market_log_thread.is_alive():
            app.state._market_log_thread = threading.Thread(
                target=_market_log_reader_loop,
                args=(app,),
                name="market-log-reader",
                daemon=True,
            )
            app.state._market_log_thread.start()

    async def event_gen():
        try:
            while True:
                try:
                    line = await asyncio.wait_for(queue.get(), timeout=25.0)
                    yield f"data: {json.dumps({'line': line})}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            with app.state.market_log_lock:
                if queue in app.state.market_log_queues:
                    app.state.market_log_queues.remove(queue)

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-store",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )

