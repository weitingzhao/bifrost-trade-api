"""System message center APIs backed by Redis stream + TTL items."""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from typing import Any, Dict, List

from fastapi import APIRouter, Query, Request
from fastapi.responses import StreamingResponse

from bifrost_api.monitor.routers.deps import daemon_log_redis_url
from bifrost_core.core.message_center import (
    MESSAGE_CENTER_MONITOR_CONSUMER,
    consumer_last_id,
    fetch_materialized_messages,
    materialize_stream_event,
    parse_system_message_event,
    read_stream_events,
    set_consumer_last_id,
)
from bifrost_core.sse.queue_utils import put_nowait_drop_oldest

logger = logging.getLogger(__name__)

router = APIRouter(tags=["messages"])


def _message_center_reader_loop(app_ref: Any) -> None:
    """Consume the Redis stream, materialize events, and fan-out to SSE queues.

    The blocking XREAD runs *without* any shared lock so that GET /api/messages
    (which only reads already-materialized items) is never blocked.
    """
    try:
        import redis as _redis

        r = _redis.from_url(daemon_log_redis_url(), decode_responses=True)
        last_id = consumer_last_id(r, MESSAGE_CENTER_MONITOR_CONSUMER)
        while True:
            try:
                entries = read_stream_events(r, last_id=last_id, block_ms=5000, count=100)
                if not entries:
                    continue
                messages: List[Dict[str, Any]] = []
                newest_id = last_id
                for stream_id, fields in entries:
                    newest_id = stream_id
                    event = parse_system_message_event(fields)
                    if event is None:
                        continue
                    body = materialize_stream_event(r, event)
                    if body is not None:
                        messages.append(body)
                if newest_id != last_id:
                    last_id = newest_id
                    set_consumer_last_id(r, MESSAGE_CENTER_MONITOR_CONSUMER, last_id)
                if not messages:
                    continue
                with app_ref.state.system_message_queue_lock:
                    queues = list(app_ref.state.system_message_queues)
                loop = getattr(app_ref.state, "_system_message_loop", None)
                for message in messages:
                    for q in queues:
                        if loop and not loop.is_closed():
                            loop.call_soon_threadsafe(put_nowait_drop_oldest, q, message)
            except _redis.ConnectionError:
                time.sleep(2)
            except Exception as e:
                logger.debug("message_center_reader_loop: %s", e)
    except Exception as e:
        logger.warning("message_center_reader_loop exited: %s", e)


def _ensure_message_center_reader_started(request: Request) -> None:
    app = request.app
    with app.state.system_message_queue_lock:
        if app.state._system_message_thread is not None and app.state._system_message_thread.is_alive():
            return
        app.state._system_message_loop = asyncio.get_running_loop()
        app.state._system_message_thread = threading.Thread(
            target=_message_center_reader_loop,
            args=(app,),
            name="monitor-system-message-reader",
            daemon=True,
        )
        app.state._system_message_thread.start()


@router.get("/api/messages")
def get_system_messages(
    request: Request,
    limit: int = Query(20, ge=1, le=100, description="Number of newest messages"),
) -> Dict[str, List[Dict[str, Any]]]:
    """Return recent materialized messages.  No lock — the reader thread
    handles stream consumption; this endpoint only reads TTL-backed items."""
    try:
        import redis

        r = redis.from_url(daemon_log_redis_url(), decode_responses=True)
        messages = fetch_materialized_messages(r, limit=limit)
        return {"messages": messages}
    except Exception as e:
        logger.warning("get_system_messages failed: %s", e)
        return {"messages": []}


@router.get("/api/messages/stream")
async def stream_system_messages(request: Request) -> StreamingResponse:
    _ensure_message_center_reader_started(request)
    queue: asyncio.Queue = asyncio.Queue(maxsize=100)
    with request.app.state.system_message_queue_lock:
        request.app.state.system_message_queues.append(queue)

    async def event_gen():
        try:
            while True:
                try:
                    message = await asyncio.wait_for(queue.get(), timeout=25.0)
                    yield f"data: {json.dumps(message)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            with request.app.state.system_message_queue_lock:
                if queue in request.app.state.system_message_queues:
                    request.app.state.system_message_queues.remove(queue)

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-store",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
