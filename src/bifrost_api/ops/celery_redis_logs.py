"""Celery worker console: Redis Stream XREAD (same keys as workers writing via src.workers.celery_app)."""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)


def celery_log_reader_single_stream(
    app_ref: Any,
    broker_url: str,
    stream_key: str,
    queue: asyncio.Queue,
    stop: threading.Event,
) -> None:
    """Background thread: XREAD one Celery console stream; push lines to one SSE queue until stop."""
    try:
        import redis
    except ImportError:
        logger.warning("celery_log_reader_single_stream: redis not installed")
        return

    try:
        r = redis.from_url(broker_url)
        last_id = "$"
        while not stop.is_set():
            try:
                result = r.xread(block=5000, streams={stream_key: last_id}, count=100)
                if not result:
                    continue
                for _stream_name, entries in result:
                    for eid, fields in entries:
                        if stop.is_set():
                            return
                        last_id = eid
                        line = (fields.get(b"line") or fields.get("line") or b"").decode(
                            "utf-8", errors="replace"
                        )
                        loop = getattr(app_ref.state, "_celery_log_loop", None)
                        if loop and not loop.is_closed():
                            from bifrost_core.sse.queue_utils import put_nowait_drop_oldest

                            loop.call_soon_threadsafe(put_nowait_drop_oldest, queue, line)
            except redis.ConnectionError:
                time.sleep(2)
            except Exception as e:
                logger.debug("celery_log_reader_single_stream: %s", e)
    except Exception as e:
        logger.warning("celery_log_reader_single_stream exited: %s", e)
