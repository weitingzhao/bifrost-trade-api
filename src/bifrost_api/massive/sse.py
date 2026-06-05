"""Redis Pub/Sub bridge for Massive option SSE (massive:channel → Status Server queues)."""

from __future__ import annotations

import json
import logging
import threading
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger(__name__)

MASSIVE_SSE_CHANNEL = "massive:channel"
MASSIVE_KEY_PREFIX = "massive:"


def _summarize_contract_blob(contract_key: str, blob: Dict[str, Any]) -> Dict[str, Any]:
    """Map WS/ingest JSON to a small SSE payload."""
    ev = blob.get("ev") or ""
    out: Dict[str, Any] = {"contract_key": contract_key, "ev": ev}
    for k in ("mid", "iv", "delta", "gamma", "theta", "vega", "bid", "ask", "last", "open_interest", "oi"):
        if k in blob and blob[k] is not None:
            out[k] = blob[k]
    t = blob.get("t") or blob.get("s")
    if t is not None:
        out["t"] = t
    return out


def run_massive_channel_subscribe_loop(
    redis_url: str,
    stop_event: threading.Event,
    broadcast: Callable[[Dict[str, Any]], None],
) -> None:
    """Blocking loop: SUBSCRIBE massive:channel; on message load massive:{contract_key}, broadcast summary."""
    try:
        import redis
    except ImportError:
        logger.warning("redis package not installed; Massive SSE subscriber not started")
        return

    try:
        r = redis.from_url(redis_url, decode_responses=True)
        pubsub = r.pubsub(ignore_subscribe_messages=True)
        pubsub.subscribe(MASSIVE_SSE_CHANNEL)
        logger.info("Massive SSE: subscribed to Redis %s", MASSIVE_SSE_CHANNEL)
    except Exception as e:
        logger.warning("Massive SSE: subscribe failed: %s", e)
        return

    while not stop_event.is_set():
        try:
            msg = pubsub.get_message(timeout=1.0)
        except Exception as e:
            logger.debug("Massive SSE get_message: %s", e)
            continue
        if not msg or msg.get("type") != "message":
            continue
        raw = msg.get("data")
        if raw is None:
            continue
        contract_key = raw if isinstance(raw, str) else str(raw)
        if not contract_key.strip():
            continue
        try:
            payload_raw = r.get(f"{MASSIVE_KEY_PREFIX}{contract_key}")
            if not payload_raw:
                broadcast({"contract_key": contract_key, "ev": "?", "note": "no_redis_key"})
                continue
            if isinstance(payload_raw, bytes):
                payload_raw = payload_raw.decode("utf-8", errors="replace")
            blob = json.loads(payload_raw)
            if not isinstance(blob, dict):
                blob = {}
            broadcast(_summarize_contract_blob(contract_key, blob))
        except Exception as e:
            logger.debug("Massive SSE payload error for %s: %s", contract_key, e)

    try:
        pubsub.close()
    except Exception:
        pass
    try:
        r.close()
    except Exception:
        pass
    logger.info("Massive SSE subscriber loop stopped")
