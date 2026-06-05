"""Synchronous IB Operator RPC helpers for Ops (e.g. reset before systemd restart)."""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional, Tuple

import redis

from bifrost_core.ib_operator.config import effective_ib_operator_settings
from bifrost_core.ib_operator.protocol import PROTOCOL_VERSION, result_key

logger = logging.getLogger(__name__)


def ib_operator_disconnect_all_sync(
    config: Dict[str, Any],
) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
    """XADD disconnect_all to operator stream; poll result key. Returns (ok, reason, envelope)."""
    settings = effective_ib_operator_settings(config)
    if not settings["enabled"] or not settings["redis_url"]:
        return False, "ib_operator_disabled", None
    import uuid

    req_id = str(uuid.uuid4())
    deadline_ms = int(__import__("time").time() * 1000 + 120_000)
    fields = {
        "req_id": req_id,
        "v": PROTOCOL_VERSION,
        "op": "disconnect_all",
        "payload": "{}",
        "caller": "ops_reset",
        "deadline_ms": str(deadline_ms),
    }
    r = redis.from_url(settings["redis_url"], decode_responses=True)
    try:
        r.xadd(settings["stream"], fields)
        rk = result_key(settings["result_prefix"], req_id)
        for _ in range(2400):
            raw = r.get(rk)
            if raw:
                try:
                    return True, "ok", json.loads(raw)
                except json.JSONDecodeError:
                    return False, "invalid_json", None
            __import__("time").sleep(0.05)
        return False, "timeout", None
    except Exception as e:
        logger.warning("ib_operator_disconnect_all_sync: %s", e)
        return False, str(e), None
    finally:
        try:
            r.close()
        except Exception:
            pass
