"""Redis health hash maintenance for Socket Services + trading_engine Ops meta (market-ingest).

- After a successful Ops **stop**, rewrite canonical health fields to a disconnected snapshot so
  Monitor GET /status updates without waiting for TTL expiry or a graceful writer exit.
- **trading_engine**: Health / Ops meta hash ``bifrost:health:daemon_strategy_trading`` uses ``engine_ops_active`` +
  ``updated_at`` for the same exclusive-start guard as Socket rows.
- Before **start** when no control lease exists, detect a still-fresh connected hash so only one
  Dev/Prod stack runs a writer against the same Redis.
- Socket Service health hashes carry a 3-minute TTL (reset by each heartbeat). Absence of the
  hash means the process has been dead for ≥ 3 minutes → ``ingest_redis_health_looks_live`` returns False.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional

from bifrost_api.ops.market_ingest_config import is_polygon_ws_service_id
from bifrost_core.core.message_center import publish_ib_service_stopped_messages
from bifrost_core.core.redis_health_keys import (
    ENGINE_OPS_ACTIVE_REDIS_FIELD,
    redis_hash_field_truthy,
)

logger = logging.getLogger(__name__)

_REDIS_SOCKET_SEC = 3.0
# If a writer is updating health, ``updated_at`` should advance within this window.
_HEALTH_RECENT_MAX_S = 90.0


def _conn(redis_url: str) -> Any:
    import redis

    return redis.from_url(
        redis_url,
        decode_responses=True,
        socket_connect_timeout=_REDIS_SOCKET_SEC,
        socket_timeout=_REDIS_SOCKET_SEC,
    )


def _parse_ts(raw: Optional[str]) -> float:
    if raw is None or str(raw).strip() == "":
        return 0.0
    try:
        return float(raw)
    except (TypeError, ValueError):
        return 0.0


def _hgetall(r: Any, key: str) -> Dict[str, str]:
    try:
        h = r.hgetall(key.strip())
        return dict(h or {})
    except Exception as e:
        logger.debug("hgetall %s: %s", key, e)
        return {}


def read_health_stack_profile(redis_url: str, meta_key: str) -> Optional[str]:
    """Return dev/prod/stg from ``config_file`` on the service health hash, if present."""
    from bifrost_api.ops.market_ingest_control_env import stack_profile_from_config_file

    key = (meta_key or "").strip()
    if not key:
        return None
    m = read_health_hash(redis_url, key)
    return stack_profile_from_config_file(m.get("config_file"))


def read_health_hash(redis_url: str, meta_key: str) -> Dict[str, str]:
    """Return raw health hash fields for a Socket Services meta key."""
    key = (meta_key or "").strip()
    if not key:
        return {}
    try:
        r = _conn(redis_url)
        try:
            return _hgetall(r, key)
        finally:
            r.close()
    except Exception as e:
        logger.debug("read_health_hash: %s", e)
        return {}


def ingest_health_is_platform_gateway(redis_url: str, meta_key: str) -> bool:
    """True when health hash is written by Platform IB Gateway plugin (plugin=ib-gateway)."""
    from bifrost_core.monitor.integrations.platform_ib_gateway import is_platform_ib_gateway_health

    return is_platform_ib_gateway_health(read_health_hash(redis_url, meta_key) or None)


def ingest_redis_health_writer_recent(redis_url: str, meta_key: str) -> bool:
    """True when the health hash exists and ``updated_at`` is fresh (process heartbeat)."""
    key = (meta_key or "").strip()
    if not key:
        return False
    try:
        r = _conn(redis_url)
        try:
            m = _hgetall(r, key)
        finally:
            r.close()
    except Exception as e:
        logger.debug("ingest_redis_health_writer_recent: %s", e)
        return False
    if not m:
        return False
    now = time.time()
    updated = _parse_ts(m.get("updated_at"))
    return updated > 0 and (now - updated) <= _HEALTH_RECENT_MAX_S


def ingest_redis_health_looks_live(redis_url: str, meta_key: str, service_id: str) -> bool:
    """True if the health hash exists and looks like a recently updated *connected* writer.

    With TTL on health hashes: absence of the key means the process has been dead for ≥ 3 min.
    Falls back to ``updated_at`` staleness check so services without TTL also work correctly.
    """
    key = (meta_key or "").strip()
    if not key:
        return False
    try:
        r = _conn(redis_url)
        try:
            m = _hgetall(r, key)
        finally:
            r.close()
    except Exception as e:
        logger.debug("ingest_redis_health_looks_live: %s", e)
        return False
    if not m:
        # Hash absent: TTL expired → service has been dead for ≥ TTL seconds.
        return False
    now = time.time()
    updated = _parse_ts(m.get("updated_at"))
    if updated <= 0 or (now - updated) > _HEALTH_RECENT_MAX_S:
        return False
    return _hash_looks_connected(m, (service_id or "").strip())


def _hash_looks_connected(m: Dict[str, str], sid: str) -> bool:
    if is_polygon_ws_service_id(sid):
        return redis_hash_field_truthy(m, "connected")
    if sid in ("ib_ingestor", "ib_market"):
        return redis_hash_field_truthy(m, "connected")
    if sid == "ib_operator":
        if redis_hash_field_truthy(m, "host_connected"):
            return True
        if redis_hash_field_truthy(m, "operator_connected"):
            return True
        return redis_hash_field_truthy(m, "connected")
    if sid == "ib_account_agent":
        if redis_hash_field_truthy(m, "host_connected"):
            return True
        return redis_hash_field_truthy(m, "connected")
    if sid == "trading_engine":
        return redis_hash_field_truthy(m, ENGINE_OPS_ACTIVE_REDIS_FIELD)
    if sid == "account_sync_daemon":
        return redis_hash_field_truthy(m, "alive")
    return False


_PLUGIN_MANAGED_IDS = frozenset({"polygon_ws", "massive_ws"})


def clear_ingest_health_after_stop(redis_url: str, meta_key: str, service_id: str) -> None:
    """HSET disconnected snapshot on the ingest health hash (does not delete the key).

    Plugin-managed services (polygon_ws / legacy massive_ws) are not cleared by Trade Ops —
    the Plugin process owns the health hash lifecycle on redis-massive.
    """
    key = (meta_key or "").strip()
    if not key:
        return
    sid = (service_id or "").strip()
    if sid in _PLUGIN_MANAGED_IDS:
        logger.info(
            "clear_ingest_health_after_stop: skipping %s — Plugin-managed service", sid
        )
        return
    now = time.time()
    r = _conn(redis_url)
    try:
        prior = _hgetall(r, key) if sid in (
            "ib_ingestor",
            "ib_market",
            "ib_account_agent",
            "ib_operator",
        ) else {}
        if sid in ("ib_ingestor", "ib_market"):
            r.hset(
                key,
                mapping={
                    "client_id": "0",
                    "connected": "0",
                    "last_msg_ts": str(now),
                    "reconnects": "0",
                    "msg_count": "0",
                    "updated_at": str(now),
                },
            )
        elif sid == "ib_operator":
            from bifrost_core.ib_operator.health_redis import (
                operator_health_dict_to_redis_hash,
                prune_legacy_operator_health_hash_fields,
            )

            h = {
                "host": {
                    "connected": False,
                    "client_id": 0,
                    "last_error": "",
                    "reconnects": 0,
                },
                "secondary": None,
                "service_alive": False,
                "operator_alive": False,
                "updated_at": now,
                "last_cmd_ts": 0.0,
                "cmd_count": 0,
            }
            mapping = operator_health_dict_to_redis_hash(h)
            r.hset(key, mapping=mapping)
            prune_legacy_operator_health_hash_fields(r, key)
        elif sid == "ib_account_agent":
            r.hset(
                key,
                mapping={
                    "connected": "0",
                    "host_connected": "0",
                    "host_alive": "0",
                    "client_id": "0",
                    "host_client_id": "0",
                    "secondary_connected": "0",
                    "last_msg_ts": str(now),
                    "reconnects": "0",
                    "msg_count": "0",
                    "updated_at": str(now),
                },
            )
        elif sid == "trading_engine":
            r.hset(
                key,
                mapping={
                    ENGINE_OPS_ACTIVE_REDIS_FIELD: "0",
                    "updated_at": str(now),
                },
            )
        elif sid == "account_sync_daemon":
            r.hset(
                key,
                mapping={
                    "alive": "0",
                    "stream_lag": "0",
                    "last_sync_version": "0",
                    "updated_at": str(now),
                },
            )
        else:
            logger.debug("clear_ingest_health_after_stop: unknown service_id=%s", sid)
        if prior:
            try:
                publish_ib_service_stopped_messages(
                    r,
                    service_id=sid,
                    health_hash=prior,
                    occurred_at=now,
                )
            except Exception as e:
                logger.warning("clear_ingest_health_after_stop: message center publish failed: %s", e)
    finally:
        r.close()
