"""Redis Ops control fields for Socket Services: which stack (dev|prod) owns control.

Socket Services store ``bifrost_ops_control_env`` / ``bifrost_ops_control_host`` directly on
their ``bifrost:health:*`` hash.  Prod Redis has shown that these health hashes are writable
while separate ``bifrost:ops:lease:*`` keys may be filtered or unavailable.
"""

from __future__ import annotations

import logging
import socket
import time
from typing import Optional

from bifrost_core.core.redis_health_keys import ENGINE_OPS_ACTIVE_REDIS_FIELD

logger = logging.getLogger(__name__)

BIFROST_OPS_CONTROL_ENV_FIELD = "bifrost_ops_control_env"
BIFROST_OPS_CONTROL_HOST_FIELD = "bifrost_ops_control_host"
BIFROST_OPS_CONTROL_UPDATED_AT_FIELD = "bifrost_ops_control_updated_at"

_REDIS_SOCKET_SEC = 3.0


def normalize_control_profile(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None
    s = str(raw).strip().lower()
    if s in ("dev", "prod", "stg"):
        return s
    return None


def stack_profile_from_config_file(config_file: Optional[str]) -> Optional[str]:
    """Infer dev/prod/stg from a resolved BIFROST_CONFIG path stored in Redis health."""
    if not config_file or not str(config_file).strip():
        return None
    from pathlib import Path

    name = Path(str(config_file).strip()).name.lower()
    if name == "config.stg.yaml":
        return "stg"
    if name == "config.dev.yaml":
        return "dev"
    if name == "config.prod.yaml":
        return "prod"
    return None


def meta_redis_url_from_ops_config(config: dict) -> Optional[str]:
    from bifrost_core.core.redis_url import redis_url_from_config

    return redis_url_from_config(config or {})


def control_hostname() -> str:
    try:
        return (socket.gethostname() or "unknown").strip() or "unknown"
    except Exception:
        return "unknown"


def _redis_conn(redis_url: str):
    import redis
    return redis.from_url(
        redis_url,
        decode_responses=True,
        socket_connect_timeout=_REDIS_SOCKET_SEC,
        socket_timeout=_REDIS_SOCKET_SEC,
    )


# ── Socket Services: lease fields on bifrost:health:* ──

def read_control_host(redis_url: str, lease_key: str) -> Optional[str]:
    """Hostname written at last Ops start; ``None`` if missing."""
    key = (lease_key or "").strip()
    if not key:
        return None
    try:
        r = _redis_conn(redis_url)
        raw = r.hget(key, BIFROST_OPS_CONTROL_HOST_FIELD)
        s = (raw or "").strip()
        return s or None
    except Exception as e:
        logger.debug("read_control_host %s: %s", key, e)
        return None


def read_control_updated_at(redis_url: str, lease_key: str) -> Optional[float]:
    """Return when Ops last wrote the Dev/Prod HOST fields."""
    key = (lease_key or "").strip()
    if not key:
        return None
    try:
        r = _redis_conn(redis_url)
        raw = r.hget(key, BIFROST_OPS_CONTROL_UPDATED_AT_FIELD)
        if raw is None or str(raw).strip() == "":
            return None
        return float(raw)
    except Exception as e:
        logger.debug("read_control_updated_at %s: %s", key, e)
        return None


def read_control_env(redis_url: str, lease_key: str) -> Optional[str]:
    """Return ``dev``/``prod`` from the health/lease hash, or ``None`` if missing/unreadable."""
    key = (lease_key or "").strip()
    if not key:
        return None
    try:
        r = _redis_conn(redis_url)
        raw = r.hget(key, BIFROST_OPS_CONTROL_ENV_FIELD)
        return normalize_control_profile(raw)
    except Exception as e:
        logger.debug("read_control_env %s: %s", key, e)
        return None


def write_control_env(redis_url: str, lease_key: str, profile: str) -> None:
    norm = normalize_control_profile(profile)
    if not norm:
        raise ValueError(f"invalid control profile: {profile!r}")
    key = (lease_key or "").strip()
    if not key:
        raise ValueError("empty lease_key")
    r = _redis_conn(redis_url)
    now = time.time()
    r.hset(key, mapping={
        BIFROST_OPS_CONTROL_ENV_FIELD: norm,
        BIFROST_OPS_CONTROL_HOST_FIELD: control_hostname(),
        BIFROST_OPS_CONTROL_UPDATED_AT_FIELD: str(now),
    })


def clear_control_env(redis_url: str, lease_key: str) -> None:
    key = (lease_key or "").strip()
    if not key:
        return
    try:
        r = _redis_conn(redis_url)
        r.hdel(
            key,
            BIFROST_OPS_CONTROL_ENV_FIELD,
            BIFROST_OPS_CONTROL_HOST_FIELD,
            BIFROST_OPS_CONTROL_UPDATED_AT_FIELD,
        )
    except Exception as e:
        logger.warning("clear_control_env %s: %s", key, e)
        raise


# ── Trading Engine: lease + active marker also live inside its health hash ──

def write_trading_engine_ops_lease(redis_url: str, meta_key: str, profile: str) -> None:
    """Lease + ``engine_ops_active`` + ``updated_at`` for Dev/Prod exclusivity (trading_engine only)."""
    norm = normalize_control_profile(profile)
    if not norm:
        raise ValueError(f"invalid control profile: {profile!r}")
    key = (meta_key or "").strip()
    if not key:
        raise ValueError("empty redis_meta_key")
    r = _redis_conn(redis_url)
    r.hset(key, mapping={
        BIFROST_OPS_CONTROL_ENV_FIELD: norm,
        BIFROST_OPS_CONTROL_HOST_FIELD: control_hostname(),
        BIFROST_OPS_CONTROL_UPDATED_AT_FIELD: str(time.time()),
        ENGINE_OPS_ACTIVE_REDIS_FIELD: "1",
        "updated_at": str(time.time()),
    })
