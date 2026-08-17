"""Semantic display state for Ops market-ingest rows (K8s / platform gateway / policy-off)."""

from __future__ import annotations

from typing import Dict, Optional

from bifrost_api.ops.market_ingest_health_clear import (
    ingest_health_is_platform_gateway,
    ingest_redis_health_looks_live,
    ingest_redis_health_writer_recent,
)

_PLATFORM_IB_INGEST_IDS = frozenset({"ib_ingestor", "ib_operator", "ib_account_agent"})
_PLUGIN_MANAGED_INGEST_IDS = frozenset({"massive_ws"})


def _process_counts_as_running(active: str) -> bool:
    a = (active or "").lower().strip()
    return a in ("active", "activating")


def massive_ws_policy_disabled(config: dict) -> bool:
    """True when Polygon WS ingest is intentionally off (REST-only / Starter tier)."""
    massive = config.get("massive") or {}
    feats = massive.get("features") or {}
    tier = str(massive.get("tier") or "starter").strip().lower()
    if "ws_enabled" in feats:
        return not bool(feats["ws_enabled"])
    return tier == "starter"


def derive_ingest_display_state(
    *,
    service_id: str,
    process_active: str,
    config: dict,
    redis_url: Optional[str],
    ib_redis_url: Optional[str],
    meta_key: str,
    runtime_externally_managed: bool,
    platform_gateway_managed: bool,
    ops_control_profile: Optional[str],
    runtime_kind: Optional[str],
) -> Dict[str, str]:
    """Return ``runtime_status`` + ``display_active`` for UI and bus-deep reachability."""
    sid = (service_id or "").strip()
    mk = (meta_key or "").strip()
    health_url = ib_redis_url or redis_url

    if sid == "massive_ws" and massive_ws_policy_disabled(config):
        return {
            "runtime_status": "policy-off",
            "display_active": "ws-disabled (REST-only)",
        }

    if sid in _PLUGIN_MANAGED_INGEST_IDS:
        if health_url and mk:
            is_live = ingest_redis_health_looks_live(health_url, mk, sid)
            writer_recent = ingest_redis_health_writer_recent(health_url, mk)
            if is_live:
                return {
                    "runtime_status": "active",
                    "display_active": "managed@plugin-market-data (redis-massive)",
                }
            if writer_recent:
                return {
                    "runtime_status": "degraded",
                    "display_active": "managed@plugin-market-data (starting)",
                }
        return {
            "runtime_status": "inactive",
            "display_active": "managed@plugin-market-data (offline)",
        }

    if sid == "trading_engine" and not _process_counts_as_running(process_active):
        looks_live = bool(redis_url and mk and ingest_redis_health_looks_live(redis_url, mk, sid))
        if not looks_live and (
            ops_control_profile == "stg"
            or runtime_kind == "kubernetes"
            or runtime_externally_managed
        ):
            return {
                "runtime_status": "policy-off",
                "display_active": "policy-off (daemon scale 0)",
            }

    if platform_gateway_managed and health_url and mk:
        is_live = ingest_redis_health_looks_live(health_url, mk, sid)
        writer_recent = ingest_redis_health_writer_recent(health_url, mk)
        if is_live:
            return {
                "runtime_status": "active",
                "display_active": "managed@platform-ib-gateway",
            }
        if writer_recent:
            return {
                "runtime_status": "degraded",
                "display_active": "managed@platform-ib-gateway (starting)",
            }
        return {
            "runtime_status": "inactive",
            "display_active": "managed@platform-ib-gateway (offline)",
        }

    if runtime_externally_managed and redis_url and mk:
        looks_live = ingest_redis_health_looks_live(redis_url, mk, sid)
        writer_recent = ingest_redis_health_writer_recent(redis_url, mk)
        if looks_live:
            return {
                "runtime_status": "active",
                "display_active": "managed@k8s",
            }
        if writer_recent:
            return {
                "runtime_status": "degraded",
                "display_active": "managed@k8s (starting)",
            }
        if not _process_counts_as_running(process_active):
            return {
                "runtime_status": "degraded",
                "display_active": "managed@k8s (no heartbeat)",
            }

    active = (process_active or "unknown").lower().strip()
    if _process_counts_as_running(process_active):
        return {"runtime_status": "active", "display_active": active}
    if active in ("inactive", "failed", "deactivating"):
        return {"runtime_status": "inactive", "display_active": active}
    if active == "activating":
        return {"runtime_status": "degraded", "display_active": active}
    return {"runtime_status": "unknown", "display_active": active or "unknown"}


def platform_gateway_managed_for_service(
    ib_redis_url: Optional[str],
    live_redis_url: Optional[str],
    meta_key: str,
    service_id: str,
) -> bool:
    """Detect Platform IB Gateway writer using redis-ib (fallback: live redis)."""
    sid = (service_id or "").strip()
    mk = (meta_key or "").strip()
    if sid not in _PLATFORM_IB_INGEST_IDS or not mk:
        return False
    url = ib_redis_url or live_redis_url
    if not url:
        return False
    return ingest_health_is_platform_gateway(url, mk)
