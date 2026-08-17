"""YAML-driven registry for Ops-managed systemd services (Socket ingest + optional Engine).

**trading_engine** uses ``redis_meta_key`` ``bifrost:health:daemon_strategy_trading`` by default (Dev/Prod
Ops lease + ``engine_ops_active``, same exclusivity rules as Socket rows in
:mod:`backend.ops.routers.market_ingest`). **account_sync_daemon** uses
``bifrost:health:daemon_account_sync`` by default for the same Ops lease fields on that hash.
YAML may omit ``redis_meta_key`` for either id and the default meta key is applied.

Official Polygon WS ingest id is ``polygon_ws``. YAML may still list ``massive_ws``;
``canonical_ingest_service_id`` normalizes it once — comparison / returned ids never treat
``massive_ws`` as a separate legal id.
"""

from __future__ import annotations

from typing import Dict, List, Optional

from bifrost_core.core.redis_health_keys import (
    BIFROST_HEALTH_ACCOUNT_SYNC_DAEMON,
    BIFROST_HEALTH_DAEMON_TRADING_ENGINE,
    LEGACY_BIFROST_HEALTH_ACCOUNT_SYNC_DAEMON,
    LEGACY_BIFROST_HEALTH_DAEMON_TRADING_ENGINE,
    BIFROST_HEALTH_IB_ACCOUNT_AGENT,
    BIFROST_HEALTH_IB_INGESTOR,
    BIFROST_HEALTH_IB_OPERATOR,
    BIFROST_HEALTH_POLYGON_WS,
    LEGACY_BIFROST_IB_ACCOUNT_AGENT,
    LEGACY_BIFROST_IB_INGESTOR,
    LEGACY_BIFROST_IB_OPERATOR,
    LEGACY_BIFROST_MASSIVE_WS,
    LEGACY_BIFROST_OPS_TRADING_ENGINE_META,
)

_LEGACY_IB_INGESTER_META_HEALTH = "ib:ingester:meta:health"
_LEGACY_IB_OPERATOR_META_HEALTH = "ib:operator:meta:health"

# Official id only (YAML ``massive_ws`` is normalized via canonical_ingest_service_id).
POLYGON_WS_SERVICE_IDS = frozenset({"polygon_ws"})
CANONICAL_POLYGON_WS_ID = "polygon_ws"

# Ingest processes that publish quotes / WS health (Socket Services page). When YAML lists only
# daemon rows (trading_engine, account_sync_daemon), merge these defaults so Ops UI still shows
# socket units alongside Daemon-only overrides.
_SOCKET_FEED_IDS: tuple[str, ...] = (
    CANONICAL_POLYGON_WS_ID,
    "ib_operator",
    "ib_ingestor",
    "ib_account_agent",
)
_DAEMON_ONLY_IDS = frozenset({"trading_engine", "account_sync_daemon"})


def is_polygon_ws_service_id(service_id: str) -> bool:
    """True for official ``polygon_ws`` (after YAML normalize)."""
    return canonical_ingest_service_id(service_id) == CANONICAL_POLYGON_WS_ID


def canonical_ingest_service_id(service_id: str) -> str:
    """Normalize legacy YAML / request ids to the official form returned to FE."""
    sid = (service_id or "").strip()
    if sid == "massive_ws":
        return CANONICAL_POLYGON_WS_ID
    if sid == "ib_market":
        return "ib_ingestor"
    return sid


def _default_row_by_id(service_id: str) -> Dict[str, str]:
    want = canonical_ingest_service_id(service_id)
    for row in DEFAULT_MARKET_INGEST_SERVICES:
        if row["id"] == want:
            return dict(row)
    raise KeyError(service_id)


def _ensure_socket_feed_rows_for_daemon_only_yaml(out: List[Dict[str, str]]) -> List[Dict[str, str]]:
    if not out:
        return out
    ids = {r["id"] for r in out}
    if not ids <= _DAEMON_ONLY_IDS:
        return out
    head = [_default_row_by_id(sid) for sid in _SOCKET_FEED_IDS]
    return head + out


DEFAULT_MARKET_INGEST_SERVICES: List[Dict[str, str]] = [
    {
        "id": CANONICAL_POLYGON_WS_ID,
        "label": "Polygon Options WS (Plugin · redis-massive)",
        # Retired systemd unit name; workload_map resolves → polygon-ws-ingestor.
        "systemd_unit": "bifrost-massive-ws.service",
        "redis_meta_key": BIFROST_HEALTH_POLYGON_WS,
    },
    {
        "id": "ib_operator",
        "label": "Platform IB Gateway · Operator RPC",
        "systemd_unit": "bifrost-ib-operator.service",
        "redis_meta_key": BIFROST_HEALTH_IB_OPERATOR,
    },
    {
        "id": "ib_ingestor",
        "label": "Platform IB Gateway · Market ingest",
        "systemd_unit": "bifrost-ib-market-gateway.service",
        "redis_meta_key": BIFROST_HEALTH_IB_INGESTOR,
    },
    {
        "id": "ib_account_agent",
        "label": "Platform IB Gateway · Account agent",
        "systemd_unit": "bifrost-ib-account-agent.service",
        "redis_meta_key": BIFROST_HEALTH_IB_ACCOUNT_AGENT,
    },
    {
        "id": "trading_engine",
        "label": "Strategy Trading Daemon",
        "systemd_unit": "bifrost-engine.service",
        "redis_meta_key": BIFROST_HEALTH_DAEMON_TRADING_ENGINE,
    },
    {
        "id": "account_sync_daemon",
        "label": "Account Sync Daemon",
        "systemd_unit": "bifrost-account-sync-daemon.service",
        "redis_meta_key": BIFROST_HEALTH_ACCOUNT_SYNC_DAEMON,
    },
]


def market_ingest_services_from_config(config: dict) -> List[Dict[str, str]]:
    """Return service rows; each has id, label, systemd_unit, redis_meta_key (may be empty).

    Official Polygon WS id is always ``polygon_ws`` in the returned list (legacy YAML
    ``massive_ws`` is normalized).
    """
    ops = config.get("ops") or {}
    raw = ops.get("market_ingest_services")
    if not isinstance(raw, list) or not raw:
        return list(DEFAULT_MARKET_INGEST_SERVICES)
    out: List[Dict[str, str]] = []
    for row in raw:
        if not isinstance(row, dict):
            continue
        sid = canonical_ingest_service_id(str(row.get("id") or "").strip())
        label = str(row.get("label") or sid).strip()
        unit = str(row.get("systemd_unit") or "").strip()
        meta = str(row.get("redis_meta_key") or "").strip()
        if not sid or not unit:
            continue
        norm_unit = unit if unit.endswith(".service") else f"{unit}.service"
        if is_polygon_ws_service_id(sid) and meta == LEGACY_BIFROST_MASSIVE_WS:
            meta = BIFROST_HEALTH_POLYGON_WS
        elif sid == "ib_ingestor" and meta in (
            _LEGACY_IB_INGESTER_META_HEALTH,
            LEGACY_BIFROST_IB_INGESTOR,
        ):
            meta = BIFROST_HEALTH_IB_INGESTOR
        elif sid == "ib_operator" and meta in (
            _LEGACY_IB_OPERATOR_META_HEALTH,
            LEGACY_BIFROST_IB_OPERATOR,
        ):
            meta = BIFROST_HEALTH_IB_OPERATOR
        elif sid == "ib_account_agent" and meta == LEGACY_BIFROST_IB_ACCOUNT_AGENT:
            meta = BIFROST_HEALTH_IB_ACCOUNT_AGENT
        elif sid == "trading_engine" and meta == LEGACY_BIFROST_OPS_TRADING_ENGINE_META:
            meta = BIFROST_HEALTH_DAEMON_TRADING_ENGINE
        elif sid == "trading_engine" and meta == LEGACY_BIFROST_HEALTH_DAEMON_TRADING_ENGINE:
            meta = BIFROST_HEALTH_DAEMON_TRADING_ENGINE
        elif sid == "account_sync_daemon" and meta == LEGACY_BIFROST_HEALTH_ACCOUNT_SYNC_DAEMON:
            meta = BIFROST_HEALTH_ACCOUNT_SYNC_DAEMON
        if sid == "trading_engine" and not meta:
            meta = BIFROST_HEALTH_DAEMON_TRADING_ENGINE
        if sid == "account_sync_daemon" and not meta:
            meta = BIFROST_HEALTH_ACCOUNT_SYNC_DAEMON
        out.append({
            "id": sid,
            "label": label or sid,
            "systemd_unit": norm_unit,
            "redis_meta_key": meta,
        })
    if not out:
        return list(DEFAULT_MARKET_INGEST_SERVICES)
    return _ensure_socket_feed_rows_for_daemon_only_yaml(out)


def market_ingest_service_by_id(config: dict, service_id: str) -> Optional[Dict[str, str]]:
    """Lookup by official id (``massive_ws`` request ids normalize to ``polygon_ws``)."""
    sid = canonical_ingest_service_id(service_id)
    for row in market_ingest_services_from_config(config):
        if row["id"] == sid:
            return row
    return None
