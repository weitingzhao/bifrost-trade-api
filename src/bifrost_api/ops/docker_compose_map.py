"""Map Legacy systemd unit names to Docker Compose service names (Phase 2C)."""

from __future__ import annotations

from typing import Optional

# systemd_unit stem (with or without .service) → compose service name
SYSTEMD_UNIT_TO_COMPOSE_SERVICE: dict[str, str] = {
    "bifrost-engine": "daemon",
    "bifrost-engine.service": "daemon",
    "bifrost-massive-ws": "massive-ws",
    "bifrost-massive-ws.service": "massive-ws",
    "bifrost-ib-operator": "ib-operator",
    "bifrost-ib-operator.service": "ib-operator",
    "bifrost-ib-ingestor": "ib-ingestor",
    "bifrost-ib-ingestor.service": "ib-ingestor",
    "bifrost-ib-account-agent": "ib-account-agent",
    "bifrost-ib-account-agent.service": "ib-account-agent",
    "bifrost-account-sync-daemon": "account-sync",
    "bifrost-account-sync-daemon.service": "account-sync",
    "bifrost-celery-worker": "celery-worker",
    "bifrost-celery-worker.service": "celery-worker",
    "redis": "redis",
    "redis.service": "redis",
}

_WORKER_UNIT_BASE = "bifrost-celery-worker"


def compose_service_for_systemd_unit(unit: str) -> Optional[str]:
    """Resolve compose service for a whitelisted systemd unit name."""
    u = (unit or "").strip()
    if not u:
        return None
    direct = SYSTEMD_UNIT_TO_COMPOSE_SERVICE.get(u)
    if direct:
        return direct
    # bifrost-celery-worker@profile-1.service → celery-worker (single container in prod compose)
    prefix = f"{_WORKER_UNIT_BASE}@"
    if u.startswith(prefix) and u.endswith(".service"):
        return "celery-worker"
    stem = u.removesuffix(".service")
    return SYSTEMD_UNIT_TO_COMPOSE_SERVICE.get(stem)


def is_compose_managed_unit(unit: str) -> bool:
    return compose_service_for_systemd_unit(unit) is not None
