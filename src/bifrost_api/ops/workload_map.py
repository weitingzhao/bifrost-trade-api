"""Map Legacy systemd unit names to Kubernetes Deployment / workload names."""

from __future__ import annotations

from typing import Optional

# systemd unit stem (with or without .service) → K8s Deployment name
UNIT_TO_DEPLOYMENT: dict[str, str] = {
    "bifrost-engine": "daemon",
    "bifrost-engine.service": "daemon",
}


def deployment_for_unit(unit: str) -> Optional[str]:
    """Resolve K8s Deployment name for a whitelisted systemd-style unit name."""
    u = (unit or "").strip()
    if not u:
        return None
    direct = UNIT_TO_DEPLOYMENT.get(u)
    if direct:
        return direct
    stem = u.removesuffix(".service")
    return UNIT_TO_DEPLOYMENT.get(stem)


def is_managed_unit(unit: str) -> bool:
    return deployment_for_unit(unit) is not None
