"""Shared unit helpers and action whitelist for the Kubernetes ops executor.

Extracted from the former RestrictedExecutor so api-ops can be K8s-only
without importing systemd / docker / agent control paths.
"""

from __future__ import annotations

import fnmatch
import re
from typing import Set

_ALLOWED_ACTIONS = frozenset({"start", "stop", "restart"})
_WORKER_UNIT_BASE = "bifrost-celery-worker"
_INSTANCE_ID_RE = re.compile(r"^[a-zA-Z0-9_-]+$")


def worker_to_unit(worker_id: str) -> str:
    """Map Celery nodename to ``bifrost-celery-worker@<instance>.service``.

    ``run_celery.py`` sets ``-n worker{instance}@{hostname}`` (e.g. ``workerib-1@myhost``).
    The part after ``@`` is the **host**, not the systemd instance id — use the nodename
    prefix ``worker`` + instance. Legacy names like ``celery@worker1`` keep the old rule
    (second segment = logical id).
    """
    if "@" not in worker_id:
        return f"{_WORKER_UNIT_BASE}@{worker_id}.service"
    nodename, tail = worker_id.split("@", 1)
    if nodename.startswith("worker") and len(nodename) > len("worker"):
        instance_id = nodename[len("worker") :]
        return f"{_WORKER_UNIT_BASE}@{instance_id}.service"
    return f"{_WORKER_UNIT_BASE}@{tail}.service"


def instance_unit(instance_id: str) -> str:
    """Build template-instance unit for a numeric/named instance."""
    if not _INSTANCE_ID_RE.match(instance_id):
        raise ValueError(f"Invalid instance_id: {instance_id!r}")
    return f"{_WORKER_UNIT_BASE}@{instance_id}.service"


def assert_celery_worker_instance_unit(unit: str) -> None:
    """Raise PermissionError unless *unit* is ``bifrost-celery-worker@<id>.service``."""
    u = (unit or "").strip()
    prefix = f"{_WORKER_UNIT_BASE}@"
    if not (u.startswith(prefix) and u.endswith(".service")):
        raise PermissionError(
            f"Expected {_WORKER_UNIT_BASE}@<instance_id>.service; got {unit!r}"
        )
    inst = u[len(prefix) : -len(".service")]
    if not _INSTANCE_ID_RE.match(inst):
        raise PermissionError(f"Invalid Celery worker instance id in unit: {unit!r}")


class ActionValidator:
    """Whitelist check for ops control actions against allowed unit patterns."""

    def __init__(self, allowed_units: list[str]) -> None:
        self._allowed: Set[str] = set(allowed_units)

    def validate(self, action: str, unit: str) -> None:
        if action not in _ALLOWED_ACTIONS:
            raise PermissionError(
                f"Action {action!r} not allowed; permitted: {sorted(_ALLOWED_ACTIONS)}"
            )
        if not self._allowed:
            return
        for allowed in self._allowed:
            if unit == allowed or unit == f"{allowed}.service":
                return
            if fnmatch.fnmatch(unit, f"{allowed}@*.service"):
                return
        raise PermissionError(
            f"Unit {unit!r} not in whitelist; permitted: {sorted(self._allowed)}"
        )
