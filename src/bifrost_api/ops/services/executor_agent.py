"""Agent-based executor — delegates systemd operations to Local Control Agent via UDS.

Drop-in replacement for RestrictedExecutor when ``executor_mode=agent``.
The Ops API process runs without sudo; all privileged operations go through the agent.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Set

from bifrost_api.ops.agent.client import AgentClient
from bifrost_api.ops.services.executor_local import (
    _ALLOWED_ACTIONS,
    RestrictedExecutor,
)

logger = logging.getLogger(__name__)

# systemd units use TimeoutStopSec=60 for several ingest agents; Ops UDS client must outlive `systemctl stop`.
_SYSTEMCTL_TIMEOUT_START_SEC = 45
_SYSTEMCTL_TIMEOUT_STOP_RESTART_SEC = 95


class AgentExecutor:
    """Execute whitelisted systemd unit actions via the Local Control Agent.

    Maintains the same public interface as RestrictedExecutor so the router
    can use it as a drop-in replacement.
    """

    def __init__(
        self,
        socket_path: str,
        allowed_units: list[str],
        broker_url: str,
        use_redis_stop: bool = True,
    ) -> None:
        self._client = AgentClient(socket_path)
        self._allowed: Set[str] = set(allowed_units)
        self._broker_url = broker_url
        self._use_redis_stop = use_redis_stop

    worker_to_unit = staticmethod(RestrictedExecutor.worker_to_unit)
    instance_unit = staticmethod(RestrictedExecutor.instance_unit)

    def _validate(self, action: str, unit: str) -> None:
        proxy = RestrictedExecutor(
            allowed_units=list(self._allowed),
            broker_url="",
            use_redis_stop=False,
        )
        proxy._validate(action, unit)  # noqa: SLF001

    async def _redis_stop_celery(self) -> Dict[str, Any]:
        try:
            import redis

            from bifrost_worker.celery.celery_app import (
                WORKER_IB_STATUS_KEY,
                WORKER_IB_STATUS_TTL_SEC,
                WORKER_STOP_REQUESTED_KEY,
            )

            r = redis.from_url(
                self._broker_url,
                socket_connect_timeout=5,
                socket_timeout=5,
            )
            r.set(WORKER_STOP_REQUESTED_KEY, "1")
            r.setex(
                WORKER_IB_STATUS_KEY,
                WORKER_IB_STATUS_TTL_SEC,
                json.dumps({"connected": False, "client_id": 0}),
            )
            return {
                "method": "redis",
                "message": "Stop signal sent via Redis; worker will exit within seconds.",
            }
        except Exception as e:
            raise RuntimeError(f"Redis-based Celery stop failed: {e}") from e

    async def _systemctl(self, action: str, unit: str, timeout: int | None = None) -> Dict[str, Any]:
        if timeout is None:
            if action in ("stop", "restart"):
                timeout = _SYSTEMCTL_TIMEOUT_STOP_RESTART_SEC
            elif action == "start":
                timeout = _SYSTEMCTL_TIMEOUT_START_SEC
            else:
                timeout = 30
        resp = await self._client.systemctl(action, unit, timeout=timeout)
        if not resp.ok:
            raise RuntimeError(
                f"Agent: systemctl {action} {unit} failed: {resp.error}"
            )
        return resp.result or {"method": "agent-systemd", "action": action, "unit": unit}

    async def list_instances(self) -> List[Dict[str, str]]:
        resp = await self._client.list_instances()
        if not resp.ok:
            raise RuntimeError(f"Agent: list_instances failed: {resp.error}")
        return resp.result.get("instances", []) if resp.result else []

    async def redis_is_local(self) -> bool:
        resp = await self._client.is_active("redis")
        if not resp.ok:
            return False
        stdout = (resp.result or {}).get("stdout", "")
        return stdout.strip() in ("active", "inactive", "failed")

    async def systemctl_redis(self, action: str) -> Dict[str, Any]:
        if action not in _ALLOWED_ACTIONS:
            raise PermissionError(f"Action {action!r} not allowed for Redis")
        return await self._systemctl(action, "redis")

    async def systemctl_is_active(self, unit: str) -> str:
        self._validate("start", unit)
        resp = await self._client.is_active(unit)
        states = RestrictedExecutor._IS_ACTIVE_STATES  # noqa: SLF001
        if resp.ok and resp.result:
            out = RestrictedExecutor._normalize_is_active_stdout(  # noqa: SLF001
                str(resp.result.get("stdout") or ""),
            )
            if out in states:
                return out
        return "unknown"

    async def force_stop_worker_unit(self, unit: str) -> Dict[str, Any]:
        self._validate("stop", unit)
        RestrictedExecutor.assert_celery_worker_instance_unit(unit)
        timeout = _SYSTEMCTL_TIMEOUT_STOP_RESTART_SEC
        resp = await self._client.worker_kill(unit, timeout=timeout)
        if not resp.ok:
            raise RuntimeError(
                f"Agent: force kill {unit} failed: {resp.error}"
            )
        return resp.result or {
            "method": "agent-systemd",
            "action": "kill",
            "unit": unit,
        }
