"""Docker Compose executor — start/stop ingest + daemon via ``docker compose``.

Used when ``ops.executor_mode=docker`` (Phase 2C prod stack). Celery broker stop still uses Redis.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from bifrost_api.ops.docker_compose_map import (
    compose_service_for_systemd_unit,
    is_compose_managed_unit,
)
from bifrost_api.ops.services.executor_local import (
    _ALLOWED_ACTIONS,
    _WORKER_UNIT_BASE,
    RestrictedExecutor,
)

logger = logging.getLogger(__name__)

_COMPOSE_TIMEOUT_SEC = 120
_STATE_TO_IS_ACTIVE = {
    "running": "active",
    "restarting": "activating",
    "starting": "activating",
    "created": "inactive",
    "exited": "inactive",
    "dead": "inactive",
    "paused": "inactive",
    "removing": "deactivating",
}


class DockerComposeExecutor:
    """Control whitelisted services via ``docker compose`` (api-ops mounts docker.sock)."""

    def __init__(
        self,
        *,
        workdir: str | Path,
        compose_files: list[str],
        allowed_units: list[str],
        broker_url: str,
        use_redis_stop: bool = True,
        compose_project: Optional[str] = None,
        docker_socket: str = "/var/run/docker.sock",
    ) -> None:
        self._workdir = Path(workdir).resolve()
        self._compose_files = [str(f).strip() for f in compose_files if str(f).strip()]
        self._project = (compose_project or os.environ.get("COMPOSE_PROJECT_NAME") or "").strip() or None
        self._socket = docker_socket
        self._allowed: Set[str] = set(allowed_units)
        self._broker_url = broker_url
        self._use_redis_stop = use_redis_stop
        self._redis_delegate = RestrictedExecutor(
            allowed_units=[],
            broker_url=broker_url,
            use_redis_stop=use_redis_stop,
        )
        self._docker_reachable = os.path.exists(self._socket)

    @property
    def docker_reachable(self) -> bool:
        return self._docker_reachable

    @property
    def compose_workdir(self) -> str:
        return str(self._workdir)

    worker_to_unit = staticmethod(RestrictedExecutor.worker_to_unit)
    instance_unit = staticmethod(RestrictedExecutor.instance_unit)

    def _validator(self) -> RestrictedExecutor:
        return RestrictedExecutor(
            allowed_units=list(self._allowed),
            broker_url="",
            use_redis_stop=False,
        )

    def _validate(self, action: str, unit: str) -> None:
        self._validator()._validate(action, unit)  # noqa: SLF001

    def _compose_base_cmd(self) -> List[str]:
        cmd = ["docker", "compose"]
        for f in self._compose_files:
            cmd.extend(["-f", f])
        if self._project:
            cmd.extend(["-p", self._project])
        return cmd

    async def _run_compose(
        self,
        args: List[str],
        *,
        timeout: int = _COMPOSE_TIMEOUT_SEC,
    ) -> tuple[int, str, str]:
        cmd = self._compose_base_cmd() + args
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=str(self._workdir),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError as e:
            proc.kill()
            raise RuntimeError(f"docker compose timed out after {timeout}s: {' '.join(args)}") from e
        out = (stdout or b"").decode(errors="replace").strip()
        err = (stderr or b"").decode(errors="replace").strip()
        return proc.returncode or 0, out, err

    async def _service_state(self, compose_service: str) -> str:
        """Return compose container State (running|exited|…) or empty if missing."""
        rc, out, err = await self._run_compose(
            ["ps", compose_service, "--format", "json"],
            timeout=30,
        )
        if rc != 0:
            logger.debug("compose ps %s rc=%s err=%s", compose_service, rc, err)
            return ""
        line = out.splitlines()[0].strip() if out else ""
        if not line:
            return ""
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            logger.debug("compose ps json parse failed: %s", line[:200])
            return ""
        state = str(row.get("State") or row.get("Status") or "").strip().lower()
        # Status may be "Up 2 hours" — treat as running
        if state.startswith("up"):
            return "running"
        return state.split()[0] if state else ""

    async def _redis_stop_celery(self) -> Dict[str, Any]:
        return await self._redis_delegate._redis_stop_celery()  # noqa: SLF001

    async def _systemctl(
        self,
        action: str,
        unit: str,
        timeout: int | None = None,
    ) -> Dict[str, Any]:
        self._validate(action, unit)
        compose_svc = compose_service_for_systemd_unit(unit)
        if not compose_svc:
            raise PermissionError(
                f"Unit {unit!r} has no compose mapping; docker executor cannot control it."
            )
        tmo = timeout or _COMPOSE_TIMEOUT_SEC
        rc, out, err = await self._run_compose([action, compose_svc], timeout=tmo)
        if rc != 0:
            raise RuntimeError(
                f"docker compose {action} {compose_svc} failed (rc={rc}): {err or out}"
            )
        return {
            "method": "docker-compose",
            "action": action,
            "unit": unit,
            "compose_service": compose_svc,
            "stdout": out,
            "message": f"compose {action} {compose_svc} ok",
        }

    async def list_instances(self) -> List[Dict[str, str]]:
        """Prod compose runs one ``celery-worker`` container (no systemd @ instances)."""
        state = await self._service_state("celery-worker")
        if state != "running":
            return []
        unit = f"{_WORKER_UNIT_BASE}@default.service"
        return [{
            "unit": unit,
            "load": "loaded",
            "active": "active",
            "sub": "running",
            "description": "docker compose service celery-worker",
        }]

    async def redis_is_local(self) -> bool:
        """True when redis service exists in compose project (embedded-infra profile)."""
        state = await self._service_state("redis")
        return bool(state)

    async def systemctl_redis(self, action: str) -> Dict[str, Any]:
        if action not in _ALLOWED_ACTIONS:
            raise PermissionError(f"Action {action!r} not allowed for Redis")
        return await self._systemctl(action, "redis.service")

    async def systemctl_is_active(self, unit: str) -> str:
        if not self._docker_reachable:
            return "unknown"
        try:
            self._validate("start", unit)
        except PermissionError:
            return "unknown"
        compose_svc = compose_service_for_systemd_unit(unit)
        if not compose_svc:
            return "unknown"
        try:
            raw = await self._service_state(compose_svc)
        except Exception as e:
            logger.debug("compose state %s: %s", compose_svc, e)
            return "unknown"
        if not raw:
            return "inactive"
        return _STATE_TO_IS_ACTIVE.get(raw, "unknown")

    async def force_stop_worker_unit(self, unit: str) -> Dict[str, Any]:
        self._validate("stop", unit)
        RestrictedExecutor.assert_celery_worker_instance_unit(unit)
        return await self._systemctl("stop", unit, timeout=_COMPOSE_TIMEOUT_SEC)

    def compose_service_for_unit(self, unit: str) -> Optional[str]:
        """Public helper for routers (WP2 runtime_kind)."""
        return compose_service_for_systemd_unit(unit)

    def manages_unit(self, unit: str) -> bool:
        return is_compose_managed_unit(unit)
