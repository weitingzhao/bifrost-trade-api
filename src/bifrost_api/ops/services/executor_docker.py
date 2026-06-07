"""Docker Compose executor — start/stop ingest + daemon via ``docker compose``.

Used when ``ops.executor_mode=docker`` (Phase 2C prod stack). Celery broker stop still uses Redis.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
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
_CELERY_COMPOSE_SERVICE = "celery-worker"
_CELERY_RUN_SCRIPT = "scripts/systemd/run_celery.py"
_INSTANCE_RE = re.compile(r"--instance\s+(\S+)")
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
        host_workdir: str | Path | None = None,
    ) -> None:
        self._workdir = Path(workdir).resolve()
        host_raw = (
            str(host_workdir or "").strip()
            or os.environ.get("BIFROST_COMPOSE_HOST_WORKDIR", "").strip()
        )
        self._host_workdir = Path(host_raw).resolve() if host_raw else None
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

    def _compose_cwd(self) -> str:
        """Host filesystem path for ``docker compose`` CLI (bind mounts must be host paths)."""
        if self._host_workdir is not None and self._host_workdir.is_dir():
            return str(self._host_workdir)
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
        compose_cwd = self._compose_cwd()
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=compose_cwd,
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

    async def _compose_exec(
        self,
        exec_args: List[str],
        *,
        detach: bool = False,
        timeout: int = 60,
    ) -> tuple[int, str, str]:
        args = ["exec"]
        if detach:
            args.append("-d")
        else:
            args.append("-T")
        args.append(_CELERY_COMPOSE_SERVICE)
        args.extend(exec_args)
        return await self._run_compose(args, timeout=timeout)

    @staticmethod
    def _instance_from_worker_unit(unit: str) -> str:
        prefix = f"{_WORKER_UNIT_BASE}@"
        if not unit.startswith(prefix) or not unit.endswith(".service"):
            raise ValueError(f"Not a {_WORKER_UNIT_BASE}@ template unit: {unit!r}")
        return unit[len(prefix) : -len(".service")]

    @staticmethod
    def _is_celery_worker_unit(unit: str) -> bool:
        u = unit.strip()
        return u.startswith(f"{_WORKER_UNIT_BASE}@") and u.endswith(".service")

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
        if state.startswith("up"):
            return "running"
        return state.split()[0] if state else ""

    async def _ensure_celery_container(self) -> None:
        state = await self._service_state(_CELERY_COMPOSE_SERVICE)
        if state == "running":
            return
        # Never ``up -d`` from api-ops: on macOS/docker.sock it resolves ./config as host /infra/config.
        rc, out, err = await self._run_compose(["start", _CELERY_COMPOSE_SERVICE])
        combined = f"{err or ''} {out or ''}".strip()
        if rc != 0:
            hint = ""
            if "mounts denied" in combined.lower() or "/infra/config" in combined:
                hint = (
                    " Celery-worker container may have been created with invalid bind mounts. "
                    "From the infra repo on the host run: "
                    "docker compose -f docker-compose.yml -f docker-compose.local.yml rm -f celery-worker "
                    "&& docker compose -f docker-compose.yml -f docker-compose.local.yml up -d celery-worker"
                )
            raise RuntimeError(
                f"docker compose start {_CELERY_COMPOSE_SERVICE} failed (rc={rc}): {combined}{hint}"
            )
        after = await self._service_state(_CELERY_COMPOSE_SERVICE)
        if after != "running":
            raise RuntimeError(
                f"{_CELERY_COMPOSE_SERVICE} is not running after compose start (state={after!r}). "
                f"Start it from the host: docker compose up -d {_CELERY_COMPOSE_SERVICE}"
            )

    async def _pgrep_celery_lines(self) -> List[str]:
        state = await self._service_state(_CELERY_COMPOSE_SERVICE)
        if state != "running":
            return []
        rc, out, err = await self._compose_exec(
            ["pgrep", "-af", "run_celery.py"],
            timeout=30,
        )
        if rc not in (0, 1):
            logger.debug("compose exec pgrep failed rc=%s err=%s", rc, err)
            return []
        text = out.strip()
        if not text:
            return []
        return [ln for ln in text.splitlines() if ln.strip()]

    def _instances_from_pgrep_lines(self, lines: List[str]) -> List[Dict[str, str]]:
        seen_instance_ids: set[str] = set()
        instances: List[Dict[str, str]] = []
        for line in lines:
            m = _INSTANCE_RE.search(line)
            if m:
                instance_id = m.group(1)
                if instance_id in seen_instance_ids:
                    continue
                seen_instance_ids.add(instance_id)
                unit = f"{_WORKER_UNIT_BASE}@{instance_id}.service"
                parts = line.split(None, 1)
                pid = parts[0] if parts else "?"
                instances.append({
                    "unit": unit,
                    "load": "loaded",
                    "active": "active",
                    "sub": "running",
                    "description": f"docker exec pid={pid}",
                })
                continue
            if "run_celery.py" in line and "--instance" not in line:
                parts = line.split(None, 1)
                pid = parts[0] if parts else "?"
                unit = f"{_WORKER_UNIT_BASE}@default.service"
                if any(i.get("unit") == unit for i in instances):
                    continue
                instances.append({
                    "unit": unit,
                    "load": "loaded",
                    "active": "active",
                    "sub": "running",
                    "description": f"docker compose service {_CELERY_COMPOSE_SERVICE} pid={pid}",
                })
        return instances

    async def _pgrep_instance_active(self, instance_id: str) -> bool:
        if instance_id == "default":
            lines = await self._pgrep_celery_lines()
            return any("run_celery.py" in ln and "--instance" not in ln for ln in lines)
        safe = instance_id.replace("\\", "\\\\").replace(".", "\\.")
        rc, out, _ = await self._compose_exec(
            ["pgrep", "-f", f"python.*run_celery\\.py.*--instance {safe}"],
            timeout=20,
        )
        return rc == 0 and bool(out.strip())

    async def _start_celery_instance(self, unit: str) -> Dict[str, Any]:
        instance_id = self._instance_from_worker_unit(unit)
        await self._ensure_celery_container()
        rc, out, err = await self._compose_exec(
            ["python", _CELERY_RUN_SCRIPT, "--instance", instance_id],
            detach=True,
            timeout=60,
        )
        if rc != 0:
            raise RuntimeError(
                f"docker compose exec start worker {instance_id} failed (rc={rc}): {err or out}"
            )
        await asyncio.sleep(1.5)
        if not await self._pgrep_instance_active(instance_id):
            raise RuntimeError(
                f"Worker {instance_id!r} exited immediately after start inside {_CELERY_COMPOSE_SERVICE}."
            )
        return {
            "method": "docker-compose-exec",
            "action": "start",
            "unit": unit,
            "compose_service": _CELERY_COMPOSE_SERVICE,
            "instance_id": instance_id,
            "stdout": out,
            "message": f"Started run_celery.py --instance {instance_id} in {_CELERY_COMPOSE_SERVICE}",
        }

    async def _stop_celery_instance(self, unit: str, *, sigkill: bool = False) -> Dict[str, Any]:
        instance_id = self._instance_from_worker_unit(unit)
        if instance_id == "default":
            return await self._systemctl_compose_service("stop", _CELERY_COMPOSE_SERVICE, unit)

        sig = "KILL" if sigkill else "TERM"
        safe = instance_id.replace("\\", "\\\\").replace(".", "\\.").replace("'", "'\\''")
        shell = f"pkill -{sig} -f 'python.*run_celery\\.py.*--instance {safe}' || true"
        rc, out, err = await self._compose_exec(["sh", "-c", shell], timeout=30)
        if rc != 0:
            raise RuntimeError(
                f"docker compose exec pkill worker {instance_id} failed (rc={rc}): {err or out}"
            )
        if sigkill:
            await asyncio.sleep(0.35)
        return {
            "method": "docker-compose-exec",
            "action": "kill" if sigkill else "stop",
            "unit": unit,
            "instance_id": instance_id,
            "message": f"pkill -{sig} for --instance {instance_id}",
            "stdout": out,
        }

    async def _systemctl_compose_service(
        self,
        action: str,
        compose_svc: str,
        unit: str,
        *,
        timeout: int | None = None,
    ) -> Dict[str, Any]:
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

    async def _redis_stop_celery(self) -> Dict[str, Any]:
        return await self._redis_delegate._redis_stop_celery()  # noqa: SLF001

    async def _systemctl(
        self,
        action: str,
        unit: str,
        timeout: int | None = None,
    ) -> Dict[str, Any]:
        self._validate(action, unit)
        if self._is_celery_worker_unit(unit):
            instance_id = self._instance_from_worker_unit(unit)
            if instance_id != "default":
                if action == "start":
                    return await self._start_celery_instance(unit)
                if action == "stop":
                    return await self._stop_celery_instance(unit)
                if action == "restart":
                    await self._stop_celery_instance(unit)
                    return await self._start_celery_instance(unit)
                raise PermissionError(
                    f"Action {action!r} not supported for docker celery instance control"
                )
            compose_svc = _CELERY_COMPOSE_SERVICE
            return await self._systemctl_compose_service(action, compose_svc, unit, timeout=timeout)

        compose_svc = compose_service_for_systemd_unit(unit)
        if not compose_svc:
            raise PermissionError(
                f"Unit {unit!r} has no compose mapping; docker executor cannot control it."
            )
        return await self._systemctl_compose_service(action, compose_svc, unit, timeout=timeout)

    async def list_instances(self) -> List[Dict[str, str]]:
        """List logical Celery worker units by scanning processes inside ``celery-worker``."""
        lines = await self._pgrep_celery_lines()
        instances = self._instances_from_pgrep_lines(lines)
        if instances:
            return instances
        state = await self._service_state(_CELERY_COMPOSE_SERVICE)
        if state != "running":
            return []
        return [{
            "unit": f"{_WORKER_UNIT_BASE}@default.service",
            "load": "loaded",
            "active": "active",
            "sub": "running",
            "description": f"docker compose service {_CELERY_COMPOSE_SERVICE} (no pgrep match)",
        }]

    async def redis_is_local(self) -> bool:
        """True when redis service exists in compose project (embedded-infra profile)."""
        state = await self._service_state("redis")
        return bool(state)

    async def systemctl_is_active(self, unit: str) -> str:
        if not self._docker_reachable:
            return "unknown"
        try:
            self._validate("start", unit)
        except PermissionError:
            return "unknown"
        if self._is_celery_worker_unit(unit):
            instance_id = self._instance_from_worker_unit(unit)
            try:
                active = await self._pgrep_instance_active(instance_id)
            except Exception as e:
                logger.debug("pgrep instance %s: %s", instance_id, e)
                return "unknown"
            return "active" if active else "inactive"
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
        instance_id = self._instance_from_worker_unit(unit)
        if instance_id == "default":
            return await self._systemctl_compose_service("stop", _CELERY_COMPOSE_SERVICE, unit)
        return await self._stop_celery_instance(unit, sigkill=True)

    def compose_service_for_unit(self, unit: str) -> Optional[str]:
        """Public helper for routers (WP2 runtime_kind)."""
        return compose_service_for_systemd_unit(unit)

    def manages_unit(self, unit: str) -> bool:
        return is_compose_managed_unit(unit)
