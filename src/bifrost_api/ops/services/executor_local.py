"""Restricted local executor — whitelisted systemd units + Redis-based Celery stop.

Phase 1: single-host. Phase 2: remote agent protocol (host_id routing).
"""

from __future__ import annotations

import asyncio
import fnmatch
import json
import logging
import os
import re
import signal
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)

_SYSTEMD_TIMEOUT_SEC = 30
_ALLOWED_ACTIONS = frozenset({"start", "stop", "restart"})
_WORKER_UNIT_BASE = "bifrost-celery-worker"
_INSTANCE_ID_RE = re.compile(r"^[a-zA-Z0-9_-]+$")

# Long-running daemon / IB edge entrypoints live under scripts/systemd/ (WorkingDirectory = project root).
_SCRIPTS_IN_SYSTEMD_SUBDIR = frozenset({
    "run_engine.py",
    "run_celery.py",
    "run_massive_ws.py",
    "run_ib_operator.py",
    "run_ib_ingestor.py",
    "run_ib_account_agent.py",
    "run_account_sync_daemon.py",
})


def _ingest_script_abs_path(project_root: Path, script_name: str) -> Path:
    if script_name in _SCRIPTS_IN_SYSTEMD_SUBDIR:
        systemd_path = project_root / "scripts" / "systemd" / script_name
        if systemd_path.is_file():
            return systemd_path
        flat_path = project_root / "scripts" / script_name
        if flat_path.is_file():
            return flat_path
        return systemd_path
    return project_root / "scripts" / script_name


def _ingest_script_log_for_unit(unit: str) -> Optional[Tuple[str, str]]:
    """Return (script_filename, log_filename) for market ingest units."""
    stem = unit.replace(".service", "").strip()
    if "massive-ws" in stem or stem == "bifrost-massive-ws":
        return ("run_massive_ws.py", "massive-ws.log")
    if "ib-operator" in stem or stem == "bifrost-ib-operator":
        return ("run_ib_operator.py", "ib-operator.log")
    if "ib-account-agent" in stem or stem == "bifrost-ib-account-agent":
        return ("run_ib_account_agent.py", "ib-account-agent.log")
    if "ib-market-gateway" in stem or "ib-ingestor" in stem or stem in (
        "bifrost-ib-market-gateway",
        "bifrost-ib-ingestor",
    ):
        return ("run_ib_market_gateway.py", "ib-market-gateway.log")
    if stem == "bifrost-engine" or "bifrost-engine" in stem:
        return ("run_engine.py", "engine.log")
    # Account Sync Daemon (not Celery): scripts/systemd/run_account_sync_daemon.py
    if "account-sync-daemon-dev" in stem:
        return ("run_account_sync_daemon.py", "account-sync-daemon-dev.log")
    if "account-sync-daemon" in stem:
        return ("run_account_sync_daemon.py", "account-sync-daemon.log")
    return None


class RestrictedExecutor:
    """Execute whitelisted systemd unit actions.

    Stop mechanism for Celery workers uses the existing Redis key pattern
    (``bifrost:worker_stop_requested``).  Start/restart use systemd.
    """

    def __init__(
        self,
        allowed_units: list[str],
        broker_url: str,
        use_redis_stop: bool = True,
    ) -> None:
        self._allowed: Set[str] = set(allowed_units)
        self._broker_url = broker_url
        self._use_redis_stop = use_redis_stop

    # ── unit helpers ──────────────────────────────────────────────────────────

    @staticmethod
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

    @staticmethod
    def instance_unit(instance_id: str) -> str:
        """Build template-instance unit for a numeric/named instance."""
        if not _INSTANCE_ID_RE.match(instance_id):
            raise ValueError(f"Invalid instance_id: {instance_id!r}")
        return f"{_WORKER_UNIT_BASE}@{instance_id}.service"

    @staticmethod
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

    def _validate(self, action: str, unit: str) -> None:
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

    async def _redis_stop_celery(self) -> Dict[str, Any]:
        """Stop Celery worker via existing Redis key pattern."""
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

    async def _systemctl(
        self, action: str, unit: str, timeout: int = _SYSTEMD_TIMEOUT_SEC,
    ) -> Dict[str, Any]:
        # Preflight 1: verify sudo is non-interactive for this process user.
        pre_sudo = await asyncio.create_subprocess_exec(
            "sudo", "-n", "true",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, pre_sudo_err = await pre_sudo.communicate()
        if pre_sudo.returncode != 0:
            raise RuntimeError(
                "Ops executor requires non-interactive sudo (NOPASSWD). "
                "Current host returned: "
                f"{(pre_sudo_err or b'').decode().strip() or 'sudo -n failed'}"
            )

        cmd = ["sudo", "systemctl", action, unit]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=timeout,
            )
        except asyncio.TimeoutError as exc:
            proc.kill()
            raise asyncio.TimeoutError(
                f"systemctl {action} {unit} timed out after {timeout}s"
            ) from exc
        if proc.returncode != 0:
            err_msg = (stderr or b"").decode().strip()
            raise RuntimeError(
                f"systemctl {action} {unit} failed (rc={proc.returncode}): {err_msg}"
            )
        return {
            "method": "systemd",
            "action": action,
            "unit": unit,
            "returncode": proc.returncode,
            "stdout": (stdout or b"").decode().strip(),
        }

    async def force_stop_worker_unit(
        self, unit: str, timeout: int = _SYSTEMD_TIMEOUT_SEC,
    ) -> Dict[str, Any]:
        """Send SIGKILL to the unit's main process (``systemctl kill``) for stuck workers."""
        self._validate("stop", unit)
        self.assert_celery_worker_instance_unit(unit)
        pre_sudo = await asyncio.create_subprocess_exec(
            "sudo", "-n", "true",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, pre_sudo_err = await pre_sudo.communicate()
        if pre_sudo.returncode != 0:
            raise RuntimeError(
                "Ops executor requires non-interactive sudo (NOPASSWD). "
                "Current host returned: "
                f"{(pre_sudo_err or b'').decode().strip() or 'sudo -n failed'}"
            )
        cmd = [
            "sudo", "systemctl", "kill", "--kill-who=main", "-s", "KILL", unit,
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=timeout,
            )
        except asyncio.TimeoutError as exc:
            proc.kill()
            raise asyncio.TimeoutError(
                f"systemctl kill {unit} timed out after {timeout}s"
            ) from exc
        if proc.returncode != 0:
            err_msg = (stderr or b"").decode().strip()
            raise RuntimeError(
                f"systemctl kill {unit} failed (rc={proc.returncode}): {err_msg}"
            )
        return {
            "method": "systemd",
            "action": "kill",
            "unit": unit,
            "returncode": proc.returncode,
            "stdout": (stdout or b"").decode().strip(),
        }

    async def list_instances(self) -> List[Dict[str, str]]:
        """List active systemd template instances for bifrost-celery-worker@*."""
        cmd = [
            "systemctl", "list-units",
            f"{_WORKER_UNIT_BASE}@*",
            "--no-legend", "--no-pager", "--plain",
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
        lines = (stdout or b"").decode().strip().splitlines()
        instances: List[Dict[str, str]] = []
        for line in lines:
            parts = line.split(None, 4)
            if len(parts) >= 4:
                instances.append({
                    "unit": parts[0],
                    "load": parts[1],
                    "active": parts[2],
                    "sub": parts[3],
                    "description": parts[4] if len(parts) > 4 else "",
                })
        return instances

    async def redis_is_local(self) -> bool:
        """Check if Redis is managed locally via systemd."""
        try:
            proc = await asyncio.create_subprocess_exec(
                "systemctl", "is-active", "redis",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=5)
            state = (stdout or b"").decode().strip()
            return state in ("active", "inactive", "failed")
        except Exception:
            return False

    async def systemctl_redis(self, action: str) -> Dict[str, Any]:
        """Start / stop / restart local Redis via systemd."""
        if action not in _ALLOWED_ACTIONS:
            raise PermissionError(f"Action {action!r} not allowed for Redis")
        return await self._systemctl(action, "redis")

    # Match ``systemctl is-active`` / ``systemctl --state=help`` active-ish names; avoid false ``unknown`` → UI yellow.
    _IS_ACTIVE_STATES = frozenset({
        "active",
        "inactive",
        "activating",
        "deactivating",
        "failed",
        "reloading",
        "dead",
        "maintenance",
        "refreshing",
    })

    @classmethod
    def _normalize_is_active_stdout(cls, raw: str) -> str:
        t = (raw or "").strip()
        if not t:
            return ""
        return t.splitlines()[0].strip()

    async def systemctl_is_active(self, unit: str) -> str:
        """Return systemd ``is-active`` stdout (active|inactive|…); ``unknown`` on error."""
        self._validate("start", unit)
        try:
            proc = await asyncio.create_subprocess_exec(
                "systemctl", "is-active", unit,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
        except Exception:
            return "unknown"
        state = self._normalize_is_active_stdout((stdout or b"").decode())
        return state if state in self._IS_ACTIVE_STATES else "unknown"


class SubprocessLocalExecutor:
    """Local executor without systemd: Celery via ``run_celery.py``; ingest via Massive/IB scripts.

    Use on macOS or any host where ``systemctl`` is unavailable. Same API as
    :class:`RestrictedExecutor` for the Ops router (scale, list_instances, market ingest).

    Redis broker control still goes through :class:`RestrictedExecutor` (systemd),
    so ``systemctl_redis`` requires Linux + sudo like before.

    Worker start spawns a detached child process; worker stop matches
    ``pgrep`` + ``SIGTERM`` (same idea as ``run_celery.py`` duplicate kill).

    Market ingest units (``bifrost-massive-ws``, ``bifrost-ib-operator``,
    ``bifrost-ib-ingestor``, ``bifrost-ib-account-agent``) start with ``scripts/systemd/run_massive_ws.py`` or
    ``scripts/systemd/run_ib_*.py``; **Account Sync Daemon** uses
    ``scripts/systemd/run_account_sync_daemon.py`` (IB account stream → PostgreSQL, not Celery).
    Optional resolved YAML path (``--config`` or positional for gateway).
    """

    worker_to_unit = staticmethod(RestrictedExecutor.worker_to_unit)
    instance_unit = staticmethod(RestrictedExecutor.instance_unit)

    def __init__(
        self,
        allowed_units: list[str],
        broker_url: str,
        use_redis_stop: bool,
        project_root: Path | str,
        python_executable: str | None = None,
        resolved_config_path: str | Path | None = None,
        socket_project_root: Path | str | None = None,
    ) -> None:
        self._project_root = Path(project_root).resolve()
        self._ingest_root = Path(socket_project_root or project_root).resolve()
        self._python = python_executable or sys.executable
        self._resolved_config_path: Optional[Path] = (
            Path(resolved_config_path).resolve()
            if resolved_config_path
            else None
        )
        self._systemd = RestrictedExecutor(
            allowed_units=allowed_units,
            broker_url=broker_url,
            use_redis_stop=use_redis_stop,
        )

    def _validate(self, action: str, unit: str) -> None:
        self._systemd._validate(action, unit)  # noqa: SLF001

    async def _redis_stop_celery(self) -> Dict[str, Any]:
        return await self._systemd._redis_stop_celery()  # noqa: SLF001

    @staticmethod
    def _instance_from_worker_unit(unit: str) -> str:
        prefix = f"{_WORKER_UNIT_BASE}@"
        if not unit.startswith(prefix) or not unit.endswith(".service"):
            raise ValueError(
                f"Not a {_WORKER_UNIT_BASE}@ template unit: {unit!r}"
            )
        return unit[len(prefix) : -len(".service")]

    async def _start_worker_unit(self, unit: str) -> Dict[str, Any]:
        instance_id = self._instance_from_worker_unit(unit)
        script = self._project_root / "scripts" / "systemd" / "run_celery.py"
        if not script.is_file():
            raise RuntimeError(f"run_celery.py not found at {script}")
        cmd = [self._python, str(script), "--instance", instance_id]
        env = os.environ.copy()
        log_dir = self._project_root / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"celery-{instance_id}.log"
        log_fp = open(log_file, "ab", buffering=0)
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                cwd=str(self._project_root),
                stdout=log_fp,
                stderr=asyncio.subprocess.STDOUT,
                start_new_session=True,
                env=env,
            )
        finally:
            log_fp.close()
        try:
            await asyncio.wait_for(proc.wait(), timeout=2.5)
            tail = ""
            if log_file.is_file():
                try:
                    with open(log_file, "rb") as lf:
                        lf.seek(0, os.SEEK_END)
                        sz = lf.tell()
                        lf.seek(max(0, sz - 4000))
                        tail = lf.read().decode("utf-8", errors="replace")
                except OSError:
                    tail = ""
            raise RuntimeError(
                f"Worker exited immediately (rc={proc.returncode}). "
                f"Log: {log_file}. Tail: {tail.strip() or '(empty)'}"
            )
        except asyncio.TimeoutError:
            pass
        return {
            "method": "subprocess",
            "action": "start",
            "unit": unit,
            "pid": proc.pid,
            "message": "Worker process started in background (run_celery.py).",
        }

    async def _pgrep_worker_pids(self, unit: str) -> List[str]:
        instance_id = self._instance_from_worker_unit(unit)
        safe = instance_id.replace("\\", "\\\\").replace(".", "\\.")
        pattern = f"python.*run_celery\\.py.*--instance {safe}"
        pg = await asyncio.create_subprocess_exec(
            "pgrep", "-f", pattern,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(pg.communicate(), timeout=15)
        if pg.returncode != 0:
            return []
        return [
            x.strip()
            for x in stdout.decode().strip().splitlines()
            if x.strip().isdigit()
        ]

    async def _stop_worker_unit(self, unit: str) -> Dict[str, Any]:
        pids = await self._pgrep_worker_pids(unit)
        if not pids:
            return {
                "method": "subprocess",
                "action": "stop",
                "unit": unit,
                "message": "No matching worker process (already stopped).",
            }
        killed: List[str] = []
        for pid_str in pids:
            try:
                os.kill(int(pid_str), signal.SIGTERM)
                killed.append(pid_str)
            except (ProcessLookupError, ValueError):
                pass
        return {
            "method": "subprocess",
            "action": "stop",
            "unit": unit,
            "pids": killed,
            "message": f"SIGTERM sent to {killed}" if killed else "No PIDs killed",
        }

    async def force_stop_worker_unit(self, unit: str) -> Dict[str, Any]:
        """SIGKILL matching ``run_celery.py --instance`` PIDs (macOS / subprocess mode)."""
        self._validate("stop", unit)
        RestrictedExecutor.assert_celery_worker_instance_unit(unit)
        pids = await self._pgrep_worker_pids(unit)
        if not pids:
            return {
                "method": "subprocess",
                "action": "kill",
                "unit": unit,
                "sigkill_pids": [],
                "message": "No matching worker process (already stopped).",
            }
        sigkill_pids: List[str] = []
        for pid_str in pids:
            try:
                pid = int(pid_str)
            except ValueError:
                continue
            try:
                os.killpg(os.getpgid(pid), signal.SIGKILL)
                sigkill_pids.append(pid_str)
            except OSError:
                try:
                    os.kill(pid, signal.SIGKILL)
                    sigkill_pids.append(pid_str)
                except (ProcessLookupError, PermissionError, ValueError):
                    pass
        await asyncio.sleep(0.35)
        return {
            "method": "subprocess",
            "action": "kill",
            "unit": unit,
            "sigkill_pids": sigkill_pids,
            "message": f"SIGKILL sent to {sigkill_pids}" if sigkill_pids else "No PIDs killed",
        }

    def _append_ingest_config_argv(self, cmd: List[str], script_name: str) -> None:
        if self._resolved_config_path is None:
            return
        cfg = str(self._resolved_config_path)
        if script_name == "run_ib_operator.py":
            cmd.append(cfg)
        else:
            cmd.extend(["--config", cfg])

    def _cmd_looks_like_repo_ingest(self, cmd: str, script_name: str) -> bool:
        """True if ``ps`` command line is this repo's ingest script (not other clones)."""
        if script_name not in cmd:
            return False
        norm_cmd = cmd.replace("\\", "/")
        norm_root = str(self._ingest_root.resolve()).replace("\\", "/")
        if norm_root in norm_cmd:
            return True
        abs_script = str(
            _ingest_script_abs_path(self._ingest_root, script_name).resolve()
        ).replace("\\", "/")
        if abs_script in norm_cmd:
            return True
        if sys.platform == "darwin":
            if norm_root.casefold() in norm_cmd.casefold():
                return True
            if abs_script.casefold() in norm_cmd.casefold():
                return True
        return False

    async def _ingest_ps_command_lines(self) -> List[Tuple[str, str]]:
        proc = await asyncio.create_subprocess_exec(
            "ps", "axww", "-o", "pid=,command=",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=25)
        rows: List[Tuple[str, str]] = []
        for line in (stdout or b"").decode("utf-8", errors="replace").splitlines():
            line = line.strip()
            if not line:
                continue
            parts = line.split(None, 1)
            if len(parts) < 2 or not parts[0].isdigit():
                continue
            rows.append((parts[0], parts[1]))
        return rows

    async def _ps_command_for_pid(self, pid: int) -> Optional[str]:
        proc = await asyncio.create_subprocess_exec(
            "ps", "-p", str(pid), "-o", "command=",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=5)
        if proc.returncode != 0:
            return None
        t = (stdout or b"").decode("utf-8", errors="replace").strip()
        return t or None

    def _ingest_ops_pid_stem(self, unit: str) -> Optional[str]:
        u = unit.replace(".service", "").strip()
        if "ib-market-gateway" in u or "ib-ingestor" in u:
            return "ib-market-gateway"
        if "ib-operator" in u:
            return "ib-operator"
        if "ib-account-agent" in u:
            return "ib-account-agent"
        if "massive-ws" in u:
            return "massive-ws"
        if "account-sync-daemon-dev" in u:
            return "account-sync-daemon-dev"
        if "account-sync-daemon" in u:
            return "account-sync-daemon"
        return None

    def _ingest_ops_pid_path(self, unit: str) -> Optional[Path]:
        stem = self._ingest_ops_pid_stem(unit)
        if not stem:
            return None
        return self._ingest_root / "logs" / f".ops-ingest-{stem}.pid"

    @staticmethod
    def _pid_is_alive(pid: int) -> bool:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        return True

    async def _ingest_subprocess_is_active(self, unit: str) -> str:
        spec = _ingest_script_log_for_unit(unit)
        if not spec:
            return "unknown"
        script_name, _ = spec
        pp = self._ingest_ops_pid_path(unit)
        if pp is not None and pp.is_file():
            try:
                raw = pp.read_text(encoding="utf-8").strip()
                if raw.isdigit() and self._pid_is_alive(int(raw)):
                    cmd_line = await self._ps_command_for_pid(int(raw))
                    if cmd_line and self._cmd_looks_like_repo_ingest(cmd_line, script_name):
                        return "active"
                    pp.unlink(missing_ok=True)
                else:
                    pp.unlink(missing_ok=True)
            except OSError:
                pass
        matches = await self._ingest_matching_pids(script_name)
        return "active" if matches else "inactive"

    async def _ingest_matching_pids(self, script_name: str) -> List[str]:
        rows = await self._ingest_ps_command_lines()
        pids = [
            pid
            for pid, cmd in rows
            if self._cmd_looks_like_repo_ingest(cmd, script_name)
        ]
        return pids

    async def _start_ingest_unit(self, unit: str) -> Dict[str, Any]:
        spec = _ingest_script_log_for_unit(unit)
        if not spec:
            raise ValueError(f"Not an ingest unit: {unit!r}")
        script_name, log_name = spec
        existing = await self._ingest_matching_pids(script_name)
        if existing:
            raise RuntimeError(
                f"ingest_already_running: pids={existing} unit={unit!r} "
                "(stop first or use restart)"
            )
        script = _ingest_script_abs_path(self._ingest_root, script_name)
        if not script.is_file():
            raise RuntimeError(f"{script_name} not found at {script}")
        cmd: List[str] = [self._python, str(script)]
        self._append_ingest_config_argv(cmd, script_name)
        log_dir = self._ingest_root / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / log_name
        log_fp = open(log_file, "ab", buffering=0)
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                cwd=str(self._ingest_root),
                stdout=log_fp,
                stderr=asyncio.subprocess.STDOUT,
                start_new_session=True,
                env=os.environ.copy(),
            )
        finally:
            log_fp.close()
        try:
            await asyncio.wait_for(proc.wait(), timeout=2.5)
            tail = ""
            if log_file.is_file():
                try:
                    with open(log_file, "rb") as lf:
                        lf.seek(0, os.SEEK_END)
                        sz = lf.tell()
                        lf.seek(max(0, sz - 4000))
                        tail = lf.read().decode("utf-8", errors="replace")
                except OSError:
                    tail = ""
            raise RuntimeError(
                f"Ingest exited immediately (rc={proc.returncode}). "
                f"Log: {log_file}. Tail: {tail.strip() or '(empty)'}"
            )
        except asyncio.TimeoutError:
            pass
        pp = self._ingest_ops_pid_path(unit)
        if pp is not None:
            try:
                pp.write_text(str(proc.pid), encoding="utf-8")
            except OSError as e:
                logger.warning("Could not write ops ingest pid file %s: %s", pp, e)
        return {
            "method": "subprocess",
            "action": "start",
            "unit": unit,
            "pid": proc.pid,
            "script": script_name,
            "message": f"Ingest started in background ({script_name}).",
        }

    async def _stop_ingest_unit(self, unit: str) -> Dict[str, Any]:
        spec = _ingest_script_log_for_unit(unit)
        if not spec:
            raise ValueError(f"Not an ingest unit: {unit!r}")
        script_name, _ = spec
        pp = self._ingest_ops_pid_path(unit)
        from_file: List[str] = []
        if pp is not None and pp.is_file():
            try:
                raw = pp.read_text(encoding="utf-8").strip()
                if raw.isdigit() and self._pid_is_alive(int(raw)):
                    cmd_line = await self._ps_command_for_pid(int(raw))
                    if cmd_line and self._cmd_looks_like_repo_ingest(cmd_line, script_name):
                        from_file.append(raw)
            except OSError:
                pass
        from_ps = await self._ingest_matching_pids(script_name)
        ordered: List[str] = []
        seen: Set[str] = set()
        for p in from_file + from_ps:
            if p not in seen:
                seen.add(p)
                ordered.append(p)
        if not ordered:
            if pp is not None and pp.is_file():
                try:
                    pp.unlink(missing_ok=True)
                except OSError:
                    pass
            return {
                "method": "subprocess",
                "action": "stop",
                "unit": unit,
                "message": "No matching ingest process (already stopped).",
            }
        killed: List[str] = []
        for pid_str in ordered:
            try:
                pid = int(pid_str)
            except ValueError:
                continue
            sent = False
            try:
                os.killpg(os.getpgid(pid), signal.SIGTERM)
                sent = True
            except OSError:
                try:
                    os.kill(pid, signal.SIGTERM)
                    sent = True
                except (ProcessLookupError, PermissionError):
                    pass
            if sent:
                killed.append(pid_str)
        await asyncio.sleep(2.0)
        lingering = await self._ingest_matching_pids(script_name)
        sigkill_pids: List[str] = []
        for pid_str in lingering:
            try:
                os.kill(int(pid_str), signal.SIGKILL)
                sigkill_pids.append(pid_str)
            except (ProcessLookupError, PermissionError, ValueError):
                pass
            except OSError:
                pass
        if pp is not None and pp.is_file():
            try:
                pp.unlink(missing_ok=True)
            except OSError:
                pass
        msg_parts = []
        if killed:
            msg_parts.append(f"SIGTERM {killed}")
        if sigkill_pids:
            msg_parts.append(f"SIGKILL {sigkill_pids}")
        return {
            "method": "subprocess",
            "action": "stop",
            "unit": unit,
            "pids": killed,
            "sigkill_pids": sigkill_pids,
            "message": "; ".join(msg_parts) if msg_parts else "No PIDs killed",
        }

    async def _restart_ingest_unit(self, unit: str) -> Dict[str, Any]:
        await self._stop_ingest_unit(unit)
        await asyncio.sleep(1)
        started = await self._start_ingest_unit(unit)
        return {
            "method": "subprocess",
            "action": "restart",
            "unit": unit,
            "start": started,
            "message": "Ingest stop + start completed.",
        }

    async def _systemctl(
        self, action: str, unit: str, timeout: int = _SYSTEMD_TIMEOUT_SEC,
    ) -> Dict[str, Any]:
        if unit == "redis":
            return await self._systemd._systemctl(action, unit, timeout=timeout)  # noqa: SLF001
        u = unit.strip()
        if u.startswith(f"{_WORKER_UNIT_BASE}@") and u.endswith(".service"):
            if action == "start":
                return await self._start_worker_unit(u)
            if action == "stop":
                return await self._stop_worker_unit(u)
            if action == "restart":
                await self._stop_worker_unit(u)
                return await self._start_worker_unit(u)
            raise PermissionError(
                f"Action {action!r} not supported for subprocess worker control"
            )
        if _ingest_script_log_for_unit(u) is not None:
            if action == "start":
                return await self._start_ingest_unit(u)
            if action == "stop":
                return await self._stop_ingest_unit(u)
            if action == "restart":
                return await self._restart_ingest_unit(u)
            raise PermissionError(
                f"Action {action!r} not supported for subprocess ingest control"
            )
        raise PermissionError(
            f"Unit {unit!r} not supported in subprocess mode "
            f"(use {_WORKER_UNIT_BASE}@…, ingest units, or account-sync-daemon units)."
        )

    async def list_instances(self) -> List[Dict[str, str]]:
        proc = await asyncio.create_subprocess_exec(
            "pgrep", "-af", "run_celery.py",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
        if proc.returncode not in (0, 1):
            return []
        text = (stdout or b"").decode(errors="replace").strip()
        if not text:
            return []
        # One logical worker unit may match multiple PIDs: prefork pool children inherit the same argv
        # (``run_celery.py --instance profile-N``), so ``pgrep -af`` returns a line per process.
        inst_re = re.compile(r"--instance\s+(\S+)")
        seen_instance_ids: set[str] = set()
        instances: List[Dict[str, str]] = []
        for line in text.splitlines():
            m = inst_re.search(line)
            if not m:
                continue
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
                "description": f"subprocess pid={pid}",
            })
        return instances

    async def redis_is_local(self) -> bool:
        return await self._systemd.redis_is_local()

    async def systemctl_redis(self, action: str) -> Dict[str, Any]:
        return await self._systemd.systemctl_redis(action)

    async def systemctl_is_active(self, unit: str) -> str:
        u = unit.strip()
        if u in ("redis", "redis.service"):
            return await self._systemd.systemctl_is_active(u)
        try:
            self._validate("start", u)
        except PermissionError:
            return "unknown"
        if u.startswith(f"{_WORKER_UNIT_BASE}@"):
            instances = await self.list_instances()
            for row in instances:
                if row.get("unit") == u:
                    act = (row.get("active") or "").lower()
                    return "active" if act == "active" else "inactive"
            return "inactive"
        if _ingest_script_log_for_unit(u) is not None:
            return await self._ingest_subprocess_is_active(u)
        try:
            return await self._systemd.systemctl_is_active(u)
        except Exception:
            return "unknown"
