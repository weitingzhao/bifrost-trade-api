"""Local Control Agent — privileged systemd executor over Unix Domain Socket.

Runs as a separate systemd service under a dedicated user with minimal sudo rights.
Listens on a UDS, validates every request against the whitelist, executes via systemctl.

Usage:
    python -m backend.ops.agent.server [--socket /run/bifrost-agent/bifrost-agent.sock]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import signal
import sys
import time
from pathlib import Path
from typing import Any, Dict

from bifrost_api.ops.agent.protocol import (
    ALLOWED_UNIT_PATTERNS,
    AgentRequest,
    AgentResponse,
    validate_action,
    validate_unit,
)

logger = logging.getLogger("bifrost.agent")

DEFAULT_SOCKET = "/run/bifrost-agent/bifrost-agent.sock"
MAX_MSG_SIZE = 8192
_RATE_LIMIT_WINDOW = 10  # seconds
_RATE_LIMIT_MAX = 5  # max commands per window per unit


class RateLimiter:
    def __init__(self, window: float = _RATE_LIMIT_WINDOW, max_count: int = _RATE_LIMIT_MAX):
        self._window = window
        self._max = max_count
        self._log: Dict[str, list[float]] = {}

    def check(self, unit: str) -> bool:
        now = time.monotonic()
        times = self._log.get(unit, [])
        times = [t for t in times if now - t < self._window]
        if len(times) >= self._max:
            return False
        times.append(now)
        self._log[unit] = times
        return True


class AgentServer:
    def __init__(self, socket_path: str = DEFAULT_SOCKET) -> None:
        self._socket_path = socket_path
        self._rate = RateLimiter()
        self._inflight: dict[str, asyncio.Lock] = {}

    def _get_lock(self, unit: str) -> asyncio.Lock:
        if unit not in self._inflight:
            self._inflight[unit] = asyncio.Lock()
        return self._inflight[unit]

    async def handle_request(self, req: AgentRequest) -> AgentResponse:
        if not validate_action(req.action):
            return AgentResponse(
                request_id=req.request_id,
                ok=False,
                error=f"Action {req.action!r} not in whitelist",
            )

        if req.action == "list-units":
            return await self._list_units(req)

        unit_norm = (req.unit or "").strip()
        if req.action == "is-active":
            if not validate_unit(unit_norm):
                return AgentResponse(
                    request_id=req.request_id,
                    ok=False,
                    error=f"Unit {req.unit!r} not in whitelist",
                )
            return await self._systemctl_is_active(req)

        if not validate_unit(unit_norm):
            return AgentResponse(
                request_id=req.request_id,
                ok=False,
                error=f"Unit {req.unit!r} not in whitelist",
            )

        if req.action == "kill":
            if not re.fullmatch(
                r"bifrost-celery-worker@[a-zA-Z0-9_-]+\.service",
                unit_norm,
            ):
                return AgentResponse(
                    request_id=req.request_id,
                    ok=False,
                    error="kill is only allowed for bifrost-celery-worker@<instance>.service",
                )

        if req.action in ("start", "stop", "restart", "kill"):
            if not self._rate.check(req.unit):
                return AgentResponse(
                    request_id=req.request_id,
                    ok=False,
                    error=f"Rate limit exceeded for {req.unit}",
                )

        lock = self._get_lock(req.unit)
        if lock.locked():
            return AgentResponse(
                request_id=req.request_id,
                ok=False,
                error=f"Another command is in-flight for {req.unit}; try again shortly",
            )
        async with lock:
            if req.action == "kill":
                return await self._systemctl_kill(req)
            return await self._systemctl(req)

    async def _systemctl_is_active(self, req: AgentRequest) -> AgentResponse:
        """systemctl is-active may exit non-zero for inactive; still report stdout."""
        # -n: non-interactive; requires NOPASSWD in /etc/sudoers.d/ (see deploy/sudoers/bifrost-agent).
        cmd = ["sudo", "-n", "/usr/bin/systemctl", "is-active", req.unit]
        logger.info("Executing: %s", " ".join(cmd))
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=req.timeout,
            )
        except asyncio.TimeoutError:
            proc.kill()
            return AgentResponse(
                request_id=req.request_id,
                ok=False,
                error=f"Timed out after {req.timeout}s",
            )
        state = (stdout or b"").decode().strip()
        return AgentResponse(
            request_id=req.request_id,
            ok=True,
            result={
                "method": "agent-systemd",
                "action": "is-active",
                "unit": req.unit,
                "returncode": proc.returncode,
                "stdout": state,
                "stderr": (stderr or b"").decode().strip(),
            },
        )

    async def _systemctl(self, req: AgentRequest) -> AgentResponse:
        cmd = ["sudo", "-n", "/usr/bin/systemctl", req.action, req.unit]
        logger.info("Executing: %s", " ".join(cmd))
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=req.timeout,
            )
        except asyncio.TimeoutError:
            proc.kill()
            return AgentResponse(
                request_id=req.request_id,
                ok=False,
                error=f"Timed out after {req.timeout}s",
            )

        ok = proc.returncode == 0
        return AgentResponse(
            request_id=req.request_id,
            ok=ok,
            result={
                "method": "agent-systemd",
                "action": req.action,
                "unit": req.unit,
                "returncode": proc.returncode,
                "stdout": (stdout or b"").decode().strip(),
            },
            error=(stderr or b"").decode().strip() if not ok else None,
        )

    async def _systemctl_kill(self, req: AgentRequest) -> AgentResponse:
        cmd = [
            "sudo",
            "-n",
            "/usr/bin/systemctl",
            "kill",
            "--kill-who=main",
            "-s",
            "KILL",
            req.unit,
        ]
        logger.info("Executing: %s", " ".join(cmd))
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=req.timeout,
            )
        except asyncio.TimeoutError:
            proc.kill()
            return AgentResponse(
                request_id=req.request_id,
                ok=False,
                error=f"Timed out after {req.timeout}s",
            )

        ok = proc.returncode == 0
        return AgentResponse(
            request_id=req.request_id,
            ok=ok,
            result={
                "method": "agent-systemd",
                "action": "kill",
                "unit": req.unit,
                "returncode": proc.returncode,
                "stdout": (stdout or b"").decode().strip(),
            },
            error=(stderr or b"").decode().strip() if not ok else None,
        )

    async def _list_units(self, req: AgentRequest) -> AgentResponse:
        pattern = req.unit or "bifrost-celery-worker@*"
        cmd = ["/usr/bin/systemctl", "list-units", pattern, "--no-legend", "--no-pager", "--plain"]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
        lines = (stdout or b"").decode().strip().splitlines()
        instances = []
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
        return AgentResponse(
            request_id=req.request_id,
            ok=True,
            result={"instances": instances},
        )

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
    ) -> None:
        try:
            data = await asyncio.wait_for(reader.read(MAX_MSG_SIZE), timeout=5)
            if not data:
                return
            req_dict = json.loads(data.decode())
            req = AgentRequest.from_dict(req_dict)
            logger.info("Request %s: %s %s", req.request_id, req.action, req.unit)
            resp = await self.handle_request(req)
            writer.write(json.dumps(resp.to_dict()).encode() + b"\n")
            await writer.drain()
        except Exception as e:
            logger.error("Client handler error: %s", e)
            try:
                err_resp = AgentResponse(request_id="?", ok=False, error=str(e))
                writer.write(json.dumps(err_resp.to_dict()).encode() + b"\n")
                await writer.drain()
            except Exception:
                pass
        finally:
            writer.close()

    async def serve(self) -> None:
        sock_path = Path(self._socket_path)
        if sock_path.exists():
            sock_path.unlink()

        server = await asyncio.start_unix_server(
            self._handle_client, path=str(sock_path),
        )
        os.chmod(str(sock_path), 0o660)
        logger.info("Agent listening on %s", sock_path)
        logger.info(
            "Whitelisted systemd unit patterns: %s",
            [p.pattern for p in ALLOWED_UNIT_PATTERNS],
        )

        loop = asyncio.get_running_loop()
        stop = loop.create_future()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop.set_result, None)

        async with server:
            await stop

        sock_path.unlink(missing_ok=True)
        logger.info("Agent stopped")


def main() -> None:
    parser = argparse.ArgumentParser(description="Bifrost Local Control Agent")
    parser.add_argument("--socket", default=DEFAULT_SOCKET, help="UDS path")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    )

    agent = AgentServer(socket_path=args.socket)
    asyncio.run(agent.serve())


if __name__ == "__main__":
    main()
