"""UDS client for communicating with the Local Control Agent."""

from __future__ import annotations

import asyncio
import json
import logging

from bifrost_api.ops.agent.protocol import AgentRequest, AgentResponse

logger = logging.getLogger(__name__)

DEFAULT_SOCKET = "/run/bifrost-agent/bifrost-agent.sock"
_CONNECT_TIMEOUT = 5
# Must exceed longest AgentRequest.timeout (stop waits for systemd TimeoutStopSec, often 60s).
_READ_TIMEOUT_MIN = 35
_READ_TIMEOUT_PADDING = 15


class AgentClient:
    """Send structured commands to the Local Control Agent over UDS."""

    def __init__(self, socket_path: str = DEFAULT_SOCKET) -> None:
        self._socket_path = socket_path

    async def send(self, req: AgentRequest) -> AgentResponse:
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_unix_connection(self._socket_path),
                timeout=_CONNECT_TIMEOUT,
            )
        except (ConnectionRefusedError, FileNotFoundError, asyncio.TimeoutError) as e:
            return AgentResponse(
                request_id=req.request_id,
                ok=False,
                error=f"Agent unavailable ({self._socket_path}): {e}",
            )

        try:
            payload = json.dumps(req.to_dict()).encode()
            writer.write(payload)
            await writer.drain()
            writer.write_eof()

            read_deadline = max(_READ_TIMEOUT_MIN, int(req.timeout) + _READ_TIMEOUT_PADDING)
            data = await asyncio.wait_for(reader.read(65536), timeout=read_deadline)
            if not data:
                return AgentResponse(
                    request_id=req.request_id,
                    ok=False,
                    error="Empty response from agent",
                )
            resp_dict = json.loads(data.decode())
            return AgentResponse.from_dict(resp_dict)
        except asyncio.TimeoutError:
            _rd = max(_READ_TIMEOUT_MIN, int(req.timeout) + _READ_TIMEOUT_PADDING)
            return AgentResponse(
                request_id=req.request_id,
                ok=False,
                error=f"Agent response timed out after {_rd}s (UDS read; increase request timeout if needed)",
            )
        except Exception as e:
            return AgentResponse(
                request_id=req.request_id,
                ok=False,
                error=f"Agent communication error: {e}",
            )
        finally:
            writer.close()

    async def systemctl(self, action: str, unit: str, timeout: int = 30) -> AgentResponse:
        """Send systemctl to agent. Use long timeouts for stop/restart via :class:`AgentExecutor`."""
        req = AgentRequest(action=action, unit=unit, timeout=timeout)
        return await self.send(req)

    async def list_instances(self, pattern: str = "bifrost-celery-worker@*") -> AgentResponse:
        req = AgentRequest(action="list-units", unit=pattern)
        return await self.send(req)

    async def is_active(self, unit: str) -> AgentResponse:
        req = AgentRequest(action="is-active", unit=unit)
        return await self.send(req)

    async def worker_kill(self, unit: str, timeout: int = 30) -> AgentResponse:
        """``systemctl kill --kill-who=main -s KILL`` for stuck Celery worker instances."""
        req = AgentRequest(action="kill", unit=unit, timeout=timeout)
        return await self.send(req)

    async def ping(self) -> bool:
        try:
            await self.is_active("bifrost-celery-worker.service")
            return True
        except Exception:  # noqa: BLE001
            return False
