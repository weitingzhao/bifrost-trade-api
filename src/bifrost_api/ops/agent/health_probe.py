"""Async probe for Local Control Agent UDS (Ops /health when executor_mode=agent)."""

from __future__ import annotations

from typing import Optional, Tuple

from bifrost_api.ops.agent.client import AgentClient


async def probe_agent_reachability(socket_path: str) -> Tuple[bool, Optional[str]]:
    """Return (reachable, error_message).

    Uses a whitelisted ``systemctl is-active`` via the agent (redis unit) so the check
    exercises the same path as real control traffic, not only TCP connect.
    """
    client = AgentClient(socket_path)
    resp = await client.is_active("redis")
    if resp.ok:
        return True, None
    return False, resp.error or "agent probe failed"
