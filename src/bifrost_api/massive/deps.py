"""Shared dependencies for Massive routers (no circular imports back to monitor HTTP routers)."""

from __future__ import annotations

from typing import Optional

from fastapi import Request


def db_config(request: Request) -> Optional[dict]:
    """Return a DB config dict from app.state, matching the old ``_db_config`` helper."""
    return request.app.state.control_via_db or getattr(request.app.state, "status_cfg_for_read", None)
