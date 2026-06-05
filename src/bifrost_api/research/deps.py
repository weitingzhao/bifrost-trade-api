"""Shared dependencies for Research routers."""
from typing import Optional
from fastapi import Request


def db_config(request: Request) -> Optional[dict]:
    """FastAPI dependency: return DB config from app.state."""
    return getattr(request.app.state, "control_via_db", None) or getattr(request.app.state, "status_cfg_for_read", None)
