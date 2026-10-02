"""Shared dependencies for Research routers."""
from typing import Optional
from fastapi import Request


def db_config(request: Request) -> Optional[dict]:
    """The DB config a Research read uses: the control config, else the read-only one.

    One copy for every Research router (TD-64); there were four identical ones."""
    return getattr(request.app.state, "control_via_db", None) or getattr(request.app.state, "status_cfg_for_read", None)
