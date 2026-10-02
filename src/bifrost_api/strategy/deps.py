"""Config lookups shared by the strategy routers (TD-64).

Writes need Postgres; without it every write route answers the same 503, from
one place, instead of four per-router helpers and a dozen inline copies.
"""

from __future__ import annotations

from typing import Optional

from fastapi import HTTPException, Request

DB_NOT_CONFIGURED = "Database control not configured"


def db_not_configured() -> HTTPException:
    """The 503 a write answers when Postgres is not configured for writes."""
    return HTTPException(status_code=503, detail=DB_NOT_CONFIGURED)


def write_config(request: Request) -> dict:
    """The write config, or raise the 503."""
    control_via_db = getattr(request.app.state, "control_via_db", None)
    if not control_via_db:
        raise db_not_configured()
    return control_via_db


def read_config(request: Request) -> Optional[dict]:
    """The read config; None lets the reader say what it can."""
    return getattr(request.app.state, "status_cfg_for_read", None)
