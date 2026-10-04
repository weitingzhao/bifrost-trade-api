"""Pydantic models for the Ops audit log."""

from __future__ import annotations

import time
from typing import Optional

from pydantic import BaseModel, Field

# ── Audit ─────────────────────────────────────────────────────────────────────


class AuditEntry(BaseModel):
    timestamp: float = Field(default_factory=time.time)
    operator: str = "unknown"
    source_ip: Optional[str] = None
    action: str
    target: str
    command_id: Optional[str] = None
    outcome: str
    detail: Optional[str] = None
