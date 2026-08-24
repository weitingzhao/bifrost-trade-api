"""Pydantic models for Ops market-ingest control and audit."""

from __future__ import annotations

import time
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class MarketIngestAction(str, Enum):
    """Market ingest systemd control (includes ``reset`` — IB client release before restart)."""

    START = "start"
    STOP = "stop"
    RESTART = "restart"
    RESET = "reset"


class MarketIngestControlRequest(BaseModel):
    service_id: str = Field(..., min_length=1)
    action: MarketIngestAction


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
