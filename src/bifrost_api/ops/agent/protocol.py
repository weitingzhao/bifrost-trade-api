"""Structured JSON protocol for the Local Control Agent (UDS).

Request:  {"id": "<uuid>", "action": "start|stop|restart|kill|…", "unit": "<systemd-unit>"}
Response: {"id": "<uuid>", "ok": true|false, "result": {...}, "error": "..."}
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

ALLOWED_ACTIONS = frozenset({
    "start",
    "stop",
    "restart",
    "kill",
    "is-active",
    "list-units",
})
ALLOWED_UNIT_PATTERNS = [
    re.compile(r"^bifrost-celery-worker(@[a-zA-Z0-9_-]+)?\.service$"),
    re.compile(r"^bifrost-celery-beat\.service$"),
    re.compile(r"^redis(\.service)?$"),
    re.compile(r"^bifrost-massive-ws\.service$"),
    re.compile(r"^bifrost-ib-operator\.service$"),
    re.compile(r"^bifrost-ib-ingestor\.service$"),
    re.compile(r"^bifrost-ib-account-agent\.service$"),
    re.compile(r"^bifrost-engine\.service$"),
    re.compile(r"^bifrost-account-sync-daemon(-dev)?\.service$"),
]


def validate_unit(unit: str) -> bool:
    """Return True if *unit* is an allowed systemd unit name (strip whitespace first)."""
    u = (unit or "").strip()
    if not u:
        return False
    for pat in ALLOWED_UNIT_PATTERNS:
        if pat.match(u):
            return True
    return False


def validate_action(action: str) -> bool:
    return action in ALLOWED_ACTIONS


@dataclass
class AgentRequest:
    action: str
    unit: str
    request_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timeout: int = 30

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.request_id,
            "action": self.action,
            "unit": self.unit,
            "timeout": self.timeout,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "AgentRequest":
        return cls(
            action=str(d.get("action", "")).strip(),
            unit=str(d.get("unit", "")).strip(),
            request_id=str(d.get("id", str(uuid.uuid4()))),
            timeout=int(d.get("timeout", 30)),
        )


@dataclass
class AgentResponse:
    request_id: str
    ok: bool
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "id": self.request_id,
            "ok": self.ok,
        }
        if self.result is not None:
            out["result"] = self.result
        if self.error is not None:
            out["error"] = self.error
        return out

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "AgentResponse":
        return cls(
            request_id=str(d.get("id", "")),
            ok=bool(d.get("ok", False)),
            result=d.get("result"),
            error=d.get("error"),
        )
