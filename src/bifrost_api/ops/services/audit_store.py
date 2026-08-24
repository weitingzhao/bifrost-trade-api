"""In-memory audit ring + platform-api sink (Wave 6).

Trade OLTP no longer persists ops_audit_log. Actuation records ship to
platform-api POST /api/v1/audit/append; failures fall back to structured logs.
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Dict, List, Optional

from bifrost_api.ops.models.schemas import AuditEntry
from bifrost_api.ops.services.platform_audit_client import PlatformAuditClient

logger = logging.getLogger(__name__)

_MAX_MEMORY_ENTRIES = 500


class AuditStore:
    """Ring buffer for recent entries + fire-and-forget Platform audit sink."""

    def __init__(self, platform_client: Optional[PlatformAuditClient] = None) -> None:
        self._memory: List[AuditEntry] = []
        self._lock = threading.Lock()
        self._platform = platform_client

    @classmethod
    def from_config(cls, config: dict) -> AuditStore:
        platform_client = PlatformAuditClient.from_config(config)
        logger.info(
            "Ops audit: platform sink %s (url=%s)",
            "enabled" if platform_client._enabled else "disabled",
            platform_client._base_url or "n/a",
        )
        return cls(platform_client=platform_client)

    def append(self, entry: AuditEntry) -> None:
        with self._lock:
            self._memory.append(entry)
            if len(self._memory) > _MAX_MEMORY_ENTRIES:
                self._memory = self._memory[-_MAX_MEMORY_ENTRIES:]

        if self._platform is not None:
            self._platform.submit(entry)
        else:
            logger.info(
                "audit_sink_skipped",
                extra={
                    "audit_payload": {
                        "actor": entry.operator,
                        "action": entry.action,
                        "target": entry.target,
                        "status": entry.outcome,
                    },
                    "reason": "no_platform_client",
                },
            )

    def list_recent(self, limit: int = 100) -> List[AuditEntry]:
        with self._lock:
            entries = sorted(self._memory, key=lambda e: e.timestamp, reverse=True)
            return entries[:limit]

    def stats(self) -> Dict[str, Any]:
        base: Dict[str, Any] = {
            "memory_entries": len(self._memory),
        }
        if self._platform is not None:
            base.update(self._platform.stats())
        else:
            base["mode"] = "logging_only"
            base["enabled"] = False
        return base
