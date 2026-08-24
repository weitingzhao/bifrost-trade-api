"""Fire-and-forget audit sink to platform-api POST /api/v1/audit/append."""

from __future__ import annotations

import logging
import os
import threading
from typing import Any, Dict, Optional

import httpx

from bifrost_api.ops.models.schemas import AuditEntry

logger = logging.getLogger(__name__)


def _format_detail(entry: AuditEntry) -> str:
    parts: list[str] = []
    if entry.command_id:
        parts.append(f"command_id={entry.command_id}")
    if entry.source_ip:
        parts.append(f"ip={entry.source_ip}")
    if entry.detail:
        parts.append(entry.detail)
    return " ".join(parts)


class PlatformAuditClient:
    """Async-capable client; submits via background thread so sync routes stay non-blocking."""

    def __init__(
        self,
        *,
        base_url: str,
        token: str,
        timeout_sec: float = 2.0,
        enabled: bool = True,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._timeout_sec = timeout_sec
        self._enabled = enabled and bool(base_url)
        self._last_error: Optional[str] = None
        self._lock = threading.Lock()

    @classmethod
    def from_config(cls, config: dict) -> PlatformAuditClient:
        ops = config.get("ops") or {}
        audit_cfg = ops.get("platform_audit") or {}
        enabled = bool(audit_cfg.get("enabled", False))
        url = str(audit_cfg.get("url") or "").strip()
        timeout_ms = audit_cfg.get("timeout_ms", 2000)
        try:
            timeout_sec = max(0.1, float(timeout_ms) / 1000.0)
        except (TypeError, ValueError):
            timeout_sec = 2.0
        token = os.environ.get("PLATFORM_SATELLITE_AUDIT_TOKEN", "").strip()
        if enabled and not token:
            logger.warning(
                "Ops platform audit: enabled but PLATFORM_SATELLITE_AUDIT_TOKEN unset; "
                "audit will log-only"
            )
            enabled = False
        return cls(base_url=url, token=token, timeout_sec=timeout_sec, enabled=enabled)

    def submit(self, entry: AuditEntry) -> None:
        payload = {
            "actor": entry.operator,
            "action": entry.action,
            "target": entry.target,
            "status": entry.outcome,
            "detail": _format_detail(entry),
        }
        if not self._enabled:
            logger.info(
                "audit_sink_skipped",
                extra={"audit_payload": payload, "reason": "platform_audit_disabled"},
            )
            return
        threading.Thread(
            target=self._post_sync,
            args=(payload,),
            daemon=True,
        ).start()

    def _post_sync(self, payload: Dict[str, Any]) -> None:
        url = f"{self._base_url}/api/v1/audit/append"
        try:
            with httpx.Client(timeout=self._timeout_sec) as client:
                response = client.post(
                    url,
                    json=payload,
                    headers={"Authorization": f"Bearer {self._token}"},
                )
            if response.status_code >= 400:
                self._record_error(f"HTTP {response.status_code}: {response.text[:200]}")
                logger.warning(
                    "audit_sink_failed",
                    extra={"audit_payload": payload, "error": self._last_error},
                )
        except Exception as exc:
            self._record_error(str(exc))
            logger.warning(
                "audit_sink_failed",
                extra={"audit_payload": payload, "error": self._last_error},
            )

    def _record_error(self, message: str) -> None:
        with self._lock:
            self._last_error = message

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            last_error = self._last_error
        if self._enabled:
            mode = "platform" if last_error is None else "logging_only"
        else:
            mode = "logging_only"
        return {"mode": mode, "last_error": last_error, "enabled": self._enabled}
