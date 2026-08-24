"""Wave 6: platform audit sink client + AuditStore integration."""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import httpx

from bifrost_api.ops.models.schemas import AuditEntry
from bifrost_api.ops.services.audit_store import AuditStore
from bifrost_api.ops.services.platform_audit_client import PlatformAuditClient


def test_platform_audit_client_happy_path():
    client = PlatformAuditClient(
        base_url="http://platform-api:8780",
        token="satellite-token",
        timeout_sec=1.0,
        enabled=True,
    )
    entry = AuditEntry(
        operator="op",
        source_ip="10.0.0.1",
        action="market_ingest_restart",
        target="ib_ingestor",
        command_id="cmd-1",
        outcome="success",
        detail="forced",
    )

    with patch("httpx.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_client.post.return_value = mock_response
        mock_client_cls.return_value.__enter__.return_value = mock_client

        client._post_sync(
            {
                "actor": entry.operator,
                "action": entry.action,
                "target": entry.target,
                "status": entry.outcome,
                "detail": "command_id=cmd-1 ip=10.0.0.1 forced",
            }
        )

        mock_client.post.assert_called_once()
        call_kwargs = mock_client.post.call_args
        assert call_kwargs[0][0] == "http://platform-api:8780/api/v1/audit/append"
        assert call_kwargs[1]["headers"]["Authorization"] == "Bearer satellite-token"


def test_platform_audit_client_http_error_logs_only():
    client = PlatformAuditClient(
        base_url="http://platform-api:8780",
        token="satellite-token",
        enabled=True,
    )
    with patch("httpx.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "internal error"
        mock_client.post.return_value = mock_response
        mock_client_cls.return_value.__enter__.return_value = mock_client

        client._post_sync({"actor": "op", "action": "a", "target": "b", "status": "ok", "detail": ""})

    stats = client.stats()
    assert stats["mode"] == "logging_only"
    assert stats["last_error"] is not None


def test_platform_audit_client_timeout_logs_only():
    client = PlatformAuditClient(
        base_url="http://platform-api:8780",
        token="satellite-token",
        enabled=True,
    )
    with patch("httpx.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_client.post.side_effect = httpx.TimeoutException("timeout")
        mock_client_cls.return_value.__enter__.return_value = mock_client

        client._post_sync({"actor": "op", "action": "a", "target": "b", "status": "ok", "detail": ""})

    stats = client.stats()
    assert stats["mode"] == "logging_only"
    assert "timeout" in stats["last_error"].lower()


def test_platform_audit_client_disabled_no_http():
    client = PlatformAuditClient(base_url="", token="", enabled=False)
    with patch("httpx.Client") as mock_client_cls:
        client.submit(
            AuditEntry(operator="op", action="a", target="b", outcome="ok"),
        )
        time.sleep(0.05)
        mock_client_cls.assert_not_called()


def test_audit_store_append_uses_platform_client():
    platform = PlatformAuditClient(
        base_url="http://platform-api:8780",
        token="satellite-token",
        enabled=True,
    )
    store = AuditStore(platform_client=platform)
    entry = AuditEntry(operator="op", action="restart", target="api", outcome="ok")

    with patch.object(platform, "submit") as mock_submit:
        store.append(entry)
        mock_submit.assert_called_once_with(entry)

    recent = store.list_recent(limit=10)
    assert len(recent) == 1
    assert recent[0].action == "restart"
