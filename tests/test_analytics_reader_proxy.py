"""Unit tests for Research API proxy helpers in analytics_reader."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from bifrost_api.research import analytics_reader as ar


def test_use_research_proxy_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEPA_USE_ANALYTICS", "true")
    monkeypatch.delenv("RESEARCH_PROXY", raising=False)
    assert ar.use_research_proxy() is True


def test_fetch_criteria_stats_via_proxy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEPA_USE_ANALYTICS", "true")
    monkeypatch.setenv("RESEARCH_PROXY", "true")
    monkeypatch.setenv("RESEARCH_API_URL", "http://research.test")

    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json.return_value = {"ok": True, "fundamental": {"pass": 1}, "technical": {"pass": 2}}

    with patch("bifrost_api.research.analytics_reader.httpx.Client") as client_cls:
        client = MagicMock()
        client.__enter__.return_value = client
        client.__exit__.return_value = False
        client.get.return_value = mock_resp
        client_cls.return_value = client
        out = ar.fetch_criteria_stats()

    assert out == {"fundamental": {"pass": 1}, "technical": {"pass": 2}}
    client.get.assert_called_once()
    assert "/analytics/sepa/criteria-stats" in client.get.call_args[0][0]


def test_fetch_screener_wide_unwraps_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEPA_USE_ANALYTICS", "true")
    monkeypatch.setenv("RESEARCH_PROXY", "true")
    monkeypatch.setenv("RESEARCH_API_URL", "http://research.test")

    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json.return_value = {"ok": True, "rows": [{"symbol": "AAPL"}], "count": 1}

    with patch("bifrost_api.research.analytics_reader.httpx.Client") as client_cls:
        client = MagicMock()
        client.__enter__.return_value = client
        client.__exit__.return_value = False
        client.get.return_value = mock_resp
        client_cls.return_value = client
        rows = ar.fetch_screener_wide(symbols=["aapl"])

    assert rows == [{"symbol": "AAPL"}]
