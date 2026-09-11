"""Unit tests for Research API proxy helpers in analytics_reader."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from bifrost_api.research import analytics_reader as ar


def test_use_research_proxy_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("RESEARCH_PROXY", raising=False)
    assert ar.use_research_proxy() is True


def test_fetch_criteria_stats_via_proxy(monkeypatch: pytest.MonkeyPatch) -> None:
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


def test_direct_criteria_stats_surfaces_the_first_error() -> None:
    """The direct-PG fallback must raise the real error, not a masked one.

    It used to catch any failure and re-run the identical SELECT on the same
    cursor. In PostgreSQL the first failure aborts the transaction, so the retry
    can never succeed -- it only replaced "relation ... does not exist" with
    "current transaction is aborted, commands ignored until end of transaction
    block", which is what the Stock Screener showed on 2026-09-11 while the real
    cause (a dbt CASCADE had dropped the view) stayed hidden.
    """
    cur = MagicMock()
    cur.__enter__.return_value = cur
    cur.__exit__.return_value = False
    cur.execute.side_effect = [
        RuntimeError('relation "dw_stock.mart_sepa_criteria_stats" does not exist'),
        RuntimeError("current transaction is aborted, commands ignored until end of transaction block"),
    ]
    conn = MagicMock()
    conn.cursor.return_value = cur
    get_conn = MagicMock()
    get_conn.return_value.__enter__.return_value = conn
    get_conn.return_value.__exit__.return_value = False

    with patch.object(ar, "get_conn", get_conn):
        with pytest.raises(RuntimeError, match="does not exist"):
            ar._fetch_criteria_stats_direct()
    assert cur.execute.call_count == 1
