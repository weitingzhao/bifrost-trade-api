"""Tests for market_data_client HTTP client and market_pg wrappers."""

from __future__ import annotations

import json
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest

from bifrost_api.research import market_data_client
from bifrost_api.research.market_pg import (
    get_option_expiration_cache_snapshot,
    get_option_expirations_from_contracts_db,
    get_option_open_interest_daily,
    get_option_snapshots_eod_per_day,
    get_option_snapshots_latest,
    get_short_interest_recent,
    get_short_volume_recent,
    get_spy_close_series,
    get_stock_day_close_series_for_crs,
    get_stock_day_series_for_sepa,
    get_strikes_for_expiry_from_contracts_db,
)

# ─── HTTP client unit tests ──────────────────────────────────────────────────


class _FakeResponse:
    """Mimics urllib response context manager."""

    def __init__(self, body: dict):
        self._body = json.dumps(body).encode()

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_fetch_stock_bars_daily(mock_urlopen: MagicMock):
    payload = {
        "data": {
            "AAPL": [
                {"symbol": "AAPL", "bar_time": "2025-01-02", "open": 150.0, "high": 155.0, "low": 149.0, "close": 153.0, "volume": 1000000}
            ],
            "MSFT": [],
        }
    }
    mock_urlopen.return_value = _FakeResponse(payload)

    result = market_data_client.fetch_stock_bars_daily(["AAPL", "MSFT"], days=400)

    assert "AAPL" in result
    assert len(result["AAPL"]) == 1
    assert result["AAPL"][0]["close"] == 153.0
    assert result["MSFT"] == []


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_fetch_stock_bars_daily_close(mock_urlopen: MagicMock):
    payload = {
        "data": {
            "TSLA": [
                {"symbol": "TSLA", "bar_time": "2025-01-02", "close": 250.0},
                {"symbol": "TSLA", "bar_time": "2025-01-03", "close": 255.0},
            ]
        }
    }
    mock_urlopen.return_value = _FakeResponse(payload)

    result = market_data_client.fetch_stock_bars_daily_close(["TSLA"], days=420)

    assert len(result["TSLA"]) == 2
    assert result["TSLA"][1]["close"] == 255.0


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_fetch_spy_close_series(mock_urlopen: MagicMock):
    payload = {"closes": [450.0, 451.5, 449.8, 452.3]}
    mock_urlopen.return_value = _FakeResponse(payload)

    result = market_data_client.fetch_spy_close_series(days=420)

    assert result == [450.0, 451.5, 449.8, 452.3]


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_fetch_stock_bars_daily_empty_response(mock_urlopen: MagicMock):
    payload: Dict[str, Any] = {"data": {}}
    mock_urlopen.return_value = _FakeResponse(payload)

    result = market_data_client.fetch_stock_bars_daily(["XYZ"], days=400)

    assert result == {}


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_plugin_base_url_from_env(mock_urlopen: MagicMock, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("MARKET_DATA_PLUGIN_URL", "http://custom-host:9999/market")
    payload = {"closes": [100.0]}
    mock_urlopen.return_value = _FakeResponse(payload)

    market_data_client.fetch_spy_close_series(days=100)

    call_args = mock_urlopen.call_args
    req = call_args[0][0]
    assert "custom-host:9999" in req.full_url


# ─── market_pg wrapper integration tests ─────────────────────────────────────


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_get_stock_day_series_for_sepa_plugin_mode(mock_urlopen: MagicMock):
    payload = {
        "data": {
            "AAPL": [{"symbol": "AAPL", "bar_time": "2025-01-02", "open": 150.0, "high": 155.0, "low": 149.0, "close": 153.0, "volume": 1000000, "source": "massive"}]
        }
    }
    mock_urlopen.return_value = _FakeResponse(payload)

    result = get_stock_day_series_for_sepa({"postgres": {"host": "localhost"}}, ["AAPL"], lookback_days=400)

    assert "AAPL" in result
    mock_urlopen.assert_called_once()


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_get_stock_day_close_series_for_crs_plugin_mode(mock_urlopen: MagicMock):
    payload = {"data": {"TSLA": [{"symbol": "TSLA", "bar_time": "2025-01-02", "close": 250.0}]}}
    mock_urlopen.return_value = _FakeResponse(payload)

    result = get_stock_day_close_series_for_crs({"postgres": {"host": "localhost"}}, ["TSLA"])

    assert "TSLA" in result
    mock_urlopen.assert_called_once()


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_get_spy_close_series_plugin_mode(mock_urlopen: MagicMock):
    payload = {"closes": [450.0, 451.5]}
    mock_urlopen.return_value = _FakeResponse(payload)

    result = get_spy_close_series({"postgres": {"host": "localhost"}})

    assert result == [450.0, 451.5]
    mock_urlopen.assert_called_once()


# ─── Option HTTP client unit tests ───────────────────────────────────────────


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_fetch_option_chain_latest(mock_urlopen: MagicMock):
    payload = {"data": [
        {"contract_key": "AAPL|OPT|20260919|150.0|C", "iv": 0.35, "delta": 0.55},
        {"contract_key": "AAPL|OPT|20260919|155.0|C", "iv": 0.32, "delta": 0.48},
    ]}
    mock_urlopen.return_value = _FakeResponse(payload)

    result = market_data_client.fetch_option_chain_latest(["AAPL|OPT|20260919|150.0|C", "AAPL|OPT|20260919|155.0|C"])

    assert len(result) == 2
    assert result[0]["iv"] == 0.35
    assert result[1]["contract_key"] == "AAPL|OPT|20260919|155.0|C"


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_fetch_option_chain_latest_empty_keys(mock_urlopen: MagicMock):
    result = market_data_client.fetch_option_chain_latest([])
    assert result == []
    mock_urlopen.assert_not_called()


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_fetch_option_chain_latest_chunking(mock_urlopen: MagicMock):
    """Keys exceeding batch size are sent in multiple requests."""
    batch1_payload = {"data": [{"contract_key": f"K{i}", "iv": 0.3} for i in range(120)]}
    batch2_payload = {"data": [{"contract_key": "K120", "iv": 0.25}]}
    mock_urlopen.side_effect = [_FakeResponse(batch1_payload), _FakeResponse(batch2_payload)]

    keys = [f"K{i}" for i in range(121)]
    result = market_data_client.fetch_option_chain_latest(keys)

    assert len(result) == 121
    assert mock_urlopen.call_count == 2


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_fetch_option_chain_eod(mock_urlopen: MagicMock):
    payload = {"data": [
        {"snap_day": "2026-08-01", "iv": 0.33, "underlying_price": 150.0, "contract_key": "K1"},
    ]}
    mock_urlopen.return_value = _FakeResponse(payload)

    result = market_data_client.fetch_option_chain_eod(["K1"], since="2026-07-01T00:00:00")

    assert len(result) == 1
    assert result[0]["snap_day"] == "2026-08-01"


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_fetch_option_chain_eod_no_since(mock_urlopen: MagicMock):
    payload = {"data": [{"snap_day": "2026-08-01", "iv": 0.33, "contract_key": "K1"}]}
    mock_urlopen.return_value = _FakeResponse(payload)

    result = market_data_client.fetch_option_chain_eod(["K1"])

    assert len(result) == 1
    call_args = mock_urlopen.call_args
    req = call_args[0][0]
    assert "since" not in req.full_url


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_fetch_option_oi(mock_urlopen: MagicMock):
    payload = {"data": [
        {"option_ticker": "O:AAPL260919C00150000", "open_interest": 5000, "trade_date": "2026-08-13"},
    ]}
    mock_urlopen.return_value = _FakeResponse(payload)

    result = market_data_client.fetch_option_oi("AAPL", expiry="20260919", limit=50, date_from="2026-08-01", date_to="2026-08-13")

    assert len(result) == 1
    assert result[0]["open_interest"] == 5000


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_fetch_option_expirations_yyyymmdd(mock_urlopen: MagicMock):
    payload = {"expirations": ["20260919", "20261016", "20261120"]}
    mock_urlopen.return_value = _FakeResponse(payload)

    result = market_data_client.fetch_option_expirations_yyyymmdd("AAPL")

    assert result == ["20260919", "20261016", "20261120"]


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_fetch_option_strikes(mock_urlopen: MagicMock):
    payload = {"strikes": [140.0, 145.0, 150.0, 155.0, 160.0]}
    mock_urlopen.return_value = _FakeResponse(payload)

    result = market_data_client.fetch_option_strikes("AAPL", "20260919")

    assert result == [140.0, 145.0, 150.0, 155.0, 160.0]


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_fetch_option_expirations(mock_urlopen: MagicMock):
    payload = {"expirations": ["2026-09-19", "2026-10-16"], "updated_at": "2026-08-14T10:00:00+00:00"}
    mock_urlopen.return_value = _FakeResponse(payload)

    result = market_data_client.fetch_option_expirations("AAPL")

    assert result is not None
    exps, updated_at = result
    assert exps == ["2026-09-19", "2026-10-16"]
    assert updated_at == "2026-08-14T10:00:00+00:00"


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_fetch_option_expirations_empty(mock_urlopen: MagicMock):
    payload: Dict[str, Any] = {"expirations": []}
    mock_urlopen.return_value = _FakeResponse(payload)

    result = market_data_client.fetch_option_expirations("XYZ")

    assert result is None


# ─── Option market_pg wrapper integration tests ──────────────────────────────


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_get_option_snapshots_latest_plugin_mode(mock_urlopen: MagicMock):
    payload = {"data": [{"contract_key": "AAPL|OPT|20260919|150.0|C", "iv": 0.35}]}
    mock_urlopen.return_value = _FakeResponse(payload)

    result = get_option_snapshots_latest({"postgres": {"host": "localhost"}}, ["AAPL|OPT|20260919|150.0|C"])

    assert len(result) == 1
    assert result[0]["iv"] == 0.35
    mock_urlopen.assert_called_once()


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_get_option_snapshots_eod_plugin_mode(mock_urlopen: MagicMock):
    payload = {"data": [{"snap_day": "2026-08-01", "iv": 0.33, "contract_key": "K1"}]}
    mock_urlopen.return_value = _FakeResponse(payload)

    from datetime import datetime
    result = get_option_snapshots_eod_per_day({"postgres": {"host": "localhost"}}, ["K1"], since_ts=datetime(2026, 7, 1))

    assert len(result) == 1
    mock_urlopen.assert_called_once()


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_get_option_open_interest_daily_plugin_mode(mock_urlopen: MagicMock):
    payload = {"data": [{"option_ticker": "O:AAPL260919C00150000", "open_interest": 5000}]}
    mock_urlopen.return_value = _FakeResponse(payload)

    result = get_option_open_interest_daily({"postgres": {"host": "localhost"}}, "AAPL", expiry="20260919")

    assert len(result) == 1
    assert result[0]["open_interest"] == 5000
    mock_urlopen.assert_called_once()


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_get_option_expirations_from_contracts_db_plugin_mode(mock_urlopen: MagicMock):
    payload = {"expirations": ["20260919", "20261016"]}
    mock_urlopen.return_value = _FakeResponse(payload)

    result = get_option_expirations_from_contracts_db({"postgres": {"host": "localhost"}}, "AAPL")

    assert result == ["20260919", "20261016"]
    mock_urlopen.assert_called_once()


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_get_strikes_for_expiry_plugin_mode(mock_urlopen: MagicMock):
    payload = {"strikes": [140.0, 145.0, 150.0]}
    mock_urlopen.return_value = _FakeResponse(payload)

    result = get_strikes_for_expiry_from_contracts_db({"postgres": {"host": "localhost"}}, "AAPL", "20260919")

    assert result == [140.0, 145.0, 150.0]
    mock_urlopen.assert_called_once()


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_get_option_expiration_cache_snapshot_plugin_mode(mock_urlopen: MagicMock):
    payload = {"expirations": ["2026-09-19", "2026-10-16"], "updated_at": "2026-08-14T10:00:00+00:00"}
    mock_urlopen.return_value = _FakeResponse(payload)

    result = get_option_expiration_cache_snapshot({"postgres": {"host": "localhost"}}, "AAPL")

    assert result is not None
    exps, updated_at = result
    assert exps == ["2026-09-19", "2026-10-16"]
    assert updated_at is not None


# ─── Short interest / short volume HTTP client tests ─────────────────────────


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_fetch_short_interest(mock_urlopen: MagicMock):
    payload = {
        "ok": True,
        "data": {
            "NVDA": [
                {"symbol": "NVDA", "settlement_date": "2026-08-01", "short_interest": 50000, "avg_daily_volume": 1000000, "days_to_cover": 1.5},
            ],
            "AAPL": [
                {"symbol": "AAPL", "settlement_date": "2026-08-01", "short_interest": 30000, "avg_daily_volume": 800000, "days_to_cover": 0.8},
            ],
        },
        "count": 2,
    }
    mock_urlopen.return_value = _FakeResponse(payload)

    result = market_data_client.fetch_short_interest(["NVDA", "AAPL"], settlements=6)

    assert "NVDA" in result
    assert "AAPL" in result
    assert result["NVDA"][0]["short_interest"] == 50000
    assert result["AAPL"][0]["days_to_cover"] == 0.8


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_fetch_short_interest_empty(mock_urlopen: MagicMock):
    payload: Dict[str, Any] = {"ok": True, "data": {}, "count": 0}
    mock_urlopen.return_value = _FakeResponse(payload)

    result = market_data_client.fetch_short_interest(["XYZ"])

    assert result == {}


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_fetch_short_volume(mock_urlopen: MagicMock):
    payload = {
        "ok": True,
        "data": {
            "NVDA": [
                {"symbol": "NVDA", "trade_date": "2026-08-13", "short_volume": 12000, "short_volume_ratio": 0.35, "total_volume": 34000},
                {"symbol": "NVDA", "trade_date": "2026-08-12", "short_volume": 11000, "short_volume_ratio": 0.33, "total_volume": 33000},
            ],
        },
        "count": 2,
    }
    mock_urlopen.return_value = _FakeResponse(payload)

    result = market_data_client.fetch_short_volume(["NVDA"], trade_days=60)

    assert "NVDA" in result
    assert len(result["NVDA"]) == 2
    assert result["NVDA"][0]["short_volume_ratio"] == 0.35


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_fetch_short_volume_empty(mock_urlopen: MagicMock):
    payload: Dict[str, Any] = {"ok": True, "data": {}, "count": 0}
    mock_urlopen.return_value = _FakeResponse(payload)

    result = market_data_client.fetch_short_volume(["XYZ"])

    assert result == {}


# ─── Short interest / short volume market_pg wrapper tests ───────────────────


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_get_short_interest_recent_plugin_mode(mock_urlopen: MagicMock):
    payload = {
        "ok": True,
        "data": {"AAPL": [{"symbol": "AAPL", "settlement_date": "2026-08-01", "short_interest": 50000}]},
        "count": 1,
    }
    mock_urlopen.return_value = _FakeResponse(payload)

    result = get_short_interest_recent({"postgres": {"host": "localhost"}}, ["AAPL"], settlements=6)

    assert "AAPL" in result
    assert result["AAPL"][0]["short_interest"] == 50000
    mock_urlopen.assert_called_once()


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_get_short_volume_recent_plugin_mode(mock_urlopen: MagicMock):
    payload = {
        "ok": True,
        "data": {"NVDA": [{"symbol": "NVDA", "trade_date": "2026-08-13", "short_volume": 12000}]},
        "count": 1,
    }
    mock_urlopen.return_value = _FakeResponse(payload)

    result = get_short_volume_recent({"postgres": {"host": "localhost"}}, ["NVDA"], trade_days=60)

    assert "NVDA" in result
    assert result["NVDA"][0]["short_volume"] == 12000
    mock_urlopen.assert_called_once()
