"""_scan_csp against the market-data plugin's real response shapes.

The screener read ``data`` where the plugin answers ``rows``, so every name
came back "No snapshot data" while the plugin held the chain. These tests
drive ``_scan_csp`` through the HTTP client with the plugin's actual payloads,
and pin that a failed fetch is reported as a failure, not as missing data.
All values are invented.
"""

from __future__ import annotations

import io
import json
import urllib.error
from datetime import date
from typing import Any, Dict
from unittest.mock import MagicMock, patch

from bifrost_api.research.routers.screener import ScreenerRequest, _scan_csp

_TODAY = date(2026, 9, 26)
_EXPIRY = "20261016"  # 20 DTE from _TODAY
_SPOT = 100.0


class _FakeResponse:
    def __init__(self, body: dict):
        self._body = json.dumps(body).encode()

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass


def _request(spot: float = _SPOT) -> MagicMock:
    request = MagicMock()
    request.app.state.reader.get_stock_day_fallback_price.return_value = (spot,)
    return request


def _plugin(routes: Dict[str, Any]):
    """urlopen stand-in answering by path; a route value that is an exception is raised."""

    def _urlopen(req, timeout=None):
        path = req.full_url.split("/market", 1)[1].split("?", 1)[0]
        answer = routes[path]
        if isinstance(answer, BaseException):
            raise answer
        return _FakeResponse(answer)

    return _urlopen


_EXPIRATIONS = {"ok": True, "expirations": [_EXPIRY], "count": 1}


def _chain_row(strike: float, **overrides: Any) -> Dict[str, Any]:
    row = {
        "contract_key": f"XYZ|OPT|{_EXPIRY}|{strike}|P",
        "iv": 0.40,
        "delta": -0.25,
        "bid": 1.10,
        "ask": 1.30,
        "underlying_price": _SPOT,
        "open_interest": 1200,
        "snapshot_ts": "2026-09-25T20:00:00+00:00",
    }
    row.update(overrides)
    return row


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_rows_shape_yields_contracts(mock_urlopen: MagicMock):
    mock_urlopen.side_effect = _plugin({
        "/options/expirations/yyyymmdd": _EXPIRATIONS,
        "/options/chain/latest": {"ok": True, "rows": [_chain_row(95.0), _chain_row(90.0)], "count": 2},
    })

    group, warn = _scan_csp("XYZ", ScreenerRequest(symbols=["XYZ"]), {}, "massive", _TODAY, _request())

    assert warn is None
    assert group is not None
    assert group["contract_count"] == 2
    assert {c["strike"] for c in group["contracts"]} == {95.0, 90.0}
    assert all(c["expiration"] == _EXPIRY and c["dte"] == 20 for c in group["contracts"])


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_empty_rows_reads_as_no_snapshot_data(mock_urlopen: MagicMock):
    mock_urlopen.side_effect = _plugin({
        "/options/expirations/yyyymmdd": _EXPIRATIONS,
        "/options/chain/latest": {"ok": True, "rows": [], "count": 0},
    })

    group, warn = _scan_csp("XYZ", ScreenerRequest(symbols=["XYZ"]), {}, "massive", _TODAY, _request())

    assert group is None
    assert warn is not None and warn.startswith("No snapshot data")


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_chain_fetch_failure_is_not_reported_as_missing_data(mock_urlopen: MagicMock):
    err = urllib.error.HTTPError(
        "http://plugin/market/options/chain/latest", 500, "Internal Server Error", {}, io.BytesIO(b"")  # type: ignore[arg-type]
    )
    mock_urlopen.side_effect = _plugin({
        "/options/expirations/yyyymmdd": _EXPIRATIONS,
        "/options/chain/latest": err,
    })

    group, warn = _scan_csp("XYZ", ScreenerRequest(symbols=["XYZ"]), {}, "massive", _TODAY, _request())

    assert group is None
    assert warn is not None
    assert warn.startswith("Snapshot fetch failed")
    assert "/options/chain/latest" in warn and "500" in warn
    assert "No snapshot data" not in warn


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_expirations_fetch_failure_is_not_reported_as_missing_data(mock_urlopen: MagicMock):
    mock_urlopen.side_effect = _plugin({"/options/expirations/yyyymmdd": TimeoutError("timed out")})

    group, warn = _scan_csp("XYZ", ScreenerRequest(symbols=["XYZ"]), {}, "massive", _TODAY, _request())

    assert group is None
    assert warn == "Expirations fetch failed (Market Data Plugin /options/expirations/yyyymmdd): TimeoutError: timed out"


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_no_expirations_reads_as_no_contracts(mock_urlopen: MagicMock):
    mock_urlopen.side_effect = _plugin({"/options/expirations/yyyymmdd": {"ok": True, "expirations": [], "count": 0}})

    group, warn = _scan_csp("XYZ", ScreenerRequest(symbols=["XYZ"]), {}, "massive", _TODAY, _request())

    assert group is None
    assert warn is not None and warn.startswith("No option contracts on file")
