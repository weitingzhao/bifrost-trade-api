"""_scan_csp against the market-data plugin's real response shapes.

The screener read ``data`` where the plugin answers ``rows``, so every name
came back "No snapshot data" while the plugin held the chain. These tests
drive ``_scan_csp`` through the HTTP client with the plugin's actual payloads,
and pin that a failed fetch is reported as a failure, not as missing data.

They also pin three readings the engine used to invent or drop: a spread with
no bid/ask, a DTE counted from the UTC date, and the quote's snapshot time;
and the IV percentile, which came from a stub and scored every contract neutral.
All values are invented.
"""

from __future__ import annotations

import io
import json
import urllib.error
import urllib.parse
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterator, List, Optional
from unittest.mock import MagicMock, patch

import httpx
import pytest

from bifrost_api.research import analytics_reader
from bifrost_api.research.routers.screener import (
    ScreenerRequest,
    _composite_score,
    _market_today,
    _prob_itm_put,
    _scan_csp,
    post_screener,
)

_TODAY = date(2026, 9, 26)
_EXPIRY = "20261016"  # 20 DTE from _TODAY
_SPOT = 100.0

# Bound before the autouse fixture replaces it on the module.
_REAL_FETCH = analytics_reader.fetch_iv_percentile_latest


def _iv_row(**overrides: Any) -> Dict[str, Any]:
    """A row as Research's /analytics/options/iv-percentile answers it."""
    row = {
        "symbol": "XYZ",
        "trade_date": "2026-09-25",
        "iv_current": 0.52,
        "iv_percentile_1y": 80.0,
        "iv_rank_1y": 64.0,
        "lookback_days": 252,
        "computed_at": "2026-09-26T02:40:00+00:00",
    }
    row.update(overrides)
    return row


@pytest.fixture(autouse=True)
def research_iv() -> Iterator[MagicMock]:
    """Research's IV percentile read, answering a fresh row unless a test says otherwise."""
    with patch.object(analytics_reader, "fetch_iv_percentile_latest", return_value=_iv_row()) as fetch:
        yield fetch


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
    """urlopen stand-in answering by path.

    A route value that is an exception is raised; a callable is called with the
    request's decoded query and its return value is the answer.
    """

    def _urlopen(req, timeout=None):
        path = req.full_url.split("/market", 1)[1].split("?", 1)[0]
        answer = routes[path]
        if isinstance(answer, BaseException):
            raise answer
        if callable(answer):
            answer = answer(urllib.parse.parse_qs(urllib.parse.urlsplit(req.full_url).query))
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
def test_window_wider_than_one_chain_batch_screens_every_expiry(mock_urlopen: MagicMock):
    # 8 expiries in DTE 7–120 × 21 put strikes = 168 keys: more than one 120-key
    # batch. The screener used to ask only the first 120, so everything after
    # the 6th expiry was never screened and nothing said so.
    in_window = [
        "20261009", "20261016", "20261023", "20261030",
        "20261106", "20261120", "20261218", "20270115",
    ]
    outside = ["20261002", "20270219"]  # 6 and 146 DTE
    batches: List[List[str]] = []

    def _chain(query: Dict[str, List[str]]) -> Dict[str, Any]:
        keys = query["keys"][0].split(",")
        batches.append(keys)
        rows = [{**_chain_row(0.0), "contract_key": k} for k in keys]
        return {"ok": True, "rows": rows, "count": len(rows)}

    mock_urlopen.side_effect = _plugin({
        "/options/expirations/yyyymmdd": {"ok": True, "expirations": sorted(in_window + outside), "count": 10},
        "/options/chain/latest": _chain,
    })

    body = ScreenerRequest(symbols=["XYZ"], dte_min=7, dte_max=120)
    group, warn = _scan_csp("XYZ", body, {}, "massive", _TODAY, _request())

    assert [len(b) for b in batches] == [120, 48]
    assert warn is None
    assert group is not None
    assert group["contract_count"] == 168
    assert {c["expiration"] for c in group["contracts"]} == set(in_window)
    assert {c["dte"] for c in group["contracts"] if c["expiration"] == "20270115"} == {111}


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


# ---------------------------------------------------------------------------
# Spread: null without both sides, never an invented 0
# ---------------------------------------------------------------------------


def _unquoted_row(strike: float, close: float = 1.20, **overrides: Any) -> Dict[str, Any]:
    """A row as the plugin's chain store answers today: no bid/ask, a session close."""
    row = _chain_row(strike, day_close=close, **overrides)
    del row["bid"], row["ask"]
    return row


def _by_strike(group: Dict[str, Any]) -> Dict[float, Dict[str, Any]]:
    return {c["strike"]: c for c in group["contracts"]}


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_unquoted_row_has_null_spread_and_close_premium(mock_urlopen: MagicMock):
    mock_urlopen.side_effect = _plugin({
        "/options/expirations/yyyymmdd": _EXPIRATIONS,
        "/options/chain/latest": {"ok": True, "rows": [_unquoted_row(95.0, close=1.20)], "count": 1},
    })

    group, warn = _scan_csp("XYZ", ScreenerRequest(symbols=["XYZ"]), {}, "massive", _TODAY, _request())

    assert warn is None
    assert group is not None
    c = group["contracts"][0]
    assert c["bid"] is None and c["ask"] is None
    assert c["spread_pct"] is None
    assert c["premium_basis"] == "close"
    assert c["mid"] == 1.20
    assert c["premium"] == 120.0


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_quoted_row_measures_spread_on_mid(mock_urlopen: MagicMock):
    mock_urlopen.side_effect = _plugin({
        "/options/expirations/yyyymmdd": _EXPIRATIONS,
        "/options/chain/latest": {"ok": True, "rows": [_chain_row(95.0, bid=1.10, ask=1.30)], "count": 1},
    })

    group, _ = _scan_csp("XYZ", ScreenerRequest(symbols=["XYZ"]), {}, "massive", _TODAY, _request())

    assert group is not None
    c = group["contracts"][0]
    assert c["premium_basis"] == "mid"
    assert c["mid"] == 1.20
    assert c["spread_pct"] == pytest.approx(0.20 / 1.20, abs=1e-4)


def test_unmeasured_spread_scores_neutral_not_best():
    args = (0.30, 0.10, 0.05, None)
    unmeasured = _composite_score(*args, None)
    # Neutral is half the liquidity weight: the score a 15% spread earns.
    assert unmeasured == pytest.approx(_composite_score(*args, 0.15))
    assert unmeasured < _composite_score(*args, 0.0)
    assert unmeasured > _composite_score(*args, 0.30)


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_unquoted_row_no_longer_outscores_the_same_contract_quoted_tight(mock_urlopen: MagicMock):
    # One contract answered twice at the same premium: once with no quote, once
    # with a 2-cent market. The invented zero spread used to rank the unquoted
    # answer above the quoted one; now only the spread term separates them.
    mock_urlopen.side_effect = _plugin({
        "/options/expirations/yyyymmdd": _EXPIRATIONS,
        "/options/chain/latest": {
            "ok": True,
            "rows": [_unquoted_row(95.0, close=1.20), _chain_row(95.0, bid=1.19, ask=1.21)],
            "count": 2,
        },
    })

    group, _ = _scan_csp("XYZ", ScreenerRequest(symbols=["XYZ"]), {}, "massive", _TODAY, _request())

    assert group is not None
    quoted, unquoted = group["contracts"]  # sorted by score, best first
    assert quoted["premium_basis"] == "mid" and unquoted["premium_basis"] == "close"
    liq_quoted = 1.0 - (0.02 / 1.20) / 0.30
    assert quoted["score"] - unquoted["score"] == pytest.approx(10.0 * (liq_quoted - 0.5), abs=0.1)


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_max_spread_filter_keeps_unquoted_rows_and_says_so(mock_urlopen: MagicMock):
    mock_urlopen.side_effect = _plugin({
        "/options/expirations/yyyymmdd": _EXPIRATIONS,
        "/options/chain/latest": {
            "ok": True,
            "rows": [
                _unquoted_row(95.0),
                _chain_row(90.0, bid=1.00, ask=1.40),  # 33% wide: cut
                _chain_row(85.0, bid=1.18, ask=1.22),  # 3% wide: kept
            ],
            "count": 3,
        },
    })

    body = ScreenerRequest(symbols=["XYZ"], max_spread_pct=0.05)
    group, warn = _scan_csp("XYZ", body, {}, "massive", _TODAY, _request())

    assert group is not None
    assert set(_by_strike(group)) == {95.0, 85.0}
    assert warn is not None
    assert warn.startswith("max_spread_pct not applied to 1 of 2 contracts")


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_max_spread_filter_on_quoted_rows_only_raises_no_warning(mock_urlopen: MagicMock):
    mock_urlopen.side_effect = _plugin({
        "/options/expirations/yyyymmdd": _EXPIRATIONS,
        "/options/chain/latest": {"ok": True, "rows": [_chain_row(85.0, bid=1.18, ask=1.22)], "count": 1},
    })

    body = ScreenerRequest(symbols=["XYZ"], max_spread_pct=0.05)
    group, warn = _scan_csp("XYZ", body, {}, "massive", _TODAY, _request())

    assert group is not None and group["contract_count"] == 1
    assert warn is None


# ---------------------------------------------------------------------------
# Quote time rides along
# ---------------------------------------------------------------------------


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_snapshot_ts_passes_through_per_contract(mock_urlopen: MagicMock):
    mock_urlopen.side_effect = _plugin({
        "/options/expirations/yyyymmdd": _EXPIRATIONS,
        "/options/chain/latest": {
            "ok": True,
            "rows": [
                _unquoted_row(95.0, snapshot_ts="2026-09-25T20:00:00Z"),
                _unquoted_row(90.0, snapshot_ts="2026-09-24T20:00:00Z"),
            ],
            "count": 2,
        },
    })

    group, _ = _scan_csp("XYZ", ScreenerRequest(symbols=["XYZ"]), {}, "massive", _TODAY, _request())

    assert group is not None
    rows = _by_strike(group)
    assert rows[95.0]["snapshot_ts"] == "2026-09-25T20:00:00Z"
    assert rows[90.0]["snapshot_ts"] == "2026-09-24T20:00:00Z"


# ---------------------------------------------------------------------------
# DTE is counted from the New York date
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("utc_now", "expected"),
    [
        (datetime(2026, 9, 27, 1, 30, tzinfo=timezone.utc), date(2026, 9, 26)),  # 21:30 EDT
        (datetime(2026, 9, 27, 4, 30, tzinfo=timezone.utc), date(2026, 9, 27)),  # 00:30 EDT
        (datetime(2026, 12, 5, 0, 30, tzinfo=timezone.utc), date(2026, 12, 4)),  # 19:30 EST
        (datetime(2026, 9, 26, 15, 0, tzinfo=timezone.utc), date(2026, 9, 26)),  # 11:00 EDT
    ],
)
def test_market_today_is_the_new_york_date(utc_now: datetime, expected: date):
    assert _market_today(utc_now) == expected


class _EveningInNewYork(datetime):
    """21:30 EDT on 2026-09-26, when the UTC date has already turned to the 27th."""

    @classmethod
    def now(cls, tz=None):
        instant = datetime(2026, 9, 27, 1, 30, tzinfo=timezone.utc)
        return instant.astimezone(tz) if tz else instant.replace(tzinfo=None)


class _UtcContainerDate(date):
    """The container's local date at that instant: it runs on UTC."""

    @classmethod
    def today(cls):
        return cls(2026, 9, 27)


@patch("bifrost_api.research.routers.screener.date", _UtcContainerDate)
@patch("bifrost_api.research.routers.screener.datetime", _EveningInNewYork)
@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_post_screener_counts_dte_from_the_new_york_date(mock_urlopen: MagicMock):
    iv, strike = 0.40, 95.0
    mock_urlopen.side_effect = _plugin({
        "/options/expirations/yyyymmdd": _EXPIRATIONS,
        "/options/chain/latest": {"ok": True, "rows": [_unquoted_row(strike, close=1.20, iv=iv)], "count": 1},
    })

    out = post_screener(_request(), ScreenerRequest(symbols=["XYZ"]))

    assert out["ok"] is True
    c = out["groups"][0]["contracts"][0]
    # 2026-10-16 less 2026-09-26 (New York); the UTC date would have read 19.
    assert c["dte"] == 20
    assert c["annualized"] == pytest.approx(120.0 / (strike * 100.0) * 365.0 / 20, abs=1e-4)
    assert c["prob_itm"] == pytest.approx(_prob_itm_put(_SPOT, strike, 20, iv), abs=1e-4)


# ---------------------------------------------------------------------------
# IV percentile: the name's IV30 against its year, read from Research
# ---------------------------------------------------------------------------


def _scan_one(mock_urlopen: MagicMock, rows: List[Dict[str, Any]], body: Optional[ScreenerRequest] = None):
    mock_urlopen.side_effect = _plugin({
        "/options/expirations/yyyymmdd": _EXPIRATIONS,
        "/options/chain/latest": {"ok": True, "rows": rows, "count": len(rows)},
    })
    return _scan_csp("XYZ", body or ScreenerRequest(symbols=["XYZ"]), {}, "massive", _TODAY, _request())


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_every_contract_carries_the_names_iv30_percentile(mock_urlopen: MagicMock, research_iv: MagicMock):
    # Two strikes at different IVs: the percentile ranks the name, not the strike,
    # so a skewed OTM put does not read as rich vol.
    group, warn = _scan_one(mock_urlopen, [_chain_row(95.0, iv=0.35), _chain_row(85.0, iv=0.60)])

    research_iv.assert_called_once_with("XYZ")
    assert warn is None
    assert group is not None
    assert group["iv30"] == 0.52
    assert group["iv_percentile"] == 80.0
    assert group["iv_percentile_as_of"] == "2026-09-25"
    assert group["iv_percentile_sessions"] == 252
    assert {c["iv_percentile"] for c in group["contracts"]} == {80.0}


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_measured_percentile_moves_the_score_by_its_weight(mock_urlopen: MagicMock, research_iv: MagicMock):
    rows = [_chain_row(95.0)]
    rich, _ = _scan_one(mock_urlopen, rows)
    research_iv.return_value = _iv_row(iv_percentile_1y=None, lookback_days=75)
    neutral, _ = _scan_one(mock_urlopen, rows)

    assert rich is not None and neutral is not None
    # 15% weight × (0.80 − the neutral 0.50) × 100.
    assert rich["contracts"][0]["score"] - neutral["contracts"][0]["score"] == pytest.approx(4.5, abs=0.1)


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_withheld_percentile_scores_neutral_and_says_how_short(mock_urlopen: MagicMock, research_iv: MagicMock):
    research_iv.return_value = _iv_row(iv_percentile_1y=None, lookback_days=75)

    group, warn = _scan_one(mock_urlopen, [_chain_row(95.0)])

    assert group is not None
    assert group["iv_percentile"] is None and group["iv30"] == 0.52
    assert group["contracts"][0]["iv_percentile"] is None
    assert warn == "IV percentile unmeasured: Research withholds it on 75 sessions of IV30 history"


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_name_without_iv_history_says_so(mock_urlopen: MagicMock, research_iv: MagicMock):
    research_iv.return_value = None

    group, warn = _scan_one(mock_urlopen, [_chain_row(95.0)])

    assert group is not None and group["iv_percentile"] is None and group["iv_percentile_as_of"] is None
    assert warn == "IV percentile unmeasured: Research holds no IV30 for this name"


@pytest.mark.parametrize(
    ("trade_date", "used"),
    [
        ("2026-09-21", True),  # 5 days: a Friday read on the Wednesday
        ("2026-09-20", False),  # 6 days
        ("2026-08-28", False),  # a name the chain store stopped covering
    ],
)
@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_stale_reading_is_withheld_and_dated(
    mock_urlopen: MagicMock, research_iv: MagicMock, trade_date: str, used: bool
):
    research_iv.return_value = _iv_row(trade_date=trade_date)

    group, warn = _scan_one(mock_urlopen, [_chain_row(95.0)])

    assert group is not None
    assert group["iv_percentile_as_of"] == trade_date
    if used:
        assert group["iv_percentile"] == 80.0 and warn is None
    else:
        age = (_TODAY - date.fromisoformat(trade_date)).days
        assert group["iv_percentile"] is None
        assert warn == f"IV percentile unmeasured: Research's newest IV30 reading is {trade_date}, {age} days old"


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_research_failure_still_screens_and_names_the_failure(mock_urlopen: MagicMock, research_iv: MagicMock):
    research_iv.side_effect = httpx.ConnectError("connection refused")

    group, warn = _scan_one(mock_urlopen, [_chain_row(95.0)])

    assert group is not None and group["contract_count"] == 1
    assert group["contracts"][0]["iv_percentile"] is None
    assert warn == (
        "IV percentile read failed (Research /analytics/options/iv-percentile): ConnectError: connection refused"
    )


@patch("bifrost_api.research.market_data_client.urllib.request.urlopen")
def test_spread_and_iv_notes_are_both_kept(mock_urlopen: MagicMock, research_iv: MagicMock):
    research_iv.return_value = None

    group, warn = _scan_one(
        mock_urlopen, [_unquoted_row(95.0)], ScreenerRequest(symbols=["XYZ"], max_spread_pct=0.05)
    )

    assert group is not None and warn is not None
    spread_note, iv_note = warn.split("; ")
    assert spread_note.startswith("max_spread_pct not applied to 1 of 1 contracts")
    assert iv_note == "IV percentile unmeasured: Research holds no IV30 for this name"


# ---------------------------------------------------------------------------
# The Research read itself
# ---------------------------------------------------------------------------


def _status_error(code: int) -> httpx.HTTPStatusError:
    req = httpx.Request("GET", "http://research.test/analytics/options/iv-percentile")
    return httpx.HTTPStatusError(f"HTTP {code}", request=req, response=httpx.Response(code, request=req))


def test_fetch_iv_percentile_latest_asks_for_one_name_and_returns_its_row():
    with patch.object(analytics_reader, "_proxy_get", return_value={"rows": [_iv_row()], "count": 1}) as get:
        row = _REAL_FETCH(" xyz ")

    get.assert_called_once_with("/analytics/options/iv-percentile", {"symbol": "XYZ"})
    assert row == _iv_row()


def test_fetch_iv_percentile_latest_reads_404_as_no_history():
    with patch.object(analytics_reader, "_proxy_get", side_effect=_status_error(404)):
        assert _REAL_FETCH("XYZ") is None


def test_fetch_iv_percentile_latest_raises_on_other_failures():
    with patch.object(analytics_reader, "_proxy_get", side_effect=_status_error(503)):
        with pytest.raises(httpx.HTTPStatusError):
            _REAL_FETCH("XYZ")
