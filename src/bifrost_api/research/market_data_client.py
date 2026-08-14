"""HTTP client for Plugin Market Data API (Golden Source).

Replaces direct SQL reads against market.stock_daily / market.option_*
with HTTP calls to bifrost-platform-plugin-market-data endpoints.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

_OPTION_CHAIN_BATCH_SIZE = 120


def _plugin_base_url() -> str:
    """Plugin API base URL. Default: http://localhost:8790/market"""
    return os.environ.get("MARKET_DATA_PLUGIN_URL", "http://localhost:8790/market")


def _get_json(path: str, params: dict[str, str] | None = None, timeout: int = 30) -> dict[str, Any]:
    """GET JSON from Plugin API."""
    base = _plugin_base_url()
    url = f"{base}{path}"
    if params:
        qs = "&".join(
            f"{k}={urllib.parse.quote(str(v))}"
            for k, v in params.items()
            if v is not None
        )
        url = f"{url}?{qs}"
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def _post_json(path: str, body: dict[str, Any], timeout: int = 30) -> dict[str, Any]:
    """POST JSON to Plugin API."""
    base = _plugin_base_url()
    url = f"{base}{path}"
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def fetch_stock_bars_daily(symbols: List[str], days: int = 400) -> Dict[str, List[Dict[str, Any]]]:
    """GET /stocks/db/bars/daily -> {symbol: [bars]}"""
    data = _get_json("/stocks/db/bars/daily", {"symbols": ",".join(symbols), "days": str(days)})
    return data.get("data", {})


def fetch_stock_bars_daily_close(symbols: List[str], days: int = 420) -> Dict[str, List[Dict[str, Any]]]:
    """GET /stocks/db/bars/daily/close -> {symbol: [{symbol, bar_time, close}]}"""
    data = _get_json("/stocks/db/bars/daily/close", {"symbols": ",".join(symbols), "days": str(days)})
    return data.get("data", {})


def fetch_spy_close_series(days: int = 420) -> List[float]:
    """GET /stocks/db/bars/daily/spy-close -> [float]"""
    data = _get_json("/stocks/db/bars/daily/spy-close", {"days": str(days)})
    return data.get("closes", [])


# ─── Option endpoints ─────────────────────────────────────────────────────────


def fetch_option_chain_latest(keys: List[str]) -> List[Dict[str, Any]]:
    """GET /options/chain/latest?keys=KEY1,KEY2,... → [{contract_key, iv, delta, ...}]

    Chunks into batches of _OPTION_CHAIN_BATCH_SIZE if needed.
    Plugin API accepts both IB and Polygon key formats.
    """
    if not keys:
        return []
    out: List[Dict[str, Any]] = []
    for i in range(0, len(keys), _OPTION_CHAIN_BATCH_SIZE):
        batch = keys[i : i + _OPTION_CHAIN_BATCH_SIZE]
        data = _get_json("/options/chain/latest", {"keys": ",".join(batch)}, timeout=45)
        out.extend(data.get("data", []))
    return out


def fetch_option_chain_eod(keys: List[str], since: Optional[str] = None) -> List[Dict[str, Any]]:
    """GET /options/chain/eod?keys=...&since=... → [{snap_day, iv, underlying_price, ...}]

    Chunks into batches of _OPTION_CHAIN_BATCH_SIZE if needed.
    """
    if not keys:
        return []
    out: List[Dict[str, Any]] = []
    for i in range(0, len(keys), _OPTION_CHAIN_BATCH_SIZE):
        batch = keys[i : i + _OPTION_CHAIN_BATCH_SIZE]
        params: Dict[str, str] = {"keys": ",".join(batch)}
        if since:
            params["since"] = since
        data = _get_json("/options/chain/eod", params, timeout=45)
        out.extend(data.get("data", []))
    return out


def fetch_option_oi(
    symbol: str,
    expiry: Optional[str] = None,
    limit: int = 100,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """GET /options/oi?symbol=...&expiry=...&... → [{option_ticker, open_interest, ...}]"""
    params: Dict[str, str] = {"symbol": symbol, "limit": str(limit)}
    if expiry:
        params["expiry"] = expiry
    if date_from:
        params["date_from"] = date_from
    if date_to:
        params["date_to"] = date_to
    data = _get_json("/options/oi", params)
    return data.get("data", [])


def fetch_option_expirations_yyyymmdd(symbol: str) -> List[str]:
    """GET /options/expirations/yyyymmdd?symbol=... → ["20260919", ...]"""
    data = _get_json("/options/expirations/yyyymmdd", {"symbol": symbol})
    return data.get("expirations", [])


def fetch_option_strikes(symbol: str, expiry: str) -> List[float]:
    """GET /options/strikes?symbol=...&expiry=... → [100.0, 105.0, ...]"""
    data = _get_json("/options/strikes", {"symbol": symbol, "expiry": expiry})
    return data.get("strikes", [])


def fetch_option_expirations(symbol: str) -> Optional[Tuple[List[str], Optional[str]]]:
    """GET /options/expirations?symbol=... → (["2026-09-19", ...], updated_at_iso | None)

    Returns None if the endpoint returns empty or missing data.
    """
    data = _get_json("/options/expirations", {"symbol": symbol})
    exps = data.get("expirations")
    if not exps:
        return None
    updated_at = data.get("updated_at")
    return (exps, updated_at)


# ─── Ticker reference endpoints ──────────────────────────────────────────────


def fetch_ticker_detail(symbol: str) -> Optional[Dict[str, Any]]:
    """GET /reference/ticker/{symbol} → ticker detail dict or None."""
    try:
        data = _get_json(f"/reference/ticker/{urllib.parse.quote(symbol.upper())}")
        if data.get("ok") and data.get("data"):
            return data["data"]
        return None
    except Exception:
        return None


# ─── Fundamentals endpoints ──────────────────────────────────────────────────


def fetch_short_interest(symbols: List[str], settlements: int = 6) -> Dict[str, List[Dict[str, Any]]]:
    """GET /stocks/fundamentals/db/short-interest?symbols=...&settlements=... → {symbol: [rows]}"""
    data = _get_json(
        "/stocks/fundamentals/db/short-interest",
        {"symbols": ",".join(symbols), "settlements": str(settlements)},
    )
    return data.get("data", {})


def fetch_short_volume(symbols: List[str], trade_days: int = 60) -> Dict[str, List[Dict[str, Any]]]:
    """GET /stocks/fundamentals/db/short-volume?symbols=...&trade_days=... → {symbol: [rows]}"""
    data = _get_json(
        "/stocks/fundamentals/db/short-volume",
        {"symbols": ",".join(symbols), "trade_days": str(trade_days)},
    )
    return data.get("data", {})


# ─── PCR / Option Daily / Coverage endpoints (W2-P4) ─────────────────────────


def fetch_pcr_aggregate(
    symbol: str, pcr_type: str = "oi", lookback_days: int = 365
) -> Dict[str, Any]:
    """GET /options/analytics/pcr → full PCR aggregate response."""
    return _get_json(
        "/options/analytics/pcr",
        {"symbol": symbol, "type": pcr_type, "lookback_days": str(lookback_days)},
        timeout=45,
    )


def fetch_option_daily(
    symbol: str,
    expiry: Optional[str] = None,
    days: int = 30,
    limit: int = 2000,
) -> Dict[str, Any]:
    """GET /options/daily → {ok, symbol, rows, count}."""
    params: Dict[str, str] = {"symbol": symbol, "days": str(days), "limit": str(limit)}
    if expiry:
        params["expiry"] = expiry
    return _get_json("/options/daily", params, timeout=45)


def fetch_option_daily_available_dates(symbol: str, limit: int = 90) -> Dict[str, Any]:
    """GET /options/daily/available-dates → {ok, symbol, dates}."""
    return _get_json("/options/daily/available-dates", {"symbol": symbol, "limit": str(limit)})


def fetch_coverage_sepa_stats() -> Dict[str, Any]:
    """GET /coverage/sepa-stats → {ok, tables}."""
    return _get_json("/coverage/sepa-stats")


def fetch_coverage_distributions(table: str, limit: int = 200) -> Dict[str, Any]:
    """GET /coverage/distributions → {ok, table, distributions, count}."""
    return _get_json("/coverage/distributions", {"table": table, "limit": str(limit)})


# ─── Chain analytics endpoints ────────────────────────────────────────────────


def fetch_chain_by_expiry(
    symbol: str, fallback_date: Optional[str] = None
) -> Dict[str, Any]:
    """GET /options/analytics/chain-by-expiry → {ok, symbol, chain, basis}."""
    params: Dict[str, str] = {"symbol": symbol}
    if fallback_date:
        params["fallback_date"] = fallback_date
    return _get_json("/options/analytics/chain-by-expiry", params, timeout=45)


# ─── Readiness data endpoints ────────────────────────────────────────────────


def fetch_readiness_bar_aggregate(window_days: int = 420) -> Dict[str, Dict[str, Any]]:
    """GET /readiness/bar-aggregate → {SYM: {bar_rows, first_bar_date, ...}}."""
    resp = _get_json("/readiness/bar-aggregate", {"window_days": str(window_days)}, timeout=60)
    return resp.get("symbols", {})


def fetch_readiness_latest_bar(
    lookback_days: int = 90,
    symbols: Optional[List[str]] = None,
) -> Dict[str, Dict[str, Any]]:
    """GET /readiness/latest-bar-per-symbol → {SYM: {bar_date, close}}."""
    params: Dict[str, str] = {"lookback_days": str(lookback_days)}
    if symbols:
        params["symbols"] = ",".join(symbols)
    resp = _get_json("/readiness/latest-bar-per-symbol", params, timeout=60)
    return resp.get("symbols", {})


def fetch_readiness_latest_bar_full(symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    """GET /readiness/latest-bar-full-history → {SYM: {bar_date, close}}."""
    if not symbols:
        return {}
    resp = _get_json(
        "/readiness/latest-bar-full-history",
        {"symbols": ",".join(symbols)},
        timeout=60,
    )
    return resp.get("symbols", {})


def fetch_readiness_financials_coverage() -> Dict[str, Any]:
    """GET /readiness/financials-coverage-symbols → coverage sets."""
    return _get_json("/readiness/financials-coverage-symbols", timeout=45)


def fetch_readiness_financials_fill_rate(
    universe_symbols: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """GET /readiness/financials-fill-rate → fill rate counts."""
    params: Dict[str, str] = {}
    if universe_symbols:
        params["universe_symbols"] = ",".join(universe_symbols)
    return _get_json("/readiness/financials-fill-rate", params, timeout=60)


def fetch_readiness_date_coverage(
    days_back: int = 420, min_symbols: int = 1000
) -> Dict[str, Any]:
    """GET /readiness/date-coverage → low coverage dates."""
    return _get_json(
        "/readiness/date-coverage",
        {"days_back": str(days_back), "min_symbols": str(min_symbols)},
        timeout=60,
    )


def fetch_readiness_financials_by_instrument_type() -> Dict[str, Any]:
    """GET /readiness/financials-by-instrument-type → counts by report type."""
    return _get_json("/readiness/financials-by-instrument-type", timeout=45)


# ─── Write endpoints ─────────────────────────────────────────────────────────


def post_replace_expirations(symbol: str, expirations: List[str]) -> dict[str, Any]:
    """POST /options/expirations/replace → {"ok": true, "symbol": "...", "replaced": N}"""
    return _post_json(
        "/options/expirations/replace",
        {"symbol": symbol, "expirations": expirations},
    )


# ─── SEPA fundamentals (Plugin) endpoints ────────────────────────────────────

_SEPA_BATCH_SIZE = 100


def fetch_sepa_financials(
    symbols: List[str],
    report_type: str,
    *,
    period_type: Optional[str] = None,
    limit: int = 20,
) -> Dict[str, List[Dict[str, Any]]]:
    """GET /stocks/fundamentals/sepa/financials → {symbol: [rows with raw data]}"""
    if not symbols:
        return {}
    out: Dict[str, List[Dict[str, Any]]] = {}
    for i in range(0, len(symbols), _SEPA_BATCH_SIZE):
        batch = symbols[i : i + _SEPA_BATCH_SIZE]
        params: Dict[str, str] = {
            "symbols": ",".join(batch),
            "report_type": report_type,
            "limit": str(limit),
        }
        if period_type:
            params["period_type"] = period_type
        data = _get_json("/stocks/fundamentals/sepa/financials", params, timeout=45)
        for sym, rows in (data.get("data") or {}).items():
            out.setdefault(sym, []).extend(rows)
    return out


def fetch_sepa_income_rows(symbol: str) -> Dict[str, Any]:
    """GET /stocks/fundamentals/sepa/income-rows → {quarterly: [...], annual: [...]}"""
    data = _get_json("/stocks/fundamentals/sepa/income-rows", {"symbol": symbol})
    return {
        "quarterly": data.get("quarterly", []),
        "annual": data.get("annual", []),
    }


def fetch_sepa_income_ext(symbols: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    """GET /stocks/fundamentals/sepa/income-ext → {symbol: [rows with raw data]}"""
    if not symbols:
        return {}
    out: Dict[str, List[Dict[str, Any]]] = {}
    for i in range(0, len(symbols), _SEPA_BATCH_SIZE):
        batch = symbols[i : i + _SEPA_BATCH_SIZE]
        data = _get_json(
            "/stocks/fundamentals/sepa/income-ext",
            {"symbols": ",".join(batch)},
            timeout=45,
        )
        for sym, rows in (data.get("data") or {}).items():
            out.setdefault(sym, []).extend(rows)
    return out


def fetch_sepa_balance_sheet_ext(
    symbols: List[str],
    max_quarters: int = 6,
) -> Dict[str, List[Dict[str, Any]]]:
    """GET /stocks/fundamentals/sepa/balance-sheet-ext → {symbol: [rows]}"""
    if not symbols:
        return {}
    out: Dict[str, List[Dict[str, Any]]] = {}
    for i in range(0, len(symbols), _SEPA_BATCH_SIZE):
        batch = symbols[i : i + _SEPA_BATCH_SIZE]
        data = _get_json(
            "/stocks/fundamentals/sepa/balance-sheet-ext",
            {"symbols": ",".join(batch), "max_quarters": str(max_quarters)},
            timeout=45,
        )
        for sym, rows in (data.get("data") or {}).items():
            out.setdefault(sym, []).extend(rows)
    return out


def fetch_sepa_cash_flow_ext(
    symbols: List[str],
    max_quarters: int = 6,
) -> Dict[str, List[Dict[str, Any]]]:
    """GET /stocks/fundamentals/sepa/cash-flow-ext → {symbol: [rows]}"""
    if not symbols:
        return {}
    out: Dict[str, List[Dict[str, Any]]] = {}
    for i in range(0, len(symbols), _SEPA_BATCH_SIZE):
        batch = symbols[i : i + _SEPA_BATCH_SIZE]
        data = _get_json(
            "/stocks/fundamentals/sepa/cash-flow-ext",
            {"symbols": ",".join(batch), "max_quarters": str(max_quarters)},
            timeout=45,
        )
        for sym, rows in (data.get("data") or {}).items():
            out.setdefault(sym, []).extend(rows)
    return out


def fetch_sepa_ratios_latest(symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    """GET /stocks/fundamentals/sepa/ratios-latest → {symbol: {date, data}}"""
    if not symbols:
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for i in range(0, len(symbols), _SEPA_BATCH_SIZE):
        batch = symbols[i : i + _SEPA_BATCH_SIZE]
        data = _get_json(
            "/stocks/fundamentals/sepa/ratios-latest",
            {"symbols": ",".join(batch)},
            timeout=45,
        )
        out.update(data.get("data") or {})
    return out


def fetch_sepa_short_interest_latest(
    symbols: List[str],
    max_rows: int = 2,
) -> Dict[str, List[Dict[str, Any]]]:
    """GET /stocks/fundamentals/sepa/short-interest-latest → {symbol: [rows]}"""
    if not symbols:
        return {}
    out: Dict[str, List[Dict[str, Any]]] = {}
    for i in range(0, len(symbols), _SEPA_BATCH_SIZE):
        batch = symbols[i : i + _SEPA_BATCH_SIZE]
        data = _get_json(
            "/stocks/fundamentals/sepa/short-interest-latest",
            {"symbols": ",".join(batch), "max_rows": str(max_rows)},
            timeout=45,
        )
        for sym, rows in (data.get("data") or {}).items():
            out.setdefault(sym, []).extend(rows)
    return out


def fetch_sepa_short_volume_recent(
    symbols: List[str],
    max_days: int = 10,
) -> Dict[str, List[Dict[str, Any]]]:
    """GET /stocks/fundamentals/sepa/short-volume-recent → {symbol: [rows]}"""
    if not symbols:
        return {}
    out: Dict[str, List[Dict[str, Any]]] = {}
    for i in range(0, len(symbols), _SEPA_BATCH_SIZE):
        batch = symbols[i : i + _SEPA_BATCH_SIZE]
        data = _get_json(
            "/stocks/fundamentals/sepa/short-volume-recent",
            {"symbols": ",".join(batch), "max_days": str(max_days)},
            timeout=45,
        )
        for sym, rows in (data.get("data") or {}).items():
            out.setdefault(sym, []).extend(rows)
    return out


def fetch_sepa_gaps(
    report_type: str,
    limit: int = 2000,
) -> Dict[str, Any]:
    """GET /stocks/fundamentals/sepa/gaps → {count: N, symbols: [...]}"""
    data = _get_json(
        "/stocks/fundamentals/sepa/gaps",
        {"report_type": report_type, "limit": str(limit)},
        timeout=60,
    )
    return {"count": data.get("count", 0), "symbols": data.get("symbols", [])}
