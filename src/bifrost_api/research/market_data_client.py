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
