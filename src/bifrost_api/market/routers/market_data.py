"""Market and bars: OHLC, coverage, trading-day, holidays.

Reads only. The bars fetch / backfill / delete, watchlist EOD refresh, index
refresh and holiday write routes had no caller and no traffic and are gone (TD-40);
the Market Data Plugin owns ingest and the holiday calendar."""

import logging
import time
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query, Request

from bifrost_core.monitor.reader.reference_indices_merge import merge_reference_indices
from bifrost_core.monitor.reader.symbol_normalize import norm_bars_symbol
from bifrost_core.monitor.services.market_jobs import (
    TOLERANCE_END_SEC_NON_TRADING,
    TOLERANCE_END_SEC_TRADING_DAY,
    coverage_status,
    get_watchlist_stock_symbols,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["market"])


def _synthetic_option_vwap_from_ohlcv(row_pg: Dict[str, Any]) -> Optional[float]:
    """When PG ``vwap`` is NULL, derive a mark from OHLCV (typical (H+L+C)/3 when volume > 0).

    Some historical rows in ``option_min`` / ``option_day`` were stored with NULL ``vwap``;
    this fills the chart without requiring a full re-backfill.
    """
    try:
        vol = float(row_pg["volume"]) if row_pg.get("volume") is not None else 0.0
    except (TypeError, ValueError):
        vol = 0.0
    if vol <= 0:
        return None
    h, l_, c = row_pg.get("high"), row_pg.get("low"), row_pg.get("close")
    try:
        hf = float(h) if h is not None else None
        lf = float(l_) if l_ is not None else None
        cf = float(c) if c is not None else None
    except (TypeError, ValueError):
        return None
    if hf is not None and lf is not None and cf is not None:
        return (hf + lf + cf) / 3.0
    if cf is not None:
        return cf
    if hf is not None and lf is not None:
        return (hf + lf) / 2.0
    return None


# --- Bars read ---

@router.get("/bars")
def get_bars(
    request: Request,
    symbol: Optional[str] = Query(None, description="Symbol, e.g. NVDA"),
    period: Optional[str] = Query("1 D", description="Bar period (e.g. 1 min, 1 D)"),
    limit: int = Query(100, ge=1, le=500),
    asset: str = Query("stock", description="stock | option"),
    source: Optional[str] = Query(None, description="For option bars: ib | massive (default ib)"),
    expiry: Optional[str] = Query(None, description="Option expiry YYYYMMDD (with asset=option)"),
    strike: Optional[float] = Query(None, description="Option strike (with asset=option)"),
    option_right: Optional[str] = Query(None, description="C or P (with asset=option)"),
) -> Dict[str, Any]:
    """K-line/OHLC bars for replay (R-A3). Stock: stock_day / stock_min. Option: option_day / option_min with source."""
    reader = request.app.state.reader
    sym = (symbol or "").strip()
    if not sym:
        return {"bars": [], "message": "Missing symbol parameter."}
    asset_l = (asset or "stock").strip().lower()
    if asset_l == "option":
        if expiry is None or strike is None or option_right is None:
            return {
                "bars": [],
                "message": "Option bars require expiry, strike, and option_right.",
                "asset": "option",
            }
        src = (source or "ib").strip().lower()
        if src not in ("ib", "massive"):
            src = "ib"
        per = (period or "1 min").strip()
        if not hasattr(reader, "get_option_bars"):
            return {"bars": [], "message": "Option bars not available.", "asset": "option"}
        items = reader.get_option_bars(
            sym,
            expiry.strip(),
            float(strike),
            (option_right or "C").strip(),
            period=per,
            source=src,
            limit=limit,
        )
        bars = []
        for r in items:
            row: Dict[str, Any] = {
                "time": float(r["time"]) if r.get("time") is not None else 0,
                "open": float(r["open"]) if r.get("open") is not None else 0,
                "high": float(r["high"]) if r.get("high") is not None else 0,
                "low": float(r["low"]) if r.get("low") is not None else 0,
                "close": float(r["close"]) if r.get("close") is not None else 0,
                "volume": float(r["volume"]) if r.get("volume") is not None else 0,
                "source": (r.get("source") or src),
            }
            resolved: Optional[float] = None
            vw = r.get("vwap")
            if vw is not None:
                try:
                    resolved = float(vw)
                except (TypeError, ValueError):
                    resolved = None
            if resolved is None:
                syn = _synthetic_option_vwap_from_ohlcv(r)
                if syn is not None:
                    resolved = syn
            if resolved is not None:
                row["vwap"] = resolved
            bars.append(row)
        return {"bars": bars, "source": src, "asset": "option"}

    per = (period or "1 D").strip()
    items = reader.get_bars(symbol=sym, period=per, limit=limit)
    bars = []
    for r in items:
        row: Dict[str, Any] = {
            "time": float(r["time"]) if r.get("time") is not None else 0,
            "open": float(r["open"]) if r.get("open") is not None else 0,
            "high": float(r["high"]) if r.get("high") is not None else 0,
            "low": float(r["low"]) if r.get("low") is not None else 0,
            "close": float(r["close"]) if r.get("close") is not None else 0,
            "volume": float(r["volume"]) if r.get("volume") is not None else 0,
        }
        vw = r.get("vwap")
        resolved: Optional[float] = None
        if vw is not None:
            try:
                resolved = float(vw)
            except (TypeError, ValueError):
                resolved = None
        if resolved is None:
            syn = _synthetic_option_vwap_from_ohlcv(r)
            if syn is not None:
                resolved = syn
        if resolved is not None:
            row["vwap"] = resolved
        bars.append(row)
    out: Dict[str, Any] = {"bars": bars, "asset": "stock"}
    if source:
        out["source"] = source
    return out


@router.get("/bars/latest")
def get_bars_latest(
    request: Request,
    symbol: Optional[str] = Query(None),
    period: Optional[str] = Query("1 D"),
) -> Dict[str, Any]:
    """Return latest bar time (Unix) for symbol+period."""
    reader = request.app.state.reader
    sym = (symbol or "").strip()
    if not sym:
        return {"latest": None, "message": "Missing symbol parameter."}
    per = (period or "1 D").strip()
    t = reader.get_bars_latest(symbol=sym, period=per)
    return {"latest": t}


@router.get("/bars/benchmark")
def get_bars_benchmark(
    request: Request,
    symbols: Optional[str] = Query(None, description="Comma-separated symbols"),
    date_str: Optional[str] = Query(None, alias="date", description="YYYY-MM-DD; default today"),
) -> Dict[str, Any]:
    """Return latest daily bar on or before date per symbol (for Daily % / Daily $)."""
    reader = request.app.state.reader
    if not symbols or not str(symbols).strip():
        return {"benchmarks": {}}
    sym_list = [s.strip() for s in str(symbols).split(",") if s and s.strip()]
    ref = date.today()
    if date_str and str(date_str).strip():
        try:
            ref = datetime.strptime(str(date_str).strip()[:10], "%Y-%m-%d").date()
        except ValueError:
            pass
    result = reader.get_bars_benchmark(symbols=sym_list, on_or_before=ref)
    out = {}
    for sym, ent in result.items():
        bar_time = ent.get("bar_time") or 0
        try:
            bar_date = datetime.fromtimestamp(bar_time).date()
        except (TypeError, ValueError, OSError):
            bar_date = ref
        is_today = (ref - bar_date).days == 0
        is_stale = (ref - bar_date).days > 1
        out[sym] = {**ent, "is_today": is_today, "is_stale": is_stale}
    return {"benchmarks": out}


@router.get("/bars/stats")
def get_bars_stats(
    request: Request,
    symbol: Optional[str] = Query(None, description="Symbol, e.g. NVDA"),
) -> Dict[str, Any]:
    """Return row counts for symbol in stock_day / stock_min."""
    reader = request.app.state.reader
    sym = (symbol or "").strip()
    if not sym:
        return {"stock_day": 0, "stock_min": {}, "message": "Missing symbol parameter."}
    stats = reader.get_bars_stats(symbol=sym)
    return stats


# --- Market calendar ---

@router.get("/market/trading-day")
def get_market_trading_day(
    request: Request,
    date_param: Optional[str] = Query(None, alias="date", description="Date YYYY-MM-DD; default today America/New_York"),
) -> Dict[str, Any]:
    """Return whether the given date is a US (NYSE) trading day."""
    reader = request.app.state.reader
    if date_param and date_param.strip():
        date_str = date_param.strip()
    else:
        from zoneinfo import ZoneInfo
        date_str = datetime.now(ZoneInfo("America/New_York")).date().isoformat()
    is_trading = reader.get_is_us_trading_day(date_str)
    return {"date": date_str, "is_trading_day": is_trading}


@router.get("/market/holidays")
def get_market_holidays(
    request: Request,
    year: Optional[int] = Query(None, description="Filter by year"),
    exchange: Optional[str] = Query(None, description="Exchange filter (e.g. NYSE). Omit to return all exchanges."),
) -> List[Dict[str, Any]]:
    """Return US market holidays from market.us_market_holiday (FDW). exchange omitted = all exchanges."""
    reader = request.app.state.reader
    return reader.get_market_holidays(exchange=exchange or None, year=year)


# --- Bars coverage ---

@router.get("/bars/coverage")
def get_bars_coverage(
    request: Request,
    symbols: Optional[str] = Query(None, description="Comma-separated symbols; if omitted, use Watchlist stocks + reference indices"),
) -> Dict[str, Any]:
    """Return coverage (count, min/max ts) plus target range from config and status: ok | gap_end | missing."""
    app = request.app
    reader = app.state.reader
    control_via_db = app.state.control_via_db
    if symbols is not None and str(symbols).strip():
        sym_list = [s.strip() for s in str(symbols).split(",") if s and s.strip()]
    else:
        sym_list = list(get_watchlist_stock_symbols(reader))
        seen_norms = {norm_bars_symbol(x) for x in sym_list}
        refs = merge_reference_indices(
            (control_via_db or {}).get("reference_indices"),
            (reader._config or {}).get("reference_indices"),
        )
        for ref in refs:
            s = (ref.get("symbol") or "").strip()
            if not s:
                continue
            nk = norm_bars_symbol(s)
            if nk not in seen_norms:
                seen_norms.add(nk)
                sym_list.append(s)
        try:
            for s in reader.get_distinct_caret_bar_symbols():
                if not s:
                    continue
                nk = norm_bars_symbol(s)
                if nk not in seen_norms:
                    seen_norms.add(nk)
                    sym_list.append(s)
        except Exception:
            pass
    coverage = reader.get_bars_coverage(symbols=sym_list)
    try:
        from bifrost_core.config.startup import read_config
        config, _ = read_config()
    except Exception:
        config = {}
    hb = (config.get("history_backfill") or {}).get("stock") or {}
    daily_years = float(hb.get("daily_years", 10.0))
    min_weeks = float(hb.get("min_weeks", 1.0))
    five_min_months = float(hb.get("5min_months", 1.0))
    one_hour_months = float(hb.get("1hour_months", 3.0))
    policy = {"daily_years": daily_years, "min_weeks": min_weeks, "5min_months": five_min_months, "1hour_months": one_hour_months}
    now_ts = time.time()
    one_day = 86400.0
    target_end_ts = now_ts
    target_daily_start = now_ts - (365 * daily_years * one_day)
    target_min_start = now_ts - (7 * min_weeks * one_day)
    target_5min_start = now_ts - (30 * five_min_months * one_day)
    target_1hour_start = now_ts - (30 * one_hour_months * one_day)
    # Today (America/New_York): if trading day, end-gap tolerance = 1 day; else (weekend/holiday) = 2 days.
    try:
        from zoneinfo import ZoneInfo
        today_str = datetime.now(ZoneInfo("America/New_York")).date().isoformat()
        is_trading_today = reader.get_is_us_trading_day(today_str)
    except Exception:
        is_trading_today = True
    tolerance_end_sec = TOLERANCE_END_SEC_TRADING_DAY if is_trading_today else TOLERANCE_END_SEC_NON_TRADING
    enriched = []
    for item in coverage:
        day = item.get("stock_day") or {}
        day_ts_s = day.get("min_ts")
        day_ts_e = day.get("max_ts")
        day_cnt = day.get("count") or 0
        day_status = coverage_status(day_ts_s, day_ts_e, day_cnt, target_daily_start, target_end_ts, tolerance_end_sec)
        stock_day_enriched = {**day, "target_start_ts": target_daily_start, "target_end_ts": target_end_ts, "status": day_status}
        mins = item.get("stock_min") or {}
        min_1 = mins.get("1 min") or {}
        min_5 = mins.get("5 mins") or {}
        min_1h = mins.get("1 hour") or {}
        stock_min_enriched = {
            "1 min": {**min_1, "target_start_ts": target_min_start, "target_end_ts": target_end_ts, "status": coverage_status(min_1.get("min_ts"), min_1.get("max_ts"), min_1.get("count") or 0, target_min_start, target_end_ts, tolerance_end_sec)},
            "5 mins": {**min_5, "target_start_ts": target_5min_start, "target_end_ts": target_end_ts, "status": coverage_status(min_5.get("min_ts"), min_5.get("max_ts"), min_5.get("count") or 0, target_5min_start, target_end_ts, tolerance_end_sec)},
            "1 hour": {**min_1h, "target_start_ts": target_1hour_start, "target_end_ts": target_end_ts, "status": coverage_status(min_1h.get("min_ts"), min_1h.get("max_ts"), min_1h.get("count") or 0, target_1hour_start, target_end_ts, tolerance_end_sec)},
        }
        enriched.append({"symbol": item.get("symbol"), "stock_day": stock_day_enriched, "stock_min": stock_min_enriched})
    return {"coverage": enriched, "policy": policy}
