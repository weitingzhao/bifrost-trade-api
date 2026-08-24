"""Market and bars: OHLC, backfill jobs, trading-day, holidays, indices."""

import logging
import time
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, Query, Request
from fastapi.exceptions import HTTPException
from pydantic import BaseModel

from bifrost_core.monitor.reader import (
    delete_stock_bars_for_symbol,
)
from bifrost_core.monitor.reader.reference_indices_merge import merge_reference_indices
from bifrost_core.monitor.reader.symbol_normalize import norm_bars_symbol
from bifrost_core.monitor.services.market_jobs import (
    TOLERANCE_END_SEC_NON_TRADING,
    TOLERANCE_END_SEC_TRADING_DAY,
    WATCHLIST_EOD_PERIODS,
    build_plugin_backfill_preview,
    coverage_status,
    enqueue_plugin_bars_backfill,
    enqueue_watchlist_eod_plugin,
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


@router.post("/market/holidays")
def post_market_holiday(request: Request, body: Dict[str, Any]) -> Dict[str, Any]:
    """Retired: holiday calendar is maintained by Market Data Plugin (Polygon ingest)."""
    _ = request, body
    raise HTTPException(
        status_code=405,
        detail="Holiday CRUD retired — calendar maintained by Market Data Plugin",
    )


@router.delete("/market/holidays")
def delete_market_holiday(
    request: Request,
    date_param: str = Query(..., alias="date", description="Date YYYY-MM-DD"),
    exchange: str = Query("NYSE", description="Exchange"),
) -> Dict[str, Any]:
    """Retired: holiday calendar is maintained by Market Data Plugin (Polygon ingest)."""
    _ = request, date_param, exchange
    raise HTTPException(
        status_code=405,
        detail="Holiday CRUD retired — calendar maintained by Market Data Plugin",
    )


# --- Bars coverage and indices ---

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


@router.post("/indices/refresh")
def post_indices_refresh(
    request: Request,
    symbol: Optional[str] = Query(None, description="Refresh only this index (e.g. ^GSPC); omit to refresh all"),
    days: Optional[int] = Query(None, description="For single-symbol refresh: number of days to fetch"),
) -> Dict[str, Any]:
    """Refresh reference index daily bars from Polygon into stock_day."""
    control_via_db = request.app.state.control_via_db
    reader = request.app.state.reader
    if not control_via_db:
        return {"ok": False, "updated": [], "errors": ["Postgres config required."]}
    try:
        from bifrost_core.monitor.integrations.index_data_client import refresh_reference_indices, refresh_one_index
        if symbol and (symbol := symbol.strip()):
            result = refresh_one_index(control_via_db, symbol, days=days, reader=reader)
        else:
            result = refresh_reference_indices(control_via_db, reader=reader)
        return {"ok": result.get("ok", True), "updated": result.get("updated", []), "errors": result.get("errors", [])}
    except Exception as e:
        logger.warning("POST /indices/refresh failed: %s", e, exc_info=True)
        return {"ok": False, "updated": [], "errors": [str(e)]}


class DeleteBarsBody(BaseModel):
    periods: Optional[List[str]] = None

    class Config:
        extra = "ignore"


@router.delete("/bars/symbol")
def delete_bars_for_symbol(
    request: Request,
    symbol: Optional[str] = Query(..., description="Symbol to delete bars for"),
    body: Optional[DeleteBarsBody] = Body(None, description="Optional: periods to delete (1 D, 1 min, 5 mins, 1 hour). Omit to delete all."),
) -> Dict[str, Any]:
    """Delete stock_day and/or stock_min rows for the given symbol."""
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return {"ok": False, "error": "PostgreSQL is required to delete bar data."}
    sym = (symbol or "").strip().upper()
    if not sym:
        return {"ok": False, "error": "Missing symbol parameter."}
    period_list = None
    if body and body.periods and len(body.periods) > 0:
        period_list = [p.strip() for p in body.periods if (p or "").strip()]
    result = delete_stock_bars_for_symbol(control_via_db, sym, periods=period_list)
    if result.get("ok"):
        return {"ok": True, "deleted_day": result.get("deleted_day", 0), "deleted_min": result.get("deleted_min", 0), "message": f"Deleted selected periods for {sym}; you can pull again."}
    return {"ok": False, "error": result.get("error", "Delete failed")}


# --- Bars fetch and backfill ---

@router.post("/bars/fetch")
async def post_bars_fetch(
    request: Request,
    symbol: Optional[str] = Query(..., description="Symbol, e.g. NVDA"),
    period: Optional[str] = Query("1 D", description="Bar period (e.g. 1 D, 1 min)"),
    duration: Optional[str] = Query("30 D", description="Lookback hint (e.g. 30 D) — mapped to Plugin enqueue range"),
    smart_duration: bool = Query(False, description="Compute range from latest bar gap"),
) -> Dict[str, Any]:
    """Enqueue Polygon ingest for symbol+period (replaces IB Gateway fetch_bars)."""
    app = request.app
    reader = app.state.reader
    sym = (symbol or "").strip().upper()
    if not sym:
        return {"ok": False, "error": "Missing symbol parameter.", "bars": [], "count": 0}
    if not getattr(app.state, "monitor_enabled", True):
        return {"ok": False, "error": "Monitor stopped; cannot fetch bars.", "bars": [], "count": 0}
    per = (period or "1 D").strip()
    days: Optional[int] = None
    if duration and str(duration).strip().upper().endswith(" D"):
        try:
            days = int(str(duration).strip().split()[0])
        except ValueError:
            days = None
    if smart_duration:
        latest_ts = reader.get_bars_latest(symbol=sym, period=per)
        if latest_ts is not None:
            gap_sec = max(0, time.time() - float(latest_ts))
            days = min(max(1, int(gap_sec / 86400) + 1), 720 if per.upper() == "1 D" else 7)
    ok, job_id, error = enqueue_plugin_bars_backfill(reader, sym, per, days=days)
    if not ok:
        return {"ok": False, "error": error or "Enqueue failed.", "bars": [], "count": 0}
    if not job_id:
        return {"ok": True, "message": error or "Nothing to fetch.", "bars": [], "count": 0, "job_id": None}
    return {
        "ok": True,
        "job_id": job_id,
        "message": "Queued Plugin ingest; poll market-data worker / ops_jobs for completion.",
        "bars": [],
        "count": 0,
    }


@router.post("/bars/watchlist/eod-refresh/preview")
async def post_watchlist_eod_refresh_preview(
    request: Request,
    override_days: float = Query(1.0, ge=0, le=7),
    api_interval_sec: int = Query(10, ge=0, le=300),
) -> Dict[str, Any]:
    """Preview watchlist EOD Plugin enqueue plan (no IB requests)."""
    app = request.app
    reader = app.state.reader
    symbols = get_watchlist_stock_symbols(reader)
    periods = WATCHLIST_EOD_PERIODS
    items = []
    failures = []
    for sym in symbols:
        for per in periods:
            item = build_plugin_backfill_preview(reader, sym, per, override_days=override_days)
            if item.get("ok") is False:
                failures.append({"symbol": sym, "period": per, "error": item.get("error", "Preview failed")})
                continue
            item["api_interval_sec"] = api_interval_sec
            items.append(item)
    return {
        "ok": True,
        "preview_only": True,
        "ready_to_enqueue": bool(getattr(app.state, "monitor_enabled", True)),
        "symbols_count": len(symbols),
        "queued_jobs_if_confirmed": len(symbols) * len(periods),
        "override_days": override_days,
        "api_interval_sec": api_interval_sec,
        "periods": periods,
        "symbols": symbols,
        "items": items,
        "failed_count": len(failures),
        "failures": failures,
        "message": f"Dry run: {len(items)} Plugin ingest job(s) would be enqueued for watchlist EOD.",
    }


@router.post("/bars/backfill")
async def post_bars_backfill(
    request: Request,
    symbol: Optional[str] = Query(..., description="Symbol, e.g. NVDA"),
    period: Optional[str] = Query("1 D", description="Bar period: 1 D | 1 min | 5 mins | 1 hour"),
    years: Optional[float] = Query(None),
    days: Optional[int] = Query(None),
    override_days: Optional[float] = Query(None),
    span_hours: Optional[float] = Query(None),
    queue: bool = Query(True, description="Ignored; enqueue always uses Market Data Plugin."),
    is_test: bool = Query(False),
    api_interval_sec: int = Query(10, ge=0, le=300),
) -> Dict[str, Any]:
    """Backfill: enqueue Market Data Plugin ingest job."""
    del queue, is_test, api_interval_sec
    app = request.app
    reader = app.state.reader
    sym = (symbol or "").strip().upper()
    if not sym:
        return {"ok": False, "error": "Missing symbol parameter.", "count": 0}
    if not getattr(app.state, "monitor_enabled", True):
        return {"ok": False, "error": "Monitor stopped; cannot backfill bars.", "count": 0}
    per = (period or "1 D").strip()
    ok, job_id, error = enqueue_plugin_bars_backfill(
        reader, sym, per, years=years, days=days, override_days=override_days, span_hours=span_hours
    )
    if not ok:
        return {"ok": False, "error": error or "Enqueue failed.", "count": 0}
    if not job_id:
        return {"ok": True, "job_id": None, "message": error or "Nothing to backfill.", "count": 0}
    return {
        "ok": True,
        "job_id": job_id,
        "message": "Queued (Market Data Plugin). Poll ops_jobs.job_ingest for status.",
    }


@router.post("/bars/watchlist/eod-refresh")
async def post_watchlist_eod_refresh(
    request: Request,
    override_days: float = Query(1.0, ge=0, le=7),
    is_test: bool = Query(False),
    api_interval_sec: int = Query(10, ge=0, le=300),
) -> Dict[str, Any]:
    """Queue EOD refresh via Market Data Plugin for every Watchlist stock."""
    del is_test, api_interval_sec
    app = request.app
    reader = app.state.reader
    if not getattr(app.state, "monitor_enabled", True):
        return {"ok": False, "error": "Monitor stopped; cannot backfill bars.", "queued_count": 0}
    return enqueue_watchlist_eod_plugin(reader, override_days=override_days)
