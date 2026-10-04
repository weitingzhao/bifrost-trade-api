"""Market and bars: OHLC, benchmark, stats, holidays.

Reads only. The bars fetch / backfill / delete, watchlist EOD refresh, index
refresh and holiday write routes had no caller and no traffic and are gone (TD-40);
the Market Data Plugin owns ingest and the holiday calendar. ``GET /bars/latest``,
``/bars/coverage`` and ``/market/trading-day`` followed in api 0.8.0 (no hits in the
release they were marked deprecated)."""

import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query, Request

from bifrost_api.common.query_vocab import expiry_query, option_right_query

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
    expiry: Optional[str] = expiry_query(None, note="With asset=option."),
    strike: Optional[float] = Query(None, description="Option strike (with asset=option)"),
    option_right: Optional[str] = option_right_query(None, note="With asset=option."),
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
