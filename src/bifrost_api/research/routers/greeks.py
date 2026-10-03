"""Research: IV & Greeks — Black-Scholes implied vol + greeks from local DB data.

Queries option_day JOIN stock_day for the given symbol + trade_date, computes:
  - T (time to expiry in years)
  - Implied Volatility (Newton-Raphson)
  - Delta, Gamma, Theta (per day), Vega (per 1% vol move)

Note: NVDA options are American-style; Black-Scholes is a European approximation.
Near-ATM 21-35 DTE straddle error is acceptable for monitoring purposes.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request

from bifrost_api.research.deps import db_config
from bifrost_core.pricing import black_scholes as bs

router = APIRouter(tags=["research"])

DEFAULT_RISK_FREE_RATE = bs.RATE_RESEARCH  # 0.045

# ---------------------------------------------------------------------------
# Black-Scholes: bifrost_core.pricing (TD-42, api 0.6.1)
# ---------------------------------------------------------------------------
# The api kept its own copy until 0.6.0. These two wrappers are what the route calls;
# they reproduce that copy bit for bit (tests/research/test_bs_core_switch.py and core's
# golden test): a right other than "C" is a put, T <= 0 or sigma <= 0 raises (strict),
# the research IV solver, Greeks rounded to 6 places.


def _right(right: str) -> str:
    return "C" if right.upper() == "C" else "P"


def implied_vol(
    market_price: float,
    S: float,
    K: float,
    T: float,
    r: float,
    right: str,
    max_iter: int = 50,
) -> Optional[float]:
    """Newton-Raphson implied volatility (core's research convention), or None when there is none."""
    return bs.implied_vol(market_price, S, K, T, r, _right(right), convention=bs.IV_RESEARCH, max_iter=max_iter)


def compute_greeks(
    S: float, K: float, T: float, r: float, sigma: float, right: str
) -> Dict[str, float]:
    """Delta, Gamma, Theta (per calendar day), Vega (per 1% vol move), rounded to 6 places."""
    return {k: round(v, 6) for k, v in bs.greeks(S, K, T, r, sigma, _right(right), strict=True).items()}


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def _fetch_greeks_rows(
    db: dict,
    symbol: str,
    trade_date: str,
    r: float,
    expiry: Optional[str],
    right: Optional[str],
    limit: int,
) -> List[Dict[str, Any]]:
    """Query option_daily via Plugin API JOIN stock_day, compute IV + greeks for each row."""
    from bifrost_api.research.market_data_client import fetch_option_daily, fetch_stock_bars_daily

    sym = symbol.upper()

    # Fetch option daily data from Plugin
    resp = fetch_option_daily(sym, expiry=expiry, days=365, limit=min(limit, 5000))
    if not resp.get("ok"):
        return []

    # Filter rows matching trade_date
    target_rows = [
        row for row in (resp.get("rows") or [])
        if str(row.get("bar_date", ""))[:10] == trade_date
    ]

    if right:
        right_upper = right.upper()
        target_rows = [
            row for row in target_rows
            if str(row.get("option_right", "")).strip().upper() == right_upper
        ]

    target_rows = target_rows[:limit]
    if not target_rows:
        return []

    # Fetch stock price for the trade_date from Plugin
    stock_bars = fetch_stock_bars_daily([sym], days=400)
    symbol_bars = stock_bars.get(sym, [])
    stock_price: Optional[float] = None
    for bar in symbol_bars:
        bar_time = str(bar.get("bar_time", ""))[:10]
        if bar_time == trade_date:
            close = bar.get("close")
            if close is not None and float(close) > 0:
                stock_price = float(close)
                break

    if stock_price is None or stock_price <= 0:
        return []

    rows: List[Dict[str, Any]] = []
    for raw_row in target_rows:
        expiry_str = str(raw_row.get("expiry") or "").strip()
        strike = raw_row.get("strike")
        opt_right = str(raw_row.get("option_right") or "").strip().upper()
        market_price = raw_row.get("close")

        if not expiry_str or not strike or not market_price:
            continue
        try:
            strike = float(strike)
            market_price = float(market_price)
        except (TypeError, ValueError):
            continue
        if strike <= 0 or market_price <= 0:
            continue

        try:
            if len(expiry_str) == 8 and expiry_str.isdigit():
                exp_date = datetime.strptime(expiry_str, "%Y%m%d").date()
            elif len(expiry_str) >= 10:
                exp_date = datetime.strptime(expiry_str[:10], "%Y-%m-%d").date()
            else:
                continue
        except ValueError:
            continue

        try:
            td = datetime.strptime(trade_date, "%Y-%m-%d").date()
        except ValueError:
            continue

        t_days = (exp_date - td).days
        if t_days <= 0:
            continue
        t_years = t_days / 365.0

        iv = implied_vol(market_price, stock_price, strike, t_years, r, opt_right)

        if iv is not None:
            greeks = compute_greeks(stock_price, strike, t_years, r, iv, opt_right)
        else:
            greeks = {"delta": None, "gamma": None, "theta": None, "vega": None}

        rows.append({
            "expiry": exp_date.strftime("%Y-%m-%d"),
            "strike": strike,
            "right": opt_right,
            "market_price": round(market_price, 4),
            "stock_price": round(stock_price, 4),
            "t_years": round(t_years, 6),
            "t_days": t_days,
            "iv": round(iv, 6) if iv is not None else None,
            "delta": greeks["delta"],
            "gamma": greeks["gamma"],
            "theta": greeks["theta"],
            "vega": greeks["vega"],
        })

    return rows


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/research/greeks/available-dates")
def get_greeks_available_dates(
    request: Request,
    symbol: str = Query(..., description="Ticker symbol (e.g. NVDA)"),
    limit: int = Query(90, ge=1, le=365),
) -> Dict[str, Any]:
    """Return distinct trade dates available in option_daily for the given symbol."""
    from bifrost_api.research.market_data_client import fetch_option_daily_available_dates

    sym = symbol.strip().upper()
    try:
        resp = fetch_option_daily_available_dates(sym, limit=limit)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    if not resp.get("ok"):
        raise HTTPException(status_code=503, detail=str(resp.get("error") or "plugin error"))
    return {"ok": True, "symbol": sym, "dates": resp.get("dates", [])}


@router.get("/research/greeks")
def get_greeks(
    request: Request,
    symbol: str = Query(..., description="Ticker symbol (e.g. NVDA)"),
    trade_date: str = Query(..., description="Trade date YYYY-MM-DD"),
    risk_free_rate: float = Query(DEFAULT_RISK_FREE_RATE, ge=0.0, le=0.5),
    expiry: Optional[str] = Query(None, description="Filter to one expiry YYYY-MM-DD"),
    right: Optional[str] = Query(None, description="Filter: C or P"),
    limit: int = Query(300, ge=1, le=2000),
) -> Dict[str, Any]:
    """Compute Black-Scholes IV and Greeks for option_day rows on a given trade date.

    Joins option_day with stock_day for the underlying price.
    Note: Black-Scholes is a European approximation for NVDA's American options.
    """
    db = db_config(request)
    if db is None:
        raise HTTPException(status_code=503, detail="no db config")

    sym = symbol.strip().upper()
    try:
        rows = _fetch_greeks_rows(db, sym, trade_date, risk_free_rate, expiry, right, limit)
        stock_price = rows[0]["stock_price"] if rows else None
        return {
            "ok": True,
            "symbol": sym,
            "trade_date": trade_date,
            "stock_price": stock_price,
            "risk_free_rate": risk_free_rate,
            "count": len(rows),
            "rows": rows,
        }
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))
