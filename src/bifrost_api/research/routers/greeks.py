"""Research: IV & Greeks — Black-Scholes implied vol + greeks from local DB data.

Queries option_day JOIN stock_day for the given symbol + trade_date, computes:
  - T (time to expiry in years)
  - Implied Volatility (Newton-Raphson)
  - Delta, Gamma, Theta (per day), Vega (per 1% vol move)

Note: NVDA options are American-style; Black-Scholes is a European approximation.
Near-ATM 21-35 DTE straddle error is acceptable for monitoring purposes.
"""

from __future__ import annotations

import math
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query, Request

router = APIRouter(tags=["research"])

DEFAULT_RISK_FREE_RATE = 0.045

# ---------------------------------------------------------------------------
# Black-Scholes math (pure Python, no scipy)
# ---------------------------------------------------------------------------


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def _bs_d1d2(S: float, K: float, T: float, r: float, sigma: float):
    """Return (d1, d2) for BS formula."""
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    return d1, d2


def _bs_price(S: float, K: float, T: float, r: float, sigma: float, right: str) -> float:
    """Black-Scholes call (right='C') or put (right='P') price."""
    d1, d2 = _bs_d1d2(S, K, T, r, sigma)
    if right.upper() == "C":
        return S * _norm_cdf(d1) - K * math.exp(-r * T) * _norm_cdf(d2)
    else:
        return K * math.exp(-r * T) * _norm_cdf(-d2) - S * _norm_cdf(-d1)


def _bs_vega(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """Vega = dPrice/dSigma (same for calls and puts)."""
    d1, _ = _bs_d1d2(S, K, T, r, sigma)
    return S * _norm_pdf(d1) * math.sqrt(T)


def implied_vol(
    market_price: float,
    S: float,
    K: float,
    T: float,
    r: float,
    right: str,
    max_iter: int = 50,
) -> Optional[float]:
    """Newton-Raphson implied volatility.

    Returns None when IV cannot be found (deep OTM/ITM, zero price, etc.).
    """
    if T <= 0 or market_price <= 0 or S <= 0 or K <= 0:
        return None

    # Intrinsic value floor
    if right.upper() == "C":
        intrinsic = max(0.0, S - K * math.exp(-r * T))
    else:
        intrinsic = max(0.0, K * math.exp(-r * T) - S)

    if market_price < intrinsic - 1e-6:
        return None

    sigma = 0.3  # initial guess
    for _ in range(max_iter):
        try:
            price = _bs_price(S, K, T, r, sigma, right)
            vega = _bs_vega(S, K, T, r, sigma)
            if vega < 1e-10:
                break
            diff = price - market_price
            sigma -= diff / vega
            sigma = max(0.001, min(5.0, sigma))
            if abs(diff) < 1e-8:
                break
        except (ValueError, ZeroDivisionError):
            break

    # Validate: price should be close to market
    try:
        check = _bs_price(S, K, T, r, sigma, right)
        if abs(check - market_price) > max(0.05 * market_price, 0.05):
            return None
    except (ValueError, ZeroDivisionError):
        return None

    return sigma if 0.001 <= sigma <= 5.0 else None


def compute_greeks(
    S: float, K: float, T: float, r: float, sigma: float, right: str
) -> Dict[str, float]:
    """Compute Delta, Gamma, Theta (per calendar day), Vega (per 1% vol move).

    Returns dict with keys: delta, gamma, theta, vega.
    """
    d1, d2 = _bs_d1d2(S, K, T, r, sigma)
    nd1 = _norm_pdf(d1)
    sqrt_T = math.sqrt(T)
    discount = math.exp(-r * T)

    gamma = nd1 / (S * sigma * sqrt_T)

    if right.upper() == "C":
        delta = _norm_cdf(d1)
        theta_annual = (
            -(S * nd1 * sigma) / (2.0 * sqrt_T)
            - r * K * discount * _norm_cdf(d2)
        )
    else:
        delta = _norm_cdf(d1) - 1.0
        theta_annual = (
            -(S * nd1 * sigma) / (2.0 * sqrt_T)
            + r * K * discount * _norm_cdf(-d2)
        )

    theta_per_day = theta_annual / 365.0
    vega_per_1pct = _bs_vega(S, K, T, r, sigma) * 0.01

    return {
        "delta": round(delta, 6),
        "gamma": round(gamma, 6),
        "theta": round(theta_per_day, 6),
        "vega": round(vega_per_1pct, 6),
    }


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def _db_config(request: Request) -> Optional[dict]:
    return request.app.state.control_via_db or getattr(request.app.state, "status_cfg_for_read", None)


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
        if not resp.get("ok"):
            return {"ok": False, "symbol": sym, "dates": [], "error": resp.get("error", "plugin error")}
        return {"ok": True, "symbol": sym, "dates": resp.get("dates", [])}
    except Exception as exc:
        return {"ok": False, "symbol": sym, "dates": [], "error": str(exc)}


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
    db = _db_config(request)
    if db is None:
        return {
            "ok": False, "symbol": symbol, "trade_date": trade_date,
            "stock_price": None, "risk_free_rate": risk_free_rate,
            "count": 0, "rows": [], "error": "no db config",
        }

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
        return {
            "ok": False, "symbol": sym, "trade_date": trade_date,
            "stock_price": None, "risk_free_rate": risk_free_rate,
            "count": 0, "rows": [], "error": str(exc),
        }
