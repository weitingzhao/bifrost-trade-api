"""Research: Option Screener — scores put/call contracts by strategy structure type.

V1 implements Cash Secured Put (CSP) only.  The structure_type dispatch point is
present so that CC / Spread / Iron Condor can be added as additional branches.
"""

import math
from datetime import date, datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Request
from pydantic import BaseModel

router = APIRouter(tags=["research"])

RISK_FREE_RATE = 0.045


# ---------------------------------------------------------------------------
# Request schema
# ---------------------------------------------------------------------------


class ScreenerRequest(BaseModel):
    structure_type: str = "cash_secured_put"
    symbols: List[str]
    dte_min: Optional[int] = None
    dte_max: Optional[int] = None
    max_prob_itm: Optional[float] = None
    min_annualized_return: Optional[float] = None
    max_spread_pct: Optional[float] = None
    include_earnings_span: bool = False
    min_premium: Optional[float] = None
    source: str = "massive"


# ---------------------------------------------------------------------------
# Black-Scholes helpers (self-contained; mirrors src/portfolio/model/core.py)
# ---------------------------------------------------------------------------


def _bs_d1(S: float, K: float, T: float, r: float, sigma: float) -> float:
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return 0.0
    return (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _prob_itm_put(spot: float, strike: float, dte: int, iv: float) -> float:
    """Probability of a put finishing in-the-money (BS N(-d2))."""
    T = dte / 365.0
    if T <= 0 or iv <= 0:
        return 1.0 if strike > spot else 0.0
    d1 = _bs_d1(spot, strike, T, RISK_FREE_RATE, iv)
    d2 = d1 - iv * math.sqrt(T)
    return _norm_cdf(-d2)


def _composite_score(
    annualized: Optional[float],
    prob_itm: float,
    safety_margin: float,
    iv_percentile: Optional[float],
    spread_pct: float,
) -> float:
    """Weighted composite score 0–100.

    Weights: annualized 30%, prob_itm 25%, safety_margin 20%, iv_pct 15%, liquidity 10%.
    """

    def clip(v: float, lo: float, hi: float) -> float:
        return max(lo, min(hi, v))

    a = clip(annualized / 1.50, 0.0, 1.0) if annualized is not None else 0.0
    p = clip(1.0 - prob_itm / 0.30, 0.0, 1.0)
    s = clip(safety_margin / 0.20, 0.0, 1.0)
    v = clip(iv_percentile / 100.0, 0.0, 1.0) if iv_percentile is not None else 0.5
    liq = clip(1.0 - spread_pct / 0.30, 0.0, 1.0)
    return (0.30 * a + 0.25 * p + 0.20 * s + 0.15 * v + 0.10 * liq) * 100.0


def _rating(score: float) -> str:
    if score >= 75:
        return "A"
    if score >= 55:
        return "B"
    if score >= 35:
        return "C"
    return "D"


def _risk(prob_itm: float) -> str:
    if prob_itm < 0.15:
        return "low"
    if prob_itm < 0.30:
        return "medium"
    return "high"


# ---------------------------------------------------------------------------
# DB / request helpers
# ---------------------------------------------------------------------------


def _db_config(request: Request) -> Optional[dict]:
    return request.app.state.control_via_db or getattr(request.app.state, "status_cfg_for_read", None)


def _norm_expiry_key(expiration: str) -> str:
    e = (expiration or "").strip()
    if len(e) >= 10 and e[4] == "-":
        return e[:4] + e[5:7] + e[8:10]
    return e


def _get_spot(sym: str, request: Request) -> Optional[float]:
    reader = getattr(request.app.state, "reader", None)
    if reader and hasattr(reader, "get_stock_day_fallback_price"):
        fallback = reader.get_stock_day_fallback_price(sym)
        if fallback and fallback[0] is not None:
            try:
                v = float(fallback[0])
                if v > 0:
                    return v
            except (TypeError, ValueError):
                pass
    return None


def _get_iv_history_series(
    db: dict, symbol: str, all_exps: List[str], source: str
) -> List[float]:
    """Average daily ATM IV per trade_date over the past year — sorted ascending.

    Uses report_option_atm_iv_daily via the public reader function.
    Groups all expirations' ATM IVs by trade_date and returns one average per day.
    """
    from bifrost_api.research.market_pg import get_report_option_atm_iv_daily

    since = (datetime.now(timezone.utc) - timedelta(days=365)).date()
    rows = get_report_option_atm_iv_daily(db, symbol, all_exps, source, since)

    by_date: Dict[Any, List[float]] = {}
    for row in rows:
        td = row.get("trade_date")
        av = row.get("atm_iv")
        if td is None or av is None:
            continue
        try:
            by_date.setdefault(td, []).append(float(av))
        except (TypeError, ValueError):
            pass

    series = [sum(vs) / len(vs) for vs in by_date.values() if vs]
    return sorted(series)


def _iv_percentile(iv: float, sorted_series: List[float]) -> Optional[float]:
    """Rank of iv in sorted historical series (0–100); None if insufficient data."""
    if len(sorted_series) < 5:
        return None
    rank = sum(1 for v in sorted_series if v <= iv)
    return round(rank / len(sorted_series) * 100.0, 1)


# ---------------------------------------------------------------------------
# CSP scanner (V1)
# ---------------------------------------------------------------------------


def _scan_csp(
    sym: str,
    body: ScreenerRequest,
    db: dict,
    src: str,
    today: date,
    request: Request,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Scan one symbol for CSP candidates.

    Returns (group_dict, warning_str).  group_dict is None on hard failure
    (no data).  warning_str is set when a soft or hard issue occurred.
    """
    from bifrost_api.research.polygon_http import contract_key_from_parts
    from bifrost_api.research.market_pg import (
        get_option_expirations_from_contracts_db,
        get_option_snapshots_latest,
    )
    from bifrost_api.research.iv_atm import strikes_around_spot, parse_contract_key

    spot = _get_spot(sym, request)

    all_exps_raw = get_option_expirations_from_contracts_db(db, sym)
    if not all_exps_raw:
        return None, "No snapshot data — run Massive sync first"

    all_exps = [_norm_expiry_key(e) for e in all_exps_raw]
    all_exps = [e for e in all_exps if len(e) == 8 and e.isdigit()]

    # Filter expirations to DTE window
    valid_exps: List[Tuple[str, int]] = []
    for exp in all_exps:
        try:
            exp_date = datetime.strptime(exp, "%Y%m%d").date()
        except ValueError:
            continue
        dte = (exp_date - today).days
        if (body.dte_min is None or dte >= body.dte_min) and (body.dte_max is None or dte <= body.dte_max):
            valid_exps.append((exp, dte))

    if not valid_exps:
        dte_desc = f"DTE {body.dte_min}–{body.dte_max}" if body.dte_min is not None or body.dte_max is not None else "any DTE"
        return None, f"No expirations found ({dte_desc})"

    # Strike ladder
    if spot and spot > 0:
        strike_set = strikes_around_spot(spot, count=10)
    else:
        return None, "No underlying price available; run stock_day sync first"

    if not strike_set:
        return None, "Cannot compute strike ladder (spot unavailable)"

    # Build put contract keys
    all_keys: List[str] = []
    key_meta: Dict[str, Tuple[str, int]] = {}
    for exp, dte in valid_exps:
        for st in strike_set:
            k = contract_key_from_parts(sym, exp, float(st), "P")
            all_keys.append(k)
            key_meta[k] = (exp, dte)

    rows = get_option_snapshots_latest(db, all_keys, source=src)
    if not rows:
        return None, "No snapshot data — run Massive sync first"

    # IV percentile history
    iv_history = _get_iv_history_series(db, sym, all_exps, src)

    contracts: List[Dict[str, Any]] = []
    ivs_for_avg: List[float] = []

    for row in rows:
        ck = row.get("contract_key") or ""
        strike, right = parse_contract_key(ck)
        if strike is None or right != "P":
            continue
        if ck not in key_meta:
            continue
        exp, dte = key_meta[ck]

        # Underlying price (snapshot-level preferred)
        up_val = row.get("underlying_price")
        contract_spot: float = spot or 0.0
        if up_val is not None:
            try:
                contract_spot = float(up_val)
            except (TypeError, ValueError):
                pass
        if contract_spot <= 0:
            continue

        # Option premium (day_close from Massive chain; NBBO not stored)
        bid_f = float(row["bid"]) if row.get("bid") is not None else None
        ask_f = float(row["ask"]) if row.get("ask") is not None else None
        mid_raw = row.get("mid")
        try:
            mid_f = float(mid_raw) if mid_raw is not None else None
        except (TypeError, ValueError):
            mid_f = None
        if mid_f is None and bid_f is not None and ask_f is not None:
            mid_f = (bid_f + ask_f) / 2.0
        if mid_f is None and row.get("day_close") is not None:
            try:
                mid_f = float(row["day_close"])
            except (TypeError, ValueError):
                mid_f = None
        if mid_f is None or mid_f <= 0:
            continue

        # Spread pct (relative to mid)
        if bid_f is not None and ask_f is not None and mid_f > 0:
            spread_pct = (ask_f - bid_f) / mid_f
        else:
            spread_pct = 0.0

        if body.max_spread_pct is not None and spread_pct > body.max_spread_pct:
            continue

        # IV
        iv_raw = row.get("iv")
        try:
            iv_f = float(iv_raw) if iv_raw is not None else None
        except (TypeError, ValueError):
            iv_f = None

        # Prob ITM: BS N(-d2) for puts, fall back to abs(delta)
        delta_raw = row.get("delta")
        if iv_f is not None and iv_f > 0:
            prob_itm = _prob_itm_put(contract_spot, strike, dte, iv_f)
        elif delta_raw is not None:
            try:
                prob_itm = min(1.0, abs(float(delta_raw)))
            except (TypeError, ValueError):
                prob_itm = 0.5
        else:
            prob_itm = 0.5

        if body.max_prob_itm is not None and prob_itm > body.max_prob_itm:
            continue

        safety_margin = max(0.0, (contract_spot - strike) / contract_spot)
        premium = mid_f * 100.0

        if body.min_premium is not None and premium < body.min_premium:
            continue

        # CSP margin = strike * 100
        margin = strike * 100.0
        annualized = (premium / margin) * (365.0 / dte) if margin > 0 and dte > 0 else None

        if body.min_annualized_return is not None and annualized is not None and annualized < body.min_annualized_return:
            continue

        iv_pct = _iv_percentile(iv_f, iv_history) if iv_f is not None else None
        sc = _composite_score(annualized, prob_itm, safety_margin, iv_pct, spread_pct)

        oi_raw = row.get("open_interest")
        if iv_f is not None:
            ivs_for_avg.append(iv_f)

        contracts.append({
            "symbol": sym,
            "spot": round(contract_spot, 2),
            "expiration": exp,
            "strike": strike,
            "right": "P",
            "dte": dte,
            "score": round(sc, 1),
            "rating": _rating(sc),
            "risk": _risk(prob_itm),
            "iv": round(iv_f, 6) if iv_f is not None else None,
            "premium": round(premium, 2),
            "prob_itm": round(prob_itm, 4),
            "safety_margin": round(safety_margin, 4),
            "annualized": round(annualized, 4) if annualized is not None else None,
            "apr_pct": round(annualized * 100.0, 1) if annualized is not None else None,
            "margin": round(margin, 2),
            "bid": round(bid_f, 4) if bid_f is not None else None,
            "ask": round(ask_f, 4) if ask_f is not None else None,
            "mid": round(mid_f, 4),
            "spread_pct": round(spread_pct, 4),
            "open_interest": int(oi_raw) if oi_raw is not None else None,
            "delta": round(float(delta_raw), 4) if delta_raw is not None else None,
            "iv_percentile": iv_pct,
            "long_strike": None,
        })

    if not contracts:
        return None, "No contracts passed filters"

    contracts.sort(key=lambda c: c["score"], reverse=True)
    avg_iv = round(sum(ivs_for_avg) / len(ivs_for_avg), 4) if ivs_for_avg else None

    group: Dict[str, Any] = {
        "symbol": sym,
        "spot": contracts[0]["spot"],
        "best_score": contracts[0]["score"],
        "avg_iv": avg_iv,
        "contract_count": len(contracts),
        "contracts": contracts,
    }
    return group, None


# ---------------------------------------------------------------------------
# Screener POST endpoint
# ---------------------------------------------------------------------------


@router.post("/research/screener")
def post_screener(request: Request, body: ScreenerRequest) -> Dict[str, Any]:
    """Option Screener — returns scored contracts grouped by symbol.

    V1: cash_secured_put only.  structure_type dispatch point is present for
    future extensions (covered_call, iron_condor, bull_put_spread, bear_call_spread).
    """
    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured", "groups": []}

    structure_type = (body.structure_type or "cash_secured_put").strip().lower()
    src = (body.source or "massive").strip().lower()
    if src not in ("massive", "ib"):
        src = "massive"

    if structure_type != "cash_secured_put":
        return {
            "ok": False,
            "error": (
                f"structure_type '{structure_type}' not yet implemented "
                "(V1 supports cash_secured_put only)."
            ),
            "groups": [],
        }

    symbols_clean = [s.strip().upper() for s in (body.symbols or []) if s.strip()]
    if not symbols_clean:
        return {"ok": False, "error": "symbols list is empty", "groups": []}

    today = date.today()
    groups: List[Dict[str, Any]] = []
    symbols_failed: List[str] = []
    warnings: Dict[str, str] = {}
    total_contracts = 0

    for sym in symbols_clean:
        # Structure-type dispatch (extend here for CC / spreads / IC)
        if structure_type == "cash_secured_put":
            group, warn = _scan_csp(sym, body, db, src, today, request)
        else:
            group, warn = None, f"structure_type '{structure_type}' not implemented"

        if group is None:
            if warn:
                warnings[sym] = warn
            symbols_failed.append(sym)
        else:
            if warn:
                warnings[sym] = warn
            groups.append(group)
            total_contracts += group["contract_count"]

    groups.sort(key=lambda g: g["best_score"], reverse=True)

    return {
        "ok": True,
        "structure_type": structure_type,
        "groups": groups,
        "total_contracts": total_contracts,
        "symbols_scanned": symbols_clean,
        "symbols_failed": symbols_failed,
        "warnings": warnings,
        "scan_ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
