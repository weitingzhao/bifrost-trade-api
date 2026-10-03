"""Research: Option Screener — scores put/call contracts by strategy structure type.

V1 implements Cash Secured Put (CSP) only.  The structure_type dispatch point is
present so that CC / Spread / Iron Condor can be added as additional branches.
"""

import math
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from bifrost_api.research.deps import db_config

router = APIRouter(tags=["research"])

RISK_FREE_RATE = 0.045
MARKET_TZ = ZoneInfo("America/New_York")


def _market_today(now: Optional[datetime] = None) -> date:
    """Today's date in New York, where expiries are dated.

    The container runs on UTC, so ``date.today()`` turns over at 20:00 ET (19:00
    in winter) and read every DTE one day short until midnight ET, overstating
    the annualised return.
    """
    return (now or datetime.now(timezone.utc)).astimezone(MARKET_TZ).date()


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
    spread_pct: Optional[float],
) -> float:
    """Weighted composite score 0–100.

    Weights: annualized 30%, prob_itm 25%, safety_margin 20%, iv_pct 15%, liquidity 10%.
    iv_pct is the name's IV30 percentile (``_read_iv_percentile``), the same for
    every contract of a name. An unmeasured IV percentile or spread scores
    neutral (0.5), neither best nor worst.
    """

    def clip(v: float, lo: float, hi: float) -> float:
        return max(lo, min(hi, v))

    a = clip(annualized / 1.50, 0.0, 1.0) if annualized is not None else 0.0
    p = clip(1.0 - prob_itm / 0.30, 0.0, 1.0)
    s = clip(safety_margin / 0.20, 0.0, 1.0)
    v = clip(iv_percentile / 100.0, 0.0, 1.0) if iv_percentile is not None else 0.5
    liq = clip(1.0 - spread_pct / 0.30, 0.0, 1.0) if spread_pct is not None else 0.5
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


def _fetch_error(e: Exception) -> str:
    """Short reason for a failed plugin call, e.g. ``HTTPError: HTTP Error 500: …``."""
    msg = str(e).strip()
    return f"{type(e).__name__}: {msg}" if msg else type(e).__name__


# A reading older than this describes another week's regime. Research writes one
# row a session after the close, so a Friday row is 4 days old on the Tuesday
# after a holiday Monday; a name the chain store stopped covering keeps
# answering its last row (one went 27 days in 2026-08/09).
IV_PERCENTILE_MAX_AGE_DAYS = 5


def _read_iv_percentile(sym: str, today: date) -> Tuple[Dict[str, Any], Optional[str]]:
    """The name's IV30 percentile over its last 252 sessions, as Research stores it.

    It ranks the name, not the strike: a contract's own IV against ATM history
    scores skew (every OTM put reads rich) and term structure, which safety
    margin and prob ITM already reward. It used to come from a stub that
    answered nothing, so every contract scored the neutral 0.5.

    Returns the reading and, when the percentile is not used, the reason.
    """
    from bifrost_api.research.analytics_reader import fetch_iv_percentile_latest

    reading: Dict[str, Any] = {
        "iv30": None,
        "iv_percentile": None,
        "iv_percentile_as_of": None,
        "iv_percentile_sessions": None,
    }
    source = "Research /analytics/options/iv-percentile"
    try:
        row = fetch_iv_percentile_latest(sym)
    except Exception as e:
        return reading, f"IV percentile read failed ({source}): {_fetch_error(e)}"
    if row is None:
        return reading, "IV percentile unmeasured: Research holds no IV30 for this name"

    iv30, pct = row.get("iv_current"), row.get("iv_percentile_1y")
    reading["iv30"] = round(float(iv30), 4) if iv30 is not None else None
    reading["iv_percentile_sessions"] = row.get("lookback_days")
    try:
        as_of = date.fromisoformat(str(row.get("trade_date"))[:10])
    except ValueError:
        return reading, f"IV percentile unmeasured: {source} answered no trade_date"
    reading["iv_percentile_as_of"] = as_of.isoformat()

    age = (today - as_of).days
    if age > IV_PERCENTILE_MAX_AGE_DAYS:
        return reading, f"IV percentile unmeasured: Research's newest IV30 reading is {as_of.isoformat()}, {age} days old"
    if pct is None:
        return reading, (
            f"IV percentile unmeasured: Research withholds it on "
            f"{reading['iv_percentile_sessions']} sessions of IV30 history"
        )
    reading["iv_percentile"] = round(float(pct), 1)
    return reading, None


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

    # A failed plugin call and an empty answer used to read the same ("No snapshot
    # data"), so an outage looked like missing data. The warning names which.
    try:
        all_exps_raw = get_option_expirations_from_contracts_db(db, sym, raise_errors=True)
    except Exception as e:
        return None, f"Expirations fetch failed (Market Data Plugin /options/expirations/yyyymmdd): {_fetch_error(e)}"
    if not all_exps_raw:
        return None, "No option contracts on file — run Market Data Plugin sync first"

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

    # Every key in the window: 21 strikes × 6+ expiries passes the default
    # 120-key cap, which used to drop the later expiries without a word.
    try:
        rows = get_option_snapshots_latest(db, all_keys, source=src, raise_errors=True, max_keys=None)
    except Exception as e:
        return None, f"Snapshot fetch failed (Market Data Plugin /options/chain/latest): {_fetch_error(e)}"
    if not rows:
        return None, "No snapshot data — run Market Data Plugin sync first"

    iv_reading, iv_note = _read_iv_percentile(sym, today)
    iv_pct = iv_reading["iv_percentile"]

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

        # Option premium. The Massive chain store keeps no NBBO under the Options
        # Starter entitlement, so a row usually has no bid/ask and the premium is
        # the session close; premium_basis says which one was used.
        bid_f = float(row["bid"]) if row.get("bid") is not None else None
        ask_f = float(row["ask"]) if row.get("ask") is not None else None
        mid_raw = row.get("mid")
        try:
            mid_f = float(mid_raw) if mid_raw is not None else None
        except (TypeError, ValueError):
            mid_f = None
        if mid_f is None and bid_f is not None and ask_f is not None:
            mid_f = (bid_f + ask_f) / 2.0
        premium_basis = "mid"
        if mid_f is None and row.get("day_close") is not None:
            try:
                mid_f = float(row["day_close"])
                premium_basis = "close"
            except (TypeError, ValueError):
                mid_f = None
        if mid_f is None or mid_f <= 0:
            continue

        # Spread pct (relative to mid). Without both sides there is no spread:
        # it is null, not 0, which scored as the tightest market there is.
        if bid_f is not None and ask_f is not None:
            spread_pct: Optional[float] = (ask_f - bid_f) / mid_f
        else:
            spread_pct = None

        if body.max_spread_pct is not None and spread_pct is not None and spread_pct > body.max_spread_pct:
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
            "premium_basis": premium_basis,
            "spread_pct": round(spread_pct, 4) if spread_pct is not None else None,
            "open_interest": int(oi_raw) if oi_raw is not None else None,
            "delta": round(float(delta_raw), 4) if delta_raw is not None else None,
            "iv_percentile": iv_pct,
            "long_strike": None,
            "snapshot_ts": row.get("snapshot_ts"),
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
        **iv_reading,
        "contracts": contracts,
    }
    notes: List[str] = []
    # A contract with no bid/ask cannot be held to max_spread_pct: it is kept,
    # and the name says the filter was not applied to it rather than passing it silently.
    unquoted = sum(1 for c in contracts if c["spread_pct"] is None)
    if body.max_spread_pct is not None and unquoted:
        notes.append(
            f"max_spread_pct not applied to {unquoted} of {len(contracts)} contracts: "
            "no bid/ask on file (the chain store keeps no NBBO), so no spread to test"
        )
    if iv_note:
        notes.append(iv_note)
    return group, "; ".join(notes) or None


# ---------------------------------------------------------------------------
# Screener POST endpoint
# ---------------------------------------------------------------------------


@router.post("/research/screener")
def post_screener(request: Request, body: ScreenerRequest) -> Dict[str, Any]:
    """Option Screener — returns scored contracts grouped by symbol.

    V1: cash_secured_put only.  structure_type dispatch point is present for
    future extensions (covered_call, iron_condor, bull_put_spread, bear_call_spread).
    """
    db = db_config(request)
    if not db:
        raise HTTPException(status_code=503, detail="PostgreSQL not configured")

    structure_type = (body.structure_type or "cash_secured_put").strip().lower()
    src = (body.source or "massive").strip().lower()
    if src not in ("massive", "ib"):
        src = "massive"

    if structure_type != "cash_secured_put":
        raise HTTPException(status_code=400, detail=f"structure_type '{structure_type}' not yet implemented "
                "(V1 supports cash_secured_put only).")

    symbols_clean = [s.strip().upper() for s in (body.symbols or []) if s.strip()]
    if not symbols_clean:
        raise HTTPException(status_code=400, detail="symbols list is empty")

    today = _market_today()
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
