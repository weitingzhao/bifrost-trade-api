"""Shared ATM IV helpers for Option Discovery IV term / volatility cone (R-OD1)."""

from __future__ import annotations

import math
import statistics
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

OPTION_SNAPSHOT_STRIKES_AROUND_ATM = 10  # strikes to each side of ATM (total 2*N+1 or capped)


def strikes_around_spot(spot: float, count: int = OPTION_SNAPSHOT_STRIKES_AROUND_ATM) -> List[float]:
    """Compute strike list around spot for US equity options. Step $5 for spot < 200, else $10."""
    if not (spot and spot > 0):
        return []
    step = 5.0 if spot < 200 else 10.0
    center = round(spot / step) * step
    strikes_set: set = set()
    for i in range(-count, count + 1):
        s = center + i * step
        if s > 0:
            strikes_set.add(s)
    return sorted(strikes_set)


def parse_contract_key(ck: str) -> Tuple[Optional[float], Optional[str]]:
    parts = (ck or "").split("|")
    if len(parts) >= 5:
        try:
            return float(parts[3]), parts[4]
        except (TypeError, ValueError):
            return None, None
    return None, None


def build_exp_iv_map(
    rows: List[Dict[str, Any]],
    key_exp_map: Dict[str, str],
    last_price: float,
) -> Dict[str, List[Tuple[float, Optional[float], Optional[float], float]]]:
    """Group snapshot rows into per-expiry tuples (distance, iv_call, iv_put, strike)."""
    exp_iv: Dict[str, List[Tuple[float, Optional[float], Optional[float], float]]] = {}
    for row in rows:
        ck = row.get("contract_key") or ""
        exp = key_exp_map.get(ck)
        if not exp:
            continue
        strike, right = parse_contract_key(ck)
        if strike is None:
            continue
        iv_val = row.get("iv")
        if iv_val is None:
            continue
        try:
            iv_f = float(iv_val)
        except (TypeError, ValueError):
            continue
        if not (0 < iv_f < 10):
            continue
        entry = exp_iv.setdefault(exp, [])
        dist = abs(strike - last_price)
        if right == "C":
            entry.append((dist, iv_f, None, strike))
        else:
            entry.append((dist, None, iv_f, strike))
    return exp_iv


def atm_iv_from_expiry_items(
    items: List[Tuple[float, Optional[float], Optional[float], float]],
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """Return (atm_iv, iv_call, iv_put, best_strike) using nearest strikes with IV."""
    if not items:
        return None, None, None, None
    items_sorted = sorted(items, key=lambda x: x[0])
    best_call: Optional[float] = None
    best_put: Optional[float] = None
    best_strike: Optional[float] = None
    for dist, iv_c, iv_p, st in items_sorted:
        if iv_c is not None and best_call is None:
            best_call = iv_c
            if best_strike is None:
                best_strike = st
        if iv_p is not None and best_put is None:
            best_put = iv_p
            if best_strike is None:
                best_strike = st
        if best_call is not None and best_put is not None:
            break

    atm_iv: Optional[float] = None
    if best_call is not None and best_put is not None:
        atm_iv = (best_call + best_put) / 2
    elif best_call is not None:
        atm_iv = best_call
    elif best_put is not None:
        atm_iv = best_put
    return atm_iv, best_call, best_put, best_strike


def linear_percentiles(sorted_vals: List[float]) -> Dict[str, Optional[float]]:
    """p10, p50, p90, min, max from sorted list."""
    n = len(sorted_vals)
    if n == 0:
        return {"p10": None, "p50": None, "p90": None, "min": None, "max": None}

    def pct(p: float) -> float:
        if n == 1:
            return sorted_vals[0]
        i = (n - 1) * (p / 100.0)
        lo = int(math.floor(i))
        hi = int(math.ceil(i))
        if lo == hi:
            return sorted_vals[lo]
        return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (i - lo)

    return {
        "p10": pct(10.0),
        "p50": pct(50.0),
        "p90": pct(90.0),
        "min": sorted_vals[0],
        "max": sorted_vals[-1],
    }


def hist_iv_parametrics(daily_ivs: List[float]) -> Dict[str, Optional[float]]:
    """Sample mean, stdev, min/max, and mean±k·σ (lower clamped to 0) from daily ATM IVs."""
    n = len(daily_ivs)
    out: Dict[str, Optional[float]] = {
        "iv_hist_mean": None,
        "iv_hist_stdev": None,
        "iv_hist_min": None,
        "iv_hist_max": None,
        "iv_hist_plus_1sd": None,
        "iv_hist_minus_1sd": None,
        "iv_hist_plus_2sd": None,
        "iv_hist_minus_2sd": None,
    }
    if n == 0:
        return out
    lo = min(daily_ivs)
    hi = max(daily_ivs)
    mu = statistics.mean(daily_ivs)
    out["iv_hist_mean"] = float(mu)
    out["iv_hist_min"] = float(lo)
    out["iv_hist_max"] = float(hi)
    if n < 2:
        return out
    sig = float(statistics.stdev(daily_ivs))
    out["iv_hist_stdev"] = sig
    out["iv_hist_plus_1sd"] = float(mu + sig)
    out["iv_hist_minus_1sd"] = float(max(0.0, mu - sig))
    out["iv_hist_plus_2sd"] = float(mu + 2.0 * sig)
    out["iv_hist_minus_2sd"] = float(max(0.0, mu - 2.0 * sig))
    return out


def median_float(vals: List[float]) -> Optional[float]:
    if not vals:
        return None
    s = sorted(vals)
    m = len(s) // 2
    if len(s) % 2:
        return s[m]
    return (s[m - 1] + s[m]) / 2.0


def group_hist_rows_by_snap_day(hist_rows: List[Dict[str, Any]]) -> Dict[Any, List[Dict[str, Any]]]:
    by_day: Dict[Any, List[Dict[str, Any]]] = {}
    for row in hist_rows:
        sd = row.get("snap_day")
        if sd is None:
            continue
        by_day.setdefault(sd, []).append(row)
    return by_day


def trade_date_to_date(val: Any) -> Optional[date]:
    """Normalize snap_day / trade_date from DB or API to date."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    if isinstance(val, str):
        try:
            return datetime.strptime(val[:10], "%Y-%m-%d").date()
        except ValueError:
            return None
    return None


def eod_atm_report_rows_for_expiration(
    exp: str,
    key_exp_wide: Dict[str, str],
    by_day: Dict[Any, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    """One row per snap_day with ATM IV (for report_option_atm_iv_daily upsert)."""
    out: List[Dict[str, Any]] = []
    for _sd in sorted(by_day.keys()):
        day_rows = by_day[_sd]
        rows_exp = [
            r
            for r in day_rows
            if key_exp_wide.get(str(r.get("contract_key") or "")) == exp
        ]
        spots: List[float] = []
        for r in rows_exp:
            up = r.get("underlying_price")
            if up is not None:
                try:
                    v = float(up)
                except (TypeError, ValueError):
                    continue
                if v > 0:
                    spots.append(v)
        day_spot = median_float(spots)
        if day_spot is None or day_spot <= 0:
            continue
        exp_iv = build_exp_iv_map(rows_exp, key_exp_wide, day_spot)
        items = exp_iv.get(exp, [])
        atm_d, ivc, ivp, stk = atm_iv_from_expiry_items(items)
        if atm_d is None:
            continue
        td = trade_date_to_date(_sd)
        if td is None:
            continue
        out.append({
            "trade_date": td,
            "atm_iv": float(atm_d),
            "iv_call": ivc,
            "iv_put": ivp,
            "strike": stk,
            "underlying_price": day_spot,
        })
    return out


def daily_atm_ivs_for_expiration(
    exp: str,
    key_exp_wide: Dict[str, str],
    by_day: Dict[Any, List[Dict[str, Any]]],
) -> List[float]:
    """One ATM IV per calendar day (same semantics as iv-volatility-cone live path)."""
    daily_ivs: List[float] = []
    for _sd in sorted(by_day.keys()):
        day_rows = by_day[_sd]
        rows_exp = [
            r
            for r in day_rows
            if key_exp_wide.get(str(r.get("contract_key") or "")) == exp
        ]
        spots: List[float] = []
        for r in rows_exp:
            up = r.get("underlying_price")
            if up is not None:
                try:
                    v = float(up)
                except (TypeError, ValueError):
                    continue
                if v > 0:
                    spots.append(v)
        day_spot = median_float(spots)
        if day_spot is None or day_spot <= 0:
            continue
        exp_iv = build_exp_iv_map(rows_exp, key_exp_wide, day_spot)
        items = exp_iv.get(exp, [])
        atm_d, _, _, _ = atm_iv_from_expiry_items(items)
        if atm_d is not None:
            daily_ivs.append(float(atm_d))
    return daily_ivs


def build_cone_key_maps(
    sym: str,
    exp_list: List[str],
    last_price: float,
) -> Tuple[List[str], List[str], Dict[str, str], Dict[str, str]]:
    """Narrow + wide contract keys for latest vs historical cone bands."""
    from bifrost_worker.data.massive.vendor.client import contract_key_from_parts

    atm_narrow = strikes_around_spot(last_price, count=2)
    atm_wide = strikes_around_spot(last_price, count=10)
    key_exp_narrow: Dict[str, str] = {}
    narrow_keys: List[str] = []
    key_exp_wide: Dict[str, str] = {}
    wide_keys: List[str] = []
    for exp in exp_list:
        for st in atm_narrow:
            for r in ("C", "P"):
                k = contract_key_from_parts(sym, exp, float(st), r)
                narrow_keys.append(k)
                key_exp_narrow[k] = exp
        for st in atm_wide:
            for r in ("C", "P"):
                k2 = contract_key_from_parts(sym, exp, float(st), r)
                wide_keys.append(k2)
                key_exp_wide[k2] = exp
    return (
        list(dict.fromkeys(narrow_keys)),
        list(dict.fromkeys(wide_keys)),
        key_exp_narrow,
        key_exp_wide,
    )


def assemble_volatility_cone_points(
    exp_list: List[str],
    last_price: float,
    exp_iv_cur_all: Dict[str, List[Tuple[float, Optional[float], Optional[float], float]]],
    per_exp_daily_ivs: Dict[str, List[float]],
    min_samples_for_bands: int = 5,
) -> Tuple[List[Dict[str, Any]], List[Tuple[str, int]]]:
    """Build cone points and band warnings from precomputed daily_ivs per expiration."""
    today = date.today()
    points: List[Dict[str, Any]] = []
    band_warns: List[Tuple[str, int]] = []

    for exp in exp_list:
        daily_ivs = list(per_exp_daily_ivs.get(exp, []))
        try:
            exp_date = datetime.strptime(exp, "%Y%m%d").date()
        except ValueError:
            continue
        dte = (exp_date - today).days
        if dte < 0:
            continue

        items_cur = exp_iv_cur_all.get(exp, [])
        atm_cur, iv_call_cur, iv_put_cur, strike_cur = atm_iv_from_expiry_items(items_cur)

        sorted_ivs = sorted(daily_ivs)
        n = len(sorted_ivs)
        if n < min_samples_for_bands:
            pct = {"p10": None, "p50": None, "p90": None, "min": None, "max": None}
            band_warns.append((exp, n))
        else:
            pct = linear_percentiles(sorted_ivs)

        hist_p = hist_iv_parametrics(daily_ivs)

        points.append({
            "expiration": exp,
            "dte_days": dte,
            "atm_iv": atm_cur,
            "iv_call": iv_call_cur,
            "iv_put": iv_put_cur,
            "strike": strike_cur,
            "iv_p10": pct["p10"],
            "iv_p50": pct["p50"],
            "iv_p90": pct["p90"],
            "iv_min": pct["min"],
            "iv_max": pct["max"],
            "sample_days": n,
            **hist_p,
        })

    points.sort(key=lambda p: p["dte_days"])
    return points, band_warns


def rollup_daily_ivs_by_expiration(
    report_rows: List[Dict[str, Any]],
    exp_list: List[str],
    min_trade_date: Optional[date] = None,
) -> Dict[str, List[float]]:
    """
    Group report_option_atm_iv_daily rows into ordered daily_ivs lists per expiration.
    Caller filters rows to the desired lookback in SQL; min_trade_date optionally drops older rows.
    """
    by_exp: Dict[str, List[Tuple[date, float]]] = {e: [] for e in exp_list}
    exp_set = set(exp_list)
    for row in report_rows:
        exp = str(row.get("expiry") or "")
        if exp not in exp_set:
            continue
        td_p = trade_date_to_date(row.get("trade_date"))
        if td_p is None:
            continue
        if min_trade_date is not None and td_p < min_trade_date:
            continue
        av = row.get("atm_iv")
        if av is None:
            continue
        try:
            v = float(av)
        except (TypeError, ValueError):
            continue
        by_exp[exp].append((td_p, v))

    out: Dict[str, List[float]] = {}
    for exp in exp_list:
        pairs = sorted(by_exp.get(exp, []), key=lambda x: x[0])
        out[exp] = [p[1] for p in pairs]
    return out
