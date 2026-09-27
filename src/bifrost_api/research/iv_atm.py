"""Shared ATM IV helpers for Option Discovery's IV term structure (R-OD1).

The per-expiry volatility cone that also used them was retired on 2026-09-27:
the per-expiry ATM IV history lives in Research (``/analytics/options/atm-iv``).
"""

from __future__ import annotations

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
