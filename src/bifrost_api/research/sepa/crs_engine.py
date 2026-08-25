from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional, Tuple

CRS_VERSION = "crs_v1_ret252_pct"


def _to_iso_day(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, date):
        return v.isoformat()
    return str(v)[:10]


def _ret_lookback(closes: List[float], lookback: int) -> Optional[float]:
    if len(closes) < (lookback + 1):
        return None
    c0 = closes[-1 - lookback]
    c1 = closes[-1]
    if c0 <= 0:
        return None
    return (c1 / c0) - 1.0


def _dense_percentiles(values: List[Tuple[str, float]]) -> Dict[str, float]:
    """Return 0..100 percentile by dense rank on ascending values."""
    if not values:
        return {}
    sorted_vals = sorted(values, key=lambda x: x[1])
    n = len(sorted_vals)
    if n == 1:
        return {sorted_vals[0][0]: 100.0}

    # Dense percentile with tie-aware average position.
    out: Dict[str, float] = {}
    i = 0
    while i < n:
        j = i
        while j + 1 < n and sorted_vals[j + 1][1] == sorted_vals[i][1]:
            j += 1
        avg_pos = (i + j) / 2.0  # 0-based
        pct = round((avg_pos / (n - 1)) * 100.0, 2)
        for k in range(i, j + 1):
            out[sorted_vals[k][0]] = pct
        i = j + 1
    return out


def compute_crs_scores(
    rows_by_symbol: Dict[str, List[Dict[str, Any]]],
    *,
    as_of_date: Optional[str] = None,
    lookback: int = 252,
    min_crs: Optional[float] = None,
) -> Dict[str, Any]:
    """Compute CRS score (0..100 percentile) for each symbol.

    rows_by_symbol: ascending bar series with fields ``close`` and ``bar_time``.
    """
    lb = max(20, min(int(lookback), 2000))
    computed: List[Dict[str, Any]] = []
    rank_inputs: List[Tuple[str, float]] = []
    warnings: Dict[str, str] = {}

    for symbol in sorted(rows_by_symbol.keys()):
        sym = str(symbol or "").strip().upper()
        rows = rows_by_symbol.get(symbol) or []
        closes: List[float] = []
        as_of_sym: Optional[str] = None
        for row in rows:
            c = row.get("close")
            if c is None:
                continue
            try:
                closes.append(float(c))
                as_of_sym = _to_iso_day(row.get("bar_time"))
            except (TypeError, ValueError):
                continue

        ret = _ret_lookback(closes, lb)
        if ret is None:
            computed.append(
                {
                    "symbol": sym,
                    "as_of_date": as_of_date or as_of_sym,
                    "ret252": None,
                    "crs_score": None,
                    "insufficient_data": True,
                    "pass": False,
                    "rows_used": len(closes),
                }
            )
            continue

        computed.append(
            {
                "symbol": sym,
                "as_of_date": as_of_date or as_of_sym,
                "ret252": round(ret, 8),
                "crs_score": None,
                "insufficient_data": False,
                "pass": False,
                "rows_used": len(closes),
            }
        )
        rank_inputs.append((sym, ret))

    pct_map = _dense_percentiles(rank_inputs)
    for item in computed:
        sym = item["symbol"]
        if item["insufficient_data"]:
            continue
        pct = pct_map.get(sym)
        item["crs_score"] = pct
        if min_crs is None:
            item["pass"] = True
        else:
            try:
                item["pass"] = pct is not None and pct >= float(min_crs)
            except (TypeError, ValueError):
                item["pass"] = False
                warnings[sym] = "invalid min_crs threshold"

    total = len(computed)
    insufficient = sum(1 for r in computed if r["insufficient_data"])
    passed = sum(1 for r in computed if r["pass"])
    failed = total - passed
    universe_size = len(rank_inputs)

    return {
        "results": computed,
        "summary": {
            "total": total,
            "passed": passed,
            "failed": failed,
            "insufficient_data": insufficient,
            "universe_size": universe_size,
        },
        "warnings": warnings,
        "crs_version": CRS_VERSION,
    }

