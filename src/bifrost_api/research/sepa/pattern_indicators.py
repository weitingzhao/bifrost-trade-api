"""Tier 3b: Pattern indicators — SEPA / O'Neil structural pattern detection.

These detect VCP (Volatility Contraction Pattern), pocket pivots, tight closes,
RSL new highs, and base structure metrics.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class PatternConfig:
    tight_close_pct: float = 0.015
    tight_close_days: int = 5
    vcp_lookback_days: int = 63
    vcp_min_contractions: int = 2
    vcp_contraction_decay: float = 0.70
    pocket_pivot_lookback: int = 50
    pocket_pivot_down_vol_window: int = 10
    rsl_new_high_lookback: int = 252
    rsl_new_high_recent_window: int = 10
    base_detection_lookback: int = 126
    swing_pct_threshold: float = 0.05


def _find_swing_points(
    closes: List[float], pct_threshold: float = 0.05
) -> Tuple[List[Tuple[int, float]], List[Tuple[int, float]]]:
    """Find swing highs and lows using a percentage threshold method.

    Returns (swing_highs, swing_lows) as lists of (index, price).
    """
    if len(closes) < 3:
        return [], []

    swing_highs: List[Tuple[int, float]] = []
    swing_lows: List[Tuple[int, float]] = []

    direction = 0  # 1=up, -1=down
    last_high_idx = 0
    last_low_idx = 0
    last_high = closes[0]
    last_low = closes[0]

    for i in range(1, len(closes)):
        c = closes[i]
        if c > last_high:
            last_high = c
            last_high_idx = i
        if c < last_low:
            last_low = c
            last_low_idx = i

        if direction >= 0 and last_high > 0 and (last_high - c) / last_high >= pct_threshold:
            swing_highs.append((last_high_idx, last_high))
            last_low = c
            last_low_idx = i
            direction = -1
        elif direction <= 0 and last_low > 0 and (c - last_low) / last_low >= pct_threshold:
            swing_lows.append((last_low_idx, last_low))
            last_high = c
            last_high_idx = i
            direction = 1

    return swing_highs, swing_lows


def compute_tight_closes(closes: List[float], days: int = 5, pct_threshold: float = 0.015) -> Dict[str, Any]:
    """Count consecutive tight-close days at the end of series."""
    if len(closes) < days:
        return {"tight_count": 0, "tight_pct": None, "is_tight": False}

    tight_count = 0
    for offset in range(0, min(len(closes), 20)):
        window = closes[-(days + offset) : len(closes) - offset] if offset > 0 else closes[-days:]
        if len(window) < days:
            break
        avg = sum(window) / len(window)
        if avg == 0:
            break
        spread = (max(window) - min(window)) / avg
        if spread <= pct_threshold:
            tight_count += 1
        else:
            break

    recent_window = closes[-days:]
    avg = sum(recent_window) / len(recent_window) if recent_window else 0
    tight_pct = ((max(recent_window) - min(recent_window)) / avg) if avg > 0 else None

    return {
        "tight_count": tight_count,
        "tight_pct": round(tight_pct, 5) if tight_pct is not None else None,
        "is_tight": tight_pct is not None and tight_pct <= pct_threshold,
    }


def compute_vcp_contractions(
    closes: List[float],
    lookback: int = 63,
    min_contractions: int = 2,
    decay: float = 0.70,
    swing_pct: float = 0.05,
) -> Dict[str, Any]:
    """Detect VCP (Volatility Contraction Pattern) within lookback window.

    Counts successive pullback depths that decrease by at least `decay` ratio.
    """
    if len(closes) < lookback:
        return {"contraction_count": 0, "pullback_depths": [], "is_vcp": False}

    segment = closes[-lookback:]
    swing_highs, swing_lows = _find_swing_points(segment, swing_pct)

    pullback_depths: List[float] = []
    for i, (h_idx, h_val) in enumerate(swing_highs):
        next_lows = [(l_idx, l_val) for l_idx, l_val in swing_lows if l_idx > h_idx]
        if next_lows and h_val > 0:
            depth = (h_val - next_lows[0][1]) / h_val
            pullback_depths.append(round(depth, 4))

    contraction_count = 0
    if len(pullback_depths) >= 2:
        for i in range(1, len(pullback_depths)):
            if pullback_depths[i] < pullback_depths[i - 1] * (1.0 + (1.0 - decay)):
                contraction_count += 1
            else:
                break

    return {
        "contraction_count": contraction_count,
        "pullback_depths": pullback_depths[-6:],
        "is_vcp": contraction_count >= min_contractions,
    }


def compute_pocket_pivots(
    closes: List[float], volumes: List[float], lookback: int = 50, down_vol_window: int = 10
) -> Dict[str, Any]:
    """Count pocket pivot occurrences in the last `lookback` days."""
    if len(closes) < lookback + 1 or len(volumes) < lookback + 1:
        return {"count": 0, "last_idx": None}

    count = 0
    last_idx: Optional[int] = None

    for i in range(len(closes) - lookback, len(closes)):
        if i < down_vol_window + 1:
            continue
        if closes[i] <= closes[i - 1]:
            continue
        down_vols = [
            volumes[j]
            for j in range(i - down_vol_window, i)
            if closes[j] < closes[j - 1] and volumes[j] > 0
        ]
        if not down_vols:
            continue
        max_down_vol = max(down_vols)
        if volumes[i] > max_down_vol:
            count += 1
            last_idx = i - (len(closes) - lookback)

    return {"count": count, "last_idx": last_idx}


def compute_rsl_new_high(
    stock_closes: List[float], spy_closes: List[float], lookback: int = 252, recent_window: int = 10
) -> Dict[str, Any]:
    """Check if Relative Strength Line (stock/SPY) made a new high in recent window."""
    min_len = max(lookback, recent_window) + 1
    if len(stock_closes) < min_len or len(spy_closes) < min_len:
        return {"is_new_high": False, "rsl_current": None, "rsl_high_252": None}

    rsl: List[float] = []
    for sc, sp in zip(stock_closes[-lookback:], spy_closes[-lookback:]):
        if sp > 0:
            rsl.append(sc / sp)
        else:
            rsl.append(0.0)

    if not rsl:
        return {"is_new_high": False, "rsl_current": None, "rsl_high_252": None}

    rsl_high = max(rsl[:-recent_window]) if len(rsl) > recent_window else max(rsl)
    recent_high = max(rsl[-recent_window:])
    is_new_high = recent_high >= rsl_high and len(rsl) > recent_window

    return {
        "is_new_high": is_new_high,
        "rsl_current": round(rsl[-1], 6) if rsl else None,
        "rsl_high_252": round(max(rsl), 6) if rsl else None,
    }


def compute_base_metrics(
    closes: List[float], highs: List[float], lows: List[float], lookback: int = 126
) -> Dict[str, Any]:
    """Compute base depth and distance from pivot (breakout level)."""
    if len(closes) < lookback or len(highs) < lookback:
        return {"base_depth_pct": None, "pivot_buy_distance_pct": None}

    segment_highs = highs[-lookback:]
    segment_lows = lows[-lookback:]
    segment_closes = closes[-lookback:]

    pivot = max(segment_highs)
    base_low = min(segment_lows)

    base_depth_pct = (pivot - base_low) / pivot if pivot > 0 else None
    current = closes[-1]
    pivot_distance = (current - pivot) / pivot if pivot > 0 else None

    return {
        "base_depth_pct": round(base_depth_pct, 4) if base_depth_pct is not None else None,
        "pivot_buy_distance_pct": round(pivot_distance, 4) if pivot_distance is not None else None,
    }


def evaluate_patterns(
    closes: List[float],
    highs: List[float],
    lows: List[float],
    volumes: List[float],
    spy_closes: List[float],
    *,
    cfg: Optional[PatternConfig] = None,
) -> Dict[str, Any]:
    """Evaluate all Tier-3b pattern indicators for a single symbol.

    Returns dict with keys: patterns (list of pattern results), metrics (dict).
    """
    conf = cfg or PatternConfig()
    patterns: List[Dict[str, Any]] = []
    metrics: Dict[str, Any] = {}

    # Tight closes
    tc = compute_tight_closes(closes, conf.tight_close_days, conf.tight_close_pct)
    patterns.append({"id": "tight_closes_5d", **tc})
    metrics["tight_close_pct"] = tc["tight_pct"]
    metrics["tight_close_count"] = tc["tight_count"]

    # VCP
    vcp = compute_vcp_contractions(
        closes, conf.vcp_lookback_days, conf.vcp_min_contractions,
        conf.vcp_contraction_decay, conf.swing_pct_threshold,
    )
    patterns.append({"id": "vcp_contraction_3m", **vcp})
    metrics["vcp_contraction_count"] = vcp["contraction_count"]

    # Pocket Pivots
    pp = compute_pocket_pivots(closes, volumes, conf.pocket_pivot_lookback, conf.pocket_pivot_down_vol_window)
    patterns.append({"id": "pocket_pivot_count", **pp})
    metrics["pocket_pivot_count_50d"] = pp["count"]

    # RSL New High
    rsl = compute_rsl_new_high(closes, spy_closes, conf.rsl_new_high_lookback, conf.rsl_new_high_recent_window)
    patterns.append({"id": "rsl_new_high", **rsl})
    metrics["rsl_new_high"] = rsl["is_new_high"]

    # Base metrics
    bm = compute_base_metrics(closes, highs, lows, conf.base_detection_lookback)
    metrics["base_depth_pct"] = bm["base_depth_pct"]
    metrics["pivot_buy_distance_pct"] = bm["pivot_buy_distance_pct"]
    patterns.append({"id": "base_metrics", **bm})

    return {
        "patterns": patterns,
        "metrics": metrics,
    }
