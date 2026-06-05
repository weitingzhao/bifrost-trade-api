"""Tier 3a: Structure / Volatility diagnostics derived from OHLCV.

These are diagnostic metrics (not scored pass/fail for overall ranking).
They help identify volatility contractions, trend strength, and accumulation.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class StructureConfig:
    atr_period: int = 14
    bb_period: int = 20
    bb_std_mult: float = 2.0
    bb_squeeze_lookback: int = 126
    bb_squeeze_percentile: float = 0.20
    realized_vol_short: int = 30
    realized_vol_long: int = 90
    vol_contraction_threshold: float = 0.70
    obv_slope_period: int = 30
    adx_period: int = 14
    aroon_period: int = 14


def _true_range(high: float, low: float, prev_close: float) -> float:
    return max(high - low, abs(high - prev_close), abs(low - prev_close))


def compute_atr(
    highs: List[float], lows: List[float], closes: List[float], period: int = 14
) -> Optional[float]:
    """Wilder's Average True Range."""
    if len(closes) < period + 1 or len(highs) < period + 1 or len(lows) < period + 1:
        return None
    tr_values: List[float] = []
    for i in range(1, len(closes)):
        tr_values.append(_true_range(highs[i], lows[i], closes[i - 1]))
    if len(tr_values) < period:
        return None
    atr = sum(tr_values[:period]) / period
    for i in range(period, len(tr_values)):
        atr = (atr * (period - 1) + tr_values[i]) / period
    return atr


def compute_realized_vol(closes: List[float], period: int) -> Optional[float]:
    """Annualized realized volatility from log returns."""
    if len(closes) < period + 1:
        return None
    log_returns: List[float] = []
    for i in range(-period, 0):
        if closes[i - 1] <= 0 or closes[i] <= 0:
            continue
        log_returns.append(math.log(closes[i] / closes[i - 1]))
    if len(log_returns) < period * 0.8:
        return None
    mean = sum(log_returns) / len(log_returns)
    var = sum((r - mean) ** 2 for r in log_returns) / len(log_returns)
    return math.sqrt(var * 252)


def compute_bollinger_bandwidth(closes: List[float], period: int = 20, std_mult: float = 2.0) -> Optional[float]:
    """Bollinger Band bandwidth = (upper - lower) / middle."""
    if len(closes) < period:
        return None
    window = closes[-period:]
    sma = sum(window) / period
    if sma == 0:
        return None
    std = math.sqrt(sum((c - sma) ** 2 for c in window) / period)
    bandwidth = (2 * std_mult * std) / sma
    return bandwidth


def compute_bb_squeeze(closes: List[float], period: int = 20, std_mult: float = 2.0, lookback: int = 126, percentile: float = 0.20) -> Optional[bool]:
    """True if current BB bandwidth is in the lowest `percentile` of the last `lookback` days."""
    needed = lookback + period
    if len(closes) < needed:
        return None
    bandwidths: List[float] = []
    for end_idx in range(len(closes) - lookback, len(closes) + 1):
        window = closes[end_idx - period : end_idx]
        sma = sum(window) / period
        if sma == 0:
            continue
        std = math.sqrt(sum((c - sma) ** 2 for c in window) / period)
        bandwidths.append((2 * std_mult * std) / sma)
    if len(bandwidths) < 2:
        return None
    current = bandwidths[-1]
    threshold_idx = max(1, int(len(bandwidths) * percentile))
    sorted_bw = sorted(bandwidths)
    return current <= sorted_bw[threshold_idx]


def compute_obv_slope(closes: List[float], volumes: List[float], period: int = 30) -> Optional[float]:
    """Linear regression slope of OBV over `period` days, normalized by mean OBV."""
    if len(closes) < period + 1 or len(volumes) < period + 1:
        return None
    obv_series: List[float] = [0.0]
    for i in range(1, len(closes)):
        if closes[i] > closes[i - 1]:
            obv_series.append(obv_series[-1] + volumes[i])
        elif closes[i] < closes[i - 1]:
            obv_series.append(obv_series[-1] - volumes[i])
        else:
            obv_series.append(obv_series[-1])

    segment = obv_series[-period:]
    n = len(segment)
    x_mean = (n - 1) / 2.0
    y_mean = sum(segment) / n
    num = sum((i - x_mean) * (segment[i] - y_mean) for i in range(n))
    den = sum((i - x_mean) ** 2 for i in range(n))
    if den == 0:
        return None
    slope = num / den
    if y_mean == 0:
        return slope
    return slope / abs(y_mean)


def _wilder_smooth(values: List[float], period: int) -> List[float]:
    """Wilder's smoothing (same as EMA with alpha=1/period)."""
    if not values:
        return []
    result = [sum(values[:period]) / period] if len(values) >= period else [values[0]]
    start = period if len(values) >= period else 1
    for i in range(start, len(values)):
        result.append((result[-1] * (period - 1) + values[i]) / period)
    return result


def compute_adx(
    highs: List[float], lows: List[float], closes: List[float], period: int = 14
) -> Optional[float]:
    """Standard Wilder ADX."""
    min_len = 2 * period + 1
    if len(highs) < min_len or len(lows) < min_len or len(closes) < min_len:
        return None

    plus_dm: List[float] = []
    minus_dm: List[float] = []
    tr_list: List[float] = []
    for i in range(1, len(closes)):
        h_diff = highs[i] - highs[i - 1]
        l_diff = lows[i - 1] - lows[i]
        plus_dm.append(max(h_diff, 0.0) if h_diff > l_diff else 0.0)
        minus_dm.append(max(l_diff, 0.0) if l_diff > h_diff else 0.0)
        tr_list.append(_true_range(highs[i], lows[i], closes[i - 1]))

    sm_tr = _wilder_smooth(tr_list, period)
    sm_pdm = _wilder_smooth(plus_dm, period)
    sm_mdm = _wilder_smooth(minus_dm, period)

    dx_values: List[float] = []
    for i in range(len(sm_tr)):
        if sm_tr[i] == 0:
            continue
        pdi = 100.0 * sm_pdm[i] / sm_tr[i]
        mdi = 100.0 * sm_mdm[i] / sm_tr[i]
        denom = pdi + mdi
        if denom == 0:
            dx_values.append(0.0)
        else:
            dx_values.append(100.0 * abs(pdi - mdi) / denom)

    if len(dx_values) < period:
        return None
    adx_series = _wilder_smooth(dx_values, period)
    return adx_series[-1] if adx_series else None


def compute_aroon_oscillator(highs: List[float], lows: List[float], period: int = 14) -> Optional[float]:
    """Aroon Oscillator = Aroon Up - Aroon Down."""
    if len(highs) < period + 1 or len(lows) < period + 1:
        return None
    window_highs = highs[-(period + 1):]
    window_lows = lows[-(period + 1):]
    high_idx = window_highs.index(max(window_highs))
    low_idx = window_lows.index(min(window_lows))
    aroon_up = 100.0 * high_idx / period
    aroon_down = 100.0 * low_idx / period
    return aroon_up - aroon_down


def evaluate_structure(
    closes: List[float],
    highs: List[float],
    lows: List[float],
    volumes: List[float],
    *,
    cfg: Optional[StructureConfig] = None,
) -> Dict[str, Any]:
    """Evaluate all Tier-3a structure/volatility diagnostics for a single symbol.

    Returns dict with keys: diagnostics (list), metrics (dict).
    """
    conf = cfg or StructureConfig()
    diagnostics: List[Dict[str, Any]] = []
    metrics: Dict[str, Any] = {}

    # ATR% (14)
    atr = compute_atr(highs, lows, closes, conf.atr_period)
    atr_pct = (atr / closes[-1]) if (atr is not None and closes and closes[-1] > 0) else None
    metrics["atr_pct_14"] = round(atr_pct, 5) if atr_pct is not None else None

    # Realized volatility 30d / 90d
    rv30 = compute_realized_vol(closes, conf.realized_vol_short)
    rv90 = compute_realized_vol(closes, conf.realized_vol_long)
    metrics["realized_vol_30d"] = round(rv30, 4) if rv30 is not None else None
    metrics["realized_vol_90d"] = round(rv90, 4) if rv90 is not None else None

    vol_contraction = None
    if rv30 is not None and rv90 is not None and rv90 > 0:
        ratio = rv30 / rv90
        vol_contraction = ratio < conf.vol_contraction_threshold
        metrics["vol_contraction_ratio"] = round(ratio, 4)
    diagnostics.append({
        "id": "realized_vol_contraction",
        "active": bool(vol_contraction) if vol_contraction is not None else False,
        "value": metrics.get("vol_contraction_ratio"),
        "threshold": conf.vol_contraction_threshold,
    })

    # Bollinger Bandwidth + Squeeze
    bb_bw = compute_bollinger_bandwidth(closes, conf.bb_period, conf.bb_std_mult)
    metrics["bb_bandwidth_20"] = round(bb_bw, 5) if bb_bw is not None else None
    squeeze = compute_bb_squeeze(closes, conf.bb_period, conf.bb_std_mult, conf.bb_squeeze_lookback, conf.bb_squeeze_percentile)
    diagnostics.append({
        "id": "bb_squeeze",
        "active": bool(squeeze) if squeeze is not None else False,
        "value": metrics["bb_bandwidth_20"],
    })

    # OBV Slope (30d)
    obv_sl = compute_obv_slope(closes, volumes, conf.obv_slope_period)
    metrics["obv_slope_30d"] = round(obv_sl, 6) if obv_sl is not None else None
    diagnostics.append({
        "id": "obv_slope_30d_positive",
        "active": obv_sl is not None and obv_sl > 0,
        "value": metrics["obv_slope_30d"],
    })

    # ADX(14)
    adx = compute_adx(highs, lows, closes, conf.adx_period)
    metrics["adx_14"] = round(adx, 2) if adx is not None else None
    diagnostics.append({
        "id": "adx_14_ge_25",
        "active": adx is not None and adx >= 25.0,
        "value": metrics["adx_14"],
        "threshold": 25.0,
    })

    # Aroon Oscillator(14)
    aroon = compute_aroon_oscillator(highs, lows, conf.aroon_period)
    metrics["aroon_oscillator_14"] = round(aroon, 2) if aroon is not None else None
    diagnostics.append({
        "id": "aroon_oscillator_ge_50",
        "active": aroon is not None and aroon >= 50.0,
        "value": metrics["aroon_oscillator_14"],
        "threshold": 50.0,
    })

    return {
        "diagnostics": diagnostics,
        "metrics": metrics,
    }
