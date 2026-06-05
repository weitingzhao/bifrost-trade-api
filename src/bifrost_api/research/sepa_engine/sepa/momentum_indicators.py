"""Tier 2: Momentum indicators derived from OHLCV + SPY benchmark.

Each indicator returns a boolean pass/fail and a numeric actual value.
All 10 indicators contribute equally to momentum_score ∈ [0,10].
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class MomentumConfig:
    rsi_period: int = 14
    rsi_lower: float = 40.0
    rsi_upper: float = 80.0
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    macd_hist_rising_days: int = 5
    roc_3m_days: int = 63
    roc_6m_days: int = 126
    roc_12m_days: int = 252
    rs_4w_days: int = 20
    rs_13w_days: int = 63
    rs_26w_days: int = 126
    sma200_slope_days: int = 20
    up_down_vol_period: int = 50
    volume_ratio_threshold: float = 1.25
    avg_volume_period: int = 50


def _wilder_rsi(closes: List[float], period: int = 14) -> Optional[float]:
    """Compute Wilder's RSI using exponential smoothing."""
    if len(closes) < period + 1:
        return None
    gains: List[float] = []
    losses: List[float] = []
    for i in range(1, len(closes)):
        delta = closes[i] - closes[i - 1]
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def _ema(values: List[float], period: int) -> List[float]:
    """Compute exponential moving average series."""
    if not values or period < 1:
        return []
    k = 2.0 / (period + 1)
    result = [values[0]]
    for i in range(1, len(values)):
        result.append(values[i] * k + result[-1] * (1.0 - k))
    return result


def _macd_histogram(closes: List[float], fast: int, slow: int, signal: int) -> Optional[List[float]]:
    """Return MACD histogram series (len = len(closes))."""
    if len(closes) < slow + signal:
        return None
    ema_fast = _ema(closes, fast)
    ema_slow = _ema(closes, slow)
    macd_line = [f - s for f, s in zip(ema_fast, ema_slow)]
    signal_line = _ema(macd_line, signal)
    hist = [m - s for m, s in zip(macd_line, signal_line)]
    return hist


def _roc(closes: List[float], days: int) -> Optional[float]:
    """Rate of change: (close_now / close_N_days_ago) - 1."""
    if len(closes) <= days:
        return None
    base = closes[-(days + 1)]
    if base <= 0:
        return None
    return (closes[-1] / base) - 1.0


def _relative_strength_excess(
    stock_closes: List[float], spy_closes: List[float], days: int
) -> Optional[float]:
    """Excess return of stock vs SPY over `days` trading days."""
    if len(stock_closes) <= days or len(spy_closes) <= days:
        return None
    s_base = stock_closes[-(days + 1)]
    b_base = spy_closes[-(days + 1)]
    if s_base <= 0 or b_base <= 0:
        return None
    stock_ret = (stock_closes[-1] / s_base) - 1.0
    spy_ret = (spy_closes[-1] / b_base) - 1.0
    return stock_ret - spy_ret


def _sma200_slope(closes: List[float], slope_days: int = 20) -> Optional[float]:
    """Annualized slope of 200-day SMA over recent `slope_days` days.

    Returns fractional change per day of the SMA200 over the window.
    """
    if len(closes) < 200 + slope_days:
        return None
    sma_now = sum(closes[-200:]) / 200.0
    sma_prev = sum(closes[-(200 + slope_days) : -slope_days]) / 200.0
    if sma_prev == 0:
        return None
    return (sma_now - sma_prev) / sma_prev


def _up_down_volume_ratio(
    closes: List[float], volumes: List[float], period: int
) -> Optional[float]:
    """Ratio of volume on up-days vs down-days over `period`."""
    if len(closes) < period + 1 or len(volumes) < period + 1:
        return None
    up_vol = 0.0
    down_vol = 0.0
    for i in range(-period, 0):
        delta = closes[i] - closes[i - 1]
        v = volumes[i] if volumes[i] else 0.0
        if delta > 0:
            up_vol += v
        elif delta < 0:
            down_vol += v
    if down_vol == 0:
        return None if up_vol == 0 else float("inf")
    return up_vol / down_vol


def evaluate_momentum(
    closes: List[float],
    volumes: List[float],
    spy_closes: List[float],
    *,
    cfg: Optional[MomentumConfig] = None,
) -> Dict[str, Any]:
    """Evaluate all Tier-2 momentum indicators for a single symbol.

    Args:
        closes: ascending daily close prices (at least 252+ entries ideal).
        volumes: matching daily volumes.
        spy_closes: ascending daily SPY closes aligned to same date grid.
        cfg: tuning parameters.

    Returns:
        Dict with keys: score, max, indicators (list of condition dicts).
    """
    conf = cfg or MomentumConfig()
    indicators: List[Dict[str, Any]] = []

    # 1. RSI(14) in band
    rsi_val = _wilder_rsi(closes, conf.rsi_period)
    rsi_pass = rsi_val is not None and conf.rsi_lower <= rsi_val <= conf.rsi_upper
    indicators.append({
        "id": "rsi_14_in_band",
        "pass": bool(rsi_pass),
        "actual": round(rsi_val, 2) if rsi_val is not None else None,
        "threshold": [conf.rsi_lower, conf.rsi_upper],
        "reason": f"RSI({conf.rsi_period}) within healthy trend band",
    })

    # 2. MACD histogram positive and rising
    hist = _macd_histogram(closes, conf.macd_fast, conf.macd_slow, conf.macd_signal)
    macd_pass = False
    macd_actual: Optional[float] = None
    if hist and len(hist) >= conf.macd_hist_rising_days + 1:
        macd_actual = round(hist[-1], 4)
        recent = hist[-conf.macd_hist_rising_days:]
        macd_pass = hist[-1] > 0 and all(recent[i] >= recent[i - 1] for i in range(1, len(recent)))
    indicators.append({
        "id": "macd_hist_positive",
        "pass": bool(macd_pass),
        "actual": macd_actual,
        "threshold": 0,
        "reason": f"MACD({conf.macd_fast},{conf.macd_slow},{conf.macd_signal}) histogram > 0 and rising {conf.macd_hist_rising_days}d",
    })

    # 3-5. ROC 3M / 6M / 12M
    for label, days in [
        ("roc_3m_positive", conf.roc_3m_days),
        ("roc_6m_positive", conf.roc_6m_days),
        ("roc_12m_positive", conf.roc_12m_days),
    ]:
        val = _roc(closes, days)
        indicators.append({
            "id": label,
            "pass": val is not None and val > 0,
            "actual": round(val, 4) if val is not None else None,
            "threshold": 0,
            "reason": f"Rate of change ({days}d) positive",
        })

    # 6-8. Multi-period relative strength vs SPY
    for label, days in [
        ("multi_period_rs_4w_positive", conf.rs_4w_days),
        ("multi_period_rs_13w_positive", conf.rs_13w_days),
        ("multi_period_rs_26w_positive", conf.rs_26w_days),
    ]:
        val = _relative_strength_excess(closes, spy_closes, days)
        indicators.append({
            "id": label,
            "pass": val is not None and val > 0,
            "actual": round(val, 4) if val is not None else None,
            "threshold": 0,
            "reason": f"Excess return vs SPY ({days}d) positive",
        })

    # 9. SMA200 slope positive
    slope = _sma200_slope(closes, conf.sma200_slope_days)
    indicators.append({
        "id": "slope_sma200_positive",
        "pass": slope is not None and slope > 0,
        "actual": round(slope, 6) if slope is not None else None,
        "threshold": 0,
        "reason": f"SMA200 slope over {conf.sma200_slope_days}d positive",
    })

    # 10. Up/Down volume ratio > 1
    ud_ratio = _up_down_volume_ratio(closes, volumes, conf.up_down_vol_period)
    ud_pass = ud_ratio is not None and ud_ratio > 1.0 and not math.isinf(ud_ratio)
    indicators.append({
        "id": "up_down_volume_50d_gt_1",
        "pass": bool(ud_pass),
        "actual": round(ud_ratio, 3) if (ud_ratio is not None and not math.isinf(ud_ratio)) else None,
        "threshold": 1.0,
        "reason": "Up-day volume exceeds down-day volume (50d)",
    })

    score = sum(1 for ind in indicators if ind["pass"])
    return {
        "score": score,
        "max": len(indicators),
        "indicators": indicators,
    }
