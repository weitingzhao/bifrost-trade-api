from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional


@dataclass
class Phase1Config:
    volume_threshold: float = 100_000.0
    close_to_low52_multiplier: float = 1.30
    close_to_high52_multiplier: float = 0.75
    sma200_rising_days: int = 20
    strict_sma200_rising: bool = False
    min_rows_for_full_check: int = 252


def _avg(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return float(sum(values) / len(values))


def _condition(
    cond_id: str,
    passed: bool,
    actual: Optional[float],
    threshold: Optional[float],
    reason: str,
) -> Dict[str, Any]:
    return {
        "id": cond_id,
        "pass": bool(passed),
        "actual": actual,
        "threshold": threshold,
        "reason": reason,
    }


def evaluate_symbol_phase1(
    symbol: str,
    rows: List[Dict[str, Any]],
    *,
    cfg: Optional[Phase1Config] = None,
) -> Dict[str, Any]:
    """Evaluate SEPA phase-1 technical conditions for one symbol.

    rows: ascending bar_time series with fields close/high/low/volume/bar_time.
    """
    conf = cfg or Phase1Config()
    sym = (symbol or "").strip().upper()
    if not sym:
        return {
            "symbol": "",
            "technical_pass": False,
            "insufficient_data": True,
            "error": "symbol is required",
            "conditions": [],
        }

    series = [r for r in rows if r and r.get("close") is not None]
    if len(series) < max(conf.min_rows_for_full_check, 200 + conf.sma200_rising_days):
        return {
            "symbol": sym,
            "technical_pass": False,
            "insufficient_data": True,
            "error": f"insufficient stock_day rows ({len(series)})",
            "conditions": [],
        }

    closes = [float(r["close"]) for r in series]
    highs = [float(r["high"]) for r in series if r.get("high") is not None]
    lows = [float(r["low"]) for r in series if r.get("low") is not None]
    vols = [float(r["volume"]) for r in series if r.get("volume") is not None]

    close_now = closes[-1]
    high52 = max(highs[-252:]) if len(highs) >= 252 else None
    low52 = min(lows[-252:]) if len(lows) >= 252 else None
    sma50 = _avg(closes[-50:])
    sma150 = _avg(closes[-150:])
    sma200 = _avg(closes[-200:])
    avg_volume_50 = _avg(vols[-50:])

    sma200_prev = _avg(closes[-(200 + conf.sma200_rising_days):-conf.sma200_rising_days])

    conditions: List[Dict[str, Any]] = []

    vol_pass = avg_volume_50 is not None and avg_volume_50 > conf.volume_threshold
    conditions.append(
        _condition(
            "avg_volume_50_gt_threshold",
            bool(vol_pass),
            avg_volume_50,
            conf.volume_threshold,
            "Average 50-day volume above liquidity threshold",
        )
    )

    close_vs_low_threshold = (low52 * conf.close_to_low52_multiplier) if low52 is not None else None
    close_vs_low_pass = close_vs_low_threshold is not None and close_now >= close_vs_low_threshold
    conditions.append(
        _condition(
            "close_ge_low52_x_1_3",
            bool(close_vs_low_pass),
            close_now,
            close_vs_low_threshold,
            "Close at least 30% above 52-week low",
        )
    )

    high52_floor = (high52 * conf.close_to_high52_multiplier) if high52 is not None else None
    close_vs_high_pass = high52_floor is not None and close_now >= high52_floor
    conditions.append(
        _condition(
            "close_ge_high52_x_0_75",
            bool(close_vs_high_pass),
            close_now,
            high52_floor,
            "Close within 25% of 52-week high",
        )
    )

    sma50_gt_150 = (sma50 is not None and sma150 is not None and sma50 > sma150)
    conditions.append(
        _condition(
            "sma50_gt_sma150",
            bool(sma50_gt_150),
            sma50,
            sma150,
            "Short-term trend above medium-term trend",
        )
    )

    sma50_gt_200 = (sma50 is not None and sma200 is not None and sma50 > sma200)
    conditions.append(
        _condition(
            "sma50_gt_sma200",
            bool(sma50_gt_200),
            sma50,
            sma200,
            "Short-term trend above long-term trend",
        )
    )

    sma150_gt_200 = (sma150 is not None and sma200 is not None and sma150 > sma200)
    conditions.append(
        _condition(
            "sma150_gt_sma200",
            bool(sma150_gt_200),
            sma150,
            sma200,
            "Medium-term trend above long-term trend",
        )
    )

    if conf.strict_sma200_rising:
        # Strict mode: slope must be positive and last 4 sampled points non-decreasing.
        window = closes[-(200 + conf.sma200_rising_days + 20):]
        checkpoints: List[float] = []
        for offset in (0, 5, 10, 15, 20):
            seg = window[-(200 + offset):-(offset) if offset > 0 else None]
            checkpoints.append(_avg(seg) or 0.0)
        rising = all(checkpoints[i] >= checkpoints[i + 1] for i in range(len(checkpoints) - 1))
    else:
        rising = sma200 is not None and sma200_prev is not None and sma200 > sma200_prev
    conditions.append(
        _condition(
            "sma200_rising_1m",
            bool(rising),
            sma200,
            sma200_prev,
            "Long-term moving average rising over one month",
        )
    )

    price_gt_sma50 = sma50 is not None and close_now > sma50
    conditions.append(
        _condition(
            "price_gt_sma50",
            bool(price_gt_sma50),
            close_now,
            sma50,
            "Price above 50-day moving average",
        )
    )

    price_gt_sma150 = sma150 is not None and close_now > sma150
    conditions.append(
        _condition(
            "price_gt_sma150",
            bool(price_gt_sma150),
            close_now,
            sma150,
            "Price above 150-day moving average",
        )
    )

    price_gt_sma200 = sma200 is not None and close_now > sma200
    conditions.append(
        _condition(
            "price_gt_sma200",
            bool(price_gt_sma200),
            close_now,
            sma200,
            "Price above 200-day moving average",
        )
    )

    pass_count = sum(1 for c in conditions if c["pass"])
    fail_count = len(conditions) - pass_count

    as_of: Optional[str] = None
    if series and series[-1].get("bar_time") is not None:
        bt = series[-1]["bar_time"]
        if isinstance(bt, date):
            as_of = bt.isoformat()
        else:
            as_of = str(bt)[:10]

    return {
        "symbol": sym,
        "as_of_date": as_of,
        "technical_pass": fail_count == 0,
        "insufficient_data": False,
        "metrics": {
            "close": close_now,
            "high52": high52,
            "low52": low52,
            "sma50": sma50,
            "sma150": sma150,
            "sma200": sma200,
            "sma200_prev": sma200_prev,
            "avg_volume_50": avg_volume_50,
            "rows_used": len(series),
        },
        "conditions": conditions,
        "pass_count": pass_count,
        "fail_count": fail_count,
    }


def evaluate_phase1_batch(
    rows_by_symbol: Dict[str, List[Dict[str, Any]]],
    *,
    cfg: Optional[Phase1Config] = None,
) -> Dict[str, Any]:
    conf = cfg or Phase1Config()
    results: List[Dict[str, Any]] = []
    warnings: Dict[str, str] = {}
    for symbol in sorted(rows_by_symbol.keys()):
        try:
            results.append(evaluate_symbol_phase1(symbol, rows_by_symbol[symbol], cfg=conf))
        except Exception as exc:
            warnings[symbol] = str(exc)
            results.append(
                {
                    "symbol": symbol,
                    "technical_pass": False,
                    "insufficient_data": True,
                    "error": "evaluation_failed",
                    "conditions": [],
                }
            )
    total = len(results)
    passed = sum(1 for r in results if r.get("technical_pass"))
    insufficient = sum(1 for r in results if r.get("insufficient_data"))
    failed = total - passed
    return {
        "results": results,
        "summary": {
            "total": total,
            "passed": passed,
            "failed": failed,
            "insufficient_data": insufficient,
        },
        "warnings": warnings,
        "rule_version": "sepa_phase1_v1",
    }

