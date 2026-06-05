"""Tier 4: Sentiment indicators from short interest + short volume data.

Reads from stock_short_interest (bi-weekly settlement) and stock_short_volume (daily).
Handles staleness gracefully when data is not current.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional


@dataclass
class SentimentConfig:
    days_to_cover_threshold: float = 5.0
    short_volume_ratio_threshold: float = 0.30
    short_volume_avg_period: int = 20
    short_volume_trend_period: int = 20
    max_staleness_days: int = 30


def _parse_date(val: Any) -> Optional[date]:
    if val is None:
        return None
    if isinstance(val, date):
        return val
    try:
        return date.fromisoformat(str(val)[:10])
    except (ValueError, TypeError):
        return None


def evaluate_sentiment(
    short_interest_rows: List[Dict[str, Any]],
    short_volume_rows: List[Dict[str, Any]],
    *,
    as_of: Optional[date] = None,
    cfg: Optional[SentimentConfig] = None,
) -> Dict[str, Any]:
    """Evaluate Tier-4 sentiment indicators from short data.

    Args:
        short_interest_rows: Rows ordered by settlement_date DESC, keys:
            settlement_date, short_interest, avg_daily_volume, days_to_cover.
        short_volume_rows: Rows ordered by trade_date DESC, keys:
            trade_date, short_volume, short_volume_ratio, total_volume.
        as_of: Reference date for staleness calculation.
        cfg: Tuning parameters.

    Returns:
        Dict with keys: short (metrics dict), indicators (list).
    """
    conf = cfg or SentimentConfig()
    today = as_of or date.today()
    indicators: List[Dict[str, Any]] = []
    short_metrics: Dict[str, Any] = {}

    # --- Short Interest (bi-weekly) ---
    si_sorted = sorted(
        [r for r in short_interest_rows if r.get("settlement_date")],
        key=lambda r: str(r["settlement_date"]),
        reverse=True,
    )

    days_to_cover: Optional[float] = None
    si_pct_change_2w: Optional[float] = None
    si_staleness: Optional[int] = None

    if si_sorted:
        latest = si_sorted[0]
        days_to_cover = float(latest["days_to_cover"]) if latest.get("days_to_cover") is not None else None
        settle_date = _parse_date(latest.get("settlement_date"))
        if settle_date:
            si_staleness = (today - settle_date).days

        if len(si_sorted) >= 2 and latest.get("short_interest") is not None:
            prev = si_sorted[1]
            prev_si = prev.get("short_interest")
            curr_si = latest["short_interest"]
            if prev_si and float(prev_si) > 0:
                si_pct_change_2w = (float(curr_si) - float(prev_si)) / float(prev_si)

    short_metrics["days_to_cover"] = round(days_to_cover, 2) if days_to_cover is not None else None
    short_metrics["si_pct_change_2w"] = round(si_pct_change_2w, 4) if si_pct_change_2w is not None else None
    short_metrics["si_staleness_days"] = si_staleness

    indicators.append({
        "id": "days_to_cover_ge_5",
        "pass": days_to_cover is not None and days_to_cover >= conf.days_to_cover_threshold,
        "actual": short_metrics["days_to_cover"],
        "threshold": conf.days_to_cover_threshold,
        "reason": "Days to cover suggests potential short squeeze pressure",
    })

    # --- Short Volume (daily) ---
    sv_sorted = sorted(
        [r for r in short_volume_rows if r.get("trade_date") and r.get("short_volume_ratio") is not None],
        key=lambda r: str(r["trade_date"]),
        reverse=True,
    )

    sv_ratio_avg_4w: Optional[float] = None
    sv_ratio_trend_falling: Optional[bool] = None
    sv_staleness: Optional[int] = None

    if sv_sorted:
        sv_date = _parse_date(sv_sorted[0].get("trade_date"))
        if sv_date:
            sv_staleness = (today - sv_date).days

        recent_ratios = [
            float(r["short_volume_ratio"])
            for r in sv_sorted[:conf.short_volume_avg_period]
            if r.get("short_volume_ratio") is not None
        ]
        if recent_ratios:
            sv_ratio_avg_4w = sum(recent_ratios) / len(recent_ratios)

        if len(sv_sorted) >= 2 * conf.short_volume_trend_period:
            first_half = sv_sorted[:conf.short_volume_trend_period]
            second_half = sv_sorted[conf.short_volume_trend_period : 2 * conf.short_volume_trend_period]
            avg_recent = sum(float(r["short_volume_ratio"]) for r in first_half) / len(first_half)
            avg_prior = sum(float(r["short_volume_ratio"]) for r in second_half) / len(second_half)
            sv_ratio_trend_falling = avg_recent < avg_prior

    short_metrics["sv_ratio_avg_4w"] = round(sv_ratio_avg_4w, 4) if sv_ratio_avg_4w is not None else None
    short_metrics["sv_ratio_trend_falling"] = sv_ratio_trend_falling
    short_metrics["sv_staleness_days"] = sv_staleness

    indicators.append({
        "id": "short_volume_ratio_le_30pct_recent",
        "pass": sv_ratio_avg_4w is not None and sv_ratio_avg_4w < conf.short_volume_ratio_threshold,
        "actual": short_metrics["sv_ratio_avg_4w"],
        "threshold": conf.short_volume_ratio_threshold,
        "reason": "Low short volume ratio signals bullish positioning",
    })

    indicators.append({
        "id": "short_volume_ratio_trend_4w_falling",
        "pass": sv_ratio_trend_falling is True,
        "actual": sv_ratio_trend_falling,
        "threshold": None,
        "reason": "Declining short volume ratio (squeeze early signal)",
    })

    staleness_days = max(
        si_staleness if si_staleness is not None else 0,
        sv_staleness if sv_staleness is not None else 0,
    )
    short_metrics["staleness_days"] = staleness_days

    return {
        "short": short_metrics,
        "indicators": indicators,
    }
