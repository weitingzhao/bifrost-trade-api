"""Technical Engine — Orchestrator for all 4 tiers of SEPA technical evaluation.

Combines:
  - Tier 1 (Core): phase1_engine + crs_engine (11 conditions, determines technical_pass)
  - Tier 2 (Momentum): momentum_indicators (10 scored indicators)
  - Tier 3 (Structure + Pattern): structure_indicators + pattern_indicators (diagnostics)
  - Tier 4 (Sentiment): short_indicators (short interest/volume signals)

The orchestrator merges all tiers into a single dict suitable for jsonb storage
in stock_readiness_daily.technical_eval while maintaining full backward compatibility
with the existing core-11 schema.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from bifrost_api.research.sepa.momentum_indicators import (
    MomentumConfig,
    evaluate_momentum,
)
from bifrost_api.research.sepa.pattern_indicators import (
    PatternConfig,
    evaluate_patterns,
)
from bifrost_api.research.sepa.short_indicators import (
    SentimentConfig,
    evaluate_sentiment,
)
from bifrost_api.research.sepa.structure_indicators import (
    StructureConfig,
    evaluate_structure,
)

TECHNICAL_RULE_VERSION = "sepa_technical_v2"


@dataclass
class TechnicalConfig:
    momentum: MomentumConfig = field(default_factory=MomentumConfig)
    structure: StructureConfig = field(default_factory=StructureConfig)
    pattern: PatternConfig = field(default_factory=PatternConfig)
    sentiment: SentimentConfig = field(default_factory=SentimentConfig)


def evaluate_symbol_all_tiers(
    symbol: str,
    core_result: Dict[str, Any],
    ohlcv_rows: List[Dict[str, Any]],
    spy_closes: List[float],
    short_interest_rows: List[Dict[str, Any]],
    short_volume_rows: List[Dict[str, Any]],
    *,
    cfg: Optional[TechnicalConfig] = None,
) -> Dict[str, Any]:
    """Evaluate all 4 tiers for a single symbol and produce merged technical_eval.

    Args:
        symbol: Ticker symbol.
        core_result: Output from phase1 + CRS merge (must contain conditions, metrics,
            technical_pass, pass_count, insufficient_data).
        ohlcv_rows: Ascending bar series with close/high/low/volume/bar_time.
        spy_closes: Ascending SPY daily closes aligned to same trading calendar.
        short_interest_rows: From stock_short_interest (settlement_date DESC).
        short_volume_rows: From stock_short_volume (trade_date DESC).
        cfg: Combined configuration for all tiers.

    Returns:
        Full technical_eval dict ready for jsonb upsert.
    """
    conf = cfg or TechnicalConfig()

    # Extract series from OHLCV rows
    closes: List[float] = []
    highs: List[float] = []
    lows: List[float] = []
    volumes: List[float] = []
    for r in ohlcv_rows:
        c = r.get("close")
        if c is None:
            continue
        closes.append(float(c))
        highs.append(float(r.get("high") or c))
        lows.append(float(r.get("low") or c))
        volumes.append(float(r.get("volume") or 0))

    # Core (Tier 1) — pass through from existing evaluation
    core_pass = bool(core_result.get("technical_pass", False))
    core_pass_count = int(core_result.get("pass_count", 0))
    core_fail_count = int(core_result.get("fail_count", 0))
    core_insufficient = bool(core_result.get("insufficient_data", False))
    core_conditions = core_result.get("conditions") or []
    core_metrics = core_result.get("metrics") or {}

    # Tier 2: Momentum
    momentum_result: Dict[str, Any] = {"score": 0, "max": 10, "indicators": []}
    if closes and not core_insufficient:
        try:
            momentum_result = evaluate_momentum(closes, volumes, spy_closes, cfg=conf.momentum)
        except Exception:
            pass

    # Tier 3a: Structure
    structure_result: Dict[str, Any] = {"diagnostics": [], "metrics": {}}
    if closes and not core_insufficient:
        try:
            structure_result = evaluate_structure(closes, highs, lows, volumes, cfg=conf.structure)
        except Exception:
            pass

    # Tier 3b: Pattern
    pattern_result: Dict[str, Any] = {"patterns": [], "metrics": {}}
    if closes and not core_insufficient:
        try:
            pattern_result = evaluate_patterns(closes, highs, lows, volumes, spy_closes, cfg=conf.pattern)
        except Exception:
            pass

    # Tier 4: Sentiment
    sentiment_result: Dict[str, Any] = {"short": {}, "indicators": []}
    try:
        sentiment_result = evaluate_sentiment(short_interest_rows, short_volume_rows, cfg=conf.sentiment)
    except Exception:
        pass

    # Assemble final technical_eval (backward compatible)
    technical_eval: Dict[str, Any] = {
        # Top-level fields preserved for backward compat
        "technical_pass": core_pass,
        "insufficient_data": core_insufficient,
        "pass_count": core_pass_count,
        "fail_count": core_fail_count,
        "conditions": core_conditions,
        "metrics": core_metrics,
        # New tiered structure
        "tiers": {
            "core": {
                "pass": core_pass,
                "pass_count": core_pass_count,
                "fail_count": core_fail_count,
            },
            "momentum": momentum_result,
            "structure": {
                "diagnostics": structure_result.get("diagnostics", []),
                "metrics": structure_result.get("metrics", {}),
                "patterns": pattern_result.get("patterns", []),
                "pattern_metrics": pattern_result.get("metrics", {}),
            },
            "sentiment": sentiment_result,
        },
        "rule_version": TECHNICAL_RULE_VERSION,
    }

    return technical_eval
