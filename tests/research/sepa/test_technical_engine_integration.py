"""Integration tests for technical_engine: verify JSONB shape + backward compat."""

import pytest

from src.research.sepa.technical_engine import (
    TechnicalConfig,
    evaluate_symbol_all_tiers,
)


def _make_core_result(pass_all=True):
    """Simulate a core-11 result dict as produced by phase1 + CRS merge."""
    conditions = [
        {"id": f"cond_{i}", "pass": pass_all, "actual": 1.0, "threshold": 0.5, "reason": f"Test {i}"}
        for i in range(11)
    ]
    pass_count = 11 if pass_all else 5
    return {
        "technical_pass": pass_all,
        "insufficient_data": False,
        "pass_count": pass_count,
        "fail_count": 11 - pass_count,
        "conditions": conditions,
        "metrics": {"close": 150.0, "sma50": 145.0, "sma200": 130.0},
    }


def _make_ohlcv_rows(n=300, base=100.0, trend=0.2):
    rows = []
    for i in range(n):
        c = base + i * trend
        rows.append({
            "bar_time": f"2025-01-{(i % 28) + 1:02d}",
            "open": c - 0.5,
            "high": c + 1.0,
            "low": c - 1.0,
            "close": c,
            "volume": 500_000.0 + i * 100,
        })
    return rows


def _make_spy_closes(n=300, base=400.0, trend=0.1):
    return [base + i * trend for i in range(n)]


class TestEvaluateSymbolAllTiers:
    def test_backward_compat_top_level_fields(self):
        core = _make_core_result(pass_all=True)
        ohlcv = _make_ohlcv_rows()
        spy = _make_spy_closes()
        result = evaluate_symbol_all_tiers("AAPL", core, ohlcv, spy, [], [])

        # Top-level fields must be preserved exactly
        assert result["technical_pass"] is True
        assert result["pass_count"] == 11
        assert result["fail_count"] == 0
        assert result["insufficient_data"] is False
        assert "conditions" in result
        assert len(result["conditions"]) == 11
        assert "metrics" in result
        assert result["metrics"]["close"] == 150.0

    def test_tiers_structure_present(self):
        core = _make_core_result()
        ohlcv = _make_ohlcv_rows()
        spy = _make_spy_closes()
        result = evaluate_symbol_all_tiers("TSLA", core, ohlcv, spy, [], [])

        assert "tiers" in result
        tiers = result["tiers"]
        assert "core" in tiers
        assert "momentum" in tiers
        assert "structure" in tiers
        assert "sentiment" in tiers

    def test_core_tier_mirrors_top_level(self):
        core = _make_core_result(pass_all=False)
        ohlcv = _make_ohlcv_rows()
        spy = _make_spy_closes()
        result = evaluate_symbol_all_tiers("GOOG", core, ohlcv, spy, [], [])

        assert result["tiers"]["core"]["pass"] == result["technical_pass"]
        assert result["tiers"]["core"]["pass_count"] == result["pass_count"]

    def test_momentum_tier_structure(self):
        core = _make_core_result()
        ohlcv = _make_ohlcv_rows()
        spy = _make_spy_closes()
        result = evaluate_symbol_all_tiers("NVDA", core, ohlcv, spy, [], [])

        momentum = result["tiers"]["momentum"]
        assert "score" in momentum
        assert "max" in momentum
        assert momentum["max"] == 10
        assert "indicators" in momentum
        assert isinstance(momentum["indicators"], list)
        assert len(momentum["indicators"]) == 10

    def test_structure_tier_structure(self):
        core = _make_core_result()
        ohlcv = _make_ohlcv_rows()
        spy = _make_spy_closes()
        result = evaluate_symbol_all_tiers("AMZN", core, ohlcv, spy, [], [])

        structure = result["tiers"]["structure"]
        assert "diagnostics" in structure
        assert "metrics" in structure
        assert "patterns" in structure
        assert "pattern_metrics" in structure

    def test_sentiment_with_short_data(self):
        core = _make_core_result()
        ohlcv = _make_ohlcv_rows()
        spy = _make_spy_closes()
        si_rows = [
            {"settlement_date": "2025-04-15", "short_interest": 5_000_000, "avg_daily_volume": 1_000_000, "days_to_cover": 5.0},
            {"settlement_date": "2025-04-01", "short_interest": 4_500_000, "avg_daily_volume": 1_000_000, "days_to_cover": 4.5},
        ]
        sv_rows = [
            {"trade_date": f"2025-04-{20-i}", "short_volume": 200_000, "short_volume_ratio": 0.25, "total_volume": 800_000}
            for i in range(40)
        ]
        result = evaluate_symbol_all_tiers("META", core, ohlcv, spy, si_rows, sv_rows)

        sentiment = result["tiers"]["sentiment"]
        assert "short" in sentiment
        assert "indicators" in sentiment
        assert sentiment["short"]["days_to_cover"] == 5.0

    def test_empty_ohlcv_still_produces_valid_output(self):
        core = _make_core_result(pass_all=False)
        core["insufficient_data"] = True
        result = evaluate_symbol_all_tiers("XYZ", core, [], [], [], [])

        assert result["technical_pass"] is False
        assert result["insufficient_data"] is True
        assert "tiers" in result
        assert result["tiers"]["momentum"]["score"] == 0

    def test_rule_version(self):
        core = _make_core_result()
        result = evaluate_symbol_all_tiers("TEST", core, _make_ohlcv_rows(), _make_spy_closes(), [], [])
        assert result["rule_version"] == "sepa_technical_v2"

    def test_pass_count_distribution_compat(self):
        """Verify pass_count stays in 0..11 range (backward compat for distribution charts)."""
        core = _make_core_result(pass_all=True)
        result = evaluate_symbol_all_tiers("A", core, _make_ohlcv_rows(), _make_spy_closes(), [], [])
        assert 0 <= result["pass_count"] <= 11

        core2 = _make_core_result(pass_all=False)
        core2["pass_count"] = 7
        core2["fail_count"] = 4
        result2 = evaluate_symbol_all_tiers("B", core2, _make_ohlcv_rows(), _make_spy_closes(), [], [])
        assert result2["pass_count"] == 7
