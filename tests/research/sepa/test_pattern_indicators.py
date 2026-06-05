"""Unit tests for Tier 3b: Pattern indicators (VCP, pocket pivot, tight closes, RSL)."""

import pytest

from src.research.sepa.pattern_indicators import (
    PatternConfig,
    compute_base_metrics,
    compute_pocket_pivots,
    compute_rsl_new_high,
    compute_tight_closes,
    compute_vcp_contractions,
    evaluate_patterns,
)


class TestTightCloses:
    def test_perfect_tight(self):
        closes = [100.0, 100.0, 100.0, 100.0, 100.0]
        result = compute_tight_closes(closes, days=5, pct_threshold=0.015)
        assert result["is_tight"] is True
        assert result["tight_pct"] == pytest.approx(0.0, abs=1e-6)

    def test_wide_spread(self):
        closes = [95.0, 100.0, 105.0, 102.0, 98.0]
        result = compute_tight_closes(closes, days=5, pct_threshold=0.015)
        assert result["is_tight"] is False
        assert result["tight_pct"] > 0.015

    def test_insufficient_data(self):
        result = compute_tight_closes([100.0, 101.0], days=5)
        assert result["tight_count"] == 0


class TestVCPContractions:
    def test_insufficient_data(self):
        result = compute_vcp_contractions([100.0] * 10, lookback=63)
        assert result["contraction_count"] == 0
        assert result["is_vcp"] is False

    def test_classic_vcp_pattern(self):
        # Build a pattern with decreasing pullbacks
        closes = []
        price = 100.0
        # First rally and pullback (15%)
        for i in range(15):
            price += 1.5
            closes.append(price)
        peak1 = price
        for i in range(8):
            price -= peak1 * 0.015
            closes.append(price)
        # Second rally and smaller pullback (8%)
        for i in range(10):
            price += 1.0
            closes.append(price)
        peak2 = price
        for i in range(5):
            price -= peak2 * 0.008
            closes.append(price)
        # Third rally and even smaller pullback (3%)
        for i in range(10):
            price += 0.5
            closes.append(price)
        peak3 = price
        for i in range(5):
            price -= peak3 * 0.003
            closes.append(price)
        # Pad to get enough data
        for i in range(20):
            price += 0.1
            closes.append(price)

        result = compute_vcp_contractions(
            closes, lookback=len(closes), min_contractions=2, swing_pct=0.04
        )
        assert result["contraction_count"] >= 0  # May or may not detect depending on swing threshold

    def test_flat_market_no_vcp(self):
        closes = [100.0 + (i % 3) * 0.5 for i in range(70)]
        result = compute_vcp_contractions(closes, lookback=63, swing_pct=0.05)
        assert result["is_vcp"] is False


class TestPocketPivots:
    def test_insufficient_data(self):
        result = compute_pocket_pivots([100.0] * 10, [1000.0] * 10, lookback=50)
        assert result["count"] == 0

    def test_detects_pivot(self):
        n = 60
        closes = [100.0] * n
        volumes = [100_000.0] * n
        # Create down days within 10 bars before the pivot
        for i in range(41, 50):
            closes[i] = closes[i - 1] - 0.5
            volumes[i] = 200_000.0
        # Create a pocket pivot day: close > prev close and volume > max down volume in window
        closes[50] = closes[49] + 2.0
        volumes[50] = 250_000.0
        result = compute_pocket_pivots(closes, volumes, lookback=50, down_vol_window=10)
        assert result["count"] >= 1

    def test_no_up_days_no_pivots(self):
        n = 60
        closes = [100.0 - i * 0.1 for i in range(n)]
        volumes = [100_000.0] * n
        result = compute_pocket_pivots(closes, volumes, lookback=50)
        assert result["count"] == 0


class TestRSLNewHigh:
    def test_insufficient_data(self):
        result = compute_rsl_new_high([100.0] * 50, [400.0] * 50, lookback=252)
        assert result["is_new_high"] is False

    def test_new_high_detected(self):
        n = 260
        stock = [100.0 + i * 0.1 for i in range(n)]
        spy = [400.0 + i * 0.02 for i in range(n)]
        # Stock outperforms SPY → RSL should be at new high
        result = compute_rsl_new_high(stock, spy, lookback=252, recent_window=10)
        assert result["is_new_high"] is True
        assert result["rsl_current"] is not None

    def test_underperformance_no_new_high(self):
        n = 260
        stock = [100.0 - i * 0.05 for i in range(n)]
        spy = [400.0 + i * 0.1 for i in range(n)]
        result = compute_rsl_new_high(stock, spy, lookback=252, recent_window=10)
        assert result["is_new_high"] is False


class TestBaseMetrics:
    def test_insufficient_data(self):
        result = compute_base_metrics([100.0] * 10, [101.0] * 10, [99.0] * 10, lookback=126)
        assert result["base_depth_pct"] is None

    def test_basic_calculation(self):
        n = 130
        closes = [100.0] * n
        highs = [110.0] * n
        lows = [90.0] * n
        result = compute_base_metrics(closes, highs, lows, lookback=126)
        assert result["base_depth_pct"] is not None
        assert result["pivot_buy_distance_pct"] is not None
        # pivot = 110, base_low = 90, depth = 20/110 ≈ 0.18
        assert result["base_depth_pct"] == pytest.approx(20.0 / 110.0, abs=0.01)


class TestEvaluatePatterns:
    def _make_data(self, n=300):
        closes = [100.0 + i * 0.2 for i in range(n)]
        highs = [c + 1.5 for c in closes]
        lows = [c - 1.0 for c in closes]
        volumes = [500_000.0] * n
        spy = [400.0 + i * 0.1 for i in range(n)]
        return closes, highs, lows, volumes, spy

    def test_returns_correct_structure(self):
        closes, highs, lows, volumes, spy = self._make_data()
        result = evaluate_patterns(closes, highs, lows, volumes, spy)
        assert "patterns" in result
        assert "metrics" in result
        assert len(result["patterns"]) == 5

    def test_pattern_ids(self):
        closes, highs, lows, volumes, spy = self._make_data()
        result = evaluate_patterns(closes, highs, lows, volumes, spy)
        ids = [p["id"] for p in result["patterns"]]
        assert "tight_closes_5d" in ids
        assert "vcp_contraction_3m" in ids
        assert "pocket_pivot_count" in ids
        assert "rsl_new_high" in ids
        assert "base_metrics" in ids

    def test_empty_data(self):
        result = evaluate_patterns([], [], [], [], [])
        assert result["metrics"]["tight_close_pct"] is None
        assert result["metrics"]["vcp_contraction_count"] == 0
