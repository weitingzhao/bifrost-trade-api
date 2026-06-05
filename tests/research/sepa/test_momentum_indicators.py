"""Unit tests for Tier 2: Momentum indicators."""

import math

import pytest

from src.research.sepa.momentum_indicators import (
    MomentumConfig,
    _ema,
    _macd_histogram,
    _roc,
    _relative_strength_excess,
    _sma200_slope,
    _up_down_volume_ratio,
    _wilder_rsi,
    evaluate_momentum,
)


class TestWilderRSI:
    def test_insufficient_data(self):
        assert _wilder_rsi([100.0] * 10, 14) is None

    def test_all_gains(self):
        closes = [float(i) for i in range(1, 20)]
        rsi = _wilder_rsi(closes, 14)
        assert rsi is not None
        assert rsi == 100.0

    def test_all_losses(self):
        closes = [float(20 - i) for i in range(20)]
        rsi = _wilder_rsi(closes, 14)
        assert rsi is not None
        assert rsi == pytest.approx(0.0, abs=0.01)

    def test_known_range(self):
        # Alternating up/down should give RSI near 50
        closes = []
        v = 100.0
        for i in range(50):
            v += 1 if i % 2 == 0 else -0.9
            closes.append(v)
        rsi = _wilder_rsi(closes, 14)
        assert rsi is not None
        assert 40 < rsi < 70


class TestEMA:
    def test_single_value(self):
        assert _ema([10.0], 5) == [10.0]

    def test_length(self):
        vals = [float(i) for i in range(20)]
        result = _ema(vals, 5)
        assert len(result) == 20


class TestMACDHistogram:
    def test_insufficient_data(self):
        assert _macd_histogram([100.0] * 36, 12, 26, 9) is not None
        assert _macd_histogram([100.0] * 30, 12, 26, 9) is None

    def test_trending_up(self):
        closes = [100.0 + i * 0.5 for i in range(60)]
        hist = _macd_histogram(closes, 12, 26, 9)
        assert hist is not None
        assert hist[-1] > 0


class TestROC:
    def test_positive(self):
        closes = [100.0] * 64
        closes[-1] = 110.0
        assert _roc(closes, 63) == pytest.approx(0.1, abs=0.001)

    def test_insufficient(self):
        assert _roc([100.0] * 5, 63) is None


class TestRelativeStrength:
    def test_outperformance(self):
        stock = [100.0] * 21
        stock[-1] = 120.0
        spy = [100.0] * 21
        spy[-1] = 105.0
        excess = _relative_strength_excess(stock, spy, 20)
        assert excess is not None
        assert excess == pytest.approx(0.15, abs=0.001)

    def test_insufficient(self):
        assert _relative_strength_excess([100.0] * 5, [100.0] * 5, 20) is None


class TestSMA200Slope:
    def test_rising(self):
        # Steadily rising closes
        closes = [100.0 + i * 0.1 for i in range(250)]
        slope = _sma200_slope(closes, 20)
        assert slope is not None
        assert slope > 0

    def test_insufficient(self):
        assert _sma200_slope([100.0] * 100, 20) is None


class TestUpDownVolumeRatio:
    def test_all_up(self):
        closes = [float(100 + i) for i in range(52)]
        volumes = [1000.0] * 52
        ratio = _up_down_volume_ratio(closes, volumes, 50)
        # All days are up, down_vol = 0 -> inf
        assert ratio is None or math.isinf(ratio)

    def test_balanced(self):
        closes = []
        v = 100.0
        for i in range(52):
            v += 1 if i % 2 == 0 else -1
            closes.append(v)
        volumes = [1000.0] * 52
        ratio = _up_down_volume_ratio(closes, volumes, 50)
        assert ratio is not None
        assert ratio == pytest.approx(1.0, abs=0.1)


class TestEvaluateMomentum:
    def _make_trending_data(self, n=300):
        closes = [100.0 + i * 0.3 for i in range(n)]
        volumes = [500_000.0] * n
        spy_closes = [100.0 + i * 0.1 for i in range(n)]
        return closes, volumes, spy_closes

    def test_returns_correct_structure(self):
        closes, volumes, spy = self._make_trending_data()
        result = evaluate_momentum(closes, volumes, spy)
        assert "score" in result
        assert "max" in result
        assert result["max"] == 10
        assert "indicators" in result
        assert len(result["indicators"]) == 10

    def test_trending_up_high_score(self):
        closes, volumes, spy = self._make_trending_data()
        result = evaluate_momentum(closes, volumes, spy)
        # Strong uptrend should pass most momentum checks
        assert result["score"] >= 5

    def test_all_indicator_ids_unique(self):
        closes, volumes, spy = self._make_trending_data()
        result = evaluate_momentum(closes, volumes, spy)
        ids = [ind["id"] for ind in result["indicators"]]
        assert len(ids) == len(set(ids))

    def test_empty_series(self):
        result = evaluate_momentum([], [], [])
        assert result["score"] == 0
        assert len(result["indicators"]) == 10

    def test_custom_config(self):
        closes, volumes, spy = self._make_trending_data()
        cfg = MomentumConfig(rsi_lower=50, rsi_upper=60)
        result = evaluate_momentum(closes, volumes, spy, cfg=cfg)
        assert result["max"] == 10
