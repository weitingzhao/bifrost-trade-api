"""Unit tests for Tier 3a: Structure / Volatility diagnostics."""

import math

import pytest

from src.research.sepa.structure_indicators import (
    StructureConfig,
    compute_adx,
    compute_aroon_oscillator,
    compute_atr,
    compute_bb_squeeze,
    compute_bollinger_bandwidth,
    compute_obv_slope,
    compute_realized_vol,
    evaluate_structure,
)


class TestComputeATR:
    def test_insufficient_data(self):
        assert compute_atr([100.0] * 5, [99.0] * 5, [100.0] * 5, 14) is None

    def test_constant_price(self):
        n = 30
        highs = [100.0] * n
        lows = [100.0] * n
        closes = [100.0] * n
        atr = compute_atr(highs, lows, closes, 14)
        assert atr is not None
        assert atr == pytest.approx(0.0, abs=1e-10)

    def test_positive_atr(self):
        n = 30
        closes = [100.0 + i * 0.5 for i in range(n)]
        highs = [c + 2.0 for c in closes]
        lows = [c - 1.5 for c in closes]
        atr = compute_atr(highs, lows, closes, 14)
        assert atr is not None
        assert atr > 0


class TestRealizedVol:
    def test_insufficient(self):
        assert compute_realized_vol([100.0] * 10, 30) is None

    def test_constant_price_zero_vol(self):
        rv = compute_realized_vol([100.0] * 50, 30)
        assert rv is not None
        assert rv == pytest.approx(0.0, abs=1e-10)

    def test_trending(self):
        closes = [100.0 + i * 0.2 for i in range(50)]
        rv = compute_realized_vol(closes, 30)
        assert rv is not None
        assert rv > 0


class TestBollingerBandwidth:
    def test_insufficient(self):
        assert compute_bollinger_bandwidth([100.0] * 10, 20) is None

    def test_constant(self):
        bw = compute_bollinger_bandwidth([100.0] * 20, 20)
        assert bw is not None
        assert bw == pytest.approx(0.0, abs=1e-10)

    def test_volatile(self):
        closes = [100.0 + (i % 2) * 5.0 for i in range(30)]
        bw = compute_bollinger_bandwidth(closes, 20)
        assert bw is not None
        assert bw > 0


class TestBBSqueeze:
    def test_insufficient(self):
        assert compute_bb_squeeze([100.0] * 50, 20, 2.0, 126, 0.20) is None

    def test_detects_narrow_bandwidth(self):
        # Start with volatile, then tighten at the end
        closes = [100.0 + (i % 2) * 10.0 for i in range(160)]
        closes[-30:] = [100.0 + (i % 2) * 0.5 for i in range(30)]
        squeeze = compute_bb_squeeze(closes, 20, 2.0, 126, 0.20)
        assert squeeze is True


class TestOBVSlope:
    def test_insufficient(self):
        assert compute_obv_slope([100.0] * 5, [1000.0] * 5, 30) is None

    def test_rising_obv(self):
        n = 50
        closes = [100.0 + i * 0.3 for i in range(n)]
        volumes = [500_000.0] * n
        slope = compute_obv_slope(closes, volumes, 30)
        assert slope is not None
        assert slope > 0


class TestComputeADX:
    def test_insufficient(self):
        assert compute_adx([100.0] * 10, [99.0] * 10, [100.0] * 10, 14) is None

    def test_trending_market(self):
        n = 50
        closes = [100.0 + i * 1.0 for i in range(n)]
        highs = [c + 2.0 for c in closes]
        lows = [c - 0.5 for c in closes]
        adx = compute_adx(highs, lows, closes, 14)
        assert adx is not None
        assert adx > 0


class TestAroonOscillator:
    def test_insufficient(self):
        assert compute_aroon_oscillator([100.0] * 5, [99.0] * 5, 14) is None

    def test_strong_uptrend(self):
        n = 20
        highs = [100.0 + i * 2.0 for i in range(n)]
        lows = [100.0 + i * 2.0 - 1.0 for i in range(n)]
        aroon = compute_aroon_oscillator(highs, lows, 14)
        assert aroon is not None
        assert aroon > 0


class TestEvaluateStructure:
    def _make_data(self, n=200):
        closes = [100.0 + i * 0.2 for i in range(n)]
        highs = [c + 1.5 for c in closes]
        lows = [c - 1.0 for c in closes]
        volumes = [500_000.0] * n
        return closes, highs, lows, volumes

    def test_returns_correct_structure(self):
        closes, highs, lows, volumes = self._make_data()
        result = evaluate_structure(closes, highs, lows, volumes)
        assert "diagnostics" in result
        assert "metrics" in result
        assert isinstance(result["diagnostics"], list)
        assert len(result["diagnostics"]) == 5

    def test_metrics_populated(self):
        closes, highs, lows, volumes = self._make_data()
        result = evaluate_structure(closes, highs, lows, volumes)
        m = result["metrics"]
        assert "atr_pct_14" in m
        assert "realized_vol_30d" in m
        assert "realized_vol_90d" in m
        assert "bb_bandwidth_20" in m
        assert "adx_14" in m
        assert "aroon_oscillator_14" in m
        assert "obv_slope_30d" in m

    def test_empty_data(self):
        result = evaluate_structure([], [], [], [])
        assert result["metrics"]["atr_pct_14"] is None
        assert result["metrics"]["adx_14"] is None
