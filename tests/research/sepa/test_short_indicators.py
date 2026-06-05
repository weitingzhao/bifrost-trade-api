"""Unit tests for Tier 4: Sentiment / Short indicators."""

from datetime import date

import pytest

from src.research.sepa.short_indicators import (
    SentimentConfig,
    evaluate_sentiment,
)


class TestEvaluateSentiment:
    def _make_si_rows(self, days_to_cover=5.0, si_current=5_000_000, si_previous=4_500_000):
        return [
            {"settlement_date": "2025-04-15", "short_interest": si_current, "avg_daily_volume": 1_000_000, "days_to_cover": days_to_cover},
            {"settlement_date": "2025-04-01", "short_interest": si_previous, "avg_daily_volume": 1_000_000, "days_to_cover": days_to_cover - 0.5},
        ]

    def _make_sv_rows(self, ratio=0.25, count=40):
        return [
            {"trade_date": f"2025-04-{20 - i if (20 - i) > 0 else 1}", "short_volume": 200_000, "short_volume_ratio": ratio, "total_volume": 800_000}
            for i in range(count)
        ]

    def test_returns_correct_structure(self):
        result = evaluate_sentiment(self._make_si_rows(), self._make_sv_rows(), as_of=date(2025, 4, 20))
        assert "short" in result
        assert "indicators" in result
        assert isinstance(result["indicators"], list)
        assert len(result["indicators"]) == 3

    def test_days_to_cover_ge_5_pass(self):
        result = evaluate_sentiment(self._make_si_rows(days_to_cover=6.0), self._make_sv_rows(), as_of=date(2025, 4, 20))
        dtc_ind = next(i for i in result["indicators"] if i["id"] == "days_to_cover_ge_5")
        assert dtc_ind["pass"] is True
        assert dtc_ind["actual"] == 6.0

    def test_days_to_cover_lt_5_fail(self):
        result = evaluate_sentiment(self._make_si_rows(days_to_cover=3.0), self._make_sv_rows(), as_of=date(2025, 4, 20))
        dtc_ind = next(i for i in result["indicators"] if i["id"] == "days_to_cover_ge_5")
        assert dtc_ind["pass"] is False

    def test_sv_ratio_low_pass(self):
        result = evaluate_sentiment(self._make_si_rows(), self._make_sv_rows(ratio=0.20), as_of=date(2025, 4, 20))
        sv_ind = next(i for i in result["indicators"] if i["id"] == "short_volume_ratio_le_30pct_recent")
        assert sv_ind["pass"] is True

    def test_sv_ratio_high_fail(self):
        result = evaluate_sentiment(self._make_si_rows(), self._make_sv_rows(ratio=0.45), as_of=date(2025, 4, 20))
        sv_ind = next(i for i in result["indicators"] if i["id"] == "short_volume_ratio_le_30pct_recent")
        assert sv_ind["pass"] is False

    def test_sv_trend_falling(self):
        # First 20 rows (recent) at 0.20, next 20 rows (prior) at 0.35
        sv_rows = []
        for i in range(20):
            sv_rows.append({"trade_date": f"2025-04-{20 - i if (20 - i) > 0 else 1}", "short_volume": 200_000, "short_volume_ratio": 0.20, "total_volume": 1_000_000})
        for i in range(20):
            sv_rows.append({"trade_date": f"2025-03-{28 - i if (28 - i) > 0 else 1}", "short_volume": 350_000, "short_volume_ratio": 0.35, "total_volume": 1_000_000})
        result = evaluate_sentiment(self._make_si_rows(), sv_rows, as_of=date(2025, 4, 20))
        trend_ind = next(i for i in result["indicators"] if i["id"] == "short_volume_ratio_trend_4w_falling")
        assert trend_ind["pass"] is True

    def test_staleness_calculation(self):
        result = evaluate_sentiment(self._make_si_rows(), self._make_sv_rows(), as_of=date(2025, 4, 25))
        assert result["short"]["si_staleness_days"] == 10  # 2025-04-25 - 2025-04-15

    def test_empty_data(self):
        result = evaluate_sentiment([], [], as_of=date(2025, 4, 20))
        assert result["short"]["days_to_cover"] is None
        assert result["short"]["sv_ratio_avg_4w"] is None
        dtc_ind = next(i for i in result["indicators"] if i["id"] == "days_to_cover_ge_5")
        assert dtc_ind["pass"] is False

    def test_missing_fields_handled(self):
        si_rows = [{"settlement_date": "2025-04-15", "short_interest": None, "avg_daily_volume": None, "days_to_cover": None}]
        sv_rows = [{"trade_date": "2025-04-15", "short_volume": None, "short_volume_ratio": None, "total_volume": None}]
        result = evaluate_sentiment(si_rows, sv_rows, as_of=date(2025, 4, 20))
        assert result["short"]["days_to_cover"] is None

    def test_si_pct_change_positive(self):
        result = evaluate_sentiment(
            self._make_si_rows(si_current=5_000_000, si_previous=4_000_000),
            self._make_sv_rows(),
            as_of=date(2025, 4, 20),
        )
        assert result["short"]["si_pct_change_2w"] == pytest.approx(0.25, abs=0.001)

    def test_custom_config(self):
        cfg = SentimentConfig(days_to_cover_threshold=3.0, short_volume_ratio_threshold=0.40)
        result = evaluate_sentiment(
            self._make_si_rows(days_to_cover=4.0),
            self._make_sv_rows(ratio=0.35),
            as_of=date(2025, 4, 20),
            cfg=cfg,
        )
        dtc = next(i for i in result["indicators"] if i["id"] == "days_to_cover_ge_5")
        assert dtc["pass"] is True  # 4 >= 3
        svr = next(i for i in result["indicators"] if i["id"] == "short_volume_ratio_le_30pct_recent")
        assert svr["pass"] is True  # 0.35 < 0.40
