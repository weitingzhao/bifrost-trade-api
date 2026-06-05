"""Unit tests for the 25 extended fundamental conditions across 7 groups.

Covers boundary cases: NULL values, zero-base division, negative bases, missing
tables (empty rows → insufficient), and normal pass/fail behavior.
"""

import pytest

from src.research.sepa.fundamentals_ext_engine import (
    FundamentalsExtConfig,
    evaluate_balance_group,
    evaluate_cashflow_group,
    evaluate_efficiency_group,
    evaluate_profitability_group,
    evaluate_quality_group,
    evaluate_sentiment_group,
    evaluate_valuation_group,
    merge_extension_into_eval,
)


# ── helpers ──────────────────────────────────────────────────────────────────

def _inc(revenue=1000, gross_profit=400, operating_income=150,
         consolidated_net_income_loss=100, interest_expense=-20,
         ebitda=200, cost_of_revenue=600, diluted_shares_outstanding=100_000_000):
    return {
        "revenue": revenue,
        "gross_profit": gross_profit,
        "operating_income": operating_income,
        "consolidated_net_income_loss": consolidated_net_income_loss,
        "interest_expense": interest_expense,
        "ebitda": ebitda,
        "cost_of_revenue": cost_of_revenue,
        "diluted_shares_outstanding": diluted_shares_outstanding,
    }


def _cf(ocf=250, capex=-80):
    return {
        "net_cash_from_operating_activities": ocf,
        "purchase_of_property_plant_and_equipment": capex,
    }


def _bs(tca=5000, tcl=2000, inv=500, cash=1000, sti=200,
        dc=300, ltd=800, ta=12000, te=5000, recv=400):
    return {
        "total_current_assets": tca,
        "total_current_liabilities": tcl,
        "inventories": inv,
        "cash_and_equivalents": cash,
        "short_term_investments": sti,
        "debt_current": dc,
        "long_term_debt_and_capital_lease_obligations": ltd,
        "total_assets": ta,
        "total_equity": te,
        "receivables": recv,
    }


def _ratios(**kw):
    defaults = {
        "debt_to_equity": 0.5,
        "return_on_equity": 0.20,
        "return_on_assets": 0.08,
        "price_to_earnings": 25.0,
        "price_to_sales": 8.0,
        "price_to_book": 5.0,
        "ev_to_ebitda": 15.0,
        "market_cap": 50_000_000_000,
    }
    defaults.update(kw)
    return defaults


# ── Quality Group Tests ──────────────────────────────────────────────────────

class TestQualityGroup:
    def test_all_pass(self):
        inc = [_inc() for _ in range(4)]
        cf = [_cf() for _ in range(4)]
        r = evaluate_quality_group(inc, cf)
        assert r["name"] == "quality"
        assert r["total"] == 5
        assert r["pass_count"] == 5
        assert r["pass"] is True
        assert r["insufficient"] is False

    def test_empty_rows_insufficient(self):
        r = evaluate_quality_group([], [])
        assert r["insufficient"] is True
        assert r["pass"] is False
        assert all(not c["pass"] for c in r["conditions"])

    def test_zero_revenue_fails_margins(self):
        inc = [_inc(revenue=0) for _ in range(4)]
        cf = [_cf() for _ in range(4)]
        r = evaluate_quality_group(inc, cf)
        gm = next(c for c in r["conditions"] if c["id"] == "gross_margin_ge_30pct")
        assert gm["pass"] is False

    def test_zero_net_income_ocf(self):
        inc = [_inc(consolidated_net_income_loss=0) for _ in range(4)]
        cf = [_cf() for _ in range(4)]
        r = evaluate_quality_group(inc, cf)
        ocf = next(c for c in r["conditions"] if c["id"] == "ocf_to_ni_ge_0_7")
        assert ocf["pass"] is False

    def test_zero_interest_expense_passes(self):
        inc = [_inc(interest_expense=0) for _ in range(4)]
        cf = [_cf() for _ in range(4)]
        r = evaluate_quality_group(inc, cf)
        ic = next(c for c in r["conditions"] if c["id"] == "interest_coverage_ge_5x")
        assert ic["pass"] is True

    def test_below_threshold_fails(self):
        inc = [_inc(gross_profit=100) for _ in range(4)]
        cf = [_cf() for _ in range(4)]
        r = evaluate_quality_group(inc, cf)
        gm = next(c for c in r["conditions"] if c["id"] == "gross_margin_ge_30pct")
        assert gm["pass"] is False
        assert gm["actual"] is not None


# ── Balance Group Tests ──────────────────────────────────────────────────────

class TestBalanceGroup:
    def test_all_pass(self):
        bs = [_bs()]
        inc = [_inc() for _ in range(4)]
        r = evaluate_balance_group(bs, _ratios(), inc)
        assert r["name"] == "balance"
        assert r["pass_count"] == 4
        assert r["pass"] is True

    def test_empty_bs_insufficient(self):
        r = evaluate_balance_group([], None, [])
        assert r["insufficient"] is True
        assert all(not c["pass"] for c in r["conditions"])

    def test_high_debt_to_equity_fails(self):
        r = evaluate_balance_group([_bs()], _ratios(debt_to_equity=2.5), [_inc() for _ in range(4)])
        de = next(c for c in r["conditions"] if c["id"] == "debt_to_equity_le_1")
        assert de["pass"] is False

    def test_no_ratios_de_fails(self):
        r = evaluate_balance_group([_bs()], None, [_inc() for _ in range(4)])
        de = next(c for c in r["conditions"] if c["id"] == "debt_to_equity_le_1")
        assert de["pass"] is False

    def test_negative_ebitda_insufficient(self):
        inc = [_inc(ebitda=-10) for _ in range(4)]
        r = evaluate_balance_group([_bs()], _ratios(), inc)
        nd = next(c for c in r["conditions"] if c["id"] == "net_debt_to_ebitda_le_3")
        assert nd["pass"] is False

    def test_zero_current_liabilities(self):
        r = evaluate_balance_group([_bs(tcl=0)], _ratios(), [_inc() for _ in range(4)])
        cr = next(c for c in r["conditions"] if c["id"] == "current_ratio_ge_1_5")
        assert cr["pass"] is False


# ── Cash-flow Group Tests ────────────────────────────────────────────────────

class TestCashflowGroup:
    def test_all_pass(self):
        inc = [_inc() for _ in range(4)]
        cf = [_cf() for _ in range(4)]
        r = evaluate_cashflow_group(inc, cf, _ratios())
        assert r["name"] == "cashflow"
        assert r["pass_count"] >= 3

    def test_empty_cf_insufficient(self):
        r = evaluate_cashflow_group([], [], None)
        assert r["insufficient"] is True

    def test_negative_fcf_fails(self):
        cf = [_cf(ocf=10, capex=-50) for _ in range(4)]
        inc = [_inc() for _ in range(4)]
        r = evaluate_cashflow_group(inc, cf, _ratios())
        fcf = next(c for c in r["conditions"] if c["id"] == "fcf_positive")
        assert fcf["pass"] is False

    def test_no_market_cap_fcf_yield_fails(self):
        inc = [_inc() for _ in range(4)]
        cf = [_cf() for _ in range(4)]
        r = evaluate_cashflow_group(inc, cf, None)
        fy = next(c for c in r["conditions"] if c["id"] == "fcf_yield_ge_3pct")
        assert fy["pass"] is False


# ── Valuation Group Tests ────────────────────────────────────────────────────

class TestValuationGroup:
    def test_all_pass(self):
        r = evaluate_valuation_group(_ratios())
        assert r["name"] == "valuation"
        assert r["pass_count"] == 4
        assert r["pass"] is True

    def test_no_ratios_insufficient(self):
        r = evaluate_valuation_group(None)
        assert r["insufficient"] is True
        assert all(not c["pass"] for c in r["conditions"])

    def test_high_pe_fails(self):
        r = evaluate_valuation_group(_ratios(price_to_earnings=120.0))
        pe = next(c for c in r["conditions"] if c["id"] == "pe_le_60")
        assert pe["pass"] is False

    def test_negative_pe_fails(self):
        r = evaluate_valuation_group(_ratios(price_to_earnings=-5.0))
        pe = next(c for c in r["conditions"] if c["id"] == "pe_le_60")
        assert pe["pass"] is False


# ── Profitability Group Tests ────────────────────────────────────────────────

class TestProfitabilityGroup:
    def test_all_pass(self):
        r = evaluate_profitability_group(_ratios())
        assert r["pass_count"] == 2
        assert r["pass"] is True

    def test_no_ratios_insufficient(self):
        r = evaluate_profitability_group(None)
        assert r["insufficient"] is True

    def test_low_roe_fails(self):
        r = evaluate_profitability_group(_ratios(return_on_equity=0.05))
        roe = next(c for c in r["conditions"] if c["id"] == "roe_ge_15pct")
        assert roe["pass"] is False

    def test_null_roa(self):
        r = evaluate_profitability_group(_ratios(return_on_assets=None))
        roa = next(c for c in r["conditions"] if c["id"] == "roa_ge_5pct")
        assert roa["pass"] is False


# ── Efficiency Group Tests ───────────────────────────────────────────────────

class TestEfficiencyGroup:
    def test_all_pass(self):
        inc = [_inc() for _ in range(4)]
        bs = [_bs() for _ in range(4)]
        r = evaluate_efficiency_group(inc, bs)
        assert r["name"] == "efficiency"
        assert r["pass_count"] >= 2

    def test_empty_data_insufficient(self):
        r = evaluate_efficiency_group([], [])
        assert r["insufficient"] is True

    def test_zero_revenue_fails_turnover(self):
        inc = [_inc(revenue=0) for _ in range(4)]
        bs = [_bs() for _ in range(4)]
        r = evaluate_efficiency_group(inc, bs)
        at = next(c for c in r["conditions"] if c["id"] == "asset_turnover_ge_0_5")
        assert at["pass"] is False


# ── Sentiment Group Tests ────────────────────────────────────────────────────

class TestSentimentGroup:
    def test_all_pass(self):
        si = [{"days_to_cover": 3.0, "short_interest": 5_000_000}]
        sv = [{"short_volume_ratio": 0.20} for _ in range(5)]
        r = evaluate_sentiment_group(si, sv, 100_000_000)
        assert r["name"] == "sentiment"
        assert r["pass_count"] == 3
        assert r["pass"] is True

    def test_no_data_insufficient(self):
        r = evaluate_sentiment_group([], [], None)
        assert r["insufficient"] is True

    def test_high_days_to_cover_fails(self):
        si = [{"days_to_cover": 10.0, "short_interest": 5_000_000}]
        r = evaluate_sentiment_group(si, [], 100_000_000)
        dtc = next(c for c in r["conditions"] if c["id"] == "days_to_cover_le_5")
        assert dtc["pass"] is False

    def test_null_short_interest_no_float_pct(self):
        si = [{"days_to_cover": 2.0}]
        r = evaluate_sentiment_group(si, [], None)
        si_pct = next(c for c in r["conditions"] if c["id"] == "short_interest_pct_of_float_le_15pct")
        assert si_pct["pass"] is False


# ── Merge Tests ──────────────────────────────────────────────────────────────

class TestMerge:
    def _base_eval(self):
        return {
            "fundamental_pass": True,
            "insufficient_data": False,
            "conditions": [
                {"id": "eps_q2q_ge_25pct", "pass": True, "actual": 0.35, "threshold": 0.25, "reason": "test"},
            ],
            "pass_count": 1,
            "fail_count": 0,
            "metrics": {"latest_eps_q2q": 0.35},
        }

    def test_merge_adds_groups_and_extension_version(self):
        base = self._base_eval()
        quality = evaluate_quality_group([_inc() for _ in range(4)], [_cf() for _ in range(4)])
        merged = merge_extension_into_eval(base, [quality])
        assert "extension_version" in merged
        assert "groups" in merged
        assert "sepa_core" in merged["groups"]
        assert "quality" in merged["groups"]

    def test_merge_tags_original_conditions(self):
        base = self._base_eval()
        merged = merge_extension_into_eval(base, [])
        for c in merged["conditions"]:
            assert "group" in c

    def test_merge_preserves_fundamental_pass(self):
        base = self._base_eval()
        val = evaluate_valuation_group(_ratios(price_to_earnings=999))
        merged = merge_extension_into_eval(base, [val])
        assert merged["fundamental_pass"] is True
        assert merged["groups"]["valuation"]["pass"] is False

    def test_merge_preserves_metrics(self):
        base = self._base_eval()
        merged = merge_extension_into_eval(base, [])
        assert merged["metrics"]["latest_eps_q2q"] == 0.35

    def test_merge_flat_conditions_include_ext(self):
        base = self._base_eval()
        prof = evaluate_profitability_group(_ratios())
        merged = merge_extension_into_eval(base, [prof])
        all_ids = [c["id"] for c in merged["conditions"]]
        assert "roe_ge_15pct" in all_ids
        assert "eps_q2q_ge_25pct" in all_ids


# ── Config Override Tests ────────────────────────────────────────────────────

class TestConfigOverride:
    def test_custom_threshold_passes(self):
        cfg = FundamentalsExtConfig(gross_margin_threshold=0.05)
        inc = [_inc(gross_profit=100) for _ in range(4)]
        r = evaluate_quality_group(inc, [_cf() for _ in range(4)], cfg=cfg)
        gm = next(c for c in r["conditions"] if c["id"] == "gross_margin_ge_30pct")
        assert gm["pass"] is True
        assert gm["threshold"] == 0.05

    def test_custom_pe_threshold(self):
        cfg = FundamentalsExtConfig(pe_threshold=100.0)
        r = evaluate_valuation_group(_ratios(price_to_earnings=80.0), cfg=cfg)
        pe = next(c for c in r["conditions"] if c["id"] == "pe_le_60")
        assert pe["pass"] is True
