"""Integration smoke test for run_fundamentals_local_backfill with extension evaluators.

Mocks 5 symbols with varying data completeness.  Verifies:
  - fundamental_eval.groups structure is present and well-formed
  - SEPA core fields (fundamental_pass / pass_count / insufficient) are not corrupted
  - Extension group conditions appear in the flat conditions[] list with group tags
  - Missing data tables → group.insufficient without breaking other groups
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from src.research.sepa.fundamentals_engine import evaluate_fundamentals, FundamentalsConfig
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


# ── Fixture data for 5 mock symbols ──────────────────────────────────────────

def _q(fy, fp, eps, rev):
    return {"fiscal_year": fy, "fiscal_period": fp, "basic_earnings_per_share": eps, "revenues": rev}

def _a(fy, eps, rev):
    return {"fiscal_year": fy, "fiscal_period": "FY", "basic_earnings_per_share": eps, "revenues": rev}


SYMBOL_DATA: Dict[str, Dict[str, Any]] = {
    "FULL": {
        "qrows": [_q(2023, "Q1", 1.0, 100), _q(2023, "Q2", 1.1, 105), _q(2023, "Q3", 1.2, 110),
                  _q(2024, "Q1", 1.3, 130), _q(2024, "Q2", 1.5, 138), _q(2024, "Q3", 1.7, 150)],
        "arows": [_a(2021, 1.0, 100), _a(2022, 1.2, 120), _a(2023, 1.5, 145), _a(2024, 2.0, 180)],
        "inc_ext": [{"revenue": 1000, "gross_profit": 400, "operating_income": 150,
                     "consolidated_net_income_loss": 100, "interest_expense": -20,
                     "ebitda": 200, "cost_of_revenue": 600, "diluted_shares_outstanding": 1e8} for _ in range(4)],
        "bs": [{"total_current_assets": 5000, "total_current_liabilities": 2000, "inventories": 500,
                "cash_and_equivalents": 1000, "short_term_investments": 200, "debt_current": 300,
                "long_term_debt_and_capital_lease_obligations": 800, "total_assets": 12000,
                "total_equity": 5000, "receivables": 400} for _ in range(4)],
        "cf": [{"net_cash_from_operating_activities": 250, "purchase_of_property_plant_and_equipment": -80} for _ in range(4)],
        "ratios": {"debt_to_equity": 0.5, "return_on_equity": 0.20, "return_on_assets": 0.08,
                   "price_to_earnings": 25, "price_to_sales": 8, "price_to_book": 5,
                   "ev_to_ebitda": 15, "market_cap": 5e10},
        "si": [{"days_to_cover": 3.0, "short_interest": 5_000_000}],
        "sv": [{"short_volume_ratio": 0.20} for _ in range(5)],
    },
    "NO_BS": {
        "qrows": [_q(2023, "Q1", 1.0, 100), _q(2023, "Q2", 1.1, 105), _q(2023, "Q3", 1.2, 110),
                  _q(2024, "Q1", 1.3, 130), _q(2024, "Q2", 1.5, 138)],
        "arows": [_a(2021, 1.0, 100), _a(2022, 1.2, 120), _a(2023, 1.5, 145), _a(2024, 2.0, 180)],
        "inc_ext": [{"revenue": 1000, "gross_profit": 400, "operating_income": 150,
                     "consolidated_net_income_loss": 100, "interest_expense": -20,
                     "ebitda": 200, "cost_of_revenue": 600, "diluted_shares_outstanding": 1e8} for _ in range(4)],
        "bs": [],
        "cf": [{"net_cash_from_operating_activities": 250, "purchase_of_property_plant_and_equipment": -80} for _ in range(4)],
        "ratios": None,
        "si": [],
        "sv": [],
    },
    "SPARSE_INCOME": {
        "qrows": [_q(2024, "Q1", 1.0, 100), _q(2025, "Q1", 1.1, 110)],
        "arows": [_a(2024, 1.0, 100), _a(2025, 1.1, 110)],
        "inc_ext": [],
        "bs": [],
        "cf": [],
        "ratios": None,
        "si": [],
        "sv": [],
    },
    "RATIOS_ONLY": {
        "qrows": [_q(2023, "Q1", 1.0, 100), _q(2023, "Q2", 1.1, 105), _q(2023, "Q3", 1.2, 110),
                  _q(2024, "Q1", 1.3, 130), _q(2024, "Q2", 1.5, 138)],
        "arows": [_a(2021, 1.0, 100), _a(2022, 1.2, 120), _a(2023, 1.5, 145), _a(2024, 2.0, 180)],
        "inc_ext": [],
        "bs": [],
        "cf": [],
        "ratios": {"debt_to_equity": 0.5, "return_on_equity": 0.20, "return_on_assets": 0.08,
                   "price_to_earnings": 25, "price_to_sales": 8, "price_to_book": 5,
                   "ev_to_ebitda": 15, "market_cap": 5e10},
        "si": [],
        "sv": [],
    },
    "ALL_EMPTY": {
        "qrows": [],
        "arows": [],
        "inc_ext": [],
        "bs": [],
        "cf": [],
        "ratios": None,
        "si": [],
        "sv": [],
    },
}


def _simulate_backfill(sym: str) -> Dict[str, Any]:
    """Simulate the per-symbol evaluation pipeline from run_fundamentals_local_backfill."""
    d = SYMBOL_DATA[sym]
    qrows = d["qrows"]
    arows = d["arows"]

    MIN_Q, MIN_A = 5, 4
    if len(qrows) >= MIN_Q and len(arows) >= MIN_A:
        base = evaluate_fundamentals(qrows, arows, cfg=FundamentalsConfig())
    else:
        base = {
            "fundamental_pass": False,
            "insufficient_data": True,
            "not_comparable": False,
            "conditions": [],
            "pass_count": 0,
            "fail_count": 0,
            "metrics": {},
            "issues": ["no_local_income_data"],
        }

    ext_cfg = FundamentalsExtConfig()
    inc_ext = d["inc_ext"]
    bs_rows = d["bs"]
    cf_rows = d["cf"]
    ratios_row = d["ratios"]
    si_rows = d["si"]
    sv_rows = d["sv"]

    diluted_shares = None
    if inc_ext:
        for row in reversed(inc_ext):
            ds = row.get("diluted_shares_outstanding")
            if ds is not None and ds > 0:
                diluted_shares = float(ds)
                break

    ext_groups = []
    ext_groups.append(evaluate_quality_group(inc_ext, cf_rows, cfg=ext_cfg))
    ext_groups.append(evaluate_balance_group(bs_rows, ratios_row, inc_ext, cfg=ext_cfg))
    ext_groups.append(evaluate_cashflow_group(inc_ext, cf_rows, ratios_row, cfg=ext_cfg))
    ext_groups.append(evaluate_valuation_group(ratios_row, cfg=ext_cfg))
    ext_groups.append(evaluate_profitability_group(ratios_row, cfg=ext_cfg))
    ext_groups.append(evaluate_efficiency_group(inc_ext, bs_rows, cfg=ext_cfg))
    ext_groups.append(evaluate_sentiment_group(si_rows, sv_rows, diluted_shares, cfg=ext_cfg))

    result = merge_extension_into_eval(base, ext_groups)
    result["symbol"] = sym
    return result


# ── Tests ────────────────────────────────────────────────────────────────────

class TestBackfillExtIntegration:
    @pytest.fixture(autouse=True)
    def run_all_symbols(self):
        self.results = {sym: _simulate_backfill(sym) for sym in SYMBOL_DATA}

    def test_groups_present_for_all(self):
        for sym, r in self.results.items():
            assert "groups" in r, f"{sym} missing groups"
            assert "sepa_core" in r["groups"]
            for gk in ("quality", "balance", "cashflow", "valuation",
                        "profitability", "efficiency", "sentiment"):
                assert gk in r["groups"], f"{sym} missing group {gk}"

    def test_sepa_core_not_corrupted(self):
        full = self.results["FULL"]
        assert full["fundamental_pass"] is True
        assert full["pass_count"] == 8
        assert full["groups"]["sepa_core"]["total"] == 8
        assert full["groups"]["sepa_core"]["pass_count"] == 8

    def test_sparse_income_sepa_insufficient(self):
        sp = self.results["SPARSE_INCOME"]
        assert sp["insufficient_data"] is True
        assert sp["fundamental_pass"] is False

    def test_no_bs_balance_insufficient(self):
        nbs = self.results["NO_BS"]
        assert nbs["groups"]["balance"]["insufficient"] is True

    def test_all_empty_all_insufficient(self):
        ae = self.results["ALL_EMPTY"]
        assert ae["fundamental_pass"] is False
        assert ae["insufficient_data"] is True
        for gk in ("quality", "balance", "cashflow", "valuation",
                    "profitability", "efficiency", "sentiment"):
            assert ae["groups"][gk]["insufficient"] is True, f"{gk} should be insufficient"

    def test_extension_version_present(self):
        for sym, r in self.results.items():
            assert r.get("extension_version") == "ext_v1"

    def test_flat_conditions_have_group_tag(self):
        full = self.results["FULL"]
        for c in full["conditions"]:
            assert "group" in c, f"condition {c['id']} missing group tag"

    def test_full_symbol_has_33_conditions(self):
        full = self.results["FULL"]
        assert len(full["conditions"]) == 33

    def test_ratios_only_valuation_passes(self):
        ro = self.results["RATIOS_ONLY"]
        assert ro["groups"]["valuation"]["insufficient"] is False
        assert ro["groups"]["valuation"]["pass_count"] == 4

    def test_groups_summary_counts_consistent(self):
        for sym, r in self.results.items():
            for gk, gs in r["groups"].items():
                group_conds = [c for c in r["conditions"] if c.get("group") == gk]
                assert len(group_conds) == gs["total"], f"{sym}/{gk}: total mismatch"
                pc = sum(1 for c in group_conds if c["pass"])
                assert pc == gs["pass_count"], f"{sym}/{gk}: pass_count mismatch"

    def test_result_json_serializable(self):
        for sym, r in self.results.items():
            try:
                json.dumps(r)
            except (TypeError, ValueError) as e:
                pytest.fail(f"{sym} result not JSON-serializable: {e}")
