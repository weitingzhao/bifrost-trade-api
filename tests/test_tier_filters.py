"""Momentum / structure / sentiment tiers and momentum grades: Research reads the marts (TD-49 step 3).

The routes check the tier and the signal ids (400) before calling Research, pass
the effective arguments on, and return Research's body as it is — including its
additive ``signals`` vocabulary. Every symbol and number below is invented.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from bifrost_api.research import analytics_reader as ar
from bifrost_api.research.routers import data_readiness as dr


@pytest.fixture(autouse=True)
def no_sql(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """No tier or grade route reads Golden Source directly any more."""
    guard = MagicMock(side_effect=AssertionError("direct SQL must not run"))
    monkeypatch.setattr(ar, "get_conn", guard)
    return guard


def _research(body: Dict[str, Any]) -> Tuple[Any, List[Tuple[str, Dict[str, Any]]]]:
    calls: List[Tuple[str, Dict[str, Any]]] = []

    def fake(path: str, params: Any = None) -> Dict[str, Any]:
        calls.append((path, dict(params or {})))
        return body

    return patch.object(ar, "_proxy_get", side_effect=fake), calls


def test_the_sql_builders_are_gone() -> None:
    for name in ("_tier_table", "_tier_passed_sql", "_tier_filter", "_MOMENTUM_GRADES"):
        assert not hasattr(dr, name), name


def test_the_vocabulary_still_refuses_unknown_ids_before_research(no_sql: MagicMock) -> None:
    assert dr._TIER_MAX_SCORE == {"momentum": 10, "structure": 8, "sentiment": 6}
    assert "bb_squeeze" in dr._TIER_INDICATOR_IDS["structure"]
    patcher, calls = _research({})
    with patcher:
        # The old ids no mart carries are refused, not silently dropped.
        with pytest.raises(HTTPException) as refused:
            dr.get_tier_filter(None, tier="structure", include="vcp_contraction_3m")  # type: ignore[arg-type]
        with pytest.raises(HTTPException) as bad_tier:
            dr.get_tier_stats(None, tier="options")  # type: ignore[arg-type]
    assert refused.value.status_code == 400 and "vcp_contraction_3m" in refused.value.detail
    assert bad_tier.value.status_code == 400
    assert calls == []


def test_tier_filter_passes_the_effective_arguments(no_sql: MagicMock) -> None:
    body = {
        "ok": True, "tier": "structure", "include": ["bb_squeeze", "vol_contracting"], "match": "any",
        "min_score": 0, "max_score": 8, "count": 3, "truncated": True, "eval_date": "2031-01-02",
        "symbols": [{"symbol": "ZZA", "score": 6}], "limit": 1,
    }
    patcher, calls = _research(body)
    with patcher:
        out = dr.get_tier_filter(  # type: ignore[arg-type]
            None, tier="structure", include="bb_squeeze, vol_contracting", match="any", limit=1
        )
    assert calls == [("/analytics/sepa/tier-filter", {
        "tier": "structure", "include": "bb_squeeze,vol_contracting", "min_score": 0, "match": "any", "limit": 1,
    })]
    assert out == body


def test_momentum_filter_clamps_before_research(no_sql: MagicMock) -> None:
    patcher, calls = _research({"ok": True, "count": 0, "symbols": []})
    with patcher:
        dr.get_momentum_filter(None, min_score=99, match="bogus", limit=0)  # type: ignore[arg-type]
    assert calls == [("/analytics/sepa/tier-filter", {
        "tier": "momentum", "include": "", "min_score": 10, "match": "all", "limit": 1,
    })]


def test_an_empty_filter_does_not_call_research(no_sql: MagicMock) -> None:
    patcher, calls = _research({})
    with patcher:
        out = dr.get_tier_filter(None, tier="sentiment")  # type: ignore[arg-type]
    assert calls == [] and out["count"] == 0 and out["symbols"] == []


def test_tier_stats_returns_researchs_body_with_signals(no_sql: MagicMock) -> None:
    body = {
        "ok": True, "tier": "momentum", "eval_date": "2031-01-02", "universe_count": 5, "max_score": 10,
        "conditions": [{"id": c, "pass": 1} for c in dr._TIER_COLUMNS["momentum"]],
        "pass_count_distribution": {str(i): (2 if i == 0 else 3 if i == 7 else 0) for i in range(11)},
        "signals": list(dr._TIER_COLUMNS["momentum"]),
    }
    patcher, calls = _research(body)
    with patcher:
        out = dr.get_tier_stats(None, tier="momentum")  # type: ignore[arg-type]
    assert calls == [("/analytics/sepa/tier-stats", {"tier": "momentum"})]
    assert out == body


def test_momentum_grades_pass_through(no_sql: MagicMock) -> None:
    body = {
        "ok": True, "trade_date": "2031-01-02", "counts": {"A+": 0, "A": 2, "B": 0, "C": 5, "D": 0},
        "graded": 7, "grades": ["A", "A+"], "count": 2, "truncated": False, "symbols": ["ZZA", "ZZB"],
    }
    patcher, calls = _research(body)
    with patcher:
        out = dr.get_momentum_grades(None, grades="a,A+,bogus", limit=50)  # type: ignore[arg-type]
    assert calls == [("/research/momentum/grades", {"grades": "a,A+,bogus", "limit": 50})]
    assert out == body
