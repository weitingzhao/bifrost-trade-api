"""Momentum / structure / sentiment filters: the ids are the mart's own columns, counts are whole matches."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, List

import pytest

from bifrost_api.research.routers import data_readiness as dr


class _Cur:
    def __init__(self, sink: List[Any], results: List[Any]) -> None:
        self.sink, self.results = sink, results

    def __enter__(self) -> "_Cur":
        return self

    def __exit__(self, *a: Any) -> None:
        return None

    def execute(self, sql: str, params: Any = None) -> None:
        self.sink.append((sql, params))

    def fetchall(self) -> Any:
        return self.results.pop(0)

    def fetchone(self) -> Any:
        return self.results.pop(0)


def _fake(monkeypatch: pytest.MonkeyPatch, results: List[Any]) -> List[Any]:
    sink: List[Any] = []

    class _Conn:
        def cursor(self, **_kw: Any) -> _Cur:
            return _Cur(sink, results)

    @contextmanager
    def _get_conn():
        yield _Conn()

    import bifrost_api.research.analytics_reader as ar

    monkeypatch.setattr(ar, "get_conn", _get_conn)
    return sink


def test_the_vocabulary_is_the_marts_columns() -> None:
    assert dr._TIER_MAX_SCORE == {"momentum": 10, "structure": 8, "sentiment": 6}
    assert "bb_squeeze" in dr._TIER_INDICATOR_IDS["structure"]
    # The old ids no mart carries are refused, not silently dropped.
    body = dr.get_tier_filter(None, tier="structure", include="vcp_contraction_3m")  # type: ignore[arg-type]
    assert body["ok"] is False and "vcp_contraction_3m" in body["error"]


def test_any_of_two_signals_counts_the_whole_match(monkeypatch: pytest.MonkeyPatch) -> None:
    # Invented symbols (fixtures are never copied from DEV).
    rows = [{"symbol": "ZZA", "score": 6, "eval_date": "2031-01-02", "total": 3}]
    sink = _fake(monkeypatch, [rows])
    body = dr.get_tier_filter(None, tier="structure", include="bb_squeeze,vol_contracting", match="any", limit=1)  # type: ignore[arg-type]
    sql, params = sink[0]
    assert "(bb_squeeze IS TRUE OR vol_contracting IS TRUE)" in sql
    assert params == [1]
    assert body["count"] == 3 and body["truncated"] is True and body["symbols"] == [{"symbol": "ZZA", "score": 6}]


def test_min_score_is_signals_passed(monkeypatch: pytest.MonkeyPatch) -> None:
    sink = _fake(monkeypatch, [[]])
    body = dr.get_momentum_filter(None, min_score=8)  # type: ignore[arg-type]
    sql, params = sink[0]
    assert "round(momentum_score * 10)::int >= %s" in sql and params == [8, 500]
    assert body["count"] == 0 and body["max_score"] == 10


def test_distribution_buckets_every_signal_count(monkeypatch: pytest.MonkeyPatch) -> None:
    head = {"d": "2031-01-02", "n": 5, **{c: 1 for c in dr._TIER_COLUMNS["momentum"]}}
    _fake(monkeypatch, [head, [{"s": 0, "c": 2}, {"s": 7, "c": 3}]])
    body = dr.get_momentum_distribution(None)  # type: ignore[arg-type]
    assert body["total"] == 5
    assert body["distribution"][0] == 2 and body["distribution"][7] == 3 and len(body["distribution"]) == 11


def test_momentum_grades_read_the_latest_session(monkeypatch: pytest.MonkeyPatch) -> None:
    sink = _fake(monkeypatch, [[{"d": "2031-01-02", "grade": "A", "n": 2}, {"d": "2031-01-02", "grade": "C", "n": 5}], [{"symbol": "ZZA"}, {"symbol": "ZZB"}]])
    body = dr.get_momentum_grades(None, grades="a,A+,bogus")  # type: ignore[arg-type]
    assert all("max(trade_date)" in sql for sql, _ in sink)
    assert body["counts"]["A"] == 2 and body["counts"]["C"] == 5 and body["graded"] == 7
    assert body["grades"] == ["A", "A+"] and body["count"] == 2 and body["symbols"] == ["ZZA", "ZZB"]
    assert body["trade_date"] == "2031-01-02"
