"""symbol-fundamental-raw-data serves the Stock Inspector's Source Data tables.

The route had lost its body and answered ``null`` (a 500 once FastAPI validated the
return type). The inspector wants ``quarterly`` / ``annual`` newest first, keyed
``{fiscal_year}-Q{fiscal_quarter}`` and ``{fiscal_year}``, plus ``metrics``.

The income-statement ``data`` jsonb comes in two vendor formats while the plugin
moves to v1: the legacy nested ``{value, unit, label}`` fields with no stored
fiscal quarter, and v1 flat fields with one. Every number below is invented.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from bifrost_api.research import analytics_reader, market_data_client
from bifrost_api.research.routers import data_readiness
from bifrost_api.research.sepa.financials_data import (
    REPORT_INCOME,
    derive_fiscal_quarter,
    is_legacy_financial_data,
)

SYM = "EXMP"


def _legacy_data(eps: float, revenue: float) -> dict[str, Any]:
    return {
        "revenues": {"value": revenue, "unit": "USD", "label": "Revenues", "order": 10},
        "basic_earnings_per_share": {"value": eps, "unit": "USD / shares", "label": "Basic EPS", "order": 20},
        "net_income_loss": {"value": revenue / 10, "unit": "USD", "label": "Net Income", "order": 30},
    }


def _v1_data(eps: float, revenue: float) -> dict[str, Any]:
    return {
        "revenue": revenue,
        "basic_earnings_per_share": eps,
        "diluted_earnings_per_share": eps - 0.01,
        "consolidated_net_income_loss": revenue / 10,
    }


def _row(period_date: str, period_type: str, fy: int, fq: int | None, data: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbol": SYM,
        "report_type": REPORT_INCOME,
        "period_date": period_date,
        "period_type": period_type,
        "fiscal_year": fy,
        "fiscal_quarter": fq,
        "data": data,
    }


# Legacy: fiscal year ends 31 March; quarters FY2024 Q1 .. FY2027 Q1, no fiscal_quarter.
_LEGACY_Q_ENDS = [
    (2024, "2023-06-30"), (2024, "2023-09-30"), (2024, "2023-12-31"), (2024, "2024-03-31"),
    (2025, "2024-06-30"), (2025, "2024-09-30"), (2025, "2024-12-31"), (2025, "2025-03-31"),
    (2026, "2025-06-30"), (2026, "2025-09-30"), (2026, "2025-12-31"), (2026, "2026-03-31"),
    (2027, "2026-06-30"),
]
LEGACY_QUARTERLY = [
    _row(pe, "quarterly", fy, None, _legacy_data(0.11 * (i + 1), 5_000_000.0 * (i + 1)))
    for i, (fy, pe) in enumerate(_LEGACY_Q_ENDS)
]
LEGACY_ANNUAL = [
    _row(f"{fy}-03-31", "annual", fy, None, _legacy_data(0.5 * (fy - 2020), 20_000_000.0 * (fy - 2020)))
    for fy in range(2021, 2027)
]

# v1: calendar fiscal year, fiscal_quarter stored; 2023 Q1 .. 2026 Q2.
_V1_Q = [(y, q) for y in (2023, 2024, 2025) for q in (1, 2, 3, 4)] + [(2026, 1), (2026, 2)]
_Q_END = {1: "03-31", 2: "06-30", 3: "09-30", 4: "12-31"}
V1_QUARTERLY = [
    _row(f"{y}-{_Q_END[q]}", "quarterly", y, q, _v1_data(0.2 * (i + 1), 7_000_000.0 * (i + 1)))
    for i, (y, q) in enumerate(_V1_Q)
]
V1_ANNUAL = [
    _row(f"{y}-12-31", "annual", y, None, _v1_data(0.9 * (y - 2018), 30_000_000.0 * (y - 2018)))
    for y in range(2019, 2026)
]

EVAL_ROW = {
    "symbol": SYM,
    "eval_date": "2026-09-29",
    "eps_q0": 1.5,
    "eps_g0": 0.3,
    "rev_fy_g1": None,
    "eps_q2q_ge_25pct": True,
    "pass_count": 1,
}


def _fake_plugin(quarterly: list[dict[str, Any]], annual: list[dict[str, Any]]):
    """Mimics /stocks/fundamentals/sepa/financials: latest ``limit`` rows, oldest first."""
    calls: list[tuple[str | None, int]] = []

    def fetch(symbols: list[str], report_type: str, *, period_type: str | None = None, limit: int = 20):
        calls.append((period_type, limit))
        assert symbols == [SYM]
        assert report_type == REPORT_INCOME
        rows = quarterly if period_type == "quarterly" else annual
        latest = sorted(rows, key=lambda r: r["period_date"])[-limit:]
        return {SYM: [dict(r) for r in latest]} if latest else {}

    return fetch, calls


@pytest.fixture
def eval_row(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(analytics_reader, "fetch_fundamental_eval_single", lambda sym: dict(EVAL_ROW))


def _serve(monkeypatch: pytest.MonkeyPatch, quarterly: list, annual: list) -> dict[str, Any]:
    fetch, _calls = _fake_plugin(quarterly, annual)
    monkeypatch.setattr(market_data_client, "fetch_sepa_financials", fetch)
    return data_readiness.get_symbol_fundamental_raw_data(None, symbol=f" {SYM.lower()} ")  # type: ignore[arg-type]


def test_formats_are_told_apart() -> None:
    assert is_legacy_financial_data(LEGACY_QUARTERLY[0]["data"])
    assert not is_legacy_financial_data(V1_QUARTERLY[0]["data"])
    assert not is_legacy_financial_data(None)


def test_legacy_rows_newest_first_with_derived_quarters(monkeypatch: pytest.MonkeyPatch, eval_row: None) -> None:
    out = _serve(monkeypatch, LEGACY_QUARTERLY, LEGACY_ANNUAL)
    assert out["ok"] is True
    assert out["symbol"] == SYM

    q = out["quarterly"]
    assert len(q) == 10
    assert [(r["fiscal_year"], r["fiscal_quarter"]) for r in q] == [
        (2027, 1),
        (2026, 4), (2026, 3), (2026, 2), (2026, 1),
        (2025, 4), (2025, 3), (2025, 2), (2025, 1),
        (2024, 4),
    ]
    newest = LEGACY_QUARTERLY[-1]["data"]
    assert q[0]["eps"] == pytest.approx(newest["basic_earnings_per_share"]["value"])
    assert q[0]["revenues"] == pytest.approx(newest["revenues"]["value"])

    a = out["annual"]
    assert [r["fiscal_year"] for r in a] == [2026, 2025, 2024, 2023, 2022]
    assert a[0]["eps"] == pytest.approx(3.0)
    assert a[0]["revenues"] == pytest.approx(120_000_000.0)
    assert set(a[0]) == {"fiscal_year", "eps", "revenues"}


def test_v1_rows_keep_their_stored_quarter(monkeypatch: pytest.MonkeyPatch, eval_row: None) -> None:
    out = _serve(monkeypatch, V1_QUARTERLY, V1_ANNUAL)
    assert out["ok"] is True
    q = out["quarterly"]
    assert [(r["fiscal_year"], r["fiscal_quarter"]) for r in q] == [
        (2026, 2), (2026, 1),
        (2025, 4), (2025, 3), (2025, 2), (2025, 1),
        (2024, 4), (2024, 3), (2024, 2), (2024, 1),
    ]
    newest = V1_QUARTERLY[-1]["data"]
    assert q[0]["eps"] == pytest.approx(newest["basic_earnings_per_share"])
    assert q[0]["revenues"] == pytest.approx(newest["revenue"])
    assert set(q[0]) == {"fiscal_year", "fiscal_quarter", "eps", "revenues"}
    assert [r["fiscal_year"] for r in out["annual"]] == [2025, 2024, 2023, 2022, 2021]


def test_a_series_holding_both_formats_serves_only_v1(monkeypatch: pytest.MonkeyPatch, eval_row: None) -> None:
    # Two legacy rows whose period dates the v1 rows did not overwrite (a day or
    # two apart), plus one legacy annual row the same way.
    stale_q = [
        _row("2025-12-29", "quarterly", 2025, None, _legacy_data(9.99, 1.0)),
        _row("2025-06-28", "quarterly", 2025, None, _legacy_data(9.99, 1.0)),
    ]
    stale_a = [_row("2024-12-28", "annual", 2024, None, _legacy_data(9.99, 1.0))]
    out = _serve(monkeypatch, V1_QUARTERLY + stale_q, V1_ANNUAL + stale_a)

    q = out["quarterly"]
    assert len(q) == 10
    assert all(r["eps"] != 9.99 for r in q)
    keys = [(r["fiscal_year"], r["fiscal_quarter"]) for r in q]
    assert len(keys) == len(set(keys))
    a = out["annual"]
    assert all(r["eps"] != 9.99 for r in a)
    assert len({r["fiscal_year"] for r in a}) == len(a) == 5


def test_metrics_are_the_mart_inspector_columns(monkeypatch: pytest.MonkeyPatch, eval_row: None) -> None:
    out = _serve(monkeypatch, V1_QUARTERLY, V1_ANNUAL)
    metrics = out["metrics"]
    assert set(metrics) == set(analytics_reader.FUND_METRIC_COLUMNS)
    assert metrics["eps_q0"] == pytest.approx(1.5)
    assert metrics["eps_g0"] == pytest.approx(0.3)
    assert metrics["rev_fy_g1"] is None
    assert metrics["eps_fy0"] is None  # column absent from the row
    assert "eps_q2q_ge_25pct" not in metrics and "pass_count" not in metrics


def test_metrics_failure_leaves_the_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(sym: str) -> None:
        raise RuntimeError("research api down")

    monkeypatch.setattr(analytics_reader, "fetch_fundamental_eval_single", boom)
    out = _serve(monkeypatch, V1_QUARTERLY, V1_ANNUAL)
    assert out["ok"] is True
    assert len(out["quarterly"]) == 10
    assert out["metrics"] == {}


def test_plugin_failure_keeps_the_shape(monkeypatch: pytest.MonkeyPatch, eval_row: None) -> None:
    def boom(*_a: Any, **_kw: Any) -> None:
        raise OSError("plugin unreachable")

    monkeypatch.setattr(market_data_client, "fetch_sepa_financials", boom)
    out = data_readiness.get_symbol_fundamental_raw_data(None, symbol=SYM)  # type: ignore[arg-type]
    assert out["ok"] is False
    assert "plugin unreachable" in out["error"]
    assert out["quarterly"] == [] and out["annual"] == [] and out["metrics"] == {}


def test_missing_symbol_keeps_the_shape() -> None:
    out = data_readiness.get_symbol_fundamental_raw_data(None, symbol="  ")  # type: ignore[arg-type]
    assert out == {"ok": False, "error": "symbol is required", "quarterly": [], "annual": [], "metrics": {}}


def test_no_rows_is_empty_not_an_error(monkeypatch: pytest.MonkeyPatch, eval_row: None) -> None:
    out = _serve(monkeypatch, [], [])
    assert out["ok"] is True
    assert out["quarterly"] == [] and out["annual"] == []


def test_route_answers_200_over_http(monkeypatch: pytest.MonkeyPatch, eval_row: None) -> None:
    fetch, calls = _fake_plugin(LEGACY_QUARTERLY, LEGACY_ANNUAL)
    monkeypatch.setattr(market_data_client, "fetch_sepa_financials", fetch)
    app = FastAPI()
    app.include_router(data_readiness.router)
    res = TestClient(app).get("/research/data/readiness/symbol-fundamental-raw-data", params={"symbol": SYM})
    assert res.status_code == 200
    body = res.json()
    assert body["ok"] is True
    assert body["quarterly"][0] == {
        "fiscal_year": 2027,
        "fiscal_quarter": 1,
        "eps": pytest.approx(LEGACY_QUARTERLY[-1]["data"]["basic_earnings_per_share"]["value"]),
        "revenues": pytest.approx(LEGACY_QUARTERLY[-1]["data"]["revenues"]["value"]),
    }
    assert sorted(calls) == [("annual", 8), ("quarterly", 16)]


@pytest.mark.parametrize(
    ("fiscal_year", "period_end", "ends", "expected"),
    [
        # 52/53-week year: the anchor year moved by 365 days is close enough.
        (2026, "2025-12-27", {2025: date(2025, 9, 27)}, 1),
        (2026, "2026-06-27", {2025: date(2025, 9, 27)}, 3),
        (2026, date(2026, 9, 26), {2026: date(2026, 9, 26)}, 4),
        # Only the following year's end is known.
        (2024, "2023-09-30", {2025: date(2025, 3, 31)}, 2),
        # No anchor, or a date that sits in no quarter of that fiscal year.
        (2026, "2025-12-31", {}, None),
        (2026, "2024-01-15", {2026: date(2026, 3, 31)}, None),
        (None, "2025-12-31", {2026: date(2026, 3, 31)}, None),
    ],
)
def test_derive_fiscal_quarter(fiscal_year: Any, period_end: Any, ends: dict[int, date], expected: int | None) -> None:
    assert derive_fiscal_quarter(fiscal_year, period_end, ends) == expected
