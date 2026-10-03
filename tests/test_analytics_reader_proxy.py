"""SEPA mart reads go to Research only; a Research failure is a 503 naming Research (TD-49 steps 1 and 3).

Before 0.6.8 each helper fell back to direct Golden Source SQL when the proxy
failed. The fallback never ran (none in six days on any environment) and
duplicated Research's own SQL, so it could only drift. Since 0.6.10 the tier
marts, the momentum grades and the criteria-stats distributions go to Research
too (Research 0.157.0). Every symbol and number below is invented.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Callable, Dict, Iterator
from unittest.mock import MagicMock, patch

import httpx
import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from bifrost_api.research import analytics_reader as ar
from bifrost_api.research.routers import data_readiness


def _status_error(code: int, detail: str | None = None) -> httpx.HTTPStatusError:
    req = httpx.Request("GET", "http://research.test/analytics/sepa/x")
    body = {"detail": detail} if detail else None
    resp = httpx.Response(code, request=req, json=body) if body else httpx.Response(code, request=req)
    return httpx.HTTPStatusError(f"HTTP {code}", request=req, response=resp)


def _connect_error() -> httpx.ConnectError:
    return httpx.ConnectError("connection refused", request=httpx.Request("GET", "http://research.test/"))


@pytest.fixture
def no_sql(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Any direct Golden Source read fails the test."""
    guard = MagicMock(side_effect=AssertionError("direct SQL must not run"))
    monkeypatch.setattr(ar, "get_conn", guard)
    return guard


# The 8 helpers that used to fall back to SQL, and the 3 that read SQL with no
# proxy at all until step 3, each with an argument list.
_HELPERS: Dict[str, Callable[[], Any]] = {
    "criteria-stats": lambda: ar.fetch_criteria_stats(),
    "fundamental-eval": lambda: ar.fetch_fundamental_eval_single("zzqa"),
    "technical-eval": lambda: ar.fetch_technical_eval_single("zzqa"),
    "fundamental-filter": lambda: ar.fetch_fundamental_filter(["eps_acc_2q"]),
    "technical-filter": lambda: ar.fetch_technical_filter(["crs_ge_70"]),
    "fundamental-distribution": lambda: ar.fetch_fundamental_distribution_symbols(4),
    "technical-distribution": lambda: ar.fetch_technical_distribution_symbols(8),
    "screener-wide": lambda: ar.fetch_screener_wide(symbols=["zzqa"]),
    "tier-stats": lambda: ar.fetch_tier_stats("momentum"),
    "tier-filter": lambda: ar.fetch_tier_filter("structure", ["bb_squeeze"], 0, "all", 10),
    "momentum-grades": lambda: ar.fetch_momentum_grades("A", 10),
}


def test_the_proxy_toggle_and_the_dead_reader_are_gone() -> None:
    for name in (
        "use_research_proxy",
        "_fetch_criteria_stats_direct",
        "fetch_screening_ranked",
        "latest_eval_date",
        "_ALLOWED_EVAL_TABLES",
        "_FUND_EVAL_TABLE",
        "_TECH_EVAL_TABLE",
    ):
        assert not hasattr(ar, name), name


@pytest.mark.parametrize("name", sorted(_HELPERS))
@pytest.mark.parametrize("failure", [_status_error(502), _connect_error(), RuntimeError("non-object JSON")])
def test_a_research_failure_raises_and_never_reads_sql(name: str, failure: Exception, no_sql: MagicMock) -> None:
    with patch.object(ar, "_proxy_get", side_effect=failure):
        with pytest.raises(ar.ResearchUnavailable, match=r"^Research API /(analytics/sepa|research/momentum)/"):
            _HELPERS[name]()
    no_sql.assert_not_called()


def test_a_research_status_error_carries_researchs_detail() -> None:
    with patch.object(ar, "_proxy_get", side_effect=_status_error(503, "Analytics DB error: timeout")):
        with pytest.raises(ar.ResearchUnavailable) as err:
            ar.fetch_criteria_stats()
    assert str(err.value) == "Research API /analytics/sepa/criteria-stats: HTTP 503 — Analytics DB error: timeout"


@pytest.mark.parametrize("fetch", [ar.fetch_fundamental_eval_single, ar.fetch_technical_eval_single])
def test_a_name_research_does_not_hold_is_none(fetch: Callable[[str], Any], no_sql: MagicMock) -> None:
    with patch.object(ar, "_proxy_get", side_effect=_status_error(404)):
        assert fetch("zzqa") is None
    no_sql.assert_not_called()


def test_a_404_is_an_outage_where_a_row_is_not_optional() -> None:
    with patch.object(ar, "_proxy_get", side_effect=_status_error(404)):
        with pytest.raises(ar.ResearchUnavailable, match="HTTP 404"):
            ar.fetch_criteria_stats()


def test_fetch_criteria_stats_via_proxy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RESEARCH_API_URL", "http://research.test")

    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json.return_value = {"ok": True, "fundamental": {"pass": 1}, "technical": {"pass": 2}}

    with patch("bifrost_api.research.analytics_reader.httpx.Client") as client_cls:
        client = MagicMock()
        client.__enter__.return_value = client
        client.__exit__.return_value = False
        client.get.return_value = mock_resp
        client_cls.return_value = client
        out = ar.fetch_criteria_stats()

    assert out == {"fundamental": {"pass": 1}, "technical": {"pass": 2}}
    client.get.assert_called_once()
    assert "/analytics/sepa/criteria-stats" in client.get.call_args[0][0]


def test_fetch_screener_wide_unwraps_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RESEARCH_API_URL", "http://research.test")

    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json.return_value = {"ok": True, "rows": [{"symbol": "ZZQA"}], "count": 1}

    with patch("bifrost_api.research.analytics_reader.httpx.Client") as client_cls:
        client = MagicMock()
        client.__enter__.return_value = client
        client.__exit__.return_value = False
        client.get.return_value = mock_resp
        client_cls.return_value = client
        rows = ar.fetch_screener_wide(symbols=["zzqa"])

    assert rows == [{"symbol": "ZZQA"}]


def test_distribution_returns_researchs_as_of(no_sql: MagicMock) -> None:
    body = {"ok": True, "conditions_passed": 4, "count": 1, "as_of": "2031-01-02",
            "symbols": [{"symbol": "ZZQA", "pass_count": 4, "passed_conditions": ["eps_acc_2q"]}]}
    with patch.object(ar, "_proxy_get", return_value=body) as get:
        symbols, as_of = ar.fetch_fundamental_distribution_symbols(4)
    assert symbols == body["symbols"] and as_of == "2031-01-02"
    assert get.call_args.args == ("/analytics/sepa/fundamental-distribution", {"conditions_passed": 4})


# ── Route level: a Research failure is 503 {detail}; success keeps the body ──


@pytest.fixture
def client() -> Iterator[TestClient]:
    app = FastAPI()
    app.include_router(data_readiness.router)
    with TestClient(app) as c:
        yield c


_ROUTES = [
    "/research/data/readiness/criteria-stats",
    "/research/data/readiness/fundamental-distribution/symbols?conditions_passed=4",
    "/research/data/readiness/technical-distribution/symbols?conditions_passed=8",
    "/research/data/readiness/fundamental-conditions?symbol=ZZQA",
    "/research/data/readiness/symbol-technical-conditions?symbol=ZZQA",
    "/research/data/readiness/tier-stats?tier=sentiment",
    "/research/data/readiness/tier-filter?tier=structure&include=bb_squeeze",
    "/research/data/readiness/momentum-filter?include=macd_bullish&min_score=5",
    "/research/data/readiness/momentum-grades?grades=A",
]


@pytest.mark.parametrize("url", _ROUTES)
@pytest.mark.parametrize("failure", [_status_error(500), _connect_error()])
def test_routes_answer_503_naming_research(client: TestClient, url: str, failure: Exception, no_sql: MagicMock) -> None:
    with patch.object(ar, "_proxy_get", side_effect=failure):
        resp = client.get(url)
    assert resp.status_code == 503
    assert set(resp.json()) == {"detail"}
    assert resp.json()["detail"].startswith(("Research API /analytics/sepa/", "Research API /research/momentum/"))
    no_sql.assert_not_called()


def test_a_research_400_on_a_tier_is_still_503(client: TestClient, no_sql: MagicMock) -> None:
    # trade-api refuses bad tiers and ids itself (400); a Research 4xx past those
    # checks means the two vocabularies drifted, which is Research failing.
    with patch.object(ar, "_proxy_get", side_effect=_status_error(400, "unknown momentum signal ids: x")):
        resp = client.get("/research/data/readiness/momentum-filter?include=macd_bullish")
    assert resp.status_code == 503
    assert resp.json()["detail"] == (
        "Research API /analytics/sepa/tier-filter: HTTP 400 — unknown momentum signal ids: x"
    )


@contextmanager
def _research(bodies: Dict[str, Any]) -> Iterator[MagicMock]:
    def fake(path: str, params: Any = None) -> Any:
        if path not in bodies:
            raise _status_error(404)
        return bodies[path]

    with patch.object(ar, "_proxy_get", side_effect=fake) as get:
        yield get


def test_distribution_route_body_is_unchanged(client: TestClient, no_sql: MagicMock) -> None:
    syms = [{"symbol": "ZZQA", "pass_count": 8, "passed_conditions": ["crs_ge_70"]}]
    with _research({"/analytics/sepa/technical-distribution": {
        "ok": True, "conditions_passed": 8, "count": 1, "symbols": syms, "as_of": "2031-01-02",
    }}):
        resp = client.get("/research/data/readiness/technical-distribution/symbols?conditions_passed=8")
    assert resp.status_code == 200
    assert resp.json() == {"ok": True, "conditions_passed": 8, "count": 1, "symbols": syms, "as_of": "2031-01-02"}


def test_conditions_route_body_is_unchanged(client: TestClient, no_sql: MagicMock) -> None:
    row = {"symbol": "ZZQA", "eval_date": "2031-01-02", "pass_count": 6, "insufficient_data": False,
           **{c: (i % 2 == 0) for i, c in enumerate(ar.FUND_CONDITION_COLUMNS)}}
    with _research({"/analytics/sepa/fundamental-eval/ZZQA": {"ok": True, "symbol": "ZZQA", "row": row}}):
        resp = client.get("/research/data/readiness/fundamental-conditions?symbol=zzqa")
    body = resp.json()
    assert resp.status_code == 200
    assert body["found"] is True and body["as_of_date"] == "2031-01-02" and body["pass_count"] == 6
    assert body["fundamental_pass"] is True and body["insufficient_data"] is False
    assert [c["pass"] for c in body["conditions"]] == [i % 2 == 0 for i in range(8)]


def test_a_name_research_lacks_is_found_false(client: TestClient, no_sql: MagicMock) -> None:
    with _research({}):
        resp = client.get("/research/data/readiness/symbol-technical-conditions?symbol=ZZQA")
    assert resp.status_code == 200
    assert resp.json() == {"ok": True, "symbol": "ZZQA", "found": False}


def test_criteria_stats_route_reshapes_researchs_stats(client: TestClient, no_sql: MagicMock) -> None:
    # The pass-count distributions and their eval dates come from Research too (0.157.0).
    fund_dist = [{"conditions_passed": i, "symbol_count": 1 if i == 4 else 0} for i in range(8, -1, -1)]
    tech_dist = [{"conditions_passed": i, "symbol_count": 2 if i == 11 else 0} for i in range(11, -1, -1)]
    stats = {
        "fundamental": {"evaluated": 10, "total": 12, "all_pass": 1, "eps_q2q_pass": 4, "eps_q2q_fail": 5},
        "technical": {"evaluated": 9, "total": 12, "all_pass": 2, "crs_pass": 3, "crs_fail": 6},
        "fundamental_distribution": fund_dist,
        "fundamental_eval_date": "2031-01-02",
        "technical_distribution": tech_dist,
        "technical_eval_date": "2031-01-03",
    }
    with _research({"/analytics/sepa/criteria-stats": {"ok": True, **stats}}):
        resp = client.get("/research/data/readiness/criteria-stats")
    body = resp.json()
    assert resp.status_code == 200
    assert body["universe_count"] == 12
    fund = body["fundamental"]
    assert fund["cached_count"] == 10 and fund["fund_pass_count"] == 1
    assert fund["pass_count_distribution"] == fund_dist and fund["eval_date"] == "2031-01-02"
    assert body["technical"]["pass_count_distribution"] == tech_dist
    assert body["technical"]["eval_date"] == "2031-01-03"
    assert "fundamental_distribution" not in body and "fundamental_distribution" not in fund
    assert fund["conditions"][0] == {
        "id": "eps_q2q_ge_25pct", "label": "eps_q2q_ge_25pct", "pass": 4, "fail": 5, "no_data": 1, "total": 10,
    }
    crs = [c for c in body["technical"]["conditions"] if c["id"] == "crs_ge_70"]
    assert crs == [{"id": "crs_ge_70", "label": "crs_ge_70", "pass": 3, "fail": 6}]


def test_criteria_stats_without_distributions_keeps_empty_lists(client: TestClient, no_sql: MagicMock) -> None:
    # A Research older than 0.157.0 sends no distribution fields: the lists stay empty, no eval_date.
    stats = {"fundamental": {"evaluated": 1, "total": 1}, "technical": {"evaluated": 1, "total": 1}}
    with _research({"/analytics/sepa/criteria-stats": {"ok": True, **stats}}):
        body = client.get("/research/data/readiness/criteria-stats").json()
    assert body["fundamental"]["pass_count_distribution"] == []
    assert body["technical"]["pass_count_distribution"] == []
    assert "eval_date" not in body["fundamental"] and "eval_date" not in body["technical"]
