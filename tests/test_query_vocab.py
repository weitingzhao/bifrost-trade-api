"""One query vocabulary (debt TD-51): canonical names since api 0.6.6, old ones refused since 0.10.0.

The canonical names are ``expiry`` (YYYY-MM-DD), ``option_right``, ``from_ts`` / ``to_ts``
(Unix seconds) and ``from_date`` / ``to_date`` (YYYY-MM-DD). The old spellings were renamed
before routing in api 0.6.6 .. 0.9.0; since 0.10.0 ``RetiredQueryNames`` answers them with a
422 that names the successor, so a caller's filter is never dropped silently. Every route
keeps its defaults. ``/executions``
and ``/transactions`` answer ``total`` only when the limit did not cut the list.
Fixtures are invented.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Set
from unittest.mock import MagicMock

import pytest
from starlette.testclient import TestClient

from bifrost_api.account.app import create_account_app
from bifrost_api.common.query_vocab import (
    CANONICAL_NAMES,
    RETIRED_QUERY_NAMES,
    RetiredQueryNames,
    normalize_expiry,
    retired_for,
    retired_sent,
)
from bifrost_api.market.app import create_market_app
from bifrost_api.research.app import create_research_app
from tests.contract.helpers import full_server_config
from tests.route_listing import served_routes
from tests.test_deprecations import _apps

ACC = "U0000001"


def _query_params(app: Any, method: str, path: str) -> Set[str]:
    op = app.openapi()["paths"][path][method.lower()]
    return {p["name"] for p in op.get("parameters", []) if p["in"] == "query"}


# --- the table ---------------------------------------------------------------------


def test_every_route_with_retired_names_is_served_by_some_app() -> None:
    served = set().union(*(served_routes(app) for app in _apps().values()))
    assert not sorted(set(RETIRED_QUERY_NAMES) - served)


def test_every_retired_name_points_at_a_canonical_name_the_route_declares() -> None:
    """The 422 names a successor; it must be one the route reads, and the old name must not be declared."""
    apps = _apps()
    for (method, path), retired in RETIRED_QUERY_NAMES.items():
        app = next(a for a in apps.values() if (method, path) in served_routes(a))
        declared = _query_params(app, method, path)
        for old, new in retired.items():
            assert new in CANONICAL_NAMES, (path, new)
            assert new in declared, (path, new, declared)
            assert old not in declared, (path, old, declared)


TD51_OLD_NAMES = {"since_ts", "until_ts", "opened_at_from", "opened_at_until", "trade_date_from", "trade_date_to",
                  "expiration", "right"}


def test_the_retired_names_are_exactly_the_td51_spellings() -> None:
    old = set().union(*(set(r) for r in RETIRED_QUERY_NAMES.values()))
    assert old == TD51_OLD_NAMES


def test_every_app_refuses_the_retired_names() -> None:
    for name, app in _apps().items():
        assert any(m.cls is RetiredQueryNames for m in app.user_middleware), name


# --- the pieces ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw, want",
    [
        ("2026-10-16", "2026-10-16"),
        ("20261016", "2026-10-16"),
        (" 20261016 ", "2026-10-16"),
        ("2026-10-16T00:00:00", "2026-10-16"),
        ("Oct 16", "Oct 16"),
        ("", ""),
        (None, None),
    ],
)
def test_normalize_expiry(raw: Optional[str], want: Optional[str]) -> None:
    assert normalize_expiry(raw) == want


def test_retired_sent_names_each_old_name_once_with_its_successor() -> None:
    errors = retired_sent("since_ts=5&account_id=U0000001&since_ts=6&until_ts=", {"since_ts": "from_ts", "until_ts": "to_ts"})
    assert [(e["type"], e["loc"], e["input"]) for e in errors] == [
        ("retired_query_param", ["query", "since_ts"], "5"),
        ("retired_query_param", ["query", "until_ts"], ""),
    ]
    assert "use from_ts" in errors[0]["msg"]


def test_retired_sent_ignores_the_canonical_names() -> None:
    assert retired_sent("from_ts=5&to_ts=9&expiry=20261016", {"since_ts": "from_ts"}) == []


def test_retired_for_matches_the_path_with_or_without_slash() -> None:
    assert retired_for("get", "/transactions/") == {"since_ts": "from_ts", "until_ts": "to_ts"}
    assert retired_for("POST", "/transactions") is None


# --- through the apps -------------------------------------------------------------------


def _account(reader: MagicMock) -> TestClient:
    reader.config = full_server_config()
    app = create_account_app(reader=reader, control_via_db=None, merged_config=reader.config)
    return TestClient(app)


def test_an_old_time_name_is_422_and_reaches_no_reader(caplog: pytest.LogCaptureFixture) -> None:
    reader = MagicMock()
    reader.get_transactions_page.return_value = {"items": [], "next_cursor": None}
    client = _account(reader)
    with caplog.at_level(logging.WARNING, logger="bifrost_api.common.query_vocab"):
        resp = client.get("/transactions?since_ts=100&until_ts=200", headers={"user-agent": "probe/1"})
    assert resp.status_code == 422, resp.text
    assert [(e["type"], e["loc"]) for e in resp.json()["detail"]] == [
        ("retired_query_param", ["query", "since_ts"]),
        ("retired_query_param", ["query", "until_ts"]),
    ]
    reader.get_transactions_page.assert_not_called()
    assert "retired query params: GET /transactions since_ts (use from_ts) until_ts (use to_ts)" in caplog.text
    assert "user_agent=probe/1" in caplog.text
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="bifrost_api.common.query_vocab"):
        assert client.get("/transactions?from_ts=100&to_ts=200").status_code == 200
    kw = reader.get_transactions_page.call_args.kwargs
    assert kw["since_ts"] == 100.0 and kw["until_ts"] == 200.0
    assert "retired query params" not in caplog.text


@pytest.mark.parametrize(
    "path, reader_method, old, new, kwarg",
    [
        ("/executions", "get_executions_page", "since_ts=7", "from_ts=7", ("since_ts", 7.0)),
        ("/performance", "get_performance_stats", "until_ts=7", "to_ts=7", ("until_ts", 7.0)),
        ("/trades/win-rate", "get_trade_win_rate", "since_ts=7", "from_ts=7", ("since_ts", 7.0)),
        ("/trades", "list_trades", "opened_at_from=7", "from_ts=7", ("opened_at_from", 7.0)),
        ("/trades", "list_trades", "opened_at_until=7", "to_ts=7", ("opened_at_until", 7.0)),
    ],
)
def test_each_new_time_name_filters_and_its_old_name_is_422(
    path: str, reader_method: str, old: str, new: str, kwarg: tuple
) -> None:
    reader = MagicMock()
    getattr(reader, reader_method).return_value = [] if path == "/executions" else {}
    client = _account(reader)  # one app per test: the metrics registry is per test
    name, value = kwarg
    assert client.get(f"{path}?{new}").status_code == 200, (path, new)
    assert getattr(reader, reader_method).call_args.kwargs[name] == value, (path, new)
    getattr(reader, reader_method).reset_mock()
    resp = client.get(f"{path}?{old}")
    assert resp.status_code == 422, (path, old, resp.text)
    assert resp.json()["detail"][0]["type"] == "retired_query_param"
    getattr(reader, reader_method).assert_not_called()


def test_stock_link_candidates_take_from_date_and_refuse_the_old_names() -> None:
    reader = MagicMock()
    reader.get_stock_link_candidates.return_value = {"executions": []}
    client = _account(reader)
    base = f"/executions/stock-link-candidates?account_id={ACC}&option_account_executions_id=4"
    resp = client.get(f"{base}&from_date=2026-01-02&to_date=2026-01-09")
    assert resp.status_code == 200, resp.text
    kw = reader.get_stock_link_candidates.call_args.kwargs
    assert (kw["trade_date_from"], kw["trade_date_to"]) == ("2026-01-02", "2026-01-09")
    reader.get_stock_link_candidates.reset_mock()
    resp = client.get(f"{base}&trade_date_from=2026-01-02&trade_date_to=2026-01-09")
    assert resp.status_code == 422, resp.text
    reader.get_stock_link_candidates.assert_not_called()


def _research() -> TestClient:
    r = MagicMock()
    r._config = full_server_config()
    app = create_research_app(reader=r, control_via_db=None, merged_config=r._config)
    return TestClient(app, raise_server_exceptions=False)


@pytest.mark.parametrize(
    "path", ["/research/option-contract/liquidity-summary", "/research/option-contract/relative-value"]
)
def test_option_contract_reads_take_expiry_and_option_right_only(
    path: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Without a database the route answers 503 -- after the query validated. A missing
    required name is FastAPI's 422, so 503 proves the names reached the route; an old
    spelling is a retired_query_param 422 before the route."""
    import bifrost_api.research.routers.option_discovery as od

    monkeypatch.setattr(od, "db_config", lambda request: None)
    client = _research()
    resp = client.get(f"{path}?symbol=ZZQ&strike=10&expiry=2026-10-16&option_right=C")
    assert resp.status_code == 503, resp.text
    assert client.get(f"{path}?symbol=ZZQ&strike=10&expiry=2026-10-16").status_code == 422
    for query in ("expiration=20261016&right=C", "expiry=2026-10-16&right=C", "expiration=20261016&option_right=C"):
        resp = client.get(f"{path}?symbol=ZZQ&strike=10&{query}")
        assert resp.status_code == 422, query
        assert {e["type"] for e in resp.json()["detail"]} == {"retired_query_param"}, query


def test_option_snapshots_take_expiry_not_expiration(monkeypatch: pytest.MonkeyPatch) -> None:
    import bifrost_api.research.routers.option_discovery as od

    monkeypatch.setattr(od, "db_config", lambda request: None)
    client = _research()
    for query in ("expiry=2026-10-16", "expiry=20261016"):
        assert client.get(f"/research/option-snapshots?symbol=ZZQ&{query}").status_code == 503, query
    resp = client.get("/research/option-snapshots?symbol=ZZQ&expiration=20261016")
    assert resp.status_code == 422
    assert resp.json()["detail"][0]["loc"] == ["query", "expiration"]


def test_greeks_takes_option_right_and_reads_a_compact_expiry_as_iso(monkeypatch: pytest.MonkeyPatch) -> None:
    import bifrost_api.research.routers.greeks as gk

    seen: Dict[str, Any] = {}

    def fake_rows(db: Any, sym: str, trade_date: str, rate: float, expiry: Any, right: Any, limit: int) -> list:
        seen.update(expiry=expiry, right=right)
        return []

    monkeypatch.setattr(gk, "db_config", lambda request: {"host": "x"})
    monkeypatch.setattr(gk, "_fetch_greeks_rows", fake_rows)
    client = _research()
    resp = client.get("/research/greeks?symbol=ZZQ&trade_date=2026-10-01&expiry=20261016&option_right=P")
    assert resp.status_code == 200, resp.text
    assert seen == {"expiry": "2026-10-16", "right": "P"}
    seen.clear()
    resp = client.get("/research/greeks?symbol=ZZQ&trade_date=2026-10-01&expiry=2026-10-16&right=P")
    assert resp.status_code == 422, resp.text
    assert seen == {}


def test_option_bars_take_an_iso_expiry() -> None:
    r = MagicMock()
    r._config = full_server_config()
    r.get_option_bars.return_value = []
    app = create_market_app(reader=r, control_via_db=None, merged_config=r._config)
    client = TestClient(app, raise_server_exceptions=False)
    resp = client.get("/bars?asset=option&symbol=ZZQ&expiry=2026-10-16&strike=10&option_right=C")
    assert resp.status_code == 200, resp.text
    assert r.get_option_bars.call_args.args[1] == "2026-10-16"


# --- total on /executions and /transactions ------------------------------------------


def _rows(n: int) -> list:
    return [{"account_executions_id": i, "symbol": "ZZQ"} for i in range(1, n + 1)]


def _page(rows: list, next_cursor: Optional[str] = None) -> dict:
    return {"items": rows, "next_cursor": next_cursor}


# Since api 0.6.9 (core 0.40.0) the reader's page method reads limit + 1 itself and says
# whether a page follows (next_cursor); the route sends total only for a first page with none.


@pytest.mark.parametrize("returned, more, total", [(3, False, 3), (5, False, 5), (5, True, None)])
def test_executions_total_is_sent_only_when_the_limit_did_not_cut(returned: int, more: bool, total: Optional[int]) -> None:
    reader = MagicMock()
    reader.get_executions_page.return_value = _page(_rows(returned), "c1" if more else None)
    body = _account(reader).get("/executions?limit=5").json()
    kwargs = reader.get_executions_page.call_args.kwargs
    assert kwargs["limit"] == 5 and kwargs["cursor"] is None
    assert body["count"] == returned
    assert body.get("total") == total
    assert ("total" in body) is (total is not None)
    assert body["next_cursor"] == ("c1" if more else None)


def test_executions_limit_zero_reads_everything_and_counts_it() -> None:
    reader = MagicMock()
    reader.get_executions_page.return_value = _page(_rows(4))
    body = _account(reader).get("/executions?limit=0").json()
    assert reader.get_executions_page.call_args.kwargs["limit"] is None
    assert body["count"] == body["total"] == 4
    assert body["next_cursor"] is None


def test_executions_default_limit_is_still_200() -> None:
    reader = MagicMock()
    reader.get_executions_page.return_value = _page(_rows(2))
    _account(reader).get("/executions")
    assert reader.get_executions_page.call_args.kwargs["limit"] == 200


def test_executions_with_pairs_total_under_the_cap_only() -> None:
    reader = MagicMock()
    reader.get_executions_with_opt_pairs.return_value = {"executions": _rows(2), "opt_pairs": []}
    client = _account(reader)
    body = client.get("/executions?include_opt_pairs=true&limit=2").json()
    assert reader.get_executions_with_opt_pairs.call_args.kwargs["limit"] == 2
    assert "total" not in body
    reader.get_executions_with_opt_pairs.return_value = {"executions": _rows(1), "opt_pairs": []}
    assert client.get("/executions?include_opt_pairs=true&limit=2").json()["total"] == 1


@pytest.mark.parametrize("returned, more, total", [(0, False, 0), (500, False, 500), (500, True, None)])
def test_transactions_total(returned: int, more: bool, total: Optional[int]) -> None:
    reader = MagicMock()
    rows = [{"account_transactions_id": i} for i in range(returned)]
    reader.get_transactions_page.return_value = _page(rows, "c1" if more else None)
    body = _account(reader).get("/transactions").json()
    kwargs = reader.get_transactions_page.call_args.kwargs
    assert kwargs["limit"] == 500 and kwargs["cursor"] is None
    assert body["count"] == returned
    assert body.get("total") == total


def test_transactions_limit_zero_is_passed_as_before() -> None:
    reader = MagicMock()
    reader.get_transactions_page.return_value = _page([])
    body = _account(reader).get("/transactions?limit=0").json()
    assert reader.get_transactions_page.call_args.kwargs["limit"] == 0
    assert body == {"items": [], "count": 0, "next_cursor": None}
