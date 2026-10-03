"""One query vocabulary, old names accepted for one release (debt TD-51, api 0.6.6).

The canonical names are ``expiry`` (YYYY-MM-DD), ``option_right``, ``from_ts`` / ``to_ts``
(Unix seconds) and ``from_date`` / ``to_date`` (YYYY-MM-DD). The old spellings are renamed
before routing by ``QueryAliasRewriter``; every route keeps its defaults. ``/executions``
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
    QUERY_ALIASES,
    QueryAliasRewriter,
    aliases_for,
    normalize_expiry,
    rewrite_query,
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


def test_every_aliased_route_is_served_by_some_app() -> None:
    served = set().union(*(served_routes(app) for app in _apps().values()))
    assert not sorted(set(QUERY_ALIASES) - served)


def test_every_alias_points_at_a_canonical_name_the_route_declares() -> None:
    """A rename onto a name the route does not read would drop the caller's filter silently."""
    apps = _apps()
    for (method, path), aliases in QUERY_ALIASES.items():
        app = next(a for a in apps.values() if (method, path) in served_routes(a))
        declared = _query_params(app, method, path)
        for old, new in aliases.items():
            assert new in CANONICAL_NAMES, (path, new)
            assert new in declared, (path, new, declared)
            assert old not in declared, (path, old, declared)


def test_every_app_runs_the_rewriter() -> None:
    for name, app in _apps().items():
        assert any(m.cls is QueryAliasRewriter for m in app.user_middleware), name


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


def test_rewrite_renames_an_old_name() -> None:
    q, used = rewrite_query("since_ts=5&account_id=U0000001", {"since_ts": "from_ts"})
    assert q == "from_ts=5&account_id=U0000001"
    assert used == ["since_ts->from_ts"]


def test_rewrite_lets_the_canonical_name_win() -> None:
    q, used = rewrite_query("since_ts=5&from_ts=9", {"since_ts": "from_ts"})
    assert q == "from_ts=9"
    assert used == ["since_ts->from_ts (ignored)"]


def test_rewrite_keeps_encoding_and_blank_values() -> None:
    q, used = rewrite_query("expiration=20261016&symbol=Z%26Q&strikes=", {"expiration": "expiry"})
    assert q == "expiry=20261016&symbol=Z%26Q&strikes="
    assert used == ["expiration->expiry"]


def test_aliases_for_matches_the_path_with_or_without_slash() -> None:
    assert aliases_for("get", "/transactions/") == {"since_ts": "from_ts", "until_ts": "to_ts"}
    assert aliases_for("POST", "/transactions") is None


# --- through the apps -------------------------------------------------------------------


def _account(reader: MagicMock) -> TestClient:
    reader.config = full_server_config()
    app = create_account_app(reader=reader, control_via_db=None, merged_config=reader.config)
    return TestClient(app)


def test_old_and_new_time_names_reach_the_reader_the_same(caplog: pytest.LogCaptureFixture) -> None:
    reader = MagicMock()
    reader.get_transactions.return_value = []
    client = _account(reader)
    with caplog.at_level(logging.WARNING, logger="bifrost_api.common.query_vocab"):
        assert client.get("/transactions?since_ts=100&until_ts=200").status_code == 200
    old = reader.get_transactions.call_args.kwargs
    assert "deprecated query params: GET /transactions since_ts->from_ts until_ts->to_ts" in caplog.text
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="bifrost_api.common.query_vocab"):
        assert client.get("/transactions?from_ts=100&to_ts=200").status_code == 200
    assert reader.get_transactions.call_args.kwargs == old
    assert old["since_ts"] == 100.0 and old["until_ts"] == 200.0
    assert "deprecated query params" not in caplog.text


@pytest.mark.parametrize(
    "path, reader_method, old, new, kwarg",
    [
        ("/executions", "get_executions", "since_ts=7", "from_ts=7", ("since_ts", 7.0)),
        ("/performance", "get_performance_stats", "until_ts=7", "to_ts=7", ("until_ts", 7.0)),
        ("/strategies/win-rate", "get_strategy_win_rate", "since_ts=7", "from_ts=7", ("since_ts", 7.0)),
        ("/strategies/instances", "list_strategy_instances", "opened_at_from=7", "from_ts=7", ("opened_at_from", 7.0)),
        ("/strategies/instances", "list_strategy_instances", "opened_at_until=7", "to_ts=7", ("opened_at_until", 7.0)),
    ],
)
def test_each_old_time_name_still_filters(
    path: str, reader_method: str, old: str, new: str, kwarg: tuple
) -> None:
    reader = MagicMock()
    getattr(reader, reader_method).return_value = [] if "instances" in path or path == "/executions" else {}
    client = _account(reader)  # one app per test: the metrics registry is per test
    for query in (old, new):
        getattr(reader, reader_method).reset_mock()
        assert client.get(f"{path}?{query}").status_code == 200, (path, query)
        name, value = kwarg
        assert getattr(reader, reader_method).call_args.kwargs[name] == value, (path, query)


def test_stock_link_candidates_take_from_date_and_the_old_names() -> None:
    reader = MagicMock()
    reader.get_stock_link_candidates.return_value = {"executions": []}
    client = _account(reader)
    for query in ("trade_date_from=2026-01-02&trade_date_to=2026-01-09", "from_date=2026-01-02&to_date=2026-01-09"):
        resp = client.get(
            f"/executions/stock-link-candidates?account_id={ACC}&option_account_executions_id=4&{query}"
        )
        assert resp.status_code == 200, resp.text
        kw = reader.get_stock_link_candidates.call_args.kwargs
        assert (kw["trade_date_from"], kw["trade_date_to"]) == ("2026-01-02", "2026-01-09"), query


def _research() -> TestClient:
    r = MagicMock()
    r._config = full_server_config()
    app = create_research_app(reader=r, control_via_db=None, merged_config=r._config)
    return TestClient(app, raise_server_exceptions=False)


@pytest.mark.parametrize(
    "path", ["/research/option-contract/liquidity-summary", "/research/option-contract/relative-value"]
)
def test_option_contract_reads_take_expiry_option_right_and_the_old_names(
    path: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Without a database the route answers 503 -- after the query validated. A missing
    required name is FastAPI's 422, so 503 proves the names reached the route."""
    import bifrost_api.research.routers.option_discovery as od

    monkeypatch.setattr(od, "db_config", lambda request: None)
    client = _research()
    for query in ("expiry=2026-10-16&option_right=C", "expiration=20261016&right=C"):
        resp = client.get(f"{path}?symbol=ZZQ&strike=10&{query}")
        assert resp.status_code == 503, (query, resp.text)
    assert client.get(f"{path}?symbol=ZZQ&strike=10&expiry=2026-10-16").status_code == 422


def test_option_snapshots_take_expiry_and_expiration(monkeypatch: pytest.MonkeyPatch) -> None:
    import bifrost_api.research.routers.option_discovery as od

    monkeypatch.setattr(od, "db_config", lambda request: None)
    client = _research()
    for query in ("expiry=2026-10-16", "expiration=20261016"):
        assert client.get(f"/research/option-snapshots?symbol=ZZQ&{query}").status_code == 503, query


def test_greeks_takes_option_right_and_reads_a_compact_expiry_as_iso(monkeypatch: pytest.MonkeyPatch) -> None:
    import bifrost_api.research.routers.greeks as gk

    seen: Dict[str, Any] = {}

    def fake_rows(db: Any, sym: str, trade_date: str, rate: float, expiry: Any, right: Any, limit: int) -> list:
        seen.update(expiry=expiry, right=right)
        return []

    monkeypatch.setattr(gk, "db_config", lambda request: {"host": "x"})
    monkeypatch.setattr(gk, "_fetch_greeks_rows", fake_rows)
    client = _research()
    for query in ("expiry=20261016&option_right=P", "expiry=2026-10-16&right=P"):
        seen.clear()
        resp = client.get(f"/research/greeks?symbol=ZZQ&trade_date=2026-10-01&{query}")
        assert resp.status_code == 200, resp.text
        assert seen == {"expiry": "2026-10-16", "right": "P"}, query


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


@pytest.mark.parametrize("returned, total", [(3, 3), (5, 5), (6, None)])
def test_executions_total_is_sent_only_when_the_limit_did_not_cut(returned: int, total: Optional[int]) -> None:
    reader = MagicMock()
    reader.get_executions.return_value = _rows(returned)
    body = _account(reader).get("/executions?limit=5").json()
    assert reader.get_executions.call_args.kwargs["limit"] == 6
    assert body["count"] == min(returned, 5)
    assert body.get("total") == total
    assert ("total" in body) is (total is not None)


def test_executions_limit_zero_reads_everything_and_counts_it() -> None:
    reader = MagicMock()
    reader.get_executions.return_value = _rows(4)
    body = _account(reader).get("/executions?limit=0").json()
    assert reader.get_executions.call_args.kwargs["limit"] is None
    assert body["count"] == body["total"] == 4


def test_executions_default_limit_is_still_200() -> None:
    reader = MagicMock()
    reader.get_executions.return_value = _rows(2)
    _account(reader).get("/executions")
    assert reader.get_executions.call_args.kwargs["limit"] == 201


def test_executions_with_pairs_total_under_the_cap_only() -> None:
    reader = MagicMock()
    reader.get_executions_with_opt_pairs.return_value = {"executions": _rows(2), "opt_pairs": []}
    client = _account(reader)
    body = client.get("/executions?include_opt_pairs=true&limit=2").json()
    assert reader.get_executions_with_opt_pairs.call_args.kwargs["limit"] == 2
    assert "total" not in body
    reader.get_executions_with_opt_pairs.return_value = {"executions": _rows(1), "opt_pairs": []}
    assert client.get("/executions?include_opt_pairs=true&limit=2").json()["total"] == 1


@pytest.mark.parametrize("returned, total", [(0, 0), (500, 500), (501, None)])
def test_transactions_total(returned: int, total: Optional[int]) -> None:
    reader = MagicMock()
    reader.get_transactions.return_value = [{"account_transactions_id": i} for i in range(returned)]
    body = _account(reader).get("/transactions").json()
    assert reader.get_transactions.call_args.kwargs["limit"] == 501
    assert body["count"] == min(returned, 500)
    assert body.get("total") == total


def test_transactions_limit_zero_is_passed_as_before() -> None:
    reader = MagicMock()
    reader.get_transactions.return_value = []
    body = _account(reader).get("/transactions?limit=0").json()
    assert reader.get_transactions.call_args.kwargs["limit"] == 0
    assert body == {"items": [], "count": 0}
