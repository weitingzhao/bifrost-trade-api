"""Routes no repo calls are marked before they go (debt TD-40, decision B).

Nothing here reaches IB, Redis or a DB: the marked write is refused by the write
guard (anonymous viewer), and the marked read is answered by whatever a test app
without stores returns — only the header and the log line are under test.
"""

from __future__ import annotations

import logging
from typing import Any, Dict
from unittest.mock import MagicMock

import pytest
from starlette.testclient import TestClient

from bifrost_api.account.app import create_account_app
from bifrost_api.deprecations import (
    DEPRECATED_ROUTES,
    REPLACED_ROUTES,
    DeprecationMarker,
    deprecated_route,
    replaced_route,
)
from bifrost_api.market.app import create_market_app
from bifrost_api.monitor.app import create_app as create_monitor_app
from bifrost_api.research.app import create_research_app
from tests.contract.helpers import full_server_config, operator_server_config
from tests.route_listing import served_routes


def _apps() -> Dict[str, Any]:
    def reader() -> MagicMock:
        r = MagicMock()
        r._config = {**full_server_config(), "redis": {"enabled": False}, "ops": {"auth": {"default_role": "viewer"}}}
        return r

    m, a, k, s = reader(), reader(), reader(), reader()
    return {
        "monitor": create_monitor_app(
            reader=m, control_via_db=None, data_lag_threshold_ms=5000, merged_config=m._config
        ),
        "account": create_account_app(reader=a, control_via_db=None, merged_config=a._config),
        "market": create_market_app(reader=k, control_via_db=None, merged_config=k._config),
        "research": create_research_app(reader=s, control_via_db=None, merged_config=s._config),
    }


def test_every_deployed_app_runs_the_marker() -> None:
    for name, app in _apps().items():
        assert any(m.cls is DeprecationMarker for m in app.user_middleware), name


def test_every_marked_route_is_still_served() -> None:
    """A deleted route left on the list is noise; a typo would mark nothing."""
    served = set().union(*(served_routes(app) for app in _apps().values()))
    assert not sorted(DEPRECATED_ROUTES - served)
    assert not sorted(set(REPLACED_ROUTES) - served)


def test_every_successor_is_served() -> None:
    served = set().union(*(served_routes(app) for app in _apps().values()))
    successors = {tuple(v.split(" ", 1)) for v in REPLACED_ROUTES.values()}
    assert not sorted(successors - served)


def test_deprecated_and_replaced_do_not_overlap() -> None:
    assert not DEPRECATED_ROUTES & set(REPLACED_ROUTES)


def test_get_instrument_classes_is_on_neither_list() -> None:
    assert ("GET", "/instrument-classes") not in DEPRECATED_ROUTES | set(REPLACED_ROUTES)


@pytest.mark.parametrize(
    "method, path, hit",
    [
        ("POST", "/control/stop", "/control/stop"),
        ("POST", "/control/stop/", "/control/stop"),
        ("GET", "/control/stop", None),
        ("POST", "/control/flatten", None),
        ("DELETE", "/strategies/structures/42", "/strategies/structures/{structure_id}"),
        ("GET", "/strategies/structures/42", None),
        ("GET", "/strategies/instances/7/open-option-legs", "/strategies/instances/{strategy_instance_id}/open-option-legs"),
        ("GET", "/instrument-classes", None),
    ],
)
def test_deprecated_route(method: str, path: str, hit: Any) -> None:
    assert deprecated_route(method, path) == hit


def test_a_marked_route_says_so_and_logs_its_caller(caplog: pytest.LogCaptureFixture) -> None:
    client = TestClient(_apps()["monitor"], raise_server_exceptions=False)
    with caplog.at_level(logging.WARNING, logger="bifrost_api.deprecations"):
        r = client.post("/control/stop", headers={"User-Agent": "td40-test", "X-Forwarded-For": "192.0.2.7"})
    # Refused by the write guard first; still marked, still logged.
    assert r.status_code == 403
    assert r.headers.get("deprecation") == "true"
    line = next(rec.getMessage() for rec in caplog.records if "deprecated route hit" in rec.getMessage())
    assert "/control/stop" in line and "td40-test" in line and "192.0.2.7" in line


@pytest.mark.parametrize(
    "method, path, hit",
    [
        ("PUT", "/strategies/plans/42", ("/strategies/plans/{strategy_plan_id}", "/strategies/plans/42")),
        ("PUT", "/executions/-7", ("/executions/{execution_id}", "/executions/-7/attribution")),
        ("PUT", "/instrument-classes/ZZFI", ("/instrument-classes/{contract_key}", "/instrument-classes/ZZFI")),
        ("PATCH", "/strategies/plans/42", None),
        ("PUT", "/strategies/templates/3/legs", None),
        ("PUT", "/strategies/gate-safety/3", None),
        ("PUT", "/position-categories/tag", None),
    ],
)
def test_replaced_route(method: str, path: str, hit: Any) -> None:
    got = replaced_route(method, path)
    assert (got[0], got[2]) == hit if hit else got is None


def test_a_replaced_route_names_its_successor_and_logs_its_caller(caplog: pytest.LogCaptureFixture) -> None:
    client = TestClient(_apps()["account"], raise_server_exceptions=False)
    with caplog.at_level(logging.WARNING, logger="bifrost_api.deprecations"):
        r = client.put(
            "/strategies/plans/42",
            json={"expires_at": None},
            headers={"User-Agent": "td15-test", "X-Forwarded-Prefix": "/api/strategy"},
        )
    # A viewer is refused by the write guard first; the PUT is still marked.
    assert r.status_code == 403
    assert r.headers.get("deprecation") == "true"
    assert r.headers.get("link") == '</api/strategy/strategies/plans/42>; rel="successor-version"'
    line = next(rec.getMessage() for rec in caplog.records if "replaced route hit" in rec.getMessage())
    assert "use PATCH /strategies/plans/{strategy_plan_id}" in line and "td15-test" in line
    assert not any("deprecated route hit" in rec.getMessage() for rec in caplog.records)


def test_a_replaced_route_answers_as_before() -> None:
    """Marked, not changed: the PUT still runs (here: 503, no Postgres in the test app)."""
    reader = MagicMock()
    reader._config = operator_server_config()
    app = create_account_app(reader=reader, control_via_db=None, merged_config=reader._config)
    r = TestClient(app, raise_server_exceptions=False).put("/executions/-7", json={"strategy_instance_id": 3})
    assert r.status_code == 503 and r.json()["detail"] == "PostgreSQL is required to write account_executions."
    assert r.headers.get("link") == '</executions/-7/attribution>; rel="successor-version"'


def test_an_unmarked_route_is_left_alone() -> None:
    client = TestClient(_apps()["monitor"], raise_server_exceptions=False)
    assert "deprecation" not in client.get("/health").headers
