"""Every write on a deployed Trade API app needs a role (debt TD-23).

The guard is ``bifrost_api.write_guard``: operator for a write, admin for a
process exit or an IB disconnect / reconnect, nothing for the few POSTs that only
read. Tokens here are invented; nothing in this file reaches IB, Redis or a DB.
"""

from __future__ import annotations

from typing import Any, Dict, Iterator, Optional
from unittest.mock import MagicMock

import pytest
from starlette.testclient import TestClient

from bifrost_api.account.app import create_account_app
from bifrost_api.market.app import create_market_app
from bifrost_api.monitor.app import create_app as create_monitor_app
from bifrost_api.research.app import create_research_app
from bifrost_api.write_guard import ADMIN_PATHS, READ_ONLY_POSTS, WriteGuard, required_role
from tests.contract.helpers import full_server_config

OPERATOR = "test-operator-token-0001"
ADMIN = "test-admin-token-0002"


@pytest.fixture(autouse=True)
def _tokens(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPS_OPERATOR_TOKEN", OPERATOR)
    monkeypatch.setenv("OPS_ADMIN_TOKEN", ADMIN)


def _config(default_role: Optional[str] = None) -> Dict[str, Any]:
    cfg: Dict[str, Any] = {**full_server_config(), "redis": {"enabled": False}}
    if default_role:
        cfg["ops"] = {"auth": {"default_role": default_role}}
    return cfg


def _apps(default_role: Optional[str] = None) -> Dict[str, Any]:
    def reader() -> MagicMock:
        r = MagicMock()
        r._config = _config(default_role)
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


def _client(app_name: str, default_role: Optional[str] = None) -> TestClient:
    return TestClient(_apps(default_role)[app_name], raise_server_exceptions=False)


def _bearer(token: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _write_routes(app: Any) -> Iterator[tuple]:
    for r in app.routes:
        for method in getattr(r, "methods", None) or ():
            if method not in ("GET", "HEAD", "OPTIONS"):
                yield method, r.path


# ── the policy ──


@pytest.mark.parametrize(
    "method, path, role",
    [
        ("GET", "/strategies/allocations", None),
        ("HEAD", "/status", None),
        ("OPTIONS", "/strategies/allocations", None),
        ("POST", "/strategies/allocations", "operator"),
        ("DELETE", "/strategies/gate-safety/5", "operator"),
        ("POST", "/control/flatten", "operator"),
        ("POST", "/research/screener", None),
        ("POST", "/research/screener/", None),
        ("POST", "/control/monitor_stop", "admin"),
        ("POST", "/account/shutdown", "admin"),
    ],
)
def test_required_role(method: str, path: str, role: Optional[str]) -> None:
    assert required_role(method, path) == role


def test_every_deployed_app_runs_the_guard() -> None:
    for name, app in _apps().items():
        assert any(m.cls is WriteGuard for m in app.user_middleware), name


def test_every_named_exception_is_a_real_write_route() -> None:
    """A path that no longer exists is a stale exception, and a typo is a write left at operator."""
    served = {path for app in _apps().values() for _, path in _write_routes(app)}
    assert not sorted((READ_ONLY_POSTS | ADMIN_PATHS) - served)


# ── what a caller gets ──


def test_anonymous_viewer_cannot_write_but_can_read() -> None:
    c = _client("account", "viewer")
    r = c.post("/strategies/saved-searches", json={"name": "x", "query": {}})
    assert r.status_code == 403
    body = r.json()
    assert body["required_role"] == "operator" and body["current_role"] == "viewer"
    assert c.get("/strategies/saved-searches").status_code != 403


def test_an_operator_token_writes() -> None:
    r = _client("account", "viewer").post(
        "/strategies/saved-searches", json={"name": "x", "query": {}}, headers=_bearer(OPERATOR)
    )
    assert r.status_code != 403


def test_an_unknown_token_is_a_viewer() -> None:
    r = _client("account", "operator").post(
        "/strategies/saved-searches", json={"name": "x", "query": {}}, headers=_bearer("not-a-token")
    )
    assert r.status_code == 403
    assert r.json()["current_role"] == "viewer"


def test_a_token_in_the_query_string_is_ignored() -> None:
    r = _client("account", "viewer").post(
        f"/strategies/saved-searches?token={OPERATOR}", json={"name": "x", "query": {}}
    )
    assert r.status_code == 403


def test_a_read_only_post_is_open_to_a_viewer() -> None:
    r = _client("account", "viewer").post("/executions/option-stock-links/query", json={})
    assert r.status_code != 403


@pytest.mark.parametrize(
    "app_name, method, path",
    [
        ("monitor", "POST", "/config/active-strategy"),
        ("account", "DELETE", "/executions/1"),
        ("market", "DELETE", "/watchlist"),
        ("research", "POST", "/research/feedback/reports"),
    ],
)
def test_each_app_refuses_an_anonymous_viewer(app_name: str, method: str, path: str) -> None:
    r = _client(app_name, "viewer").request(method, path, json={})
    assert r.status_code == 403


def test_disconnecting_ib_needs_admin() -> None:
    c = _client("monitor", "viewer")
    refused = c.post("/control/monitor_connect", headers=_bearer(OPERATOR))
    assert refused.status_code == 403 and refused.json()["required_role"] == "admin"
    # Past the guard the handler finds no IB Operator client in a test app and says so.
    assert c.post("/control/monitor_connect", headers=_bearer(ADMIN)).status_code == 503


def test_a_route_appended_onto_monitor_is_guarded() -> None:
    """The docs routes are appended as route objects; a router dependency would miss them."""
    r = _client("monitor", "viewer").post("/research/docs/shutdown", headers=_bearer(OPERATOR))
    assert r.status_code == 403 and r.json()["required_role"] == "admin"


def test_default_role_operator_keeps_todays_behaviour() -> None:
    """STG and PROD run default_role operator until each env lowers it."""
    r = _client("account", "operator").post("/strategies/saved-searches", json={"name": "x", "query": {}})
    assert r.status_code != 403
