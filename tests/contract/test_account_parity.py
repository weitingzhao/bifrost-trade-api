"""Contract: account API health + OpenAPI (merged trading + portfolio)."""

from __future__ import annotations

from unittest.mock import MagicMock

from starlette.testclient import TestClient

from bifrost_api.account.app import create_account_app
from tests.contract.helpers import full_server_config


def _client() -> TestClient:
    reader = MagicMock()
    reader._config = full_server_config()
    app = create_account_app(reader=reader, control_via_db=None, merged_config=reader._config)
    return TestClient(app, raise_server_exceptions=False)


def test_account_health_shape() -> None:
    r = _client().get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["service"] == "bifrost-account"
    assert body.get("port") == 8769


def test_account_openapi_includes_merged_paths() -> None:
    r = _client().get("/account/openapi.json")
    assert r.status_code == 200
    paths = r.json()["paths"]
    assert any("/executions" in p or p.startswith("/executions") for p in paths)
    assert any("portfolio" in p or "position-categories" in p for p in paths)
    assert any("/strategies" in p or p.startswith("/strategies") for p in paths)
