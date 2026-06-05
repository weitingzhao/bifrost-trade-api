"""Contract: portfolio API health + OpenAPI."""

from __future__ import annotations

from unittest.mock import MagicMock

from starlette.testclient import TestClient

from bifrost_api.portfolio.app import create_portfolio_app

from tests.contract.helpers import full_server_config


def _client() -> TestClient:
    reader = MagicMock()
    reader._config = full_server_config()
    app = create_portfolio_app(reader=reader, control_via_db=None, merged_config=reader._config)
    return TestClient(app, raise_server_exceptions=False)


def test_portfolio_health_shape() -> None:
    r = _client().get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["service"] == "bifrost-portfolio"
    assert body.get("port") == 8771


def test_portfolio_openapi_prefixed() -> None:
    r = _client().get("/portfolio/openapi.json")
    assert r.status_code == 200
    assert "paths" in r.json()
