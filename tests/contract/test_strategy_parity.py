"""Contract: strategy API health + OpenAPI."""

from __future__ import annotations

from unittest.mock import MagicMock

from starlette.testclient import TestClient

from bifrost_api.strategy.app import create_strategy_app

from tests.contract.helpers import full_server_config


def _client() -> TestClient:
    reader = MagicMock()
    reader._config = full_server_config()
    app = create_strategy_app(reader=reader, control_via_db=None, merged_config=reader._config)
    return TestClient(app, raise_server_exceptions=False)


def test_strategy_health_shape() -> None:
    r = _client().get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["service"] == "bifrost-strategy"
    assert body.get("port") == 8770


def test_strategy_openapi_prefixed() -> None:
    r = _client().get("/strategy/openapi.json")
    assert r.status_code == 200
    assert "paths" in r.json()
