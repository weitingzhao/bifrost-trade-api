"""Contract: docs API health + OpenAPI shell."""

from __future__ import annotations

from unittest.mock import patch

from starlette.testclient import TestClient

from bifrost_api.docs_api.app import DOCS_PATH_PREFIX, create_docs_app

_FULL_SERVER = {
    "monitor_port": 8765,
    "massive_port": 8766,
    "docs_port": 8767,
    "ops_port": 8768,
    "trading_port": 8769,
    "strategy_port": 8770,
    "portfolio_port": 8771,
    "market_port": 8772,
    "research_port": 8773,
}


def _client() -> TestClient:
    app = create_docs_app(
        "http://127.0.0.1:8765/openapi.json",
        "http://127.0.0.1:8773/openapi.json",
        config={"server": dict(_FULL_SERVER)},
    )
    return TestClient(app, raise_server_exceptions=False)


def test_docs_health_shape() -> None:
    r = _client().get(f"{DOCS_PATH_PREFIX}/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["service"] == "bifrost-docs"
    assert body.get("massive_retired") is True
    assert "ts" in body


def test_docs_openapi_reachable() -> None:
    minimal = {"openapi": "3.0.0", "info": {"title": "T", "version": "1"}, "paths": {}}
    with patch("bifrost_api.docs_api.app.fetch_openapi", side_effect=[minimal, minimal]):
        r = _client().get(f"{DOCS_PATH_PREFIX}/openapi.json")
    assert r.status_code == 200
    assert "paths" in r.json()
