"""Contract: market API health + quotes stream route registration."""

from __future__ import annotations

from unittest.mock import MagicMock

from starlette.testclient import TestClient

from bifrost_api.market.app import create_market_app

from tests.contract.helpers import full_server_config


def _client() -> TestClient:
    reader = MagicMock()
    reader._config = {**full_server_config(), "redis": {"enabled": False}}
    app = create_market_app(
        reader=reader,
        control_via_db=None,
        merged_config=reader._config,
    )
    return TestClient(app, raise_server_exceptions=False)


def test_market_health_shape() -> None:
    r = _client().get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["service"] == "bifrost-market"
    assert body.get("port") == 8772


def test_market_openapi_has_quotes_paths() -> None:
    spec = _client().get("/market/openapi.json").json()
    paths = spec.get("paths") or {}
    assert any("quotes" in p or "watchlist" in p for p in paths)
