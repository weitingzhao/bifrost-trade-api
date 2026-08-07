"""Tests for the Docs API (merged OpenAPI) — health and docs endpoints."""

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


def _minimal_openapi(title: str = "Test") -> dict:
    return {
        "openapi": "3.0.0",
        "info": {"title": title, "version": "1.0.0"},
        "paths": {"/x": {"get": {"responses": {"200": {"description": "ok"}}}}},
    }


def _make_client(
    *,
    config: dict | None = None,
    resolved_config_path: str | None = None,
) -> TestClient:
    base = {"server": dict(_FULL_SERVER)}
    if config:
        cfg = {**base, **config}
        if "server" in config:
            cfg["server"] = {**_FULL_SERVER, **config["server"]}
    else:
        cfg = base
    app = create_docs_app(
        "http://127.0.0.1:1/openapi.json",
        "http://127.0.0.1:3/openapi.json",
        config=cfg,
        resolved_config_path=resolved_config_path,
    )
    return TestClient(app, raise_server_exceptions=False)


class TestDocsHealth:
    def test_root_health(self):
        client = _make_client()
        r = client.get("/health")
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "ok"
        assert body["service"] == "bifrost-docs"
        assert "ts" in body
        assert "main_url" in body
        assert "research_url" in body
        assert "massive_url" not in body

    def test_prefixed_health(self):
        client = _make_client(config={"server": {"docs_port": 9902}})
        r = client.get(f"{DOCS_PATH_PREFIX}/health")
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "ok"
        assert body["service"] == "bifrost-docs"
        assert body["port"] == 9902

    def test_config_profile_dev(self, tmp_path):
        fake = tmp_path / "config.dev.yaml"
        fake.write_text("server: {}")
        client = _make_client(resolved_config_path=str(fake))
        body = client.get(f"{DOCS_PATH_PREFIX}/health").json()
        assert body["config_profile"] == "dev"
        assert "config_path" in body

    def test_config_profile_prod(self, tmp_path):
        fake = tmp_path / "config.prod.yaml"
        fake.write_text("server: {}")
        client = _make_client(resolved_config_path=str(fake))
        body = client.get(f"{DOCS_PATH_PREFIX}/health").json()
        assert body["config_profile"] == "prod"

    def test_config_profile_absent(self):
        client = _make_client()
        body = client.get(f"{DOCS_PATH_PREFIX}/health").json()
        assert "config_profile" not in body
        assert body.get("port") == 8767


class TestDocsOpenApi:
    def test_prefixed_openapi_json(self):
        client = _make_client()
        with patch("bifrost_api.docs_api.app.fetch_openapi") as m:
            m.side_effect = [
                _minimal_openapi("Main"),
                _minimal_openapi("Research"),
            ]
            r = client.get(f"{DOCS_PATH_PREFIX}/openapi.json")
        assert r.status_code == 200
        spec = r.json()
        assert "paths" in spec

    def test_swagger_ui_prefixed(self):
        client = _make_client()
        with patch("bifrost_api.docs_api.app.fetch_openapi") as m:
            m.side_effect = [_minimal_openapi(), _minimal_openapi()]
            r = client.get(f"{DOCS_PATH_PREFIX}/docs")
        assert r.status_code == 200
        assert "swagger" in r.text.lower() or "text/html" in r.headers.get("content-type", "")

    def test_redoc_prefixed(self):
        client = _make_client()
        with patch("bifrost_api.docs_api.app.fetch_openapi") as m:
            m.side_effect = [_minimal_openapi(), _minimal_openapi()]
            r = client.get(f"{DOCS_PATH_PREFIX}/redoc")
        assert r.status_code == 200
