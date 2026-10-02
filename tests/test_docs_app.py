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
        # docs_port names a port no pod listens on; monitor serves the docs routes (TD-64).
        assert "port" not in body

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


class TestDocsAggregateCoversEveryApp:
    """TD-28: account and market were never merged, and the research spec was read from 127.0.0.1."""

    def _app(self) -> TestClient:
        app = create_docs_app(
            "http://main/openapi.json",
            "http://research/openapi.json",
            extra_openapi_urls={"Account": "http://account/openapi.json", "Market": "http://market/openapi.json"},
            config={"server": dict(_FULL_SERVER)},
        )
        return TestClient(app, raise_server_exceptions=False)

    def test_merges_research_account_and_market(self):
        specs = {
            "http://main/openapi.json": {**_minimal_openapi("Main"), "paths": {"/status": {}}},
            "http://research/openapi.json": {**_minimal_openapi("Research"), "paths": {"/research/x": {}}},
            "http://account/openapi.json": {**_minimal_openapi("Account"), "paths": {"/strategies/plans": {}}},
            "http://market/openapi.json": {**_minimal_openapi("Market"), "paths": {"/market/quotes": {}}},
        }
        with patch("bifrost_api.docs_api.app.fetch_openapi", side_effect=lambda url: specs[url]):
            body = self._app().get(f"{DOCS_PATH_PREFIX}/openapi.json").json()
        assert set(body["paths"]) == {"/status", "/research/x", "/strategies/plans", "/market/quotes"}
        assert "x-bifrost-unreachable" not in body

    def test_an_unreachable_secondary_is_named_not_fatal(self):
        def fetch(url):
            if "market" in url:
                raise OSError("connection refused")
            return _minimal_openapi(url)

        with patch("bifrost_api.docs_api.app.fetch_openapi", side_effect=fetch):
            r = self._app().get(f"{DOCS_PATH_PREFIX}/openapi.json")
        assert r.status_code == 200
        assert list(r.json()["x-bifrost-unreachable"]) == ["Market"]

    def test_in_kubernetes_siblings_are_reached_by_service_name(self, monkeypatch):
        from bifrost_api.docs_api.app import _sibling_openapi

        monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
        assert _sibling_openapi("api-research", 8773, "/openapi.json") == "http://127.0.0.1:8773/openapi.json"
        monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.43.0.1")
        assert _sibling_openapi("api-account", 8769, "/account/openapi.json") == "http://api-account:8769/account/openapi.json"
