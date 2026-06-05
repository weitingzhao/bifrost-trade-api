"""Tests for the slim Research FastAPI app (option discovery + max pain only)."""

from __future__ import annotations

from unittest.mock import MagicMock

from starlette.testclient import TestClient

from bifrost_api.research.app import create_research_app

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
    "skip_monitor_ib": True,
}


def _make_client(
    resolved_config_path: str | None = None,
    reader_config: dict | None = None,
    merged_config: dict | None = None,
) -> TestClient:
    reader = MagicMock()
    base = {"server": dict(_FULL_SERVER)}
    if reader_config is not None:
        rc = {**base, **reader_config}
        if "server" in reader_config:
            rc["server"] = {**_FULL_SERVER, **reader_config["server"]}
        reader._config = rc
    else:
        reader._config = base

    if merged_config is None:
        mc: dict = reader._config
    else:
        mc = {**merged_config}
        if "server" in merged_config:
            mc["server"] = {**_FULL_SERVER, **merged_config["server"]}
        else:
            mc["server"] = dict(_FULL_SERVER)

    app = create_research_app(
        reader=reader,
        control_via_db=None,
        resolved_config_path=resolved_config_path,
        merged_config=mc,
    )
    return TestClient(app, raise_server_exceptions=False)


class TestResearchHealth:
    def test_root_health(self):
        client = _make_client(merged_config={"server": {}})
        r = client.get("/health")
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "ok"
        assert body["service"] == "bifrost-research"
        assert "ts" in body

    def test_health_port_from_config(self):
        client = _make_client(merged_config={"server": {"research_port": 9903}})
        body = client.get("/health").json()
        assert body["port"] == 9903

    def test_config_profile_dev(self, tmp_path):
        fake = tmp_path / "config.dev.yaml"
        fake.write_text("server: {}")
        client = _make_client(resolved_config_path=str(fake), merged_config={"server": {}})
        body = client.get("/health").json()
        assert body["config_profile"] == "dev"


class TestResearchOpenApi:
    def test_openapi_json_reachable(self):
        client = _make_client(merged_config={"server": {}})
        r = client.get("/openapi.json")
        assert r.status_code == 200
        spec = r.json()
        assert "paths" in spec

    def test_no_massive_stream_route(self):
        """Massive SSE lives on backend.massive only."""
        client = _make_client(merged_config={"server": {}})
        spec = client.get("/openapi.json").json()
        paths = spec.get("paths") or {}
        assert "/research/massive/stream" not in paths

