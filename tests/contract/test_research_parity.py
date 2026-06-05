"""Contract: research API health + SEPA router import chain."""

from __future__ import annotations

from unittest.mock import MagicMock

from starlette.testclient import TestClient

from bifrost_api.research.app import create_research_app

_SERVER = {
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


def _client() -> TestClient:
    reader = MagicMock()
    reader._config = {"server": dict(_SERVER)}
    app = create_research_app(
        reader=reader,
        control_via_db=None,
        merged_config=reader._config,
    )
    return TestClient(app, raise_server_exceptions=False)


def test_research_health_shape() -> None:
    r = _client().get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["service"] == "bifrost-research"
    assert body.get("port") == 8773


def test_research_openapi_reachable() -> None:
    r = _client().get("/openapi.json")
    assert r.status_code == 200
    paths = r.json().get("paths") or {}
    assert len(paths) >= 1


def test_research_sepa_engine_importable() -> None:
    from bifrost_api.research.sepa.phase1_engine import Phase1Config, evaluate_phase1_batch

    assert Phase1Config is not None
    assert callable(evaluate_phase1_batch)
