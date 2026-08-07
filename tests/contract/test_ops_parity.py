"""Contract: ops API health + celery capabilities shape."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from starlette.testclient import TestClient

from bifrost_api.ops.app import create_ops_app

_SERVER = {
    "monitor_port": 8765,
    "ops_port": 8768,
    "trading_port": 8769,
    "strategy_port": 8770,
    "portfolio_port": 8771,
    "market_port": 8772,
    "research_port": 8773,
    "massive_port": 8766,
    "docs_port": 8767,
}


def _client() -> TestClient:
    cfg = {"server": dict(_SERVER), "redis": {"enabled": True, "host": "127.0.0.1", "port": 6379}}
    app = create_ops_app(cfg)
    return TestClient(app, raise_server_exceptions=False)


def test_ops_health_shape() -> None:
    r = _client().get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["service"] == "bifrost-ops"


def test_ops_celery_capabilities_imports_worker_tasks() -> None:
    import bifrost_worker.data.bars.tasks  # noqa: F401
    from bifrost_api.ops.services.celery_capabilities import build_celery_capabilities_payload
    from bifrost_worker.celery.celery_app import app as celery_app

    out = build_celery_capabilities_payload(celery_app)
    assert out["ok"] is True
    assert out.get("beat_tasks") == []
    assert out.get("run_massive_job_matrix") == []
    assert out.get("massive_retired") is True
    names = {t["name"] for t in out.get("registered_tasks") or []}
    assert "src.bars.tasks.backfill_bars" in names
