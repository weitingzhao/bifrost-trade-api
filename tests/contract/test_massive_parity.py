"""Contract: massive API prefixed health + beat schedule."""

from __future__ import annotations

from unittest.mock import MagicMock

from starlette.testclient import TestClient

from bifrost_api.massive.app import create_massive_app

from tests.contract.helpers import full_server_config


def _client() -> TestClient:
    reader = MagicMock()
    reader._config = full_server_config()
    app = create_massive_app(reader=reader, control_via_db=None)
    return TestClient(app, raise_server_exceptions=False)


def test_massive_prefixed_health() -> None:
    r = _client().get("/research/massive/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["service"] == "bifrost-massive"
    assert body.get("port") == 8766


def test_massive_celery_beat_schedule() -> None:
    from bifrost_worker.data.massive.celery_queues import MASSIVE_QUEUES_DISABLED

    r = _client().get("/research/massive/celery-beat-schedule")
    assert r.status_code == 200
    body = r.json()
    assert body.get("ok") is True
    if MASSIVE_QUEUES_DISABLED:
        assert body.get("massive_queues_disabled") is True
        assert len(body.get("entries") or []) == 0
    else:
        assert len(body.get("entries") or []) >= 1


def test_massive_openapi_prefixed() -> None:
    r = _client().get("/research/massive/openapi.json")
    assert r.status_code == 200
    assert "paths" in r.json()
