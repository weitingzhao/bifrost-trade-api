"""Contract: monitor API /status and app bootstrap."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from starlette.testclient import TestClient

from bifrost_api.monitor.app import create_app

from tests.contract.helpers import full_server_config


def _client() -> TestClient:
    reader = MagicMock()
    reader._config = full_server_config()
    app = create_app(
        reader=reader,
        control_via_db=None,
        data_lag_threshold_ms=5000,
        merged_config=reader._config,
    )
    return TestClient(app, raise_server_exceptions=False)


def test_monitor_status_route_exists() -> None:
    with patch("bifrost_api.monitor.routers.status._assemble_status_v3") as mock_asm:
        mock_asm.return_value = {"status": "ok", "status_schema_version": 9}
        r = _client().get("/status")
    assert r.status_code in (200, 503)


def test_monitor_openapi_reachable() -> None:
    r = _client().get("/openapi.json")
    assert r.status_code == 200
    spec = r.json()
    assert "paths" in spec
    assert "/status" in spec.get("paths", {})
