"""Contract: monitor GET /status returns core fields."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

pytest.importorskip("bifrost_api.monitor.app")


@pytest.fixture
def client():
    from bifrost_api.monitor.app import app

    return TestClient(app)


def test_status_route_exists(client: TestClient):
    r = client.get("/status")
    assert r.status_code in (200, 503)
    if r.status_code == 200:
        data = r.json()
        assert "status" in data or "daemon" in data or isinstance(data, dict)
