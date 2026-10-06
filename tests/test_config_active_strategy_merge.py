"""POST /config/active-strategy writes only the fields it was sent (debt TD-38)."""

from __future__ import annotations

from typing import Any, Dict, List

import pytest
from starlette.testclient import TestClient

import bifrost_api.monitor.routers.config as config_router
from bifrost_api.monitor.app import create_app
from tests.contract.helpers import operator_server_config
from tests.reader_mock import reader_mock


@pytest.fixture
def calls(monkeypatch: pytest.MonkeyPatch) -> List[Dict[str, Any]]:
    seen: List[Dict[str, Any]] = []

    def fake(_cfg: Any, **kw: Any) -> bool:
        seen.append(kw)
        return True

    monkeypatch.setattr(config_router, "write_active_strategy_and_gates", fake)
    return seen


def _client() -> TestClient:
    reader = reader_mock()
    reader.config = operator_server_config()
    app = create_app(
        reader=reader, control_via_db={"sink": "postgres"}, data_lag_threshold_ms=1000, merged_config=reader.config
    )
    return TestClient(app, raise_server_exceptions=False)


def test_allocation_only_leaves_structure_and_gate(calls: List[Dict[str, Any]]) -> None:
    r = _client().post("/config/active-strategy", json={"active_strategy_allocation_id": 7})
    assert r.status_code == 200
    assert r.json() == {"ok": True, "active_strategy_allocation_id": 7}
    assert calls[-1]["only"] == {"active_strategy_allocation_id"}


def test_an_explicit_null_still_clears(calls: List[Dict[str, Any]]) -> None:
    r = _client().post("/config/active-strategy", json={"active_strategy_structure_id": None, "active_strategy_allocation_id": 7})
    assert r.status_code == 200
    assert calls[-1]["only"] == {"active_strategy_structure_id", "active_strategy_allocation_id"}
    assert calls[-1]["active_strategy_structure_id"] is None


def test_an_empty_body_writes_nothing(calls: List[Dict[str, Any]]) -> None:
    r = _client().post("/config/active-strategy", json={})
    assert r.status_code == 400
    assert calls == []
