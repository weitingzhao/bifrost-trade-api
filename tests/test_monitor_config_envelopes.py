"""Monitor config failures keep their statuses and add ``detail`` / ``ok: false`` (TD-16, TD-17)."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from starlette.testclient import TestClient

import bifrost_api.monitor.routers.config as config_router
from bifrost_api.monitor.app import create_app
from tests.contract.helpers import operator_server_config
from tests.envelope_asserts import assert_error


PG = {"sink": "postgres"}


def _client(control_via_db: Any = PG) -> TestClient:
    reader = MagicMock()
    reader.config = operator_server_config()
    reader.get_ib_config.return_value = {}
    app = create_app(reader=reader, control_via_db=control_via_db, data_lag_threshold_ms=1000, merged_config=reader.config)
    return TestClient(app, raise_server_exceptions=False)


@pytest.mark.parametrize(
    "path,body",
    [("/config/ib", {"ib_host_account_id": "U0000001"}), ("/config/active-strategy", {"active_strategy_allocation_id": 7})],
)
def test_without_postgres_is_503(path: str, body: Any) -> None:
    assert_error(_client(None).post(path, json=body), 503, "postgres required")


def test_ib_write_failure_is_500(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config_router, "write_ib_config", lambda *_a: False)
    assert_error(_client().post("/config/ib", json={"ib_host_account_id": "U0000001"}), 500, "failed to write settings")


def test_active_strategy_empty_body_is_400() -> None:
    assert_error(_client().post("/config/active-strategy", json={}), 400, "no active_* field")


def test_active_strategy_write_failure_is_500(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config_router, "write_active_strategy_and_gates", lambda *_a, **_k: False)
    r = _client().post("/config/active-strategy", json={"active_strategy_allocation_id": 7})
    assert_error(r, 500, "failed to write active strategy")


def test_active_strategy_unknown_reference_is_409(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*_a: Any, **_k: Any) -> bool:
        raise ValueError("active_strategy_allocation_id=7 does not exist in strategy_allocation")

    monkeypatch.setattr(config_router, "write_active_strategy_and_gates", refuse)
    r = _client().post("/config/active-strategy", json={"active_strategy_allocation_id": 7})
    assert_error(r, 409, "does not exist")
