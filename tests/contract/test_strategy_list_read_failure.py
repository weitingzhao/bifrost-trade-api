"""Every /strategies list answers 503 when its read fails — never 200 {items: []} (debt TD-08)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from starlette.testclient import TestClient

from bifrost_api.account.app import create_account_app
from bifrost_core.monitor.reader.errors import ReadFailed
from tests.contract.helpers import full_server_config
from tests.reader_mock import reader_mock

LISTS = [
    ("/strategies/structures", "list_structures"),
    ("/strategies/opportunities", "list_opportunities"),
    ("/strategies/allocations", "list_allocations"),
    ("/gate-sets", "list_gate_safety_sets"),
    ("/trades", "list_trades"),
]


def _client(reader: MagicMock) -> TestClient:
    reader.config = full_server_config()
    app = create_account_app(reader=reader, control_via_db=None, merged_config=reader.config)
    return TestClient(app, raise_server_exceptions=False)


@pytest.mark.parametrize("path, method", LISTS)
def test_a_failed_read_is_503_with_the_reason(path: str, method: str) -> None:
    reader = reader_mock()
    getattr(reader, method).side_effect = ReadFailed(f"{method}: database unavailable")
    r = _client(reader).get(path)
    assert r.status_code == 503
    assert r.json() == {"detail": f"{method}: database unavailable", "reason": "read_failed"}


@pytest.mark.parametrize("path, method", LISTS)
def test_an_empty_read_is_still_200(path: str, method: str) -> None:
    reader = reader_mock()
    getattr(reader, method).return_value = []
    r = _client(reader).get(path)
    assert r.status_code == 200
    assert r.json()["items"] == []
