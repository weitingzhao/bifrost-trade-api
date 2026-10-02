"""Every /strategies list carries ``count`` beside ``items`` (TD-17). Fixtures are invented."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from starlette.testclient import TestClient

from bifrost_api.account.app import create_account_app
from tests.contract.helpers import full_server_config
from tests.envelope_asserts import assert_list

LISTS = [
    ("/strategies/templates", "list_templates"),
    ("/strategies/structures", "list_structures"),
    ("/strategies/opportunities", "list_opportunities"),
    ("/strategies/instances", "list_strategy_instances"),
    ("/strategies/instances/5/open-option-legs", "get_instance_open_option_legs"),
    ("/strategies/allocations", "list_allocations"),
    ("/strategies/gate-safety", "list_gate_safety_sets"),
]


def _client(reader: MagicMock) -> TestClient:
    reader._config = full_server_config()
    app = create_account_app(reader=reader, control_via_db=None, merged_config=reader._config)
    return TestClient(app, raise_server_exceptions=False)


@pytest.mark.parametrize("path, method", LISTS)
def test_list_has_items_and_count(path: str, method: str) -> None:
    reader = MagicMock()
    rows = [{"id": 1}, {"id": 2}]
    getattr(reader, method).return_value = rows
    assert_list(_client(reader).get(path), expected=rows)


def test_open_option_legs_keeps_the_instance_id() -> None:
    reader = MagicMock()
    reader.get_instance_open_option_legs.return_value = []
    body = assert_list(_client(reader).get("/strategies/instances/5/open-option-legs"), expected=[])
    assert body == {"items": [], "count": 0, "strategy_instance_id": 5}
