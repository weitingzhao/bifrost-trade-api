"""Every /strategies list carries ``count`` beside ``items`` (TD-17). Fixtures are invented."""

from __future__ import annotations

from typing import Any, Callable, Dict, List
from unittest.mock import MagicMock

import pytest
from starlette.testclient import TestClient

from bifrost_api.account.app import create_account_app
from tests import strategy_rows
from tests.contract.helpers import full_server_config
from tests.envelope_asserts import assert_list
from tests.reader_mock import reader_mock

# Lists with a response model (TD-24) take their reader's real answer; the others any rows.
_ANY_ROWS = lambda: [{"id": 1}, {"id": 2}]  # noqa: E731

LISTS = [
    ("/strategies/templates", "list_templates", _ANY_ROWS),
    ("/strategies/structures", "list_structures", _ANY_ROWS),
    ("/strategies/opportunities", "list_opportunities", strategy_rows.opportunities),
    ("/trades", "list_trades", strategy_rows.instances),
    ("/strategies/allocations", "list_allocations", strategy_rows.allocations),
    ("/gate-sets", "list_gate_safety_sets", strategy_rows.gate_sets),
]


def _client(reader: MagicMock) -> TestClient:
    reader.config = full_server_config()
    app = create_account_app(reader=reader, control_via_db=None, merged_config=reader.config)
    return TestClient(app, raise_server_exceptions=False)


@pytest.mark.parametrize("path, method, answer", LISTS)
def test_list_has_items_and_count(path: str, method: str, answer: Callable[[], List[Dict[str, Any]]]) -> None:
    reader = reader_mock()
    rows = answer()
    getattr(reader, method).return_value = rows
    assert_list(_client(reader).get(path), expected=strategy_rows.as_sent_before(rows)["items"])
