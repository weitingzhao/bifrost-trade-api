"""Watchlist routes answer real statuses and the shared envelopes (TD-16, TD-17). Fixtures are invented."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from starlette.testclient import TestClient

from bifrost_api.market.app import create_market_app
from tests.contract.helpers import operator_server_config
from tests.envelope_asserts import assert_error, assert_list

PG = {"sink": "postgres"}


def _client(reader: MagicMock, control_via_db: Any = PG) -> TestClient:
    reader._config = {**operator_server_config(), "redis": {"enabled": False}}
    app = create_market_app(reader=reader, control_via_db=control_via_db, merged_config=reader._config)
    return TestClient(app, raise_server_exceptions=False)


def test_list_has_items_and_count() -> None:
    reader = MagicMock()
    rows = [{"contract_key": "ZZQ|STK|||"}, {"contract_key": "ZZR|STK|||"}]
    reader.get_watchlist.return_value = rows
    assert_list(_client(reader).get("/watchlist"), expected=rows)


@pytest.mark.parametrize(
    "method,path,body,contains",
    [
        ("POST", "/watchlist", {"contract_key": "ZZQ"}, "write watchlist"),
        ("DELETE", "/watchlist?contract_key=ZZQ%7CSTK%7C%7C%7C", None, "modify watchlist"),
    ],
)
def test_without_postgres_is_503(method: str, path: str, body: Any, contains: str) -> None:
    reader = MagicMock()
    c = _client(reader, None)
    r = c.request(method, path, json=body) if body is not None else c.request(method, path)
    assert_error(r, 503, contains)
    reader.add_watchlist.assert_not_called()
    reader.delete_watchlist.assert_not_called()


def test_post_blank_key_is_400() -> None:
    reader = MagicMock()
    assert_error(_client(reader).post("/watchlist", json={"contract_key": "  "}), 400, "contract_key is required.")
    reader.add_watchlist.assert_not_called()


def test_delete_without_key_is_400() -> None:
    assert_error(_client(MagicMock()).delete("/watchlist"), 400, "Provide contract_key")


def test_post_write_failure_is_500() -> None:
    reader = MagicMock()
    reader.add_watchlist.return_value = False
    assert_error(_client(reader).post("/watchlist", json={"contract_key": "ZZQ"}), 500, "Failed to write watchlist.")


def test_delete_failure_is_500() -> None:
    reader = MagicMock()
    reader.delete_watchlist.return_value = False
    assert_error(_client(reader).delete("/watchlist?contract_key=ZZQ%7CSTK%7C%7C%7C"), 500, "Delete failed")


def test_post_success_keeps_its_shape() -> None:
    reader = MagicMock()
    reader.add_watchlist.return_value = True
    r = _client(reader).post("/watchlist", json={"contract_key": "ZZQ"})
    assert r.status_code == 200 and r.json() == {"ok": True, "message": "Watchlist item added or updated."}
