"""Portfolio config routes answer real statuses and the shared envelopes (TD-16, TD-17).

Every failure used to be 200 ``{"ok": false, "error"}``. Now: 503 without
Postgres or a connection, 400 for input, 404 when nothing matched, 409 for
executions under split allocations, 500 when the write failed; ``ok`` /
``error`` stay beside ``detail`` for one release. Fixtures are invented.
"""

from __future__ import annotations

from typing import Any, Dict, Optional
from unittest.mock import MagicMock

import pytest
from starlette.testclient import TestClient

from bifrost_api.account.app import create_account_app
from tests.contract.helpers import operator_server_config
from tests.envelope_asserts import assert_error, assert_list

PG = {"sink": "postgres"}


def _client(reader: MagicMock, control_via_db: Any = PG) -> TestClient:
    reader.config = operator_server_config()
    app = create_account_app(
        reader=reader,
        control_via_db=control_via_db,
        status_cfg_for_read=control_via_db,
        merged_config=reader.config,
    )
    return TestClient(app, raise_server_exceptions=False)


def _call(c: TestClient, method: str, path: str, body: Optional[Dict[str, Any]] = None) -> Any:
    if body is None:
        return c.request(method, path)
    return c.request(method, path, json=body)


# --- 503: no Postgres write config -------------------------------------------------

NO_PG_CASES = [
    ("POST", "/position-categories", {"name": "Core"}, {"id": None}),
    ("PATCH", "/executions/strategy-attribution", {"account_id": "U0000001", "contract_key": "ZZQ|STK|||"}, {}),
    ("PUT", "/position-categories/tag", {"account_id": "U0000001", "contract_key": "ZZQ|STK|||"}, {}),
    ("PUT", "/position-categories/symbol-order", {"category_name": "Core", "symbols": ["ZZQ"]}, {}),
    ("PUT", "/instrument-classes/ZZFI", {"instrument_class": "cash_like"}, {}),
]
# PATCH / DELETE position categories and DELETE instrument classes are TD-15 writes
# since 0.3.0; their 503 / 500 / 404 are in tests/test_write_semantics.py.


@pytest.mark.parametrize("method,path,body,legacy", NO_PG_CASES)
def test_without_postgres_every_write_is_503(method: str, path: str, body: Any, legacy: Dict[str, Any]) -> None:
    reader = MagicMock()
    r = _call(_client(reader, None), method, path, body)
    assert_error(r, 503, "Postgres required.", legacy)
    reader.create_position_category.assert_not_called()
    reader.batch_update_execution_strategy.assert_not_called()
    reader.set_instrument_class.assert_not_called()


# --- 400: input ---------------------------------------------------------------------

BAD_INPUT_CASES = [
    ("POST", "/position-categories", {"name": "  "}, "name is required.", {"id": None}),
    ("PATCH", "/executions/strategy-attribution", {"contract_key": "ZZQ|STK|||"}, "account_id is required.", {}),
    ("PATCH", "/executions/strategy-attribution", {"account_id": "U0000001"}, "contract_key or execution_ids", {}),
    ("PUT", "/position-categories/tag", {"contract_key": "ZZQ|STK|||"}, "account_id is required.", {}),
    ("PUT", "/position-categories/tag", {"account_id": "U0000001"}, "contract_key is required.", {}),
    ("PUT", "/position-categories/symbol-order", {"symbols": []}, "category_name is required.", {}),
    ("PUT", "/position-categories/symbol-order", {"category_name": "Core"}, "symbols must be an array.", {}),
    ("PUT", "/instrument-classes/ZZFI", {"instrument_class": "bond"}, "must be one of", {}),
]


@pytest.mark.parametrize("method,path,body,contains,legacy", BAD_INPUT_CASES)
def test_bad_input_is_400(method: str, path: str, body: Any, contains: str, legacy: Dict[str, Any]) -> None:
    reader = MagicMock()
    assert_error(_call(_client(reader), method, path, body), 400, contains, legacy)
    reader.set_instrument_class.assert_not_called()


# --- 500 / 503: the writer refused ------------------------------------------------------


def test_create_category_db_error_is_500_with_the_reason() -> None:
    reader = MagicMock()
    reader.create_position_category.return_value = (None, "value too long for type character varying")
    r = _client(reader).post("/position-categories", json={"name": "Core"})
    assert_error(r, 500, "value too long", {"id": None})


def test_create_category_without_a_connection_is_503() -> None:
    reader = MagicMock()
    reader.create_position_category.return_value = (None, "Database connection failed.")
    assert_error(_client(reader).post("/position-categories", json={"name": "Core"}), 503, "connection", {"id": None})


WRITE_FAILED_CASES = [
    (
        "PUT",
        "/position-categories/tag",
        {"account_id": "U0000001", "contract_key": "ZZQ|STK|||", "category_id": 3},
        "set_position_category_tag",
        False,
        "Failed to set tag.",
    ),
    (
        "PUT",
        "/position-categories/symbol-order",
        {"category_name": "Core", "symbols": ["ZZQ"]},
        "set_market_streams_symbol_order",
        False,
        "Failed to save symbol order.",
    ),
    (
        "PUT",
        "/instrument-classes/ZZFI",
        {"instrument_class": "cash_like"},
        "set_instrument_class",
        (False, "Failed to save the instrument class."),
        "Failed to save the instrument class.",
    ),
]


@pytest.mark.parametrize("method,path,body,writer,answer,message", WRITE_FAILED_CASES)
def test_a_failed_write_is_500(method: str, path: str, body: Any, writer: str, answer: Any, message: str) -> None:
    reader = MagicMock()
    getattr(reader, writer).return_value = answer
    assert_error(_call(_client(reader), method, path, body), 500, message)


def test_instrument_class_without_a_connection_is_503() -> None:
    reader = MagicMock()
    reader.set_instrument_class.return_value = (False, "Database connection failed.")
    r = _client(reader).put("/instrument-classes/ZZFI", json={"instrument_class": "stock"})
    assert_error(r, 503, "Database connection failed.")


# --- strategy attribution: 404 / 409 ----------------------------------------------------


def test_attribution_with_nothing_matched_is_404() -> None:
    reader = MagicMock()
    reader.batch_update_execution_strategy.return_value = 0
    r = _client(reader).patch(
        "/executions/strategy-attribution", json={"account_id": "U0000001", "execution_ids": [11, 12]}
    )
    assert_error(r, 404, "No matching executions", {"updated": 0})


def test_attribution_over_split_allocations_is_409_with_ok_and_error_now() -> None:
    reader = MagicMock()
    reader.batch_update_execution_strategy.return_value = -1
    r = _client(reader).patch(
        "/executions/strategy-attribution", json={"account_id": "U0000001", "contract_key": "ZZQ|STK|||"}
    )
    assert_error(r, 409, "instance_allocations", {"updated": 0})


def test_attribution_success_keeps_its_shape() -> None:
    reader = MagicMock()
    reader.batch_update_execution_strategy.return_value = 2
    r = _client(reader).patch(
        "/executions/strategy-attribution",
        json={"account_id": "U0000001", "execution_ids": [11, 12], "strategy_instance_id": 4},
    )
    assert r.status_code == 200 and r.json() == {"ok": True, "updated": 2}


# --- lists ------------------------------------------------------------------------------


def test_position_categories_list_has_items_and_count_and_no_ok() -> None:
    reader = MagicMock()
    rows = [{"id": 1, "name": "Core"}, {"id": 2, "name": "Hedge"}]
    reader.get_position_categories.return_value = rows
    # category_id beside id (TD-57, api 0.6.7)
    expected = [{**r, "category_id": r["id"]} for r in rows]
    body = assert_list(_client(reader).get("/position-categories"), expected=expected)
    assert body == {"items": expected, "count": 2}


def test_create_category_success_keeps_its_shape() -> None:
    reader = MagicMock()
    reader.create_position_category.return_value = (7, None)
    r = _client(reader).post("/position-categories", json={"name": "Core"})
    assert r.status_code == 200 and r.json() == {"ok": True, "id": 7, "category_id": 7, "name": "Core"}
