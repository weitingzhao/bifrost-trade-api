"""Portfolio config routes answer real statuses and the shared envelopes (TD-16, TD-17).

Every failure used to be 200 ``{"ok": false, "error"}``. Now a failure is
``{"detail"}`` only. Since api 0.9.0 (core 0.47.0, TD-80 C2) POST / PUT call core's
strict writers, so they answer as PATCH / DELETE do: 503 without Postgres or a
connection, 400 for input core refuses (before it connects), 409 for a name in use,
500 when a statement failed (rolled back). Success keeps ``ok: true`` beside what was
written. Fixtures are invented.
"""

from __future__ import annotations

from typing import Any, Dict, Optional
from unittest.mock import MagicMock

import pytest
from starlette.testclient import TestClient

import psycopg2
from bifrost_api.account.app import create_account_app
from bifrost_core.monitor.reader import write_support
from bifrost_core.portfolio.reader import instrument_class
from bifrost_core.portfolio.reader import position_categories
from tests.contract.helpers import operator_server_config
from tests.envelope_asserts import assert_error, assert_list
from tests.reader_mock import reader_mock

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
    ("PUT", "/position-categories/tag", {"account_id": "U0000001", "contract_key": "ZZQ|STK|||"}, {}),
    ("PUT", "/position-categories/symbol-order", {"category_name": "Core", "symbols": ["ZZQ"]}, {}),
    ("PUT", "/instrument-classes/ZZFI", {"instrument_class": "cash_like"}, {}),
]
# PATCH / DELETE position categories and DELETE instrument classes are TD-15 writes
# since 0.3.0; their 503 / 500 / 404 are in tests/test_write_semantics.py. POST / PUT
# call core's strict twins since api 0.9.0 (core 0.47.0, TD-80 C2) and answer the same way.


@pytest.mark.parametrize("method,path,body,legacy", NO_PG_CASES)
def test_without_postgres_every_write_is_503(
    monkeypatch: pytest.MonkeyPatch, method: str, path: str, body: Any, legacy: Dict[str, Any]
) -> None:
    connect = MagicMock()
    monkeypatch.setattr(write_support, "connect", connect)
    r = _call(_client(MagicMock(), None), method, path, body)
    assert_error(r, 503, "Postgres is not configured.", legacy)
    connect.assert_not_called()


# --- 400: input core refuses before it connects ------------------------------------------------

BAD_INPUT_CASES = [
    ("POST", "/position-categories", {"name": "  "}, "name is required.", {"id": None}),
    ("POST", "/position-categories", {"name": "Uncategorized"}, "reserved", {"id": None}),
    ("POST", "/position-categories", {"name": "Core", "description": " "}, "description is blank", {}),
    ("PUT", "/position-categories/tag", {"contract_key": "ZZQ|STK|||", "category_id": 3}, "account_id is required.", {}),
    ("PUT", "/position-categories/tag", {"account_id": "U0000001", "category_id": 3}, "contract_key is required.", {}),
    ("PUT", "/position-categories/tag", {"account_id": "U0000001", "contract_key": "ZZQ|STK|||"},
     "category_id is required", {}),
    ("PUT", "/position-categories/symbol-order", {"symbols": []}, "category_name is required.", {}),
    ("PUT", "/position-categories/symbol-order", {"category_name": "Core"}, "symbols cannot be null", {}),
    ("PUT", "/position-categories/symbol-order", {"category_name": "Core", "symbols": ["ZZQ", "ZZQ"]},
     "more than once", {}),
    ("PUT", "/position-categories/symbol-order", {"category_name": "Core", "symbols": ["ZZQ", " "]},
     "symbols[1] is required", {}),
    ("PUT", "/instrument-classes/ZZFI", {"instrument_class": "bond"}, "must be one of", {}),
    ("PUT", "/instrument-classes/ZZFI", {"instrument_class": "stock", "note": " "}, "note is blank", {}),
]


@pytest.mark.parametrize("method,path,body,contains,legacy", BAD_INPUT_CASES)
def test_bad_input_is_400(
    monkeypatch: pytest.MonkeyPatch, method: str, path: str, body: Any, contains: str, legacy: Dict[str, Any]
) -> None:
    connect = MagicMock()
    monkeypatch.setattr(write_support, "connect", connect)
    assert_error(_call(_client(MagicMock()), method, path, body), 400, contains, legacy)
    connect.assert_not_called()  # refused before any connection: nothing written


# --- the writer's outcomes ------------------------------------------------------------------

WRITES = [
    ("POST", "/position-categories", {"name": "Core"}),
    ("PUT", "/position-categories/tag", {"account_id": "U0000001", "contract_key": "ZZQ|STK|||", "category_id": 3}),
    ("PUT", "/position-categories/symbol-order", {"category_name": "Core", "symbols": ["ZZQ"]}),
    ("PUT", "/instrument-classes/ZZFI", {"instrument_class": "cash_like"}),
]


def _connection(execute_error: Optional[BaseException] = None, fetchone: Any = None) -> MagicMock:
    conn = MagicMock()
    cur = conn.cursor.return_value.__enter__.return_value
    cur.execute.side_effect = execute_error
    cur.fetchone.return_value = fetchone
    return conn


@pytest.mark.parametrize("method,path,body", WRITES)
def test_an_unreachable_database_is_503(monkeypatch: pytest.MonkeyPatch, method: str, path: str, body: Any) -> None:
    def refuse(params: Any, golden: bool = False) -> Any:
        raise psycopg2.OperationalError("could not connect")

    monkeypatch.setattr(write_support, "connect", refuse)
    assert_error(_call(_client(MagicMock()), method, path, body), 503, "unreachable")


@pytest.mark.parametrize("method,path,body", WRITES)
def test_a_failed_statement_is_500_and_rolled_back(
    monkeypatch: pytest.MonkeyPatch, method: str, path: str, body: Any
) -> None:
    conn = _connection(execute_error=psycopg2.OperationalError("server closed the connection unexpectedly"))
    monkeypatch.setattr(write_support, "connect", lambda params, golden=False: conn)
    assert_error(_call(_client(MagicMock()), method, path, body), 500, "the database write failed")
    conn.commit.assert_not_called()
    conn.rollback.assert_called()
    conn.close.assert_called_once()


def test_a_category_name_in_use_is_409(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _connection(fetchone=(1,))  # the name check finds another category
    monkeypatch.setattr(write_support, "connect", lambda params, golden=False: conn)
    assert_error(_client(MagicMock()).post("/position-categories", json={"name": "Core"}), 409, "already exists")
    conn.commit.assert_not_called()


def test_tagging_with_a_category_that_does_not_exist_is_400(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _connection(fetchone=None)  # no such category
    monkeypatch.setattr(write_support, "connect", lambda params, golden=False: conn)
    body = {"account_id": "U0000001", "contract_key": "ZZQ|STK|||", "category_id": 3}
    assert_error(_client(MagicMock()).put("/position-categories/tag", json=body), 400, "No position category 3.")
    conn.commit.assert_not_called()


# --- lists ------------------------------------------------------------------------------


def test_position_categories_list_has_items_and_count_and_no_ok() -> None:
    reader = reader_mock()
    rows = [{"id": 1, "name": "Core"}, {"id": 2, "name": "Hedge"}]
    reader.get_position_categories.return_value = rows
    # Keyed category_id only (TD-57 api 0.6.7 sent both; id dropped in 0.6.12, TD-56)
    expected = [{"name": r["name"], "category_id": r["id"]} for r in rows]
    body = assert_list(_client(reader).get("/position-categories"), expected=expected)
    assert body == {"items": expected, "count": 2}


def test_create_category_answers_the_row_keyed_category_id(monkeypatch: pytest.MonkeyPatch) -> None:
    create = MagicMock(return_value={"id": 7, "name": "Core", "description": None, "sort_order": 2})
    monkeypatch.setattr(position_categories, "create_position_category_strict", create)
    r = _client(MagicMock()).post("/position-categories", json={"name": "Core", "sort_order": 2})
    assert r.status_code == 200
    # ok / category_id / name as before (useEnsureWatchlistCategories, SharesBand), plus the row.
    assert r.json() == {"ok": True, "category_id": 7, "name": "Core", "description": None, "sort_order": 2}
    assert create.call_args.args == (PG, "Core") and create.call_args.kwargs == {"description": None, "sort_order": 2}


def test_puts_answer_what_was_written_with_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    tag = {"account_id": "U0000001", "contract_key": "ZZQ|STK|||", "category_id": 3, "cleared": False}
    order = {"category_name": "Core", "symbols": ["ZZQ"]}
    row = {"contract_key": "ZZFI", "instrument_class": "cash_like", "note": "T-bill fund"}
    monkeypatch.setattr(position_categories, "set_position_category_tag_strict", MagicMock(return_value=tag))
    monkeypatch.setattr(position_categories, "set_market_streams_symbol_order_strict", MagicMock(return_value=order))
    put_class = MagicMock(return_value=row)
    monkeypatch.setattr(instrument_class, "set_instrument_class_strict", put_class)
    c = _client(MagicMock())
    sent = {k: v for k, v in tag.items() if k != "cleared"}
    assert c.put("/position-categories/tag", json=sent).json() == {**tag, "ok": True}
    assert c.put("/position-categories/symbol-order", json=order).json() == {**order, "ok": True}
    r = c.put("/instrument-classes/ZZFI", json={"instrument_class": "cash_like", "note": "T-bill fund"})
    assert r.json() == {**row, "ok": True}
    assert put_class.call_args.args == (PG, "ZZFI", "cash_like") and put_class.call_args.kwargs == {"note": "T-bill fund"}
