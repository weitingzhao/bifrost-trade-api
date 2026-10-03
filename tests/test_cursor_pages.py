"""Cursor paging on GET /executions and GET /transactions (TD-51, api 0.6.9, core 0.40.0).

``cursor`` goes to the reader's page method; ``next_cursor`` comes back (null on the last
page); a page after the first has no ``total``; a cursor the API did not issue is 400
``{detail}``; ``include_opt_pairs`` refuses a cursor; ``/transactions`` caps ``limit`` at
10000. Callers that send no cursor read the same rows as before. The real paging through
ties and NULLs is proven against Postgres in core (``tests/test_keyset_pages_db.py``).
Fixtures are invented.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Dict
from unittest.mock import MagicMock

import pytest
from starlette.testclient import TestClient

from bifrost_api.account.app import create_account_app
from bifrost_core.portfolio.reader import keyset
from tests.contract.helpers import full_server_config
from tests.envelope_asserts import assert_error

TS = datetime(2026, 9, 18, 15, 0, 0, 1, tzinfo=timezone.utc)
EXEC_CURSOR = keyset.encode_executions(date(2026, 9, 18), TS, 41)
TXN_CURSOR = keyset.encode_transactions(TS, 7)


def _client(reader: MagicMock) -> TestClient:
    reader.config = full_server_config()
    app = create_account_app(reader=reader, control_via_db=None, merged_config=reader.config)
    return TestClient(app, raise_server_exceptions=False)


def _validating_reader() -> MagicMock:
    """Page methods that decode the cursor as core's StatusReader does, before reading."""
    reader = MagicMock()

    def exec_page(**kw: Any) -> Dict[str, Any]:
        if kw.get("cursor") is not None:
            keyset.decode_executions(kw["cursor"])
        return {"items": [{"account_executions_id": 40}], "next_cursor": None}

    def txn_page(**kw: Any) -> Dict[str, Any]:
        if kw.get("cursor") is not None:
            keyset.decode_transactions(kw["cursor"])
        return {"items": [{"account_transactions_id": 6}], "next_cursor": None}

    reader.get_executions_page.side_effect = exec_page
    reader.get_transactions_page.side_effect = txn_page
    return reader


# --- /executions --------------------------------------------------------------------------


def test_executions_first_page_hands_out_the_next_cursor() -> None:
    reader = MagicMock()
    reader.get_executions_page.return_value = {"items": [{"account_executions_id": 42}], "next_cursor": EXEC_CURSOR}
    body = _client(reader).get("/executions?limit=1&account_id=U0000001&source_scope=performance_book").json()
    kw = reader.get_executions_page.call_args.kwargs
    assert kw["cursor"] is None and kw["limit"] == 1 and kw["account_id"] == "U0000001"
    assert kw["source_scope"] == "performance_book"
    assert body == {"items": [{"account_executions_id": 42}], "count": 1, "next_cursor": EXEC_CURSOR}


def test_executions_cursor_is_forwarded_and_a_later_page_has_no_total() -> None:
    reader = _validating_reader()
    r = _client(reader).get("/executions", params={"limit": 1, "cursor": EXEC_CURSOR})
    assert r.status_code == 200, r.text
    assert reader.get_executions_page.call_args.kwargs["cursor"] == EXEC_CURSOR
    body = r.json()
    assert body["next_cursor"] is None and "total" not in body and body["count"] == 1


@pytest.mark.parametrize("bad", ["garbage", "e30", TXN_CURSOR, "A" * 600])
def test_executions_bad_cursor_is_400(bad: str) -> None:
    r = _client(_validating_reader()).get("/executions", params={"cursor": bad})
    assert_error(r, 400, "Invalid cursor")


def test_executions_cursor_with_opt_pairs_is_400_and_reads_nothing() -> None:
    reader = MagicMock()
    r = _client(reader).get("/executions", params={"include_opt_pairs": "true", "cursor": EXEC_CURSOR,
                                                    "from_ts": 1, "to_ts": 2})
    assert_error(r, 400, "include_opt_pairs")
    reader.get_executions_with_opt_pairs.assert_not_called()
    reader.get_executions_page.assert_not_called()


def test_executions_with_opt_pairs_sends_no_next_cursor() -> None:
    reader = MagicMock()
    reader.get_executions_with_opt_pairs.return_value = {"executions": [{"account_executions_id": 1}], "opt_pairs": []}
    body = _client(reader).get("/executions?include_opt_pairs=true").json()
    assert "next_cursor" not in body and body["total"] == 1


# --- /transactions --------------------------------------------------------------------


def test_transactions_first_page_hands_out_the_next_cursor() -> None:
    reader = MagicMock()
    reader.get_transactions_page.return_value = {"items": [{"account_transactions_id": 8}], "next_cursor": TXN_CURSOR}
    body = _client(reader).get("/transactions?limit=1").json()
    assert reader.get_transactions_page.call_args.kwargs["cursor"] is None
    assert body == {"items": [{"account_transactions_id": 8}], "count": 1, "next_cursor": TXN_CURSOR}


def test_transactions_cursor_is_forwarded_and_a_later_page_has_no_total() -> None:
    reader = _validating_reader()
    body = _client(reader).get("/transactions", params={"limit": 1, "cursor": TXN_CURSOR}).json()
    assert reader.get_transactions_page.call_args.kwargs["cursor"] == TXN_CURSOR
    assert body == {"items": [{"account_transactions_id": 6}], "count": 1, "next_cursor": None}


@pytest.mark.parametrize("bad", ["garbage", EXEC_CURSOR])
def test_transactions_bad_cursor_is_400(bad: str) -> None:
    assert_error(_client(_validating_reader()).get("/transactions", params={"cursor": bad}), 400, "Invalid cursor")


@pytest.mark.parametrize("limit, status", [(10000, 200), (10001, 422), (500, 200)])
def test_transactions_limit_is_capped_at_10000(limit: int, status: int) -> None:
    reader = MagicMock()
    reader.get_transactions_page.return_value = {"items": [], "next_cursor": None}
    r = _client(reader).get(f"/transactions?limit={limit}")
    assert r.status_code == status, r.text
    if status == 422:
        reader.get_transactions_page.assert_not_called()


def test_openapi_documents_cursor_and_the_cap() -> None:
    reader = MagicMock()
    reader.config = full_server_config()
    spec = create_account_app(reader=reader, control_via_db=None, merged_config=reader.config).openapi()
    for path in ("/executions", "/transactions"):
        params = {p["name"]: p for p in spec["paths"][path]["get"]["parameters"]}
        assert "cursor" in params and "Opaque" in params["cursor"]["description"]
    limit = {p["name"]: p for p in spec["paths"]["/transactions"]["get"]["parameters"]}["limit"]
    assert limit["schema"]["maximum"] == 10000
