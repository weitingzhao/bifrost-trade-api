"""Trading executions routes answer real statuses and the shared envelopes (TD-16, TD-17).

Failures used to be 200 ``{"ok": false, "error"}`` (or 200 ``{"executions": [],
"error"}``); lists were ``{"executions"}`` / ``{"attributions"}`` /
``{"transactions"}``. Now failures carry a status and ``detail`` and lists carry
``items`` / ``count``, with the old keys beside them for one release.
Fixtures are invented.
"""

from __future__ import annotations

from typing import Any, Dict, Optional
from unittest.mock import MagicMock

import pytest
from starlette.testclient import TestClient

import bifrost_api.trading.routers.executions as ex
from bifrost_api.account.app import create_account_app
from bifrost_core.monitor.reader.errors import WriteNotFound
from tests.contract.helpers import operator_server_config
from tests.envelope_asserts import assert_error, assert_list
from tests.reader_mock import reader_mock

PG = {"sink": "postgres"}
ACC = "U0000001"


def _client(reader: Optional[MagicMock] = None, control_via_db: Any = PG, gateway: Any = None) -> TestClient:
    reader = reader or reader_mock()
    reader.config = operator_server_config()
    app = create_account_app(
        reader=reader,
        control_via_db=control_via_db,
        status_cfg_for_read=control_via_db,
        merged_config=reader.config,
    )
    app.state.ib_operator_client = gateway
    return TestClient(app, raise_server_exceptions=False)


# --- lists ------------------------------------------------------------------------------


def test_executions_list_has_items_count_and_the_old_key() -> None:
    reader = reader_mock()
    rows = [{"account_executions_id": 1, "symbol": "ZZQ"}, {"account_executions_id": 2, "symbol": "ZZQ"}]
    reader.get_executions_page.return_value = {"items": rows, "next_cursor": None}
    body = assert_list(_client(reader).get("/executions"), "executions", rows)
    assert body["next_cursor"] is None


def test_executions_with_opt_pairs_keeps_its_pairs() -> None:
    reader = reader_mock()
    rows = [{"account_executions_id": 1}]
    reader.get_executions_with_opt_pairs.return_value = {"executions": rows, "opt_pairs": [[1, 2]]}
    body = assert_list(_client(reader).get("/executions?include_opt_pairs=true"), "executions", rows)
    assert body["opt_pairs"] == [[1, 2]]


@pytest.mark.parametrize(
    "path,reader_method,legacy_key",
    [
        ("/executions/position-attribution", "get_position_trade_attribution", "attributions"),
        ("/executions/freshness", "get_executions_freshness", None),
        ("/transactions", "get_transactions_page", "transactions"),
    ],
)
def test_list_routes(path: str, reader_method: str, legacy_key: Optional[str]) -> None:
    reader = reader_mock()
    rows = [{"id": 1}, {"id": 2}, {"id": 3}]
    # the paged readers (core 0.40.0) answer {"items", "next_cursor"}
    paged = reader_method.endswith("_page")
    getattr(reader, reader_method).return_value = {"items": rows, "next_cursor": None} if paged else rows
    assert_list(_client(reader).get(path), legacy_key, rows)


def test_option_stock_links_list_keeps_slippage_total() -> None:
    reader = reader_mock()
    links = [{"stock_account_executions_id": 5}]
    reader.get_option_stock_links.return_value = {"links": links, "slippage_total": 1.5}
    body = assert_list(
        _client(reader).get(f"/executions/option-stock-links?account_id={ACC}&option_account_executions_id=4"), "links", links
    )
    assert body["slippage_total"] == 1.5


def test_stock_link_candidates_list_keeps_its_window() -> None:
    reader = reader_mock()
    rows = [{"account_executions_id": 9}]
    reader.get_stock_link_candidates.return_value = {
        "executions": rows,
        "underlying_symbol": "ZZQ",
        "trade_date_from": "2026-01-01",
        "trade_date_to": "2026-01-15",
    }
    body = assert_list(
        _client(reader).get(f"/executions/stock-link-candidates?account_id={ACC}&option_account_executions_id=4"),
        "executions",
        rows,
    )
    assert body["underlying_symbol"] == "ZZQ" and body["trade_date_to"] == "2026-01-15"


# --- 400 ---------------------------------------------------------------------------------


def test_links_query_without_batches_is_400() -> None:
    assert_error(_client().post("/executions/option-stock-links/query", json={}), 400, "batches", {"by_option_id": {}})


def test_post_execution_missing_price_is_400(monkeypatch: pytest.MonkeyPatch) -> None:
    insert = MagicMock()
    monkeypatch.setattr(ex, "insert_one_execution", insert)
    r = _client().post("/executions", json={"account_id": ACC, "symbol": "ZZQ", "quantity": 1})
    assert_error(r, 400, "required fields", {"account_executions_id": None})
    insert.assert_not_called()


def test_post_execution_splits_not_a_list_is_422(monkeypatch: pytest.MonkeyPatch) -> None:
    """A typed body since 0.3.1 (TD-24): the wrong type is FastAPI's 422, nothing written."""
    insert = MagicMock()
    monkeypatch.setattr(ex, "insert_one_execution", insert)
    r = _client().post(
        "/executions", json={"account_id": ACC, "symbol": "ZZQ", "quantity": 1, "price": 2, "fill_splits": {}}
    )
    assert r.status_code == 422 and "fill_splits" in r.text
    insert.assert_not_called()


def test_post_execution_rejected_splits_is_400(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ex, "insert_one_execution", MagicMock(return_value=None))
    body = {
        "account_id": ACC,
        "symbol": "ZZQ",
        "quantity": 1,
        "price": 2,
        "fill_splits": [{"trade_id": 3, "quantity": 5}],
    }
    assert_error(_client().post("/executions", json=body), 400, "fill_splits rejected", {"account_executions_id": None})


@pytest.mark.parametrize(
    "err",
    [
        "account_id is required.",
        "role must be exercise, assignment, or omitted.",
        "option_account_executions_id must refer to an OPT row.",
        "Underlying symbol (ZZQ) does not match stock symbol (ZZR).",
    ],
)
def test_link_input_refusals_are_400(monkeypatch: pytest.MonkeyPatch, err: str) -> None:
    monkeypatch.setattr(ex, "insert_option_stock_link", lambda _cfg, _body: (False, None, err, None))
    r = _client().post("/executions/option-stock-links", json={"account_id": ACC})
    assert_error(r, 400, err, {"link_id": None, "warning": None})


# --- 404 ---------------------------------------------------------------------------------


def test_link_to_a_missing_execution_is_404(monkeypatch: pytest.MonkeyPatch) -> None:
    err = "Option execution not found in performance book (Flex/journal)."
    monkeypatch.setattr(ex, "insert_option_stock_link", lambda _cfg, _body: (False, None, err, None))
    assert_error(_client().post("/executions/option-stock-links", json={"account_id": ACC}), 404, "not found", {"link_id": None})


def test_delete_a_missing_link_is_404(monkeypatch: pytest.MonkeyPatch) -> None:
    def _missing(_cfg: Any, lid: int, acc: str) -> Any:
        raise WriteNotFound(f"No option/stock link {lid} on account {acc}.")

    monkeypatch.setattr(ex, "delete_option_stock_link_strict", _missing)
    assert_error(_client().delete(f"/executions/option-stock-links/7?account_id={ACC}"), 404, "No option/stock link 7")


def test_put_a_missing_execution_is_404(monkeypatch: pytest.MonkeyPatch) -> None:
    # DELETE /executions/{id} is strict since TD-15 (tests/test_write_semantics.py).
    monkeypatch.setattr(ex, "update_one_execution", MagicMock(return_value=False))
    assert_error(_client().put("/executions/42", json={"price": 2}), 404, "account_executions_id missing")


def test_stock_link_candidates_for_a_missing_option_is_404() -> None:
    reader = reader_mock()
    reader.get_stock_link_candidates.return_value = {"executions": [], "error": "Option execution not found in performance book."}
    r = _client(reader).get(f"/executions/stock-link-candidates?account_id={ACC}&option_account_executions_id=4")
    assert_error(r, 404, "not found", {"executions": []})


# --- 409 ---------------------------------------------------------------------------------


def test_a_link_that_exists_is_409(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        ex, "insert_option_stock_link", lambda _cfg, _body: (False, None, "Link already exists or insert failed.", None)
    )
    assert_error(_client().post("/executions/option-stock-links", json={"account_id": ACC}), 409, "already exists")


# --- 503 ---------------------------------------------------------------------------------

NO_PG_WRITES = [
    ("POST", "/executions", {"symbol": "ZZQ", "quantity": 1, "price": 2}, "account_executions", {"account_executions_id": None}),
    ("PUT", "/executions/42", {"price": 2}, "account_executions", {}),
    ("DELETE", "/executions/42", None, "Postgres is not configured", {}),
    ("POST", "/executions/option-stock-links", {"account_id": ACC}, "PostgreSQL is required.", {"link_id": None}),
    ("DELETE", f"/executions/option-stock-links/7?account_id={ACC}", None, "Postgres is not configured", {}),
    ("POST", "/executions/fetch", None, "account_executions", {"count": 0}),
]


@pytest.mark.parametrize("method,path,body,contains,legacy", NO_PG_WRITES)
def test_without_postgres_writes_are_503(method: str, path: str, body: Any, contains: str, legacy: Dict[str, Any]) -> None:
    c = _client(control_via_db=None)
    r = c.request(method, path, json=body) if body is not None else c.request(method, path)
    assert_error(r, 503, contains, legacy)


def test_option_stock_links_without_a_connection_is_503() -> None:
    reader = reader_mock()
    reader.get_option_stock_links.return_value = {"links": [], "slippage_total": None, "error": "database_unavailable"}
    r = _client(reader).get(f"/executions/option-stock-links?account_id={ACC}&option_account_executions_id=4")
    assert_error(r, 503, "database_unavailable", {"links": [], "slippage_total": None})


def test_links_query_without_a_connection_is_503() -> None:
    reader = reader_mock()
    reader.get_option_stock_links_bulk.return_value = {"by_option_id": {}, "error": "database_unavailable"}
    body = {"batches": [{"account_id": ACC, "option_account_executions_ids": [1]}]}
    assert_error(_client(reader).post("/executions/option-stock-links/query", json=body), 503, "database_unavailable")


def test_fetch_without_a_gateway_client_is_503() -> None:
    assert_error(_client(gateway=None).post("/executions/fetch"), 503, "not configured", {"count": 0})


class _Gateway:
    def __init__(self, answer: Dict[str, Any]) -> None:
        self.answer = answer

    async def request_async(self, _method: str, _params: Dict[str, Any], caller: str = "") -> Dict[str, Any]:
        return self.answer


def test_fetch_when_the_gateway_answers_an_error_is_503(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ex, "_publish_tws_fetch_system_message", lambda *_a, **_k: None)
    r = _client(gateway=_Gateway({"ok": False, "error": "slot not connected"})).post("/executions/fetch?days=3")
    assert_error(r, 503, "slot not connected", {"count": 0, "days": 3, "fetched_total": 0})


# --- 500 ---------------------------------------------------------------------------------


def test_post_execution_write_failure_is_500(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ex, "insert_one_execution", MagicMock(return_value=None))
    r = _client().post("/executions", json={"account_id": ACC, "symbol": "ZZQ", "quantity": 1, "price": 2})
    assert_error(r, 500, "database error", {"account_executions_id": None})


def test_link_database_error_is_500(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ex, "insert_option_stock_link", lambda _cfg, _body: (False, None, "server closed the connection", None))
    assert_error(_client().post("/executions/option-stock-links", json={"account_id": ACC}), 500, "server closed")


def test_fetch_write_failure_is_500(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ex, "_publish_tws_fetch_system_message", lambda *_a, **_k: None)
    monkeypatch.setattr(ex, "write_account_executions_to_db", lambda *_a, **_k: False)
    gw = _Gateway({"ok": True, "data": {"executions": [_plugin_fill()]}})
    r = _client(gateway=gw).post("/executions/fetch")
    assert_error(r, 500, "Failed to write account_executions.", {"count": 0, "fetched_total": 1})


# --- fetch: the plugin's fills are mapped to the writer's row (api 0.3.3) -------------


def _plugin_fill(**over: Any) -> Dict[str, Any]:
    """The shape bifrost-platform-plugin ib_gateway fetch_executions answers (values made up)."""
    fill = {
        "exec_id": "zz-1", "account": ACC, "symbol": "ZZQ", "sec_type": "STK", "side": "BOT",
        "shares": 10.0, "price": 12.5, "commission": 1.0, "realized_pnl": None, "ts": 1_790_000_000.0,
    }
    fill.update(over)
    return fill


def test_fetch_writes_mapped_rows_and_counts_the_incomplete(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ex, "_publish_tws_fetch_system_message", lambda *_a, **_k: None)
    written: Dict[str, Any] = {}

    def fake_write(_cfg: Any, rows: Any, stats_out: Dict[str, Any]) -> bool:
        written["rows"] = rows
        stats_out.update({"tws_raw_inserted": 1, "tws_raw_inserted_ids": [5]})
        return True

    monkeypatch.setattr(ex, "write_account_executions_to_db", fake_write)
    fills = [_plugin_fill(), _plugin_fill(exec_id="zz-2", account=None), _plugin_fill(exec_id="zz-3", sec_type="OPT")]
    r = _client(gateway=_Gateway({"ok": True, "data": {"executions": fills}})).post("/executions/fetch")
    assert r.status_code == 200
    body = r.json()
    assert (body["count"], body["fetched_total"], body["tws_raw_inserted"]) == (3, 3, 1)
    assert body["skipped_incomplete"] == 2 and body["skipped_exec_ids"] == ["zz-2", "zz-3"]
    assert "Not written (incomplete fill" in body["message"]
    (row,) = written["rows"]
    assert (row["account_id"], row["quantity"], row["time"], row["source"]) == (ACC, 10.0, 1_790_000_000.0, "tws_client")
    assert row["contract_key"] == "ZZQ|STK|||"


def test_fetch_with_only_incomplete_fills_writes_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ex, "_publish_tws_fetch_system_message", lambda *_a, **_k: None)
    monkeypatch.setattr(ex, "write_account_executions_to_db", MagicMock(side_effect=AssertionError("no write")))
    gw = _Gateway({"ok": True, "data": {"executions": [{"exec_id": "zz-1"}]}})
    r = _client(gateway=gw).post("/executions/fetch")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True and body["fetched_total"] == 1
    assert body["skipped_incomplete"] == 1 and body["skipped_exec_ids"] == ["zz-1"]
    assert "none could be written" in body["message"]


# --- success shapes unchanged ------------------------------------------------------------


def test_link_success_keeps_its_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ex, "insert_option_stock_link", lambda _cfg, _body: (True, 12, None, "differs"))
    r = _client().post("/executions/option-stock-links", json={"account_id": ACC})
    assert r.status_code == 200
    assert r.json() == {
        "ok": True,
        "link_id": 12,
        "account_execution_option_stock_link_id": 12,
        "error": None,
        "warning": "differs",
    }


def test_post_execution_success_keeps_its_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ex, "insert_one_execution", MagicMock(return_value=31))
    r = _client().post("/executions", json={"account_id": ACC, "symbol": "ZZQ", "quantity": 1, "price": 2})
    assert r.status_code == 200 and r.json()["account_executions_id"] == 31 and r.json()["ok"] is True
