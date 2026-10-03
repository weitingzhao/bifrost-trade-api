"""Watchlist routes answer real statuses and the shared envelopes (TD-16, TD-17; writes TD-15). Fixtures are invented."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from starlette.testclient import TestClient

import bifrost_core.monitor.reader.watchlist as wl
from bifrost_api.market.app import create_market_app
from bifrost_core.monitor.reader.errors import WriteFailed, WriteNotFound
from tests.contract.helpers import operator_server_config
from tests.envelope_asserts import assert_error, assert_list

PG = {"sink": "postgres"}


def _client(reader: MagicMock, control_via_db: Any = PG) -> TestClient:
    reader.config = {**operator_server_config(), "redis": {"enabled": False}}
    app = create_market_app(reader=reader, control_via_db=control_via_db, merged_config=reader.config)
    return TestClient(app, raise_server_exceptions=False)


def test_list_has_items_and_count() -> None:
    reader = MagicMock()
    rows = [{"contract_key": "ZZQ|STK|||"}, {"contract_key": "ZZR|STK|||"}]
    reader.get_watchlist.return_value = rows
    assert_list(_client(reader).get("/watchlist"), expected=rows)


@pytest.mark.parametrize(
    "method,path,body",
    [
        ("POST", "/watchlist", {"contract_key": "ZZQ"}),
        ("PATCH", "/watchlist/ZZQ%7CSTK%7C%7C%7C", {"category_id": None}),
        ("DELETE", "/watchlist?contract_key=ZZQ%7CSTK%7C%7C%7C", None),
    ],
)
def test_without_postgres_is_503(method: str, path: str, body: Any) -> None:
    reader = MagicMock()
    c = _client(reader, None)
    r = c.request(method, path, json=body) if body is not None else c.request(method, path)
    assert_error(r, 503, "Postgres is not configured")


def test_post_blank_key_is_400() -> None:
    # Core refuses a blank key before it connects.
    assert_error(_client(MagicMock()).post("/watchlist", json={"contract_key": "  "}), 400, "contract_key is required.")


def test_delete_without_key_is_400() -> None:
    assert_error(_client(MagicMock()).delete("/watchlist"), 400, "contract_key is required.")


def _raise(exc: Exception) -> Any:
    def _f(*_a: Any, **_kw: Any) -> Any:
        raise exc

    return _f


def test_post_write_failure_is_500(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(wl, "upsert_watchlist", _raise(WriteFailed("Cannot write watchlist item ZZQ|STK|||: the database write failed.")))
    assert_error(_client(MagicMock()).post("/watchlist", json={"contract_key": "ZZQ"}), 500, "the database write failed")


def test_unreachable_database_is_503(monkeypatch: pytest.MonkeyPatch) -> None:
    exc = WriteFailed("Cannot write watchlist item ZZQ|STK|||: the database is unreachable.", unavailable=True)
    monkeypatch.setattr(wl, "upsert_watchlist", _raise(exc))
    assert_error(_client(MagicMock()).post("/watchlist", json={"contract_key": "ZZQ"}), 503, "unreachable")


def test_delete_missing_is_404(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(wl, "delete_watchlist_strict", _raise(WriteNotFound("ZZQ|STK||| is not on the watchlist.")))
    assert_error(_client(MagicMock()).delete("/watchlist?contract_key=ZZQ%7CSTK%7C%7C%7C"), 404, "not on the watchlist")


def test_post_success_answers_the_row_with_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    row = {"contract_key": "ZZQ|STK|||", "symbol": "ZZQ", "category_id": None, "source": "manual"}
    monkeypatch.setattr(wl, "upsert_watchlist", lambda _cfg, _key, _fields: row)
    r = _client(MagicMock()).post("/watchlist", json={"contract_key": "ZZQ"})
    assert r.status_code == 200
    assert r.json() == {**row, "ok": True, "message": "Watchlist item added or updated."}
