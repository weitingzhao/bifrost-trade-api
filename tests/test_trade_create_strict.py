"""POST /trades through core's strict create (api 0.9.0, core 0.47.0, TD-80 C2).

Before: every failure -- an opportunity that does not exist, a blank account, a dead
database -- was the facade's ``None`` and a 500 "Failed to create trade". Now the route
answers as PATCH /trades does: 400 for input core refuses, 503 without a reachable
database, 500 when a statement failed (rolled back). Core's real writer runs over a
mocked connection; ids and accounts are invented.
"""

from __future__ import annotations

from typing import Any, Optional
from unittest.mock import MagicMock

import psycopg2
import pytest
from starlette.testclient import TestClient

from bifrost_api.account.app import create_account_app
from bifrost_core.monitor.reader import strategy_instance
from bifrost_core.monitor.reader import write_support
from tests import strategy_rows
from tests.contract.helpers import operator_server_config
from tests.envelope_asserts import assert_error

PG = {"sink": "postgres"}
BODY = {"strategy_opportunity_id": 3, "account_id": "U0000001", "opened_at": "2031-01-02T15:00:00Z"}


def _client(control_via_db: Any = PG) -> TestClient:
    reader = MagicMock()
    reader.config = operator_server_config()
    app = create_account_app(
        reader=reader, control_via_db=control_via_db, status_cfg_for_read=control_via_db, merged_config=reader.config
    )
    return TestClient(app, raise_server_exceptions=False)


def _connection(monkeypatch: pytest.MonkeyPatch, fetches: list, error: Optional[BaseException] = None) -> MagicMock:
    conn = MagicMock()
    cur = conn.cursor.return_value.__enter__.return_value
    cur.fetchone.side_effect = fetches
    if error is not None:
        cur.execute.side_effect = error
    monkeypatch.setattr(write_support, "connect", lambda params, golden=False: conn)
    return conn


def test_a_trade_is_created_and_answers_its_id(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _connection(monkeypatch, [(1,), (41,)])  # the opportunity exists; the insert returns 41
    row = strategy_rows.instance()  # read before the reader is replaced: it reads through it
    monkeypatch.setattr(strategy_instance, "get_instance_by_id", lambda c, tid: {**row, "trade_id": tid})
    r = _client().post("/trades", json={**BODY, "label": "ZZQ Jan 40P"})
    assert r.status_code == 200 and r.json() == {"trade_id": 41}
    conn.commit.assert_called_once()


def test_an_opportunity_that_does_not_exist_is_400_not_500(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _connection(monkeypatch, [None])
    assert_error(_client().post("/trades", json=BODY), 400, "No strategy opportunity 3")
    conn.commit.assert_not_called()


@pytest.mark.parametrize(
    "change,message",
    [({"account_id": "  "}, "account_id is required"), ({"label": " "}, "label is blank"),
     ({"opened_at": "soon"}, "opened_at must be ISO 8601")],
)
def test_bad_input_is_400_and_connects_to_nothing(monkeypatch: pytest.MonkeyPatch, change: dict, message: str) -> None:
    connect = MagicMock()
    monkeypatch.setattr(write_support, "connect", connect)
    assert_error(_client().post("/trades", json={**BODY, **change}), 400, message)
    connect.assert_not_called()


def test_an_unreachable_database_is_503(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(params: Any, golden: bool = False) -> Any:
        raise psycopg2.OperationalError("could not connect")

    monkeypatch.setattr(write_support, "connect", refuse)
    assert_error(_client().post("/trades", json=BODY), 503, "unreachable")


def test_a_failed_statement_is_500_and_rolled_back(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _connection(monkeypatch, [], error=psycopg2.OperationalError("server closed the connection unexpectedly"))
    assert_error(_client().post("/trades", json=BODY), 500, "the database write failed")
    conn.commit.assert_not_called()
    conn.rollback.assert_called()


def test_without_postgres_it_is_503() -> None:
    assert _client(None).post("/trades", json=BODY).status_code == 503
