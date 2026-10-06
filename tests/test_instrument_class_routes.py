"""Instrument class routes: on the account app (which serves /api/portfolio), and passing the reader's answer through."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

from starlette.testclient import TestClient

from bifrost_api.account.app import create_account_app
from bifrost_core.monitor.reader import write_support
from bifrost_core.portfolio.reader import instrument_class as instrument_class_module
from tests.contract.helpers import operator_server_config
from tests.reader_mock import reader_mock


def _client(reader: MagicMock, control_via_db: Any = None) -> TestClient:
    reader.config = operator_server_config()
    app = create_account_app(
        reader=reader,
        control_via_db=control_via_db,
        status_cfg_for_read=control_via_db,
        merged_config=reader.config,
    )
    return TestClient(app, raise_server_exceptions=False)


def test_mounted_where_the_gateway_strips_to() -> None:
    paths = set(_client(MagicMock()).app.openapi()["paths"])
    assert {"/instrument-classes", "/instrument-classes/{contract_key}"} <= paths


def test_list_and_set_pass_through(monkeypatch) -> None:
    reader = reader_mock()
    # Invented key (fixtures are never copied from DEV).
    reader.list_instrument_classes.return_value = [{"contract_key": "ZZFI", "instrument_class": "fixed_income"}]
    row = {"contract_key": "ZZFI", "instrument_class": "cash_like", "note": "T-bill fund"}
    put = MagicMock(return_value=row)
    monkeypatch.setattr(instrument_class_module, "set_instrument_class_strict", put)
    c = _client(reader, {"sink": "postgres"})
    listed = c.get("/instrument-classes").json()
    assert listed["count"] == 1 and listed["items"] == reader.list_instrument_classes.return_value
    r = c.put("/instrument-classes/ZZFI", json={"instrument_class": "cash_like", "note": "T-bill fund"})
    assert r.json() == {**row, "ok": True}
    # Core's strict full replace (core 0.47.0, TD-80 C2): the stored note is not kept.
    put.assert_called_once_with({"sink": "postgres"}, "ZZFI", "cash_like", note="T-bill fund")


def test_a_refusal_says_why(monkeypatch) -> None:
    # A class core does not know is a 400 that names the three, before anything connects.
    connect = MagicMock()
    monkeypatch.setattr(write_support, "connect", connect)
    r = _client(MagicMock(), {"sink": "postgres"}).put("/instrument-classes/ZZFI", json={"instrument_class": "bond"})
    assert r.status_code == 400
    body = r.json()
    assert "must be one of" in body["detail"] and set(body) == {"detail"}
    connect.assert_not_called()


def test_without_postgres_nothing_is_written(monkeypatch) -> None:
    # One app per test: a second app in the same test registers the metrics twice.
    # DELETE goes to the strict module writer, not the reader (StatusReader.delete_instrument_class is dead, TD-80).
    strict = MagicMock()
    monkeypatch.setattr(instrument_class_module, "delete_instrument_class_strict", strict)
    r = _client(MagicMock()).delete("/instrument-classes/ZZFI")
    assert r.status_code == 503 and set(r.json()) == {"detail"}
    strict.assert_not_called()
