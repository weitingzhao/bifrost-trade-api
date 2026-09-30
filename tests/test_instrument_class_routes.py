"""Instrument class routes: on the account app (which serves /api/portfolio), and passing the reader's answer through."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

from starlette.testclient import TestClient

from bifrost_api.account.app import create_account_app
from tests.contract.helpers import full_server_config


def _client(reader: MagicMock, control_via_db: Any = None) -> TestClient:
    reader._config = full_server_config()
    app = create_account_app(
        reader=reader,
        control_via_db=control_via_db,
        status_cfg_for_read=control_via_db,
        merged_config=reader._config,
    )
    return TestClient(app, raise_server_exceptions=False)


def test_mounted_where_the_gateway_strips_to() -> None:
    paths = set(_client(MagicMock()).app.openapi()["paths"])
    assert {"/instrument-classes", "/instrument-classes/{contract_key}"} <= paths


def test_list_and_set_pass_through() -> None:
    reader = MagicMock()
    # Invented key (fixtures are never copied from DEV).
    reader.list_instrument_classes.return_value = [{"contract_key": "ZZFI", "instrument_class": "fixed_income"}]
    reader.set_instrument_class.return_value = (True, None)
    c = _client(reader, {"sink": "postgres"})
    assert c.get("/instrument-classes").json()["count"] == 1
    assert c.put("/instrument-classes/ZZFI", json={"instrument_class": "cash_like", "note": "T-bill fund"}).json() == {"ok": True}
    reader.set_instrument_class.assert_called_once_with("ZZFI", "cash_like", note="T-bill fund")


def test_a_refusal_says_why() -> None:
    reader = MagicMock()
    reader.set_instrument_class.return_value = (False, "instrument_class must be one of stock, fixed_income, cash_like.")
    body = _client(reader, {"sink": "postgres"}).put("/instrument-classes/ZZFI", json={"instrument_class": "bond"}).json()
    assert body["ok"] is False and "must be one of" in body["error"]


def test_without_postgres_nothing_is_written() -> None:
    # One app per test: a second app in the same test registers the metrics twice.
    reader = MagicMock()
    assert _client(reader).delete("/instrument-classes/ZZFI").json()["ok"] is False
    reader.delete_instrument_class.assert_not_called()
