"""Instrument class routes: on the account app (which serves /api/portfolio), and passing the reader's answer through."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

from starlette.testclient import TestClient

from bifrost_api.account.app import create_account_app
from tests.contract.helpers import operator_server_config


def _client(reader: MagicMock, control_via_db: Any = None) -> TestClient:
    reader._config = operator_server_config()
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
    listed = c.get("/instrument-classes").json()
    assert listed["count"] == 1 and listed["items"] == reader.list_instrument_classes.return_value
    assert c.put("/instrument-classes/ZZFI", json={"instrument_class": "cash_like", "note": "T-bill fund"}).json() == {"ok": True}
    # A full replace since api 0.6.0 (TD-15): the stored note is not kept.
    reader.set_instrument_class.assert_called_once_with("ZZFI", "cash_like", note="T-bill fund", keep_note=False)


def test_a_refusal_says_why() -> None:
    # A class core does not know is a 400 that names the three, before the writer is called.
    reader = MagicMock()
    r = _client(reader, {"sink": "postgres"}).put("/instrument-classes/ZZFI", json={"instrument_class": "bond"})
    assert r.status_code == 400
    body = r.json()
    assert "must be one of" in body["detail"] and set(body) == {"detail"}
    reader.set_instrument_class.assert_not_called()


def test_without_postgres_nothing_is_written() -> None:
    # One app per test: a second app in the same test registers the metrics twice.
    reader = MagicMock()
    r = _client(reader).delete("/instrument-classes/ZZFI")
    assert r.status_code == 503 and set(r.json()) == {"detail"}
    reader.delete_instrument_class.assert_not_called()
