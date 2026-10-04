"""TD-73 (api 0.7.1, core 0.43.0): a trade's notes live in the Research journal only.

`notes` on the trade POST / PATCH and `note` on the review PATCH were deprecated in api
0.7.0 and are gone now; core no longer has a reader or writer for the columns, which an
Owner db-step drops after this release. A client that still sends one -- a value or null --
gets a 422 that names the field and says where notes live, and nothing is written: a
request answered ok must have written what it sent. `notes` is gone from the trade rows
too. Checked on the /trades routes and on the replaced /strategies paths. Fixtures are
invented.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from starlette.testclient import TestClient

from bifrost_api import deprecations
from bifrost_api.account.app import create_account_app
from bifrost_core.monitor.reader import strategy_instance, trade_review
from tests import strategy_rows
from tests.contract.helpers import operator_server_config

PG = {"sink": "postgres"}
CREATE = {"strategy_opportunity_id": 3, "account_id": "U0000001", "opened_at": "2026-01-02T12:00:00Z"}


def _client() -> TestClient:
    reader = MagicMock()
    reader.config = operator_server_config()
    app = create_account_app(reader=reader, control_via_db=PG, status_cfg_for_read=PG, merged_config=reader.config)
    return TestClient(app, raise_server_exceptions=False)


def _retired(r: Any, field: str) -> None:
    assert r.status_code == 422, r.text
    (err,) = r.json()["detail"]
    assert err["type"] == "retired_field"
    assert err["msg"].startswith(f"`{field}` was removed in api 0.7.1 (TD-73)")
    assert "Research journal" in err["msg"] and "/research/journal/notes" in err["msg"]


@pytest.mark.parametrize("value", ["Rolled early.", None])
@pytest.mark.parametrize("path", ["/trades/7", "/trades/7"])
def test_trade_patch_notes_is_a_422_that_writes_nothing(monkeypatch: pytest.MonkeyPatch, path: str, value: Any) -> None:
    writer = MagicMock()
    monkeypatch.setattr(strategy_instance, "patch_instance", writer)
    _retired(_client().patch(path, json={"label": "ZZ trade", "notes": value}), "notes")
    writer.assert_not_called()


@pytest.mark.parametrize("value", ["Held too long.", None])
@pytest.mark.parametrize("path", ["/trade-reviews/7"])
def test_review_patch_note_is_a_422_that_writes_nothing(monkeypatch: pytest.MonkeyPatch, path: str, value: Any) -> None:
    writer = MagicMock()
    monkeypatch.setattr(trade_review, "patch_review", writer)
    _retired(_client().patch(path, json={"reviewed": True, "note": value}), "note")
    writer.assert_not_called()


@pytest.mark.parametrize("path", ["/trades"])
def test_trade_create_notes_is_a_422_and_creates_nothing(monkeypatch: pytest.MonkeyPatch, path: str) -> None:
    create = MagicMock(return_value={**strategy_rows.instance(), "trade_id": 41})
    monkeypatch.setattr(strategy_instance, "create_instance_strict", create)
    c = _client()
    _retired(c.post(path, json={**CREATE, "notes": "zz"}), "notes")
    create.assert_not_called()
    ok = c.post(path, json=CREATE)
    assert ok.status_code == 200 and ok.json() == {"trade_id": 41}
    assert "notes" not in create.call_args.kwargs and create.call_args.args[:3] == (PG, 3, "U0000001")


def test_the_writes_without_the_fields_still_answer(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(strategy_instance, "patch_instance", lambda _c, _i, f: strategy_rows.instance())
    monkeypatch.setattr(trade_review, "patch_review", lambda _c, _i, f: {"trade_id": 7, "reviewed": True})
    c = _client()
    r = c.patch("/trades/7", json={"label": "ZZ trade"})
    assert r.status_code == 200 and "notes" not in r.json()
    assert "deprecation" not in {k.lower() for k in r.headers}
    assert c.patch("/trade-reviews/7", json={"reviewed": True}).status_code == 200


def test_openapi_has_no_note_fields() -> None:
    schemas = _client().app.openapi()["components"]["schemas"]
    assert "notes" not in schemas["TradePatch"]["properties"]
    assert "note" not in schemas["ReviewPatch"]["properties"]
    assert "notes" not in schemas["TradeCreate"]["properties"]
    assert "notes" not in schemas["TradeRow"]["properties"]


def test_the_deprecated_body_field_helper_is_gone() -> None:
    assert not hasattr(deprecations, "deprecated_fields_sent")
