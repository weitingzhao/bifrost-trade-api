"""TD-73: a trade's notes live in the Research journal only.

`notes` on the instance POST / PATCH and `note` on the review PATCH still write for
one release, marked deprecated: OpenAPI says so, the response carries
`Deprecation: true` and each hit is logged with who sent it. A request that leaves
them out is not marked. Fixtures are invented.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List
from unittest.mock import MagicMock

import pytest
from starlette.testclient import TestClient

from bifrost_api.account.app import create_account_app
from bifrost_core.monitor.reader import strategy_instance, trade_review
from tests import strategy_rows
from tests.contract.helpers import operator_server_config

PG = {"sink": "postgres"}


def _client() -> TestClient:
    reader = MagicMock()
    reader.config = operator_server_config()
    reader.create_strategy_instance.return_value = 41
    app = create_account_app(reader=reader, control_via_db=PG, status_cfg_for_read=PG, merged_config=reader.config)
    return TestClient(app, raise_server_exceptions=False)


def test_openapi_marks_the_two_fields_deprecated() -> None:
    schemas = _client().app.openapi()["components"]["schemas"]
    assert schemas["InstancePatch"]["properties"]["notes"]["deprecated"] is True
    assert schemas["ReviewPatch"]["properties"]["note"]["deprecated"] is True
    assert "deprecated" not in schemas["InstancePatch"]["properties"]["label"]


def test_review_note_still_writes_but_is_marked(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    calls: List[Dict[str, Any]] = []

    def _writer(_cfg: Any, _rid: Any, fields: Dict[str, Any]) -> Dict[str, Any]:
        calls.append(fields)
        return {"strategy_instance_id": 7, "tags_added": [], "tags_dropped": [], "reviewed": False}

    monkeypatch.setattr(trade_review, "patch_review", _writer)
    c = _client()
    with caplog.at_level(logging.WARNING, logger="bifrost_api.deprecations"):
        marked = c.patch("/strategies/reviews/7", json={"note": "zz", "reviewed": True}, headers={"X-Forwarded-For": "10.0.0.9"})
    plain = c.patch("/strategies/reviews/7", json={"reviewed": True})
    assert marked.status_code == 200 and plain.status_code == 200
    assert marked.headers.get("deprecation") == "true"
    assert "deprecation" not in plain.headers
    assert calls == [{"note": "zz", "reviewed": True}, {"reviewed": True}]
    line = next(r.getMessage() for r in caplog.records if "deprecated body fields" in r.getMessage())
    assert "PATCH /strategies/reviews/7 note" in line and "forwarded_for=10.0.0.9" in line


def test_instance_patch_notes_is_marked(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(strategy_instance, "patch_instance", lambda _c, _i, _f: strategy_rows.instance())
    c = _client()
    marked = c.patch("/strategies/instances/7", json={"notes": None})
    plain = c.patch("/strategies/instances/7", json={"label": "ZZ trade"})
    assert marked.status_code == 200 and plain.status_code == 200
    assert marked.headers.get("deprecation") == "true"
    assert "deprecation" not in plain.headers


def test_instance_create_notes_is_marked_and_still_passed() -> None:
    c = _client()
    body = {"strategy_opportunity_id": 3, "account_id": "U0000001", "opened_at": "2026-01-02T12:00:00Z"}
    marked = c.post("/strategies/instances", json={**body, "notes": "zz"})
    plain = c.post("/strategies/instances", json=body)
    assert marked.status_code == 200 and plain.status_code == 200, (marked.text, plain.text)
    assert marked.headers.get("deprecation") == "true"
    assert "deprecation" not in plain.headers
    reader = c.app.state.reader
    assert reader.create_strategy_instance.call_args_list[0].kwargs["notes"] == "zz"
    assert reader.create_strategy_instance.call_args_list[1].kwargs["notes"] is None
