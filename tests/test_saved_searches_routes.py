"""Saved-search routes (design Rev .139): mounted where the gateway looks, and
the table's refusals passed through."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from starlette.testclient import TestClient

from bifrost_api.account.app import create_account_app
from bifrost_core.monitor.reader import saved_search as saved_search_module
from bifrost_core.monitor.reader.errors import WriteNotFound
from bifrost_core.monitor.reader.saved_search import SavedSearchError
from tests.contract.helpers import operator_server_config


def _client(control_via_db: Any = None) -> TestClient:
    reader = MagicMock()
    reader.config = operator_server_config()
    app = create_account_app(
        reader=reader,
        control_via_db=control_via_db,
        status_cfg_for_read=control_via_db,
        merged_config=reader.config,
    )
    return TestClient(app, raise_server_exceptions=False)


def test_mounted_on_the_account_app() -> None:
    paths = set(_client().app.openapi()["paths"])
    assert {"/strategies/saved-searches", "/strategies/saved-searches/{preference_saved_search_id}"} <= paths


def test_writes_without_postgres_are_503() -> None:
    client = _client()
    assert client.post("/strategies/saved-searches", json={"route": "/trade/plans", "label": "x", "state": {}}).status_code == 503
    assert client.delete("/strategies/saved-searches/1").status_code == 503


def test_lists_and_saves(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(saved_search_module, "list_saved_searches", lambda *_a, **_kw: [{"preference_saved_search_id": 1}])
    seen = {}

    def _create(_cfg: Any, route: str, label: str, state: Any) -> int:
        seen.update(route=route, label=label, state=state)
        return 7

    monkeypatch.setattr(saved_search_module, "create_saved_search", _create)
    client = _client({"sink": "postgres"})
    assert client.get("/strategies/saved-searches").json() == {"items": [{"preference_saved_search_id": 1}], "count": 1}
    r = client.post("/strategies/saved-searches", json={"route": "/trade/plans", "label": "AMD · all", "state": {"q": "sym:AMD"}})
    assert r.json() == {"preference_saved_search_id": 7}
    assert seen == {"route": "/trade/plans", "label": "AMD · all", "state": {"q": "sym:AMD"}}


def test_a_refusal_is_400_with_the_reason(monkeypatch: pytest.MonkeyPatch) -> None:
    def _refuse(*_a: Any, **_kw: Any) -> int:
        raise SavedSearchError("route must be an app path, like /trade/plans.")

    monkeypatch.setattr(saved_search_module, "create_saved_search", _refuse)
    r = _client({"sink": "postgres"}).post("/strategies/saved-searches", json={"route": "x", "label": "y", "state": {}})
    assert r.status_code == 400
    assert r.json()["detail"] == "route must be an app path, like /trade/plans."


def test_delete_missing_is_404(monkeypatch: pytest.MonkeyPatch) -> None:
    def _missing(*_a: Any, **_kw: Any) -> Any:
        raise WriteNotFound("No saved search 404.")

    monkeypatch.setattr(saved_search_module, "delete_saved_search_strict", _missing)
    r = _client({"sink": "postgres"}).delete("/strategies/saved-searches/404")
    assert r.status_code == 404 and r.json()["detail"] == "No saved search 404."


def test_delete_answers_deleted(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        saved_search_module,
        "delete_saved_search_strict",
        lambda _cfg, sid, **_kw: {"deleted": "hard", "preference_saved_search_id": sid},
    )
    r = _client({"sink": "postgres"}).delete("/strategies/saved-searches/9")
    assert r.json() == {"deleted": "hard", "preference_saved_search_id": 9, "ok": True}
