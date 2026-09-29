"""Review routes: mounted where the gateway looks, and a missing instance is a 404.

`/api/strategy/*` is served by the account app, so the reviews router has to
be mounted there as well as on the strategy app.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import psycopg2
import pytest
from starlette.testclient import TestClient

from bifrost_api.account.app import create_account_app
from bifrost_core.monitor.reader import trade_review as trade_review_module
from tests.contract.helpers import full_server_config


def _client(control_via_db: Any = None) -> TestClient:
    reader = MagicMock()
    reader._config = full_server_config()
    app = create_account_app(
        reader=reader,
        control_via_db=control_via_db,
        status_cfg_for_read=control_via_db,
        merged_config=reader._config,
    )
    return TestClient(app, raise_server_exceptions=False)


def test_the_review_routes_are_mounted_on_the_account_app() -> None:
    paths = set(_client().app.openapi()["paths"])
    assert {"/strategies/reviews", "/strategies/reviews/{strategy_instance_id}"} <= paths


def test_list_passes_rows_through(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        trade_review_module,
        "list_reviews",
        lambda _cfg: [{"strategy_instance_id": 7, "tags_added": [], "tags_dropped": [], "reviewed": True}],
    )
    body = _client({"sink": "postgres"}).get("/strategies/reviews").json()
    assert body["count"] == 1 and body["items"][0]["strategy_instance_id"] == 7


def test_a_failed_read_is_a_500_not_an_empty_book(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(_cfg: Any) -> Any:
        raise RuntimeError("relation does not exist")

    monkeypatch.setattr(trade_review_module, "list_reviews", boom)
    assert _client({"sink": "postgres"}).get("/strategies/reviews").status_code == 500


def test_save_passes_only_the_fields_sent(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict = {}

    def save(_cfg: Any, instance_id: int, payload: dict) -> dict:
        seen.update(instance_id=instance_id, payload=payload)
        return {"strategy_instance_id": instance_id, "reviewed": True}

    monkeypatch.setattr(trade_review_module, "save_review", save)
    res = _client({"sink": "postgres"}).put("/strategies/reviews/7", json={"reviewed": True})
    assert res.status_code == 200
    assert seen == {"instance_id": 7, "payload": {"reviewed": True}}


def test_a_missing_instance_is_a_404(monkeypatch: pytest.MonkeyPatch) -> None:
    def save(*_a: Any) -> Any:
        raise psycopg2.errors.ForeignKeyViolation("violates foreign key constraint")

    monkeypatch.setattr(trade_review_module, "save_review", save)
    assert _client({"sink": "postgres"}).put("/strategies/reviews/999", json={"reviewed": True}).status_code == 404


def test_writing_without_postgres_is_a_503() -> None:
    assert _client(None).put("/strategies/reviews/7", json={"reviewed": True}).status_code == 503
