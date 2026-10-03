"""Review routes: mounted where the gateway looks, and a missing instance is a 404.

`/api/strategy/*` is served by the account app, so the reviews router has to
be mounted there as well as on the strategy app.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from starlette.testclient import TestClient

from bifrost_api.account.app import create_account_app
from bifrost_core.monitor.reader import trade_review as trade_review_module
from tests.contract.helpers import operator_server_config


def _client(control_via_db: Any = None) -> TestClient:
    reader = MagicMock()
    reader._config = operator_server_config()
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


def test_the_merge_put_is_gone() -> None:
    """TD-15 (api 0.6.0): PATCH /strategies/reviews/{id} is the upsert; PUT went after a release with no caller."""
    assert _client({"sink": "postgres"}).put("/strategies/reviews/7", json={"reviewed": True}).status_code == 405


