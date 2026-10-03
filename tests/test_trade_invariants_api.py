"""api 0.6.12 over core 0.41.0: instance state on the list, category name rules, scope_type
(TD-43, TD-56, TD-71). Fixtures are invented."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from starlette.testclient import TestClient

from bifrost_api.account.app import create_account_app
from bifrost_api.strategy.schemas import responses as models
from bifrost_core.monitor.reader.errors import WriteConflict, WriteInvalid
from bifrost_core.portfolio.reader import position_categories
from tests.contract.helpers import operator_server_config

PG = {"sink": "postgres"}


def _client(reader: Any = None) -> TestClient:
    reader = reader or MagicMock()
    reader.config = operator_server_config()
    app = create_account_app(reader=reader, control_via_db=PG, status_cfg_for_read=PG, merged_config=reader.config)
    return TestClient(app, raise_server_exceptions=False)


def test_the_instance_list_sends_state_and_closed_on() -> None:
    reader = MagicMock()
    row = {
        "trade_id": 41,  # core's add_trade_names (naming R1)
        "strategy_instance_id": 41,
        "strategy_opportunity_id": 5,
        "strategy_opportunity_name": "Wheel",
        "strategy_structure_id": 2,
        "strategy_structure_name": "Short put",
        "account_id": "U0000001",
        "opened_at": "2026-09-01T14:30:00+00:00",
        "label": None,
        "notes": None,
        "created_at": "2026-09-01T14:30:00+00:00",
        "updated_at": "2026-09-01T14:30:00+00:00",
        "executions_count": 2,
        "state": "expired",
        "closed_on": "2026-09-18",
    }
    reader.list_strategy_instances.return_value = [row]
    body = _client(reader).get("/strategies/instances").json()
    assert body["items"][0]["state"] == "expired" and body["items"][0]["closed_on"] == "2026-09-18"
    assert {"state", "closed_on"} <= set(models.InstanceRow.model_fields)
    with pytest.raises(ValueError):
        models.InstanceRow.model_validate({**row, "state": "active"})


def test_category_rows_are_keyed_category_id_only() -> None:
    reader = MagicMock()
    reader.get_position_categories.return_value = [{"id": 3, "name": "Income", "description": None, "sort_order": 1}]
    body = _client(reader).get("/position-categories").json()
    assert body["items"] == [{"name": "Income", "description": None, "sort_order": 1, "category_id": 3}]


@pytest.mark.parametrize(
    "error,status",
    [(WriteConflict("A position category named 'Income' already exists."), 409),
     (WriteInvalid("'Uncategorized' is reserved for positions without a category; choose another name."), 400)],
)
def test_category_name_refusals_reach_the_client(monkeypatch: pytest.MonkeyPatch, error: Exception, status: int) -> None:
    reader = MagicMock()
    reader.create_position_category.side_effect = error

    def refuse(*_a: Any, **_k: Any) -> Any:
        raise error

    monkeypatch.setattr(position_categories, "patch_position_category", refuse)
    client = _client(reader)  # one app per test: the metrics registry is cleared between tests
    r = client.post("/position-categories", json={"name": "Income"})
    assert r.status_code == status and r.json()["detail"] == str(error.reason)
    r = client.patch("/position-categories/3", json={"name": "Income"})
    assert r.status_code == status


def test_scope_type_outside_the_vocabulary_is_refused_on_create() -> None:
    client = _client()
    r = client.post(
        "/strategies/opportunities",
        json={"name": "Rule", "strategy_structure_id": 2, "scope_type": "symbols", "symbols": ["ZZZQ"]},
    )
    assert r.status_code == 422
    r = client.post(
        "/strategies/opportunities",
        json={"name": "Rule", "strategy_structure_id": 2, "scope_type": "watchlist_stk", "symbols": []},
    )
    assert r.status_code == 400 and "at least one symbol" in r.json()["detail"]
