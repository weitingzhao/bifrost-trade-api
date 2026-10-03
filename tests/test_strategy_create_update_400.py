"""POST allocations and opportunities refuse what core cannot store (TD-48, core 0.35.0).

An unparseable allocation limit or gate id used to be stored as NULL with a 200. Core now
raises WriteInvalid and the shared write_errors handler answers 400 with its reason. The
database is never reached: core refuses before connecting. Ids are made up.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from starlette.testclient import TestClient

from bifrost_api.account.app import create_account_app
from bifrost_core.monitor.reader import write_support as ws
from tests.contract.helpers import operator_server_config
from tests.envelope_asserts import assert_error

PG = {"sink": "postgres"}


def _client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    def no_db(*_a: Any, **_k: Any) -> Any:
        raise AssertionError("the database must not be reached")

    monkeypatch.setattr(ws, "conn_from_config", no_db)
    reader = MagicMock()
    reader._config = operator_server_config()
    app = create_account_app(reader=reader, control_via_db=PG, status_cfg_for_read=PG, merged_config=reader._config)
    return TestClient(app, raise_server_exceptions=False)


@pytest.mark.parametrize(
    "limits,reason",
    [
        ({"max_positions": "four"}, "max_positions must be a whole number"),
        ({"max_positions": -1}, "max_positions must be 0 or more"),
        ({"max_bp_pct": "lots"}, "max_bp_pct must be a number"),
        ({"max_risk": 3}, "Unknown allocation_limits field: max_risk"),
    ],
)
def test_bad_allocation_limits_are_400(monkeypatch: pytest.MonkeyPatch, limits: dict, reason: str) -> None:
    client = _client(monkeypatch)
    body = {"name": "Sleeve", "strategy_opportunity_ids": [1], "allocation_limits": limits}
    assert_error(client.post("/strategies/allocations", json=body), 400, reason)


def test_a_bad_gate_id_is_400(monkeypatch: pytest.MonkeyPatch) -> None:
    """The bodies type the gate ids as int, so only a non-positive id reaches core."""
    client = _client(monkeypatch)
    body = {"name": "Sleeve", "strategy_opportunity_ids": [], "gate_safety_strategy_id": 0}
    assert_error(client.post("/strategies/allocations", json=body), 400, "gate_safety_strategy_id must be 1 or more")
    opp = {"name": "Opp", "strategy_structure_id": 5, "default_gate_safety_strategy_id": -1}
    assert_error(client.post("/strategies/opportunities", json=opp), 400, "default_gate_safety_strategy_id")


@pytest.mark.parametrize("path", ["/strategies/allocations/7", "/strategies/opportunities/9", "/strategies/templates/9"])
def test_the_merge_puts_are_gone(monkeypatch: pytest.MonkeyPatch, path: str) -> None:
    """TD-15 (api 0.6.0): PATCH is the update; the merge-style PUT went after a release with no caller."""
    assert _client(monkeypatch).put(path, json={"name": "x"}).status_code == 405
