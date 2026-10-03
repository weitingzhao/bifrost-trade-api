"""DELETE for a draft plan and the Desk's rule objects (design Rev .138 / .140).

The UI deletes without asking and holds the call until its Undo toast closes,
so these must refuse with the reason (409), miss with 404, and say plainly
when there is no database (503). Strict since TD-15 (core ``*_strict``).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from starlette.testclient import TestClient

from bifrost_api.account.app import create_account_app
from bifrost_core.monitor.reader import strategy_plan as strategy_plan_module
from bifrost_core.monitor.reader import strategy_rules_delete as rules_module
from bifrost_core.monitor.reader.errors import WriteNotFound
from bifrost_core.monitor.reader.strategy_plan import PlanRuleError
from bifrost_core.monitor.reader.strategy_rules_delete import RuleInUseError
from tests.contract.helpers import operator_server_config

RULE_ROUTES = [
    ("/strategies/opportunities/5", "delete_opportunity_strict"),
    ("/strategies/allocations/5", "delete_allocation_strict"),
    ("/strategies/gate-safety/5", "delete_gate_safety_strict"),
]


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


def test_deletes_without_postgres_are_503() -> None:
    client = _client()
    assert client.delete("/strategies/plans/1").status_code == 503
    for path, _fn in RULE_ROUTES:
        assert client.delete(path).status_code == 503


def test_a_plan_past_draft_is_409_with_the_reason(monkeypatch: pytest.MonkeyPatch) -> None:
    def _refuse(*_a: Any, **_kw: Any) -> Any:
        raise PlanRuleError("This plan is intended; only a draft can be deleted.")

    monkeypatch.setattr(strategy_plan_module, "delete_plan_strict", _refuse)
    r = _client({"sink": "postgres"}).delete("/strategies/plans/1")
    assert r.status_code == 409
    assert r.json()["detail"] == "This plan is intended; only a draft can be deleted."


def test_a_deleted_draft_and_a_missing_one(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _client({"sink": "postgres"})
    monkeypatch.setattr(
        strategy_plan_module, "delete_plan_strict", lambda _cfg, pid: {"deleted": "hard", "strategy_plan_id": pid}
    )
    assert client.delete("/strategies/plans/1").json() == {"deleted": "hard", "strategy_plan_id": 1, "ok": True}

    def _missing(_cfg: Any, pid: int) -> Any:
        raise WriteNotFound(f"No strategy plan {pid}.")

    monkeypatch.setattr(strategy_plan_module, "delete_plan_strict", _missing)
    r = client.delete("/strategies/plans/404")
    assert r.status_code == 404 and r.json()["detail"] == "No strategy plan 404."


@pytest.mark.parametrize("path,fn", RULE_ROUTES)
def test_a_rule_in_use_is_409_with_the_reason(monkeypatch: pytest.MonkeyPatch, path: str, fn: str) -> None:
    def _refuse(*_a: Any, **_kw: Any) -> Any:
        raise RuleInUseError("It has 2 trades; a rule with trades stays.")

    monkeypatch.setattr(rules_module, fn, _refuse)
    r = _client({"sink": "postgres"}).delete(path)
    assert r.status_code == 409
    assert r.json()["detail"] == "It has 2 trades; a rule with trades stays."


@pytest.mark.parametrize("path,fn", RULE_ROUTES)
def test_a_rule_deleted_or_missing(monkeypatch: pytest.MonkeyPatch, path: str, fn: str) -> None:
    client = _client({"sink": "postgres"})
    monkeypatch.setattr(rules_module, fn, lambda _cfg, rid: {"deleted": "hard", "id": rid})
    assert client.delete(path).json() == {"deleted": "hard", "id": 5, "ok": True}

    def _missing(*_a: Any, **_kw: Any) -> Any:
        raise WriteNotFound("Not there.")

    monkeypatch.setattr(rules_module, fn, _missing)
    assert client.delete(path).status_code == 404
