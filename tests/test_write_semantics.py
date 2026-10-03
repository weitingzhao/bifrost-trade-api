"""PATCH and strict DELETE on every Trade write route (TD-15, batch 3b-2; Owner decision B).

PATCH changes only the fields sent (an explicit null clears), refuses an empty body
or an unknown field (422) and answers the row. DELETE is strict: 404 for a missing
row, 409 with the reason while in use, and ``{"deleted": ..., "ok": true}`` on
success. Core's outcomes map once (``bifrost_api.common.write_errors``):
WriteNotFound 404 · WriteConflict 409 · WriteInvalid 400 · WriteFailed 503 when the
database is not configured or unreachable, else 500.

Most cases stand core's writer in with a recorder (the rules are core's, tested
there). The plan, instance, template and watchlist cases run core's real writer
over a scripted fake connection, because the route's answer depends on those
rules. Fixtures are invented.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock

import psycopg2
import pytest
from starlette.testclient import TestClient

import bifrost_api.trading.routers.executions as executions_router
from bifrost_api.account.app import create_account_app
from bifrost_api.common.write_errors import write_error_status
from bifrost_api.market.app import create_market_app
from bifrost_api.market.routers.watchlist import WatchlistItemPatch
from bifrost_api.portfolio.routers.config import InstrumentClassPatch, PositionCategoryPatch
from bifrost_api.strategy import patch_bodies
from bifrost_api.trading.routers.executions import ExecutionAttributionPatch
from bifrost_core.monitor.reader import gate_safety_write
from bifrost_core.monitor.reader import saved_search
from bifrost_core.monitor.reader import strategy_allocation_write
from bifrost_core.monitor.reader import strategy_instance
from bifrost_core.monitor.reader import strategy_opportunity_write
from bifrost_core.monitor.reader import strategy_plan
from bifrost_core.monitor.reader import strategy_rules_delete
from bifrost_core.monitor.reader import strategy_structure_write
from bifrost_core.monitor.reader import template_config_write
from bifrost_core.monitor.reader import trade_review
from bifrost_core.monitor.reader import watchlist
from bifrost_core.monitor.reader import write_support
from bifrost_core.monitor.reader.errors import (
    WriteConflict,
    WriteError,
    WriteFailed,
    WriteInvalid,
    WriteNotFound,
)
from bifrost_core.monitor.reader.strategy_rules_delete import RuleInUseError
from bifrost_core.portfolio.reader import accounts
from bifrost_core.portfolio.reader import instrument_class
from bifrost_core.portfolio.reader import position_categories
from tests import strategy_rows
from tests.contract.helpers import full_server_config, operator_server_config
from tests.envelope_asserts import assert_error

PG = {"sink": "postgres"}


def _account(control_via_db: Any = PG, config: Optional[dict] = None) -> TestClient:
    reader = MagicMock()
    reader.config = config or operator_server_config()
    app = create_account_app(
        reader=reader, control_via_db=control_via_db, status_cfg_for_read=control_via_db, merged_config=reader.config
    )
    return TestClient(app, raise_server_exceptions=False)


def _market(control_via_db: Any = PG) -> TestClient:
    reader = MagicMock()
    reader.config = {**operator_server_config(), "redis": {"enabled": False}}
    app = create_market_app(reader=reader, control_via_db=control_via_db, merged_config=reader.config)
    return TestClient(app, raise_server_exceptions=False)


def _client(app: str, control_via_db: Any = PG) -> TestClient:
    return _market(control_via_db) if app == "market" else _account(control_via_db)


def _raise(exc: BaseException) -> Any:
    def _f(*_a: Any, **_kw: Any) -> Any:
        raise exc

    return _f


# --- the mapping -----------------------------------------------------------------------


@pytest.mark.parametrize(
    "exc,status",
    [
        (WriteNotFound("No plan 1."), 404),
        (WriteConflict("In use."), 409),
        (RuleInUseError("It has 2 trades."), 409),
        (WriteInvalid("qty is required."), 400),
        (WriteFailed("Cannot write x: Postgres is not configured.", unavailable=True), 503),
        (WriteFailed("Cannot write x: the database is unreachable.", unavailable=True), 503),
        (WriteFailed("Cannot write x: the database write failed."), 500),
        (WriteError("Something else."), 500),
    ],
)
def test_write_error_status(exc: WriteError, status: int) -> None:
    assert write_error_status(exc) == status


# --- PATCH -------------------------------------------------------------------------------

# (app, path, core module, writer, the id the route passes, a body with one value and one null)
PATCH_CASES: List[Tuple[str, str, Any, str, Any, Dict[str, Any]]] = [
    ("account", "/strategies/templates/7", template_config_write, "patch_template", 7,
     {"display_name": "Covered call", "explanation": None}),
    ("account", "/strategies/structures/7", strategy_structure_write, "patch_structure", 7,
     {"name": "CC 30 delta", "notes": None}),
    ("account", "/strategies/opportunities/7", strategy_opportunity_write, "patch_opportunity", 7,
     {"is_active": False, "default_gate_safety_strategy_id": None}),
    ("account", "/strategies/allocations/7", strategy_allocation_write, "patch_allocation", 7,
     {"max_positions": 3, "gate_safety_strategy_id": None}),
    ("account", "/strategies/gate-safety/7", gate_safety_write, "patch_gate_safety", 7,
     {"gates": {"strategy": {"min_dte": 21}}, "dim_risk": None}),
    ("account", "/strategies/instances/7", strategy_instance, "patch_instance", 7,
     {"opened_at": 1767225600, "label": None}),
    ("account", "/strategies/plans/7", strategy_plan, "patch_plan", 7,
     {"qty": 2, "rationale": None}),
    ("account", "/strategies/reviews/7", trade_review, "patch_review", 7,
     {"tags_added": ["early exit"], "note": None}),
    ("account", "/position-categories/7", position_categories, "patch_position_category", 7,
     {"name": "Core", "description": None}),
    ("account", "/instrument-classes/ZZFI%7CSTK%7C%7C%7C", instrument_class, "patch_instrument_class", "ZZFI|STK|||",
     {"instrument_class": "fixed_income", "note": None}),
    ("account", "/executions/-42/attribution", accounts, "patch_execution", -42,
     {"instance_allocations": [], "strategy_instance_id": None}),
    ("market", "/watchlist/ZZQ%7CSTK%7C%7C%7C", watchlist, "patch_watchlist_item", "ZZQ|STK|||",
     {"optionable": True, "category_id": None}),
]
PATCH_IDS = [c[1] for c in PATCH_CASES]

# Routes whose old FE callers read `ok` from the answer keep it one release.
KEEPS_OK = {"/position-categories/7"}

# Routes with a response model (TD-24) answer their reader's real row; the others any row.
MODEL_ROWS = {
    "/strategies/opportunities/7": strategy_rows.opportunity,
    "/strategies/allocations/7": strategy_rows.allocation,
    "/strategies/gate-safety/7": strategy_rows.gate_set,
    "/strategies/instances/7": strategy_rows.instance,
    "/strategies/plans/7": strategy_rows.plan,
}


@pytest.mark.parametrize("app,path,module,fn,rid,body", PATCH_CASES, ids=PATCH_IDS)
def test_patch_passes_exactly_what_was_sent_and_answers_the_row(
    monkeypatch: pytest.MonkeyPatch, app: str, path: str, module: Any, fn: str, rid: Any, body: Dict[str, Any]
) -> None:
    calls: List[Tuple[Any, Any, Dict[str, Any]]] = []
    row = MODEL_ROWS[path]() if path in MODEL_ROWS else {"row_id": str(rid), "name": "stored"}

    def _writer(cfg: Any, wid: Any, fields: Dict[str, Any]) -> Dict[str, Any]:
        calls.append((cfg, wid, fields))
        return row

    monkeypatch.setattr(module, fn, _writer)
    r = _client(app).patch(path, json=body)
    assert r.status_code == 200, r.text
    assert r.json() == strategy_rows.as_sent_before({**row, "ok": True} if path in KEEPS_OK else row)
    # The explicit null reaches core as a key with None: that is what clears the column.
    assert calls == [(PG, rid, body)]
    assert any(v is None for v in calls[0][2].values())


@pytest.mark.parametrize("app,path,module,fn,rid,body", PATCH_CASES, ids=PATCH_IDS)
def test_patch_empty_or_unknown_is_422_and_writes_nothing(
    monkeypatch: pytest.MonkeyPatch, app: str, path: str, module: Any, fn: str, rid: Any, body: Dict[str, Any]
) -> None:
    writer = MagicMock()
    monkeypatch.setattr(module, fn, writer)
    c = _client(app)
    empty = c.patch(path, json={})
    assert empty.status_code == 422 and "at least one field" in empty.text
    unknown = c.patch(path, json={**body, "bogus": 1})
    assert unknown.status_code == 422 and "bogus" in unknown.text
    assert c.patch(path).status_code == 422  # no body at all
    writer.assert_not_called()


@pytest.mark.parametrize(
    "exc,status",
    [
        (WriteNotFound("No such row 7."), 404),
        (WriteConflict("It is in use: the reason."), 409),
        (WriteInvalid("name is required."), 400),
        (WriteFailed("Cannot write row 7: the database is unreachable.", unavailable=True), 503),
        (WriteFailed("Cannot write row 7: the database write failed."), 500),
    ],
)
@pytest.mark.parametrize("app,path,module,fn,rid,body", PATCH_CASES, ids=PATCH_IDS)
def test_patch_outcomes(
    monkeypatch: pytest.MonkeyPatch,
    app: str,
    path: str,
    module: Any,
    fn: str,
    rid: Any,
    body: Dict[str, Any],
    exc: WriteError,
    status: int,
) -> None:
    monkeypatch.setattr(module, fn, _raise(exc))
    out = assert_error(_client(app).patch(path, json=body), status)
    assert out["detail"] == exc.reason


@pytest.mark.parametrize("app,path,module,fn,rid,body", PATCH_CASES, ids=PATCH_IDS)
def test_patch_without_postgres_is_503(
    monkeypatch: pytest.MonkeyPatch, app: str, path: str, module: Any, fn: str, rid: Any, body: Dict[str, Any]
) -> None:
    writer = MagicMock()
    monkeypatch.setattr(module, fn, writer)
    assert_error(_client(app, None).patch(path, json=body), 503, "Postgres is not configured")
    writer.assert_not_called()


def test_patch_types_are_strict(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(strategy_opportunity_write, "patch_opportunity", MagicMock())
    c = _account()
    assert c.patch("/strategies/opportunities/7", json={"is_active": "yes"}).status_code == 422
    assert c.patch("/strategies/opportunities/7", json={"strategy_structure_id": "5"}).status_code == 422
    assert c.patch("/strategies/opportunities/7", json={"name": 5}).status_code == 422


def test_patch_is_a_write_the_guard_refuses_a_viewer() -> None:
    viewer = {**full_server_config(), "ops": {"auth": {"default_role": "viewer"}}}
    assert _account(config=viewer).patch("/strategies/plans/7", json={"qty": 2}).status_code == 403


@pytest.mark.parametrize(
    "model,patchable",
    [
        (patch_bodies.TemplatePatch, template_config_write.TEMPLATE_PATCHABLE),
        (patch_bodies.StructurePatch, strategy_structure_write.STRUCTURE_PATCHABLE),
        (patch_bodies.OpportunityPatch, strategy_opportunity_write.OPPORTUNITY_PATCHABLE),
        (patch_bodies.AllocationPatch, strategy_allocation_write.ALLOCATION_PATCHABLE),
        (patch_bodies.GateSafetyPatch, gate_safety_write.GATE_SAFETY_PATCHABLE),
        (patch_bodies.InstancePatch, strategy_instance.INSTANCE_PATCHABLE),
        (patch_bodies.PlanPatch, strategy_plan.PLAN_PATCHABLE),
        (patch_bodies.ReviewPatch, trade_review.REVIEW_PATCHABLE),
        (PositionCategoryPatch, position_categories.POSITION_CATEGORY_PATCHABLE),
        (InstrumentClassPatch, instrument_class.INSTRUMENT_CLASS_PATCHABLE),
        (ExecutionAttributionPatch, accounts.EXECUTION_PATCHABLE),
        (WatchlistItemPatch, watchlist.WATCHLIST_PATCHABLE),
    ],
)
def test_each_patch_body_offers_exactly_what_core_patches(model: Any, patchable: Tuple[str, ...]) -> None:
    """A body may also take a field's read name (PlanPatch's legs_json, TD-57), mapped onto a patchable one."""
    read_names: Dict[str, str] = getattr(model, "READ_NAMES", {})
    assert set(model.model_fields) - set(read_names) == set(patchable)
    assert set(read_names.values()) <= set(patchable)


# --- DELETE ------------------------------------------------------------------------------

# (app, path, module holding the writer, writer, its answer)
DELETE_CASES: List[Tuple[str, str, Any, str, Dict[str, Any]]] = [
    ("account", "/strategies/templates/7", template_config_write, "delete_template_strict",
     {"deleted": "hard", "strategy_template_id": 7}),
    ("account", "/strategies/structures/7", strategy_structure_write, "delete_structure_strict",
     {"deleted": "soft", "strategy_structure_id": 7, "was_active": True, "cleared_daemon_setting": False}),
    ("account", "/strategies/opportunities/7", strategy_rules_delete, "delete_opportunity_strict",
     {"deleted": "hard", "strategy_opportunity_id": 7}),
    ("account", "/strategies/allocations/7", strategy_rules_delete, "delete_allocation_strict",
     {"deleted": "hard", "strategy_allocation_id": 7}),
    ("account", "/strategies/gate-safety/7", strategy_rules_delete, "delete_gate_safety_strict",
     {"deleted": "hard", "gate_safety_strategy_id": 7}),
    ("account", "/strategies/instances/7", strategy_instance, "delete_instance_strict",
     {"deleted": "hard", "strategy_instance_id": 7}),
    ("account", "/strategies/plans/7", strategy_plan, "delete_plan_strict",
     {"deleted": "hard", "strategy_plan_id": 7}),
    ("account", "/strategies/saved-searches/7", saved_search, "delete_saved_search_strict",
     {"deleted": "hard", "preference_saved_search_id": 7}),
    ("account", "/position-categories/7", position_categories, "delete_position_category_strict",
     {"deleted": "hard", "id": 7, "tags_removed": 2, "watchlist_uncategorized": 1}),
    ("account", "/instrument-classes/ZZFI", instrument_class, "delete_instrument_class_strict",
     {"deleted": "hard", "contract_key": "ZZFI"}),
    ("account", "/executions/-42", accounts, "delete_execution_strict",
     {"deleted": "hard", "account_executions_id": -42, "allocations_removed": 0}),
    ("account", "/executions/option-stock-links/7?account_id=U0000001", executions_router, "delete_option_stock_link_strict",
     {"deleted": "hard", "account_execution_option_stock_link_id": 7}),
    ("market", "/watchlist?contract_key=ZZQ%7CSTK%7C%7C%7C", watchlist, "delete_watchlist_strict",
     {"deleted": "hard", "contract_key": "ZZQ|STK|||"}),
]
DELETE_IDS = [c[1] for c in DELETE_CASES]


@pytest.mark.parametrize("app,path,module,fn,answer", DELETE_CASES, ids=DELETE_IDS)
def test_delete_answers_what_was_deleted(
    monkeypatch: pytest.MonkeyPatch, app: str, path: str, module: Any, fn: str, answer: Dict[str, Any]
) -> None:
    monkeypatch.setattr(module, fn, lambda *_a, **_kw: answer)
    r = _client(app).delete(path)
    assert r.status_code == 200, r.text
    assert r.json() == {**answer, "ok": True}
    assert r.json()["deleted"] in ("hard", "soft")


@pytest.mark.parametrize(
    "exc,status",
    [
        (WriteNotFound("No such row 7."), 404),
        (WriteConflict("2 structures use this template: A and B. Point them at another template first."), 409),
        (WriteFailed("Cannot write row 7: the database is unreachable.", unavailable=True), 503),
        (WriteFailed("Cannot write row 7: the database write failed."), 500),
    ],
)
@pytest.mark.parametrize("app,path,module,fn,answer", DELETE_CASES, ids=DELETE_IDS)
def test_delete_outcomes(
    monkeypatch: pytest.MonkeyPatch,
    app: str,
    path: str,
    module: Any,
    fn: str,
    answer: Dict[str, Any],
    exc: WriteError,
    status: int,
) -> None:
    monkeypatch.setattr(module, fn, _raise(exc))
    out = assert_error(_client(app).delete(path), status)
    assert out["detail"] == exc.reason


@pytest.mark.parametrize("app,path,module,fn,answer", DELETE_CASES, ids=DELETE_IDS)
def test_delete_without_postgres_is_503(
    monkeypatch: pytest.MonkeyPatch, app: str, path: str, module: Any, fn: str, answer: Dict[str, Any]
) -> None:
    writer = MagicMock()
    monkeypatch.setattr(module, fn, writer)
    assert_error(_client(app, None).delete(path), 503, "Postgres is not configured")
    writer.assert_not_called()


# --- core's real rules over a scripted connection ---------------------------------------


class _Cursor:
    def __init__(self, conn: "_Conn") -> None:
        self._conn = conn
        self._one: Any = None
        self._all: List[Any] = []
        self.rowcount = 1

    def execute(self, sql: str, params: Any = None) -> None:
        text = re.sub(r"\s+", " ", sql).strip()
        self._conn.executed.append((text, params))
        for fragment, reply in self._conn.rules:
            if fragment in text:
                if isinstance(reply, BaseException):
                    raise reply
                self._one, self._all = reply.get("one"), list(reply.get("all") or [])
                self.rowcount = reply.get("rowcount", 1)
                return
        self._one, self._all, self.rowcount = None, [], 1

    def fetchone(self) -> Any:
        return self._one

    def fetchall(self) -> List[Any]:
        return self._all

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_exc: Any) -> None:
        return None


class _Conn:
    def __init__(self, rules: List[Tuple[str, Any]]) -> None:
        self.rules = rules
        self.executed: List[Tuple[str, Any]] = []
        self.commits = 0

    def cursor(self, **_kw: Any) -> _Cursor:
        return _Cursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        pass

    def ran(self, fragment: str) -> bool:
        return any(fragment in sql for sql, _ in self.executed)


def _connect(monkeypatch: pytest.MonkeyPatch, env: _Conn, golden_source: Optional[_Conn] = None) -> None:
    """Core opens its connections through ``write_support.connect``; answer with the fakes."""

    def _open(_params: Any, golden: bool = False) -> _Conn:
        return golden_source if golden and golden_source is not None else env

    monkeypatch.setattr(write_support, "connect", _open)


def _plan_conn(status: str) -> _Conn:
    stored = {"status": status, "target_kind": None, "target_value": None, "stop_kind": None, "stop_value": None}
    return _Conn([("FROM strategy_plan WHERE strategy_plan_id = %s FOR UPDATE", {"one": stored})])


def test_an_intended_plan_takes_a_new_expiry(monkeypatch: pytest.MonkeyPatch) -> None:
    """The plan card's "Extend 7 days" / "Re-issue intent": PUT refused these with 409."""
    conn = _plan_conn("intended")
    _connect(monkeypatch, conn)
    plan = strategy_rows.plan(status="intended")
    monkeypatch.setattr(strategy_plan, "_get_plan_on", lambda _conn, _pid: plan)
    r = _account().patch("/strategies/plans/12", json={"expires_at": "2026-10-09T20:00:00Z"})
    assert r.status_code == 200, r.text
    assert r.json() == strategy_rows.as_sent_before(plan)
    assert conn.ran("UPDATE strategy_plan SET expires_at = %s") and conn.commits == 1


def test_an_intended_plan_refuses_any_other_field_with_409(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _plan_conn("intended")
    _connect(monkeypatch, conn)
    r = _account().patch("/strategies/plans/12", json={"expires_at": None, "qty": 3})
    out = assert_error(r, 409, "only its expiry (expires_at) can change, not qty")
    assert "Cancel it" in out["detail"]
    assert not conn.ran("UPDATE strategy_plan")


def test_a_draft_plan_with_bad_input_is_400_not_409(monkeypatch: pytest.MonkeyPatch) -> None:
    _connect(monkeypatch, _plan_conn("draft"))
    assert_error(_account().patch("/strategies/plans/12", json={"qty": None}), 400, "qty")


def test_a_missing_plan_is_404(monkeypatch: pytest.MonkeyPatch) -> None:
    _connect(monkeypatch, _Conn([("FROM strategy_plan WHERE strategy_plan_id = %s FOR UPDATE", {"one": None})]))
    assert_error(_account().patch("/strategies/plans/404", json={"rationale": "x"}), 404, "No plan 404.")


def _instance_env(split: int = 0, exists: bool = True, direct: int = 0) -> _Conn:
    # core 0.37.0 (TD-09): both counts come from this env's strategy_instance_execution.
    return _Conn(
        [
            ("SELECT 1 FROM strategy_instance WHERE strategy_instance_id = %s FOR UPDATE", {"one": (1,) if exists else None}),
            ("FROM strategy_instance_execution WHERE strategy_instance_id", {"one": (direct, split)}),
            ("DELETE FROM strategy_instance", {"rowcount": 1}),
        ]
    )


def test_an_instance_with_attributed_executions_is_409(monkeypatch: pytest.MonkeyPatch) -> None:
    env = _instance_env(direct=2)
    _connect(monkeypatch, env, _Conn([]))
    assert_error(_account().delete("/strategies/instances/7"), 409, "2 fills are attributed to this trade.")
    assert not env.ran("DELETE FROM strategy_instance")


def test_an_instance_with_split_executions_is_409(monkeypatch: pytest.MonkeyPatch) -> None:
    _connect(monkeypatch, _instance_env(split=1), _Conn([]))
    assert_error(_account().delete("/strategies/instances/7"), 409, "split to this trade")


def test_an_instance_nothing_points_at_is_deleted(monkeypatch: pytest.MonkeyPatch) -> None:
    env = _instance_env()
    _connect(monkeypatch, env, _Conn([]))
    r = _account().delete("/strategies/instances/7")
    assert r.json() == {"deleted": "hard", "strategy_instance_id": 7, "trade_id": 7, "ok": True}
    assert env.ran("DELETE FROM strategy_instance") and env.commits == 1


def test_a_missing_instance_is_404(monkeypatch: pytest.MonkeyPatch) -> None:
    _connect(monkeypatch, _instance_env(exists=False), _Conn([]))
    assert_error(_account().delete("/strategies/instances/404"), 404, "No trade 404.")


def test_the_instance_delete_does_not_need_the_golden_source(monkeypatch: pytest.MonkeyPatch) -> None:
    """core 0.37.0 (TD-09): attribution is this env's; an unreachable Golden Source no longer matters."""
    env = _instance_env()

    def _connect_or_refuse(_params: Any, golden: bool = False) -> Any:
        if golden:
            raise psycopg2.OperationalError("connection refused")
        return env

    monkeypatch.setattr(write_support, "connect", _connect_or_refuse)
    assert _account().delete("/strategies/instances/7").json()["deleted"] == "hard"
    assert env.ran("DELETE FROM strategy_instance")


def test_a_template_in_use_is_409_naming_its_structures(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _Conn(
        [
            ("SELECT 1 FROM strategy_template", {"one": (1,)}),
            ("FROM strategy_structure WHERE strategy_template_id", {"all": [("CC 30 delta", True), ("Old CC", False)]}),
        ]
    )
    _connect(monkeypatch, conn)
    out = assert_error(_account().delete("/strategies/templates/3"), 409)
    assert "CC 30 delta" in out["detail"] and "Old CC (deactivated)" in out["detail"]
    assert not conn.ran("DELETE FROM strategy_template")


def test_a_missing_template_is_404_not_200(monkeypatch: pytest.MonkeyPatch) -> None:
    _connect(monkeypatch, _Conn([("SELECT 1 FROM strategy_template", {"one": None})]))
    assert_error(_account().delete("/strategies/templates/404"), 404, "No template 404.")


# One app serves requests per test: instrument_app registers process-wide gauges
# (tests/conftest.py clears them between tests).


def test_a_failed_statement_is_500_without_the_database_text(monkeypatch: pytest.MonkeyPatch) -> None:
    _connect(monkeypatch, _Conn([("SELECT 1 FROM strategy_template", RuntimeError("disk full"))]))
    out = assert_error(_account().delete("/strategies/templates/3"), 500, "the database write failed")
    assert "disk full" not in out["detail"]


def test_an_unreachable_database_is_503(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(write_support, "connect", _raise(psycopg2.OperationalError("connection refused")))
    assert_error(_account().delete("/strategies/templates/3"), 503, "the database is unreachable")


# --- POST /watchlist: a re-add keeps what it does not send --------------------------------


def _record_upsert(monkeypatch: pytest.MonkeyPatch) -> List[Tuple[str, Dict[str, Any]]]:
    calls: List[Tuple[str, Dict[str, Any]]] = []

    def _upsert(_cfg: Any, key: str, fields: Dict[str, Any]) -> Dict[str, Any]:
        calls.append((key, fields))
        return {"contract_key": key}

    monkeypatch.setattr(watchlist, "upsert_watchlist", _upsert)
    return calls


def test_watchlist_readd_sends_only_what_the_surface_sent(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _record_upsert(monkeypatch)
    body = {"contract_key": "ZZQ|STK|||", "symbol": "ZZQ", "sec_type": "STK", "source": "omnibar"}
    assert _market().post("/watchlist", json=body).status_code == 200
    # No category_id, no display_label: the stored list and label are kept.
    assert calls == [("ZZQ|STK|||", {"symbol": "ZZQ", "sec_type": "STK", "source": "omnibar"})]


def test_watchlist_explicit_null_category_clears_it(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _record_upsert(monkeypatch)
    _market().post("/watchlist", json={"contract_key": "ZZQ|STK|||", "category_id": None, "source": "manual"})
    assert calls[0][1] == {"category_id": None, "source": "manual"}


def test_watchlist_source_is_passed_through_not_defaulted(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _record_upsert(monkeypatch)
    _market().post("/watchlist", json={"contract_key": "ZZQ|STK|||", "category_id": 4})
    assert calls[0][1] == {"category_id": 4}  # core gives a new row 'manual'; a re-add keeps its source


def test_watchlist_post_keeps_its_old_meaning_for_a_null_optionable_and_blank_text(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _record_upsert(monkeypatch)
    body = {"contract_key": "ZZQ|STK|||", "optionable": None, "expiry": "", "option_right": " ", "symbol": "ZZQ"}
    _market().post("/watchlist", json=body)
    assert calls[0][1] == {"symbol": "ZZQ"}


def test_watchlist_readd_over_core_updates_only_the_sent_columns(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _Conn([("FROM watchlist w", {"one": {"contract_key": "ZZQ|STK|||", "category_id": 4}})])
    _connect(monkeypatch, conn)
    r = _market().post("/watchlist", json={"contract_key": "ZZQ|STK|||", "symbol": "ZZQ", "source": "drag"})
    assert r.status_code == 200 and r.json()["category_id"] == 4
    sql = next(s for s, _ in conn.executed if s.startswith("INSERT INTO watchlist"))
    assert "DO UPDATE SET symbol = EXCLUDED.symbol, source = EXCLUDED.source" in sql
    assert "category_id" not in sql and "display_label" not in sql


def test_watchlist_null_category_over_core_sets_it_null(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _Conn([("FROM watchlist w", {"one": {"contract_key": "ZZQ|STK|||", "category_id": None}})])
    _connect(monkeypatch, conn)
    _market().post("/watchlist", json={"contract_key": "ZZQ|STK|||", "category_id": None})
    sql, params = next((s, p) for s, p in conn.executed if s.startswith("INSERT INTO watchlist"))
    assert "DO UPDATE SET category_id = EXCLUDED.category_id" in sql and None in params


def test_watchlist_post_with_an_unknown_category_is_400(monkeypatch: pytest.MonkeyPatch) -> None:
    fk = psycopg2.errors.ForeignKeyViolation("insert or update on table watchlist violates foreign key")
    _connect(monkeypatch, _Conn([("INSERT INTO watchlist", fk)]))
    assert_error(_market().post("/watchlist", json={"contract_key": "ZZQ", "category_id": 999}), 400, "referenced row")
