"""ops_feedback DDL runs in db-init, never on a request (TD-77, api 0.6.8).

Until 0.6.8 every process's first feedback request, reads included, ran
CREATE SCHEMA / TABLE / INDEX IF NOT EXISTS on Golden Source as the runtime
role. Now ``feedback_schema.ensure_feedback_schema`` is called by
``scripts/run_db_refresh_schema.py`` and a store that was never created
answers 503.
"""

from __future__ import annotations

import importlib.util
import re
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, List
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from bifrost_api.research import feedback_schema, feedback_store as store
from bifrost_api.research.routers import feedback as feedback_router

_REPO = Path(__file__).resolve().parents[1]


class _Cur:
    def __init__(self, sink: List[str], fail_on: str | None = None, one: Any = None) -> None:
        self.sink, self.fail_on, self.one = sink, fail_on, one

    def __enter__(self) -> "_Cur":
        return self

    def __exit__(self, *a: Any) -> None:
        return None

    def execute(self, sql: str, params: Any = None) -> None:
        self.sink.append(" ".join(sql.split()))
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError(f"permission denied for schema ops_feedback ({self.fail_on})")

    def fetchone(self) -> Any:
        return self.one

    def fetchall(self) -> Any:
        return []


class _Conn:
    def __init__(self, fail_on: str | None = None, one: Any = None) -> None:
        self.sink: List[str] = []
        self.fail_on, self.one = fail_on, one
        self.commits = 0
        self.rollbacks = 0

    def cursor(self, **_kw: Any) -> _Cur:
        return _Cur(self.sink, self.fail_on, self.one)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


def test_steps_are_ordered_and_idempotent() -> None:
    names = [n for n, _ in feedback_schema.FEEDBACK_SCHEMA_STEPS]
    assert names == [
        "schema ops_feedback",
        "table ops_feedback.report",
        "table ops_feedback.report_image",
        "index idx_feedback_report_status",
    ]
    for _, sql in feedback_schema.FEEDBACK_SCHEMA_STEPS:
        assert re.match(r"\s*CREATE (SCHEMA|TABLE|INDEX) IF NOT EXISTS ", sql), sql


def test_ensure_runs_every_step_in_one_transaction_and_commits() -> None:
    conn, lines = _Conn(), []
    feedback_schema.ensure_feedback_schema(conn, log=lines.append)
    ddl = [s for s in conn.sink if s.startswith("CREATE")]
    assert len(ddl) == 4 and conn.sink[0].startswith("SET LOCAL lock_timeout")
    assert conn.commits == 1 and conn.rollbacks == 0
    assert lines == [f"{n}: ok" for n, _ in feedback_schema.FEEDBACK_SCHEMA_STEPS]


def test_ensure_rolls_back_and_raises_on_a_failed_step() -> None:
    conn = _Conn(fail_on="ops_feedback.report_image")
    with pytest.raises(RuntimeError, match="permission denied"):
        feedback_schema.ensure_feedback_schema(conn, log=lambda _m: None)
    assert conn.commits == 0 and conn.rollbacks == 1


def test_the_store_has_no_ddl_left() -> None:
    for name in ("ensure_schema", "_DDL", "_ddl_done", "_ddl_lock"):
        assert not hasattr(store, name), name
    src = (_REPO / "src/bifrost_api/research/feedback_store.py").read_text()
    assert "CREATE " not in src


@pytest.mark.parametrize(
    "call,one",
    [
        (lambda c: store.list_reports(c, scope="open"), None),
        (lambda c: store.summary(c), (0, 0, 0, 0)),
        (lambda c: store.get_image(c, 1, 0), None),
        (lambda c: store.mark_read(c, 1), None),
        (lambda c: store.set_status(c, 1, "triaged"), None),
        (lambda c: store.set_reply(c, 1, "thanks"), None),
    ],
)
def test_request_path_statements_are_dml_only(call: Any, one: Any) -> None:
    conn = _Conn(one=one)
    call(conn)
    assert conn.sink, "the call ran no statement"
    assert not [s for s in conn.sink if s.split()[0].upper() in ("CREATE", "ALTER", "DROP")]


# ── 503 when the store was never created ──


class _PgError(Exception):
    def __init__(self, pgcode: str, msg: str) -> None:
        super().__init__(msg)
        self.pgcode = pgcode


@pytest.fixture
def client_with(monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    def make(exc: Exception) -> TestClient:
        @contextmanager
        def _get_conn() -> Iterator[Any]:
            conn = MagicMock()
            conn.cursor.return_value.__enter__.return_value.execute.side_effect = exc
            yield conn

        monkeypatch.setattr(feedback_router, "get_conn", _get_conn)
        app = FastAPI()
        app.include_router(feedback_router.router)
        return TestClient(app)

    yield make


@pytest.mark.parametrize("pgcode", ["42P01", "3F000"])
@pytest.mark.parametrize(
    "method,url,body",
    [
        ("GET", "/research/feedback/summary", None),
        ("GET", "/research/feedback/reports", None),
        ("POST", "/research/feedback/reports", {"kind": "bug", "title": "t"}),
        ("POST", "/research/feedback/reports/FB-0001/read", None),
        ("POST", "/research/feedback/reports/FB-0001/status", {"status": "triaged"}),
        ("POST", "/research/feedback/reports/FB-0001/reply", {"reply_md": "ok"}),
    ],
)
def test_a_missing_store_is_503_not_migrated(client_with: Any, pgcode: str, method: str, url: str, body: Any) -> None:
    client = client_with(_PgError(pgcode, 'relation "ops_feedback.report" does not exist'))
    resp = client.request(method, url, json=body)
    assert resp.status_code == 503
    assert resp.json() == {"detail": feedback_router.NOT_MIGRATED_DETAIL}


def test_other_store_failures_keep_their_message(client_with: Any) -> None:
    client = client_with(_PgError("57014", "canceling statement due to statement timeout"))
    resp = client.get("/research/feedback/summary")
    assert resp.status_code == 503
    assert resp.json() == {"detail": "feedback store error: canceling statement due to statement timeout"}


# ── db-init calls it ──


def _load_script() -> Any:
    spec = importlib.util.spec_from_file_location("run_db_refresh_schema", _REPO / "scripts/run_db_refresh_schema.py")
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_db_init_runs_the_feedback_ddl_after_brokerage(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    import psycopg2

    from bifrost_core.persistence.postgres import brokerage_ddl, ddl

    cfg = tmp_path / "runtime.yaml"
    cfg.write_text(
        "postgres: {host: db.test, port: 5432, dbname: bifrost_test, user: u0, password: p0}\n"
        "golden_source: {host: gs.test, port: 5432, dbname: gs_test, user: u1, password: p1}\n"
    )
    monkeypatch.setenv("BIFROST_CONFIG", str(cfg))
    order: List[str] = []
    monkeypatch.setattr(psycopg2, "connect", lambda **kw: MagicMock(name=kw.get("host")))
    monkeypatch.setattr(ddl, "ensure_tables", lambda conn: order.append("trade"))
    monkeypatch.setattr(brokerage_ddl, "ensure_brokerage_schema", lambda conn, log: order.append("brokerage"))
    monkeypatch.setattr(brokerage_ddl, "setup_fdw_foreign_tables", lambda *a, **k: order.append("fdw"))
    monkeypatch.setattr(brokerage_ddl, "setup_fdw_market_tables", lambda *a, **k: order.append("fdw_market"))
    monkeypatch.setattr(feedback_schema, "ensure_feedback_schema", lambda conn, log: order.append("feedback"))

    assert _load_script().main() == 0
    assert order == ["trade", "brokerage", "feedback", "fdw", "fdw_market"]


def test_db_init_fails_the_job_when_feedback_ddl_fails(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    import psycopg2

    from bifrost_core.persistence.postgres import brokerage_ddl, ddl

    cfg = tmp_path / "runtime.yaml"
    cfg.write_text(
        "postgres: {host: db.test, port: 5432, dbname: bifrost_test, user: u0, password: p0}\n"
        "golden_source: {host: gs.test, port: 5432, dbname: gs_test, user: u1, password: p1}\n"
    )
    monkeypatch.setenv("BIFROST_CONFIG", str(cfg))
    order: List[str] = []
    monkeypatch.setattr(psycopg2, "connect", lambda **kw: MagicMock())
    monkeypatch.setattr(ddl, "ensure_tables", lambda conn: None)
    monkeypatch.setattr(brokerage_ddl, "ensure_brokerage_schema", lambda conn, log: None)
    monkeypatch.setattr(brokerage_ddl, "setup_fdw_foreign_tables", lambda *a, **k: order.append("fdw"))
    monkeypatch.setattr(brokerage_ddl, "setup_fdw_market_tables", lambda *a, **k: None)

    def _denied(conn: Any, log: Any) -> None:
        raise RuntimeError("permission denied for schema ops_feedback")

    monkeypatch.setattr(feedback_schema, "ensure_feedback_schema", _denied)

    assert _load_script().main() == 1
    assert order == ["fdw"]  # the steps after it still ran
