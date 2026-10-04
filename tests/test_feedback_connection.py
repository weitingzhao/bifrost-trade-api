"""The feedback store owns its Golden Source connection (TD-49 D4 / TD-77 E5, api 0.7.5).

Until 0.7.5 the store borrowed ``analytics_reader.get_conn`` and with it the
shared ``analytics_writer`` role. Now it reads ``FEEDBACK_PG_*`` (role
``feedback_writer``), and nothing else in the research app holds a database
pool aimed at Golden Source.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Any, Dict, Iterator

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from bifrost_api.research import analytics_reader, feedback_store as store
from bifrost_api.research.routers import feedback as feedback_router

_RESEARCH = Path(store.__file__).resolve().parent
_ENV = {
    "FEEDBACK_PG_HOST": "pg.feedback.test",
    "FEEDBACK_PG_USER": "feedback_writer",
    "FEEDBACK_PG_PASSWORD": "not-a-real-password",
}


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[pytest.MonkeyPatch]:
    for k in (*_ENV, "FEEDBACK_PG_PORT", "FEEDBACK_PG_DATABASE"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(store, "_pool", None)
    yield monkeypatch


# ── ownership: one module, one role ──


def test_analytics_reader_holds_no_connection() -> None:
    for name in ("get_conn", "_get_pool", "_pool", "ThreadedConnectionPool"):
        assert not hasattr(analytics_reader, name), name
    src = Path(analytics_reader.__file__).read_text()
    assert "ANALYTICS_PG_" not in src
    assert "psycopg2" not in src


def _imports(tree: ast.AST) -> Iterator[str]:
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            for alias in node.names:
                yield f"{node.module}.{alias.name}"
        elif isinstance(node, ast.Import):
            for alias in node.names:
                yield alias.name


def test_only_the_feedback_store_builds_a_pool_or_reads_pg_env() -> None:
    """A second Golden Source pool, or a module reading ANALYTICS_PG_* / FEEDBACK_PG_*, fails here."""
    offenders = []
    for path in sorted(_RESEARCH.rglob("*.py")):
        rel = path.relative_to(_RESEARCH).as_posix()
        src = path.read_text()
        if rel == "feedback_store.py":
            continue
        names = set(_imports(ast.parse(src)))
        if any(n.endswith("ThreadedConnectionPool") or n.startswith("psycopg2.pool") for n in names):
            offenders.append(f"{rel}: builds a pool")
        if "ANALYTICS_PG_" in src or "FEEDBACK_PG_" in src:
            offenders.append(f"{rel}: reads a Golden Source connection env")
        if "bifrost_api.research.analytics_reader.get_conn" in names:
            offenders.append(f"{rel}: imports analytics_reader.get_conn")
    assert offenders == []


def test_the_router_uses_the_stores_connection() -> None:
    assert feedback_router.get_conn is store.get_conn


# ── FEEDBACK_PG_* ──


def test_conn_params_read_feedback_pg_env_with_defaults(clean_env: pytest.MonkeyPatch) -> None:
    for k, v in _ENV.items():
        clean_env.setenv(k, v)
    assert store.conn_params() == {
        "host": "pg.feedback.test",
        "port": 5432,
        "dbname": "bifrost_golden_source",
        "user": "feedback_writer",
        "password": "not-a-real-password",
    }
    clean_env.setenv("FEEDBACK_PG_PORT", "6543")
    clean_env.setenv("FEEDBACK_PG_DATABASE", "gs_test")
    p = store.conn_params()
    assert (p["port"], p["dbname"]) == (6543, "gs_test")


@pytest.mark.parametrize("missing", sorted(_ENV))
def test_a_missing_variable_is_named_not_defaulted(clean_env: pytest.MonkeyPatch, missing: str) -> None:
    for k, v in _ENV.items():
        if k != missing:
            clean_env.setenv(k, v)
    clean_env.setenv(missing, "   ")
    with pytest.raises(RuntimeError, match=missing):
        store.conn_params()


def test_analytics_env_alone_does_not_connect(clean_env: pytest.MonkeyPatch) -> None:
    """The 0.7.3 env (ANALYTICS_PG_*) is not a fallback: the store refuses."""
    clean_env.setenv("ANALYTICS_PG_HOST", "pg.analytics.test")
    clean_env.setenv("ANALYTICS_PG_USER", "analytics_writer")
    clean_env.setenv("ANALYTICS_PG_PASSWORD", "not-a-real-password")
    with pytest.raises(RuntimeError, match="FEEDBACK_PG_HOST, FEEDBACK_PG_USER, FEEDBACK_PG_PASSWORD"):
        store.conn_params()


def test_the_pool_is_built_once_with_the_feedback_params(clean_env: pytest.MonkeyPatch) -> None:
    for k, v in _ENV.items():
        clean_env.setenv(k, v)
    built: list[Dict[str, Any]] = []

    class _Pool:
        closed = False

        def __init__(self, **kw: Any) -> None:
            built.append(kw)

        def getconn(self) -> str:
            return "conn"

        def putconn(self, conn: Any) -> None:
            assert conn == "conn"

    clean_env.setattr(store, "ThreadedConnectionPool", _Pool)
    with store.get_conn() as c1:
        assert c1 == "conn"
    with store.get_conn():
        pass
    assert len(built) == 1
    kw = built[0]
    assert kw["user"] == "feedback_writer" and kw["host"] == "pg.feedback.test"
    assert kw["dbname"] == "bifrost_golden_source" and kw["application_name"] == "trade-api-feedback"


def test_an_unconfigured_store_is_503_naming_the_variables(clean_env: pytest.MonkeyPatch) -> None:
    app = FastAPI()
    app.include_router(feedback_router.router)
    resp = TestClient(app).get("/research/feedback/summary")
    assert resp.status_code == 503
    assert "FEEDBACK_PG_HOST" in resp.json()["detail"]
