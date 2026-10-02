"""ticker-overview reads related tickers from the FDW table the Trade DB has (debt TD-03)."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict, List

import psycopg2
import pytest

import bifrost_api.research.market_data_client as market_data_client
from bifrost_api.research.routers import data_readiness


class _Cur:
    def __init__(self, conn: "_Conn") -> None:
        self.conn = conn

    def execute(self, sql: str, params: Any = None) -> None:
        self.conn.sql.append(sql)
        if self.conn.fail and "ticker_related" in sql:
            raise RuntimeError("boom")

    def fetchall(self) -> List[Dict[str, Any]]:
        return [{"to_symbol": "ZZB"}, {"to_symbol": "ZZC"}]

    def __enter__(self) -> "_Cur":
        return self

    def __exit__(self, *exc: Any) -> None:
        return None


class _Conn:
    def __init__(self, fail: bool = False) -> None:
        self.sql: List[str] = []
        self.fail = fail
        self.closed = False

    def cursor(self, **_: Any) -> _Cur:
        return _Cur(self)

    def close(self) -> None:
        self.closed = True


def _request() -> Any:
    state = SimpleNamespace(control_via_db={"sink": "postgres"}, status_cfg_for_read=None)
    return SimpleNamespace(app=SimpleNamespace(state=state))


@pytest.fixture
def wired(monkeypatch: pytest.MonkeyPatch) -> Dict[str, Any]:
    box: Dict[str, Any] = {}
    # Invented symbol (fixtures are never copied from DEV).
    monkeypatch.setattr(market_data_client, "fetch_ticker_detail", lambda s: {"symbol": s, "name": "Zz Corp"})
    monkeypatch.setattr(
        "bifrost_core.persistence.postgres.connection._get_conn_params", lambda db: {"host": "x"}
    )

    def connect(**_: Any) -> _Conn:
        box["conn"] = _Conn(fail=box.get("fail", False))
        return box["conn"]

    monkeypatch.setattr(psycopg2, "connect", connect)
    return box


def test_reads_market_ticker_related_and_closes(wired: Dict[str, Any]) -> None:
    out = data_readiness.get_ticker_overview("zza", _request())
    sql = " ".join(wired["conn"].sql[-1].split())
    assert "FROM market.ticker_related rt" in sql
    assert "raw_market" not in sql
    assert wired["conn"].closed
    assert out["related_tickers"] == ["ZZB", "ZZC"]


def test_a_failed_read_still_closes_the_connection(wired: Dict[str, Any]) -> None:
    wired["fail"] = True
    out = data_readiness.get_ticker_overview("zza", _request())
    assert wired["conn"].closed
    assert out["related_tickers"] == []
