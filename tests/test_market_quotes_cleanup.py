"""Tests for POST /quotes/cleanup on-demand STK unsubscribe."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from bifrost_api.market.routers.quotes import router


def _app_with_rq(rq: MagicMock | None) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    app.state.reader = MagicMock()
    app.state.redis_quotes = rq
    return TestClient(app)


def test_post_quotes_cleanup_removes_stale() -> None:
    ib = MagicMock()
    ib.smembers.return_value = {"NVDA", "AAPL", "SPY", "GHOST"}
    rq = MagicMock()
    rq.available = True
    rq.ib_redis_client = ib

    with patch(
        "bifrost_core.core.realtime.on_demand_stk.remove_on_demand_stk"
    ) as remove_fn:
        remove_fn.return_value = 1
        client = _app_with_rq(rq)
        resp = client.post(
            "/quotes/cleanup",
            json={"keep_symbols": ["nvda", "AAPL", "SPY"]},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["removed"] == ["GHOST"]
        assert body["kept"] == ["AAPL", "NVDA", "SPY"]
        remove_fn.assert_called_once()
        args, _kwargs = remove_fn.call_args
        assert args[0] is ib
        assert args[1] == ["GHOST"]


def test_post_quotes_cleanup_nothing_to_remove() -> None:
    ib = MagicMock()
    ib.smembers.return_value = {"NVDA"}
    rq = MagicMock()
    rq.available = True
    rq.ib_redis_client = ib

    with patch(
        "bifrost_core.core.realtime.on_demand_stk.remove_on_demand_stk"
    ) as remove_fn:
        client = _app_with_rq(rq)
        resp = client.post("/quotes/cleanup", json={"keep_symbols": ["NVDA"]})
        assert resp.status_code == 200
        assert resp.json() == {"removed": [], "kept": ["NVDA"]}
        remove_fn.assert_not_called()


def test_post_quotes_cleanup_redis_unavailable() -> None:
    rq = MagicMock()
    rq.available = False
    client = _app_with_rq(rq)
    resp = client.post("/quotes/cleanup", json={"keep_symbols": ["NVDA"]})
    assert resp.status_code == 503
    assert "unavailable" in resp.json()["detail"].lower()


def test_post_quotes_cleanup_no_rq() -> None:
    client = _app_with_rq(None)
    resp = client.post("/quotes/cleanup", json={"keep_symbols": []})
    assert resp.status_code == 503
