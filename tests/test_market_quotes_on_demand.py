"""Tests for GET /quotes on-demand STK registration."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from bifrost_api.market.routers.quotes import router


def _app_with_rq(rq: MagicMock) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    app.state.reader = MagicMock()
    app.state.redis_quotes = rq
    return TestClient(app)


def test_get_quotes_registers_on_demand_stk() -> None:
    rq = MagicMock()
    rq.available = True
    rq.ib_redis_client = MagicMock()
    rq.get_ingester_tick.return_value = {
        "symbol": "SGOV",
        "last": 100.5,
        "bid": 100.4,
        "ask": 100.6,
        "contract_key": "SGOV|STK|||",
    }

    with patch("bifrost_core.core.realtime.on_demand_stk.ensure_on_demand_stk") as ensure:
        ensure.return_value = ["SGOV", "GOOG"]
        client = _app_with_rq(rq)
        resp = client.get("/quotes?symbols=SGOV,GOOG")
        assert resp.status_code == 200
        ensure.assert_called_once()
        args, _kwargs = ensure.call_args
        assert args[0] is rq.ib_redis_client
        assert args[1] == ["SGOV", "GOOG"]
        body = resp.json()
        assert len(body["quotes"]) >= 1


def test_get_quotes_on_demand_failure_still_returns() -> None:
    rq = MagicMock()
    rq.available = True
    rq.ib_redis_client = MagicMock()
    rq.get_ingester_tick.return_value = None

    with patch(
        "bifrost_core.core.realtime.on_demand_stk.ensure_on_demand_stk",
        side_effect=RuntimeError("redis down"),
    ):
        client = _app_with_rq(rq)
        resp = client.get("/quotes?symbols=SGOV")
        assert resp.status_code == 200
        assert resp.json()["quotes"] == []
