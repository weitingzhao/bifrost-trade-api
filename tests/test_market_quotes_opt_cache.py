"""Tests for GET /quotes OPT Redis cache + POST /quotes/refresh-options."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from bifrost_api.market.routers.quotes import router

CK = "GOOG|OPT|20260717|300.0|C"


def _app(rq: MagicMock | None, reader: MagicMock | None = None) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    app.state.reader = reader or MagicMock()
    app.state.redis_quotes = rq
    return TestClient(app)


def test_get_quotes_opt_from_redis_cache() -> None:
    rq = MagicMock()
    rq.available = True
    rq.ib_redis_client = MagicMock()
    rq.get_option_cache.return_value = {
        "symbol": "GOOG",
        "contract_key": CK,
        "sec_type": "OPT",
        "bid": 1.0,
        "ask": 1.2,
        "last": 1.1,
        "mid": 1.1,
        "updated_ts": 1000.0,
        "ts": 1000.0,
    }
    reader = MagicMock()
    reader.get_contract_quotes.return_value = []

    with patch("bifrost_core.core.realtime.on_demand_opt.ensure_on_demand_opt") as ensure:
        ensure.return_value = [CK]
        client = _app(rq, reader)
        resp = client.get(f"/quotes?contract_keys={CK}")
        assert resp.status_code == 200
        ensure.assert_called_once()
        body = resp.json()
        assert len(body["quotes"]) == 1
        assert body["quotes"][0]["contract_key"] == CK
        reader.get_contract_quotes.assert_not_called()


def test_get_quotes_opt_fallback_on_cache_miss() -> None:
    rq = MagicMock()
    rq.available = True
    rq.ib_redis_client = MagicMock()
    rq.get_option_cache.return_value = None
    reader = MagicMock()
    reader.get_contract_quotes.return_value = [
        {"symbol": "GOOG", "contract_key": CK, "last": 0.5, "bid": None, "ask": None}
    ]

    with patch("bifrost_core.core.realtime.on_demand_opt.ensure_on_demand_opt") as ensure:
        ensure.return_value = [CK]
        client = _app(rq, reader)
        resp = client.get(f"/quotes?contract_keys={CK}")
        assert resp.status_code == 200
        reader.get_contract_quotes.assert_called_once_with([CK])
        assert len(resp.json()["quotes"]) == 1


def test_post_quotes_refresh_options() -> None:
    rq = MagicMock()
    rq.available = True
    rq.ib_redis_client = MagicMock()

    with patch("bifrost_core.core.realtime.on_demand_opt.ensure_on_demand_opt") as ensure:
        ensure.return_value = [CK]
        client = _app(rq)
        resp = client.post("/quotes/refresh-options", json={"contract_keys": [CK]})
        assert resp.status_code == 200
        body = resp.json()
        assert body["registered"] == 1
        assert body["contract_keys"] == [CK]


def test_post_quotes_refresh_options_unavailable() -> None:
    rq = MagicMock()
    rq.available = False
    client = _app(rq)
    resp = client.post("/quotes/refresh-options", json={"contract_keys": [CK]})
    assert resp.status_code == 503
