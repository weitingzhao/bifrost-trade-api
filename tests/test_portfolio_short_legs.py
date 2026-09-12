"""Tests for GET /portfolio/short-legs."""

from __future__ import annotations

from unittest.mock import MagicMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from bifrost_api.portfolio.routers.short_legs import router

LEG = {
    "account_id": "U1",
    "symbol": "NVDA",
    "expiry": "20261120",
    "strike": 180.0,
    "right": "C",
    "qty": -2,
    "contract_key": "NVDA_20261120_180_C",
    "spot": 172.15,
}


def _client(reader: MagicMock) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    app.state.reader = reader
    return TestClient(app)


def test_returns_the_legs_and_their_count() -> None:
    reader = MagicMock()
    reader.get_short_option_legs.return_value = [LEG]
    r = _client(reader).get("/portfolio/short-legs")
    assert r.status_code == 200
    assert r.json() == {"legs": [LEG], "count": 1}


def test_passes_the_account_filter_through_and_defaults_to_every_account() -> None:
    reader = MagicMock()
    reader.get_short_option_legs.return_value = []
    client = _client(reader)
    client.get("/portfolio/short-legs?account_id=U1&account_id=U2")
    assert reader.get_short_option_legs.call_args[0][0] == ["U1", "U2"]
    client.get("/portfolio/short-legs")
    assert reader.get_short_option_legs.call_args[0][0] is None


def test_an_unreachable_database_is_503_not_an_empty_book() -> None:
    """Zero short legs and "we could not look" must never render the same."""
    reader = MagicMock()
    reader.get_short_option_legs.return_value = None
    assert _client(reader).get("/portfolio/short-legs").status_code == 503


def test_carries_no_cushion_and_no_verdict() -> None:
    reader = MagicMock()
    reader.get_short_option_legs.return_value = [LEG]
    leg = _client(reader).get("/portfolio/short-legs").json()["legs"][0]
    assert "cushion" not in leg and "band" not in leg and "tight" not in leg
