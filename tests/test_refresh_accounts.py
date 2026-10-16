"""POST /control/refresh_accounts says why it could not refresh (envelope TD-16/17).

It answered every gateway refusal with 500 ``{"ok": false, "error": "slot_not_connected"}``
and logged nothing, so the desk showed "Refresh accounts: 500" while TWS was simply logged
out (PROD, 2026-10-15). A gateway that cannot serve the primary slot is now 503 with a
``detail`` the desk can show; only a failed write or an unexpected error is 500.

The gateway here is a script: nothing in this file reaches Redis, IB or a DB. Accounts
and addresses are invented.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock

import pytest
from starlette.testclient import TestClient

import bifrost_api.monitor.routers.daemon as daemon
from bifrost_api.monitor.app import create_app as create_monitor_app
from tests.contract.helpers import operator_server_config
from tests.envelope_asserts import assert_error
from tests.reader_mock import reader_mock

PATH = "/control/refresh_accounts"
PG = {"sink": "postgres"}
PRIMARY_ROW = {"account_id": "U0000001", "positions": []}
SECONDARY_ROW = {"account_id": "U0000002", "positions": []}


class _Gateway:
    """Answers each ``account_slot`` from a script and records which slots were asked."""

    def __init__(self, answers: Dict[str, Any]) -> None:
        self.answers = answers
        self.slots: List[str] = []

    async def request_async(
        self,
        op: str,
        payload: Optional[Dict[str, Any]] = None,
        *,
        timeout_sec: Optional[float] = None,
        caller: str = "fastapi",
    ) -> Dict[str, Any]:
        assert op == "fetch_accounts_snapshot"
        slot = (payload or {})["account_slot"]
        self.slots.append(slot)
        answer = self.answers[slot]
        if isinstance(answer, Exception):
            raise answer
        return answer


def _ok(*rows: Dict[str, Any]) -> Dict[str, Any]:
    return {"ok": True, "data": {"accounts": list(rows)}}


def _client(gateway: Any, control_via_db: Any = PG, secondary: bool = False) -> TestClient:
    cfg: Dict[str, Any] = {**operator_server_config(), "redis": {"enabled": False}}
    if secondary:
        cfg["ib"] = {"secondary": {"ip": "192.0.2.32"}}
    reader = reader_mock()
    reader.config = cfg
    reader._config = cfg
    app = create_monitor_app(
        reader=reader, control_via_db=control_via_db, data_lag_threshold_ms=5000, merged_config=cfg
    )
    app.state.ib_operator_client = gateway
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture
def written(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    sink = MagicMock(return_value=True)
    monkeypatch.setattr(daemon, "sync_accounts_snapshot_to_db", sink)
    return sink


# --- the gateway could not serve the primary slot: 503 and the reason ----------------------


@pytest.mark.parametrize("code", ["slot_not_connected", "host_not_connected"])
def test_no_tws_session_is_503_and_names_the_host_slot(code: str, written: MagicMock) -> None:
    body = assert_error(
        _client(_Gateway({"primary": {"ok": False, "error": code}})).post(PATH),
        503,
        "IB is not connected on the host slot",
        legacy={"ok": None, "error": None},
    )
    assert body["detail"].endswith(f"({code})")
    written.assert_not_called()


@pytest.mark.parametrize(
    "code, says",
    [
        ("timeout_waiting_for_operator", "did not answer for the host slot in time"),
        ("redis_xadd:Connection refused", "request queue is not reachable"),
        ("redis_get:Connection reset", "request queue is not reachable"),
        ("unknown_op", "could not fetch accounts for the host slot"),
    ],
)
def test_other_gateway_refusals_are_503_with_their_code(code: str, says: str, written: MagicMock) -> None:
    body = assert_error(_client(_Gateway({"primary": {"ok": False, "error": code}})).post(PATH), 503, says)
    assert body["detail"].endswith(f"({code})")
    written.assert_not_called()


def test_a_refusal_without_a_code_still_says_what_failed(written: MagicMock) -> None:
    assert_error(_client(_Gateway({"primary": {"ok": False}})).post(PATH), 503, "fetch_accounts_snapshot failed")


def test_the_refusal_is_logged(written: MagicMock, caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level("WARNING", logger=daemon.__name__):
        _client(_Gateway({"primary": {"ok": False, "error": "slot_not_connected"}})).post(PATH)
    assert "refresh_accounts primary: slot_not_connected" in caplog.text


# --- not configured: 503 -------------------------------------------------------------------


def test_no_gateway_client_is_503(written: MagicMock) -> None:
    assert_error(_client(None).post(PATH), 503, "IB Gateway client is not configured")


def test_no_control_is_503(written: MagicMock) -> None:
    gateway = _Gateway({"primary": _ok(PRIMARY_ROW)})
    assert_error(_client(gateway, control_via_db=None).post(PATH), 503, "Control is not available")
    assert gateway.slots == []


# --- our own failures stay 500 -------------------------------------------------------------


def test_a_failed_write_is_500(written: MagicMock) -> None:
    written.return_value = False
    assert_error(_client(_Gateway({"primary": _ok(PRIMARY_ROW)})).post(PATH), 500, "Failed to write account data")


def test_an_unexpected_error_is_500_with_its_text(written: MagicMock) -> None:
    gateway = _Gateway({"primary": RuntimeError("boom")})
    assert_error(_client(gateway).post(PATH), 500, "Refresh accounts failed: boom")


# --- success is unchanged ------------------------------------------------------------------


def test_the_primary_accounts_are_written(written: MagicMock) -> None:
    resp = _client(_Gateway({"primary": _ok(PRIMARY_ROW)})).post(PATH)
    assert resp.status_code == 200, resp.text
    assert resp.json()["ok"] is True
    written.assert_called_once_with(PG, [PRIMARY_ROW])


def test_a_down_secondary_is_left_out_and_the_primary_is_written(written: MagicMock) -> None:
    gateway = _Gateway({"primary": _ok(PRIMARY_ROW), "secondary": {"ok": False, "error": "slot_not_connected"}})
    resp = _client(gateway, secondary=True).post(PATH)
    assert resp.status_code == 200, resp.text
    assert gateway.slots == ["primary", "secondary"]
    written.assert_called_once_with(PG, [PRIMARY_ROW])


def test_both_slots_are_written_when_both_answer(written: MagicMock) -> None:
    gateway = _Gateway({"primary": _ok(PRIMARY_ROW), "secondary": _ok(SECONDARY_ROW)})
    assert _client(gateway, secondary=True).post(PATH).status_code == 200
    written.assert_called_once_with(PG, [PRIMARY_ROW, SECONDARY_ROW])


def test_no_accounts_from_ib_is_still_200_and_writes_nothing(written: MagicMock) -> None:
    resp = _client(_Gateway({"primary": _ok()})).post(PATH)
    assert resp.status_code == 200, resp.text
    assert "No account data received" in resp.json()["message"]
    written.assert_not_called()
