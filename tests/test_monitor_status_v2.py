"""Shape tests for GET /status JSON (schema v9)."""

from __future__ import annotations

import time

from bifrost_core.monitor.integrations.ib_socket_status import build_ib_socket_status

from bifrost_api.monitor.routers.status import (
    STATUS_SCHEMA_VERSION,
    _assemble_status_v3,
    _status_error_payload,
    apply_platform_gateway_ib_heartbeat_overlay,
)

_NOW = 1_700_000_000.0
_IB_CFG = {
    "client_id_ib_ingestor": 150,
    "client_id_account_agent": 151,
    "ib2_client_id_account_agent": 152,
    "client_id_operator": 100,
    "ib2_client_id_operator": 101,
    "ib2_host": "10.0.0.2",
}


def _assert_config_shape(body: dict) -> None:
    assert body["status_schema_version"] == STATUS_SCHEMA_VERSION == 9
    cfg = body["config"]
    assert set(cfg.keys()) >= {"ib_client", "ib_flex", "redis"}
    assert "subscribe_channel" in cfg["redis"]
    assert isinstance(cfg["redis"]["subscribe_channel"], str)
    assert "ib_config" not in cfg
    assert "flex" not in cfg
    assert "flex_config" not in cfg

    ic = cfg["ib_client"]
    assert "client" in ic
    assert "host_ip" in ic["client"]
    assert "port" in ic
    assert "trading" in ic["port"]
    assert "account" in ic
    assert "trading" in ic["account"]
    assert "event_host" in ic["account"]
    assert "event_secondary" in ic["account"]
    assert "timeout_sec" in ic
    assert "client_id" not in ic
    assert "connect_timeout_sec" not in ic
    assert "trading_account_id" not in ic["account"]

    fl = cfg["ib_flex"]
    assert "default_range_days" in fl
    assert "init_range_days" in fl
    assert "host_token" in fl
    assert "rows" in fl

    assert "strategy_active" not in body
    st = body["strategy"]
    assert "active" in st
    act = st["active"]
    for key in ("structure", "gate_safety", "allocation"):
        assert key in act
        assert "id" in act[key] and "name" in act[key]

    assert "feeds" not in body
    assert "ib_status" not in body["monitor"]
    sk = body["socket"]
    assert set(sk.keys()) >= {
        "polygon_ws",
        "massive",
        "ib_ingestor",
        "ib_account_agent",
        "ib_operator",
    }
    assert "ib_status" not in sk


def test_status_error_payload_shape_v8() -> None:
    body = _status_error_payload()
    _assert_config_shape(body)


def test_assemble_status_v8_config_shape() -> None:
    body = _assemble_status_v3(
        health_self_check="ok",
        health_block_reasons=[],
        health_status_lamp="green",
        trading_suspended=False,
        daemon_heartbeat=None,
        daemon_self_check="ok",
        daemon_lamp="green",
        daemon_block_reasons=[],
        auto_status=None,
        subscribed_tickers=[],
        reference_indices=[],
        accounts=[],
        accounts_fetched_at=None,
        ib_config={
            "host": "10.0.0.1",
            "port_type": "tws_paper",
            "port": 7497,
            "client_id_daemon": 1,
            "client_id_listener": 2,
            "client_id_operator": 100,
            "client_id_worker_market": 500,
            "client_id_ib_ingestor": 150,
            "ib_host_account_id": "U1",
            "stream_host_account_id": "U2",
            "stream_secondary_account_id": None,
            "flex_default_range_days": 30,
            "flex_init_range_days": 360,
        },
        flex_config={"host_token": "t", "secondary_token": None, "rows": []},
        redis_subscribe_channel="ib:ingester:channel",
        open_orders=[],
        active_structure_id=None,
        active_structure_name=None,
        active_gate_id=None,
        active_gate_name=None,
        active_alloc_id=None,
        active_alloc_name=None,
        monitor_ib_status={
            "connected": True,
            "host": {"connected": True, "client_id": 100, "last_error": None},
        },
        monitor_enabled=True,
        monitor_health="ok",
        monitor_self_check="ok",
        monitor_lamp="green",
        monitor_block_reasons=[],
        quotes_redis_reader_ok=False,
        celery_broker_connected=False,
        celery_workers=[],
        celery_worker_ib_connected=False,
        celery_worker_ib_client_id=None,
        celery_worker_last_updated_ts=None,
        massive={"configured": False},
        ib_ingestor={"connected": False},
        ib_account_agent={"connected": False},
    )
    _assert_config_shape(body)
    ic = body["config"]["ib_client"]
    assert ic["client"]["host_ip"] == "10.0.0.1"
    assert ic["port"]["trading"] == 1
    assert ic["account"]["trading"] == "U1"
    assert ic["account"]["event_host"] == "U2"
    assert body["config"]["redis"]["subscribe_channel"] == "ib:ingester:channel"
    assert body["config"]["ib_flex"]["default_range_days"] == 30
    assert body["config"]["ib_flex"]["host_token"] == "t"
    assert body["socket"]["ib_operator"]["connected"] is True
    assert body["socket"]["ib_operator"]["host"]["connected"] is True
    assert body["socket"]["ib_operator"]["host"]["client_id"] == 100
    assert body["socket"]["massive"]["configured"] is False
    assert body["socket"]["polygon_ws"]["configured"] is False
    assert body["socket"]["polygon_ws"] is body["socket"]["massive"]


def test_build_ib_socket_status_unified_host_slot_fields() -> None:
    ingestor_hash = {
        "connected": "1",
        "client_id": "150",
        "reconnects": "1",
        "msg_count": "42",
        "last_msg_ts": str(_NOW - 2),
        "ib_probe_at": str(_NOW - 1),
        "ib_probe_ok": "1",
        "ib_probe_interval_sec": "15",
        "host_ib_probe_at": str(_NOW - 1),
        "host_ib_probe_ok": "1",
        "host_ib_probe_interval_sec": "15",
    }
    ingestor = build_ib_socket_status("ib_ingestor", ingestor_hash, _IB_CFG, now=_NOW)
    assert ingestor["host"]["client_id"] == 150
    assert ingestor["host"]["last_ib_probe_at"] == _NOW - 1
    assert ingestor["secondary"] is None
    assert ingestor["last_ib_probe_at"] == _NOW - 1

    aa_hash = {
        "host_connected": "1",
        "host_client_id": "151",
        "host_ib_probe_at": str(_NOW - 1),
        "host_ib_probe_ok": "1",
        "host_ib_probe_interval_sec": "15",
        "secondary_present": "1",
        "secondary_connected": "1",
        "secondary_client_id": "152",
        "secondary_ib_probe_at": str(_NOW - 1),
        "secondary_ib_probe_ok": "1",
        "secondary_ib_probe_interval_sec": "15",
        "last_msg_ts": str(_NOW - 1),
    }
    aa = build_ib_socket_status("ib_account_agent", aa_hash, _IB_CFG, now=_NOW)
    assert aa["host"]["last_ib_probe_at"] == _NOW - 1
    assert aa["secondary"] is not None
    assert aa["secondary"]["last_ib_probe_at"] == _NOW - 1


def test_apply_platform_gateway_ib_heartbeat_overlay_applies_when_gateway() -> None:
    hb = {"ib_connected": False, "ib_client_id": None}
    applied = apply_platform_gateway_ib_heartbeat_overlay(
        hb,
        {"ib_connected": True, "ib_client_id": 70, "ib_transport": "platform_gateway"},
    )
    assert applied is True
    assert hb["ib_connected"] is True
    assert hb["ib_client_id"] == 70
    assert hb["ib_transport"] == "platform_gateway"


def test_apply_platform_gateway_ib_heartbeat_overlay_skips_legacy() -> None:
    hb = {"ib_connected": False, "ib_client_id": None}
    applied = apply_platform_gateway_ib_heartbeat_overlay(
        hb,
        {"ib_connected": True, "ib_client_id": 70, "ib_transport": "legacy_socket"},
    )
    assert applied is False
    assert hb["ib_connected"] is False
    assert "ib_transport" not in hb
