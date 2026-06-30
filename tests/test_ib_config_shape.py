"""IB YAML: required ib.host / ib.secondary shape."""

import pytest

from bifrost_core.config.startup import get_effective_ib_config


def test_host_secondary_blocks():
    cfg = {
        "ib": {
            "connect_timeout": 60.0,
            "host": {
                "ip": "192.168.10.30",
                "port_type": "tws_live",
                "client_id": {
                    "daemon": 110,
                    "listener": 101,
                    "operator": 120,
                    "worker_market": 140,
                    "ingestor": 150,
                },
            },
            "secondary": {
                "ip": "192.168.10.32",
                "port_type": "tws_live",
                "client_id": {
                    "listener": 11,
                    "operator": 120,
                },
            },
        }
    }
    eff = get_effective_ib_config(cfg)
    assert eff["ib_probe_interval_sec"] == 5.0
    assert eff["host"] == "192.168.10.30"
    assert eff["port_type"] == "tws_live"
    assert eff["client_id_daemon"] == 110
    assert eff["client_id_listener"] == 101
    assert eff["client_id_operator"] == 120
    assert eff["client_id_worker_market"] == 140
    assert eff["client_id_ib_ingestor"] == 150
    assert eff["ib_client_id_ib_ingestor"] == 150
    assert eff["port_market_data"] == eff["port"]
    assert eff["ib_port_market_data"] == eff["ib_port"]
    assert eff["ib2_host"] == "192.168.10.32"
    assert eff["ib2_client_id_listener"] == 11
    assert eff["ib2_client_id_operator"] == 120


def test_host_legacy_ib_market_ingest_yaml_key():
    """YAML key `ib_market_ingest` under ib.host.client_id still maps to client_id_ib_ingestor."""
    cfg = {
        "ib": {
            "host": {
                "ip": "10.0.0.1",
                "port_type": "tws_paper",
                "client_id": {
                    "daemon": 1,
                    "listener": 2,
                    "operator": 100,
                    "worker_market": 500,
                    "ib_market_ingest": 155,
                },
            },
        }
    }
    eff = get_effective_ib_config(cfg)
    assert eff["client_id_ib_ingestor"] == 155
    assert eff["ib_client_id_ib_ingestor"] == 155


def test_secondary_legacy_account_yaml_key():
    """YAML key `account` under ib.secondary.client_id still maps to ib2_client_id_operator."""
    cfg = {
        "ib": {
            "host": {
                "ip": "10.0.0.1",
                "port_type": "tws_paper",
                "client_id": {"daemon": 1, "listener": 2, "operator": 100, "worker_market": 500},
            },
            "secondary": {
                "ip": "10.0.0.2",
                "port_type": "tws_paper",
                "client_id": {"listener": 3, "account": 88},
            },
        }
    }
    eff = get_effective_ib_config(cfg)
    assert eff["ib2_client_id_operator"] == 88


def test_omit_secondary():
    cfg = {
        "ib": {
            "host": {
                "ip": "10.0.0.1",
                "port_type": "tws_paper",
                "client_id": {
                    "daemon": 1,
                    "listener": 2,
                    "operator": 100,
                    "worker_market": 500,
                },
            },
        }
    }
    eff = get_effective_ib_config(cfg)
    assert eff["ib2_host"] is None
    assert eff["client_id_ib_ingestor"] == 150


def test_market_gateway_merges_listener_and_worker_slots():
    """W6: explicit market_gateway collapses listener + worker_market + ingestor."""
    cfg = {
        "ib": {
            "host": {
                "ip": "10.0.0.1",
                "port_type": "tws_paper",
                "client_id": {
                    "daemon": 10,
                    "market_gateway": 250,
                    "operator": 220,
                    "account_agent": 260,
                },
            },
        }
    }
    eff = get_effective_ib_config(cfg)
    assert eff["client_id_market_gateway"] == 250
    assert eff["client_id_listener"] == 250
    assert eff["client_id_worker_market"] == 250
    assert eff["client_id_ib_ingestor"] == 250


def test_missing_ib_raises():
    with pytest.raises(ValueError, match="config\\['ib'\\] is required"):
        get_effective_ib_config({})


def test_missing_host_raises():
    with pytest.raises(ValueError, match="config\\['ib'\\]\\['host'\\] is required"):
        get_effective_ib_config({"ib": {"connect_timeout": 30}})
