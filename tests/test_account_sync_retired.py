"""account-sync is gone from every API surface (debt TD-22)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from starlette.testclient import TestClient

from bifrost_api.monitor.app import create_app
from bifrost_api.ops.market_ingest_config import market_ingest_services_from_config
from bifrost_api.ops.services.executor_kubernetes import KubernetesExecutor
from bifrost_api.ops.workload_map import deployment_for_unit
from tests.contract.helpers import full_server_config


def _monitor() -> TestClient:
    reader = MagicMock()
    reader._config = full_server_config()
    app = create_app(reader=reader, control_via_db={"sink": "postgres"}, data_lag_threshold_ms=1000, merged_config=reader._config)
    return TestClient(app, raise_server_exceptions=False)


@pytest.mark.parametrize("cmd", ["suspend", "resume", "stop", "force-sync", "set_heartbeat_interval"])
def test_control_routes_are_gone(cmd: str) -> None:
    paths = {r.path for r in _monitor().app.routes}
    assert f"/account-sync/control/{cmd}" not in paths


def test_no_unit_maps_to_account_sync() -> None:
    assert deployment_for_unit("bifrost-account-sync-daemon") is None
    assert deployment_for_unit("bifrost-account-sync-daemon.service") is None


def test_a_config_row_still_naming_it_is_skipped() -> None:
    cfg = {
        "ops": {
            "market_ingest_services": [
                {"id": "trading_engine", "label": "Engine", "systemd_unit": "bifrost-engine"},
                {"id": "account_sync_daemon", "label": "Account Sync", "systemd_unit": "bifrost-account-sync-daemon"},
            ]
        }
    }
    ids = [r["id"] for r in market_ingest_services_from_config(cfg)]
    assert "trading_engine" in ids and "account_sync_daemon" not in ids
    assert "account_sync_daemon" not in [r["id"] for r in market_ingest_services_from_config({})]


@pytest.mark.asyncio
async def test_daemon_start_touches_only_the_daemon(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(KubernetesExecutor, "_init_clients", lambda self: setattr(self, "_k8s_reachable", True) or True)
    ex = KubernetesExecutor(namespace="bifrost-dev", allowed_units=["bifrost-engine"], daemon_scale_guard="off")
    ex._workload_ready_replicas = AsyncMock(return_value=(1, 1, "deployment"))
    ex._scale_workload = AsyncMock()
    result = await ex._systemctl_workload("start", "daemon", "bifrost-engine.service")
    assert "co_scale_account_sync" not in result
    ex._scale_workload.assert_not_called()
