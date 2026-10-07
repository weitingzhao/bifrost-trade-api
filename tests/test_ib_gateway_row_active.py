"""TD-104: the three IB rows are active when the redis-ib hash is fresh and data/ib-gateway is ready."""

from __future__ import annotations

import time
from typing import Any

from fastapi import FastAPI
from starlette.testclient import TestClient

from bifrost_api.ops.market_ingest_display import process_active_from_replica_counts
from bifrost_api.ops.market_ingest_health_clear import read_health_updated_at
from bifrost_api.ops.routers.market_ingest import router
from bifrost_api.ops.services.executor_kubernetes import KubernetesExecutor

_IB_IDS = ("ib_operator", "ib_ingestor", "ib_account_agent")


def test_replica_counts_map_to_process_active() -> None:
    assert process_active_from_replica_counts(1, 1) == "active"
    assert process_active_from_replica_counts(2, 1) == "activating"
    assert process_active_from_replica_counts(0, 0) == "inactive"
    assert process_active_from_replica_counts(None, None) == "unknown"


def test_read_health_updated_at_uses_heartbeat_field(monkeypatch) -> None:
    import bifrost_api.ops.market_ingest_health_clear as mod

    stamp = time.time()

    class _Redis:
        def hgetall(self, key: str):  # noqa: ARG002
            return {"updated_at": str(stamp), "plugin": "ib-gateway"}

        def close(self) -> None:
            return None

    monkeypatch.setattr(mod, "_conn", lambda _url: _Redis())
    got = read_health_updated_at("redis://redis-ib:6379/0", "bifrost:health:ws_ib_ingestor")
    assert got is not None
    assert abs(got - stamp) < 0.001


def _config() -> dict:
    return {
        "redis": {"enabled": True, "host": "redis-live", "port": 6379, "db": 0},
        "redis_ib": {
            "enabled": True,
            "host": "redis-ib",
            "port": 6379,
            "db": 0,
            "username": "trade-prod",
            "password": "fixture",
        },
        "ops": {
            "control_profile": "prod",
            "executor_mode": "kubernetes",
            "kubernetes": {"namespace": "bifrost-prod"},
        },
    }


class _HashRedis:
    def __init__(self, url: str, stamp: float) -> None:
        self._url = url
        self._stamp = stamp

    def hgetall(self, key: str) -> dict[str, str]:
        if "redis-ib" in self._url and key.startswith("bifrost:health:ws_ib_"):
            return {
                "plugin": "ib-gateway",
                "connected": "1",
                "host_connected": "1",
                "operator_connected": "1",
                "updated_at": str(self._stamp),
            }
        return {}

    def hget(self, key: str, field: str) -> None:  # noqa: ARG002
        return None

    def hdel(self, *args: Any) -> None:
        raise AssertionError("services table must not write the health hash")

    def close(self) -> None:
        return None


def _client(monkeypatch, replicas: int, ready: int) -> TestClient:
    stamp = time.time()
    urls: list[str] = []

    def _connect(url: str) -> _HashRedis:
        urls.append(url)
        return _HashRedis(url, stamp)

    import bifrost_api.ops.market_ingest_control_env as control_env
    import bifrost_api.ops.market_ingest_health_clear as health

    monkeypatch.setattr(health, "_conn", _connect)
    monkeypatch.setattr(control_env, "_redis_conn", _connect)
    monkeypatch.setattr(KubernetesExecutor, "_init_clients", lambda self: True)
    exc = KubernetesExecutor(
        namespace="bifrost-prod",
        allowed_units=["bifrost-engine"],
        daemon_scale_guard="freeze",
    )

    async def _counts(deployment: str, namespace: str | None = None):
        if deployment == "ib-gateway":
            assert namespace == "data"
            return replicas, ready
        return 0, 0

    async def _systemctl(unit: str) -> str:  # noqa: ARG001
        return "inactive"

    exc.deployment_replica_counts = _counts  # type: ignore[method-assign]
    exc.systemctl_is_active = _systemctl  # type: ignore[method-assign]

    app = FastAPI()
    app.include_router(router)
    app.state.bifrost_config = _config()
    app.state.bifrost_config_profile = "prod"
    app.state.executor = exc
    app.state._probe_urls = urls
    app.state._probe_stamp = stamp
    return TestClient(app)


def _ib_rows(body: dict) -> list[dict]:
    rows = [row for row in body["services"] if row["id"] in _IB_IDS]
    assert len(rows) == 3
    return rows


def test_ib_rows_active_when_hash_fresh_and_deployment_ready(monkeypatch) -> None:
    client = _client(monkeypatch, replicas=1, ready=1)
    body = client.get("/ops/market-ingest/services").json()
    assert body["ok"] is True
    urls = client.app.state._probe_urls
    assert any("redis-ib" in url for url in urls)
    stamp = client.app.state._probe_stamp
    for row in _ib_rows(body):
        assert row["k8s_namespace"] == "data"
        assert row["k8s_deployment"] == "ib-gateway"
        assert row["k8s_replicas"] == 1
        assert row["k8s_ready"] == 1
        assert row["process_active"] == "active"
        assert row["runtime_status"] == "active"
        assert row["redis_control_updated_at"] is not None
        assert abs(row["redis_control_updated_at"] - stamp) < 2
    live_only = [url for url in urls if "redis-ib" not in url]
    assert live_only, "ops-control fields are still read from redis-live"
    assert all("redis-live" in url for url in live_only)


def test_ib_rows_stay_inactive_when_deployment_is_not_ready(monkeypatch) -> None:
    client = _client(monkeypatch, replicas=0, ready=0)
    body = client.get("/ops/market-ingest/services").json()
    for row in _ib_rows(body):
        assert row["process_active"] == "inactive"
        assert row["k8s_replicas"] == 0
        assert row["k8s_ready"] == 0
        assert row["redis_control_updated_at"] is not None
