"""Tests for market ingest display semantics and redis-ib platform gateway detection."""

from bifrost_api.ops.market_ingest_config import DEFAULT_MARKET_INGEST_SERVICES
from bifrost_api.ops.market_ingest_display import (
    derive_ingest_display_state,
    platform_gateway_managed_for_service,
)
from bifrost_api.ops.market_ingest_health_clear import ingest_health_is_platform_gateway


def test_default_ib_rows_use_platform_gateway_labels() -> None:
    by_id = {r["id"]: r["label"] for r in DEFAULT_MARKET_INGEST_SERVICES}
    assert "Platform IB Gateway" in by_id["ib_ingestor"]
    assert "Platform IB Gateway" in by_id["ib_account_agent"]
    assert "Platform IB Gateway" in by_id["ib_operator"]


def test_ib_rows_point_at_data_ib_gateway_not_retired_units() -> None:
    """TD-104: the three health rows are Deployment data/ib-gateway, with no systemd unit."""
    from bifrost_api.ops.market_ingest_config import market_ingest_services_from_config
    from bifrost_api.ops.workload_map import deployment_for_unit

    for row in DEFAULT_MARKET_INGEST_SERVICES:
        if row["id"] == "trading_engine":
            continue
        assert row["k8s_namespace"] == "data"
        assert row["k8s_deployment"] == "ib-gateway"
        assert "systemd_unit" not in row
        assert "ib-operator" not in str(row)
        assert "ib-market-gateway" not in str(row)
        assert "ib-account-agent" not in str(row)
    # A YAML row that still names the retired unit is rewritten.
    rewritten = market_ingest_services_from_config(
        {
            "ops": {
                "market_ingest_services": [
                    {
                        "id": "ib_ingestor",
                        "label": "old",
                        "systemd_unit": "bifrost-ib-market-gateway.service",
                        "redis_meta_key": "bifrost:health:ws_ib_ingestor",
                    }
                ]
            }
        }
    )
    assert rewritten[0]["k8s_deployment"] == "ib-gateway"
    assert rewritten[0]["k8s_namespace"] == "data"
    assert "systemd_unit" not in rewritten[0]
    assert deployment_for_unit("bifrost-ib-market-gateway.service") is None
    assert deployment_for_unit("bifrost-engine") == "daemon"


def test_health_hash_without_updated_at_is_not_live() -> None:
    """TD-104: a frozen hash (no timestamp) is not a live gateway."""
    from bifrost_api.ops.market_ingest_health_clear import ingest_redis_health_looks_live
    import bifrost_api.ops.market_ingest_health_clear as mod
    import time

    class _FakeRedis:
        def __init__(self, fields):
            self._fields = fields

        def hgetall(self, key: str):  # noqa: ARG002
            return self._fields

        def close(self) -> None:
            pass

    orig = mod._conn
    try:
        mod._conn = lambda _url: _FakeRedis({"plugin": "ib-gateway", "connected": "1"})  # type: ignore[assignment]
        assert ingest_redis_health_looks_live("redis://x", "bifrost:health:ws_ib_ingestor", "ib_ingestor") is False
        mod._conn = lambda _url: _FakeRedis(  # type: ignore[assignment]
            {"plugin": "ib-gateway", "connected": "1", "updated_at": str(time.time())}
        )
        assert ingest_redis_health_looks_live("redis://x", "bifrost:health:ws_ib_ingestor", "ib_ingestor") is True
    finally:
        mod._conn = orig


def test_ingest_health_is_platform_gateway_by_plugin() -> None:
    class _FakeRedis:
        def hgetall(self, key: str):  # noqa: ARG002
            return {"plugin": "ib-gateway", "mode": "mock", "connected": "1"}

        def close(self) -> None:
            pass

    import bifrost_api.ops.market_ingest_health_clear as mod

    orig = mod._conn
    mod._conn = lambda _url: _FakeRedis()  # type: ignore[assignment]
    try:
        assert ingest_health_is_platform_gateway("redis://x", "bifrost:health:ws_ib_ingestor") is True
    finally:
        mod._conn = orig


def test_platform_gateway_uses_ib_redis_not_live_redis() -> None:
    class _LiveRedis:
        def hgetall(self, key: str):  # noqa: ARG002
            return {"connected": "1"}

        def close(self) -> None:
            pass

    class _IbRedis:
        def hgetall(self, key: str):  # noqa: ARG002
            return {"plugin": "ib-gateway", "connected": "1"}

        def close(self) -> None:
            pass

    import bifrost_api.ops.market_ingest_health_clear as mod

    orig = mod._conn
    calls: list[str] = []

    def _conn(url: str):
        calls.append(url)
        if url == "redis://ib/0":
            return _IbRedis()
        return _LiveRedis()

    mod._conn = _conn  # type: ignore[assignment]
    try:
        assert (
            platform_gateway_managed_for_service(
                "redis://ib/0",
                "redis://live/0",
                "bifrost:health:ws_ib_ingestor",
                "ib_ingestor",
            )
            is True
        )
        assert calls == ["redis://ib/0"]
    finally:
        mod._conn = orig


def test_trading_engine_stg_policy_off() -> None:
    out = derive_ingest_display_state(
        service_id="trading_engine",
        process_active="inactive",
        redis_url=None,
        ib_redis_url=None,
        meta_key="bifrost:health:daemon_strategy_trading",
        runtime_externally_managed=False,
        platform_gateway_managed=False,
        ops_control_profile="stg",
        runtime_kind="kubernetes",
    )
    assert out["runtime_status"] == "policy-off"
    assert "daemon scale 0" in out["display_active"]
