"""Tests for market ingest display semantics and redis-ib platform gateway detection."""

from bifrost_api.ops.market_ingest_config import DEFAULT_MARKET_INGEST_SERVICES
from bifrost_api.ops.market_ingest_display import (
    derive_ingest_display_state,
    massive_ws_policy_disabled,
    platform_gateway_managed_for_service,
)
from bifrost_api.ops.market_ingest_health_clear import ingest_health_is_platform_gateway


def test_default_ib_rows_use_platform_gateway_labels() -> None:
    by_id = {r["id"]: r["label"] for r in DEFAULT_MARKET_INGEST_SERVICES}
    assert "Platform IB Gateway" in by_id["ib_ingestor"]
    assert "Platform IB Gateway" in by_id["ib_account_agent"]
    assert "Platform IB Gateway" in by_id["ib_operator"]


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


def test_polygon_ws_policy_off_when_ws_disabled() -> None:
    cfg = {"massive": {"tier": "developer", "features": {"ws_enabled": False}}}
    assert massive_ws_policy_disabled(cfg) is True
    for sid in ("polygon_ws", "massive_ws"):
        out = derive_ingest_display_state(
            service_id=sid,
            process_active="inactive",
            config=cfg,
            redis_url="redis://live/0",
            ib_redis_url=None,
            meta_key="bifrost:health:ws_massive_option",
            runtime_externally_managed=False,
            platform_gateway_managed=False,
            ops_control_profile="stg",
            runtime_kind="kubernetes",
        )
        assert out["runtime_status"] == "policy-off"
        assert "REST-only" in out["display_active"]


def test_default_services_use_official_polygon_ws_id() -> None:
    by_id = {r["id"]: r for r in DEFAULT_MARKET_INGEST_SERVICES}
    assert "polygon_ws" in by_id
    assert "massive_ws" not in by_id
    assert by_id["polygon_ws"]["redis_meta_key"] == "bifrost:health:ws_massive_option"


def test_trading_engine_stg_policy_off() -> None:
    out = derive_ingest_display_state(
        service_id="trading_engine",
        process_active="inactive",
        config={},
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
