"""Tests for Platform IB Gateway detection in Ops market ingest health helpers."""

from bifrost_api.ops.market_ingest_config import DEFAULT_MARKET_INGEST_SERVICES
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
