"""Ops stop health clear publishes IB disconnect messages."""

from __future__ import annotations

import time

from bifrost_api.ops.market_ingest_health_clear import clear_ingest_health_after_stop


class FakeRedis:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = {}
        self.stream: list[tuple[str, dict[str, str]]] = []

    def hgetall(self, key: str) -> dict[str, str]:
        return dict(self.hashes.get(key.strip(), {}))

    def hset(self, key: str, mapping: dict[str, str]) -> None:
        k = key.strip()
        self.hashes.setdefault(k, {}).update({str(a): str(b) for a, b in mapping.items()})

    def close(self) -> None:
        return None

    def xadd(self, key: str, fields: dict[str, str], maxlen=None, approximate=None) -> str:
        entry_id = f"{len(self.stream) + 1}-0"
        self.stream.append((entry_id, {"_stream_key": key, **fields}))
        return entry_id


def test_clear_stop_ib_ingestor_publishes_disconnect_message(monkeypatch) -> None:
    store = FakeRedis()
    key = "bifrost:health:ws_ib_ingestor"
    store.hashes[key] = {
        "connected": "1",
        "client_id": "58",
        "updated_at": str(time.time()),
    }

    def _conn(_url: str):
        return store

    monkeypatch.setattr(
        "bifrost_api.ops.market_ingest_health_clear._conn",
        _conn,
    )
    clear_ingest_health_after_stop("redis://unused/0", key, "ib_ingestor")
    assert store.hashes[key].get("connected") == "0"
    assert len(store.stream) == 1
    _eid, payload = store.stream[0]
    assert payload["service"] == "ib_ingestor"
    assert payload["status_to"] == "disconnected"
    assert payload["reason"] == "Service stopped"


def test_clear_stop_ib_account_agent_publishes_host_and_secondary(monkeypatch) -> None:
    store = FakeRedis()
    key = "bifrost:health:ws_ib_account_agent"
    store.hashes[key] = {
        "host_connected": "1",
        "host_alive": "1",
        "host_client_id": "60",
        "secondary_connected": "1",
        "secondary_client_id": "61",
        "updated_at": str(time.time()),
    }

    monkeypatch.setattr(
        "bifrost_api.ops.market_ingest_health_clear._conn",
        lambda _url: store,
    )
    clear_ingest_health_after_stop("redis://unused/0", key, "ib_account_agent")
    assert len(store.stream) == 2
    slots = {p["slot"] for _, p in store.stream}
    assert slots == {"host", "secondary"}


def test_clear_stop_ib_operator_publishes_when_host_alive(monkeypatch) -> None:
    store = FakeRedis()
    key = "bifrost:health:ws_ib_operator"
    store.hashes[key] = {
        "host_connected": "0",
        "host_alive": "1",
        "host_client_id": "20",
        "secondary_present": "1",
        "secondary_connected": "1",
        "secondary_client_id": "21",
        "updated_at": str(time.time()),
    }

    monkeypatch.setattr(
        "bifrost_api.ops.market_ingest_health_clear._conn",
        lambda _url: store,
    )
    clear_ingest_health_after_stop("redis://unused/0", key, "ib_operator")
    assert len(store.stream) == 2
    for _eid, payload in store.stream:
        assert payload["status_to"] == "disconnected"
        assert "HOST" in payload["title"] or payload["slot"] in ("host", "secondary")
