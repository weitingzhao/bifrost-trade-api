"""TD-75: the api uses the trading daemon's new core names, so core can drop the old ones.

core 0.39.0 renamed ``BIFROST_HEALTH_DAEMON_TRADING_ENGINE`` to
``BIFROST_HEALTH_DAEMON_STRATEGY_TRADING`` and ``PostgreSQLSink`` to ``TradingDaemonSink``
and kept the old names as aliases for one version. The Redis key itself never changed.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from bifrost_api.ops.market_ingest_config import market_ingest_services_from_config

_SRC = Path(__file__).resolve().parents[1] / "src"
_DEPRECATED_CORE_ALIASES = re.compile(r"(?<!LEGACY_)\b(BIFROST_HEALTH_DAEMON_TRADING_ENGINE|PostgreSQLSink)\b")
_DAEMON_KEY = "bifrost:health:daemon_strategy_trading"


def test_src_uses_no_deprecated_daemon_alias() -> None:
    hits = [
        f"{p.relative_to(_SRC)}:{n}"
        for p in sorted(_SRC.rglob("*.py"))
        for n, line in enumerate(p.read_text(encoding="utf-8").splitlines(), 1)
        if _DEPRECATED_CORE_ALIASES.search(line)
    ]
    assert hits == []


@pytest.mark.parametrize(
    "meta",
    ["", "bifrost:ops:trading_engine", "bifrost:health:daemon_trading_engine", _DAEMON_KEY],
)
def test_trading_engine_row_reads_the_daemon_health_key(meta: str) -> None:
    config = {
        "ops": {
            "market_ingest_services": [
                {"id": "trading_engine", "systemd_unit": "bifrost-engine", "redis_meta_key": meta},
            ]
        }
    }
    rows = market_ingest_services_from_config(config)
    row = next(r for r in rows if r["id"] == "trading_engine")
    assert row["redis_meta_key"] == _DAEMON_KEY


def test_default_trading_engine_row_reads_the_daemon_health_key() -> None:
    rows = market_ingest_services_from_config({})
    assert next(r for r in rows if r["id"] == "trading_engine")["redis_meta_key"] == _DAEMON_KEY
