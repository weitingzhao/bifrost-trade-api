"""Routes retired with no caller and no traffic stay retired; their neighbours stay served.

TD-40: every retired path had 0 hits in all retained access logs (25+ days, every
env) and no caller in any sibling repo. TD-58: the dims writes went with core's
writers. Each list below pairs what went with what had to stay.
"""

from __future__ import annotations

from typing import Any, Dict, Set, Tuple
from unittest.mock import MagicMock

import pytest

from bifrost_api.account.app import create_account_app
from bifrost_api.market.app import create_market_app
from bifrost_api.monitor.app import create_app as create_monitor_app
from bifrost_api.research.app import create_research_app
from tests.contract.helpers import full_server_config

R = "/research/data/readiness"

RETIRED: Dict[str, Set[Tuple[str, str]]] = {
    "research": {
        *(("POST", f"{R}/backfill-{k}") for k in (
            "balance-sheets", "cash-flows", "fundamentals", "grouped-history", "income-statements",
            "price-gaps", "ratios", "short-interest", "short-volume", "technical",
        )),
        *(("POST", f"{R}/{k}") for k in ("gap-ack", "snapshot", "stock-unified-snapshot", "sync-holidays")),
        *(("GET", f"{R}/{k}") for k in (
            "balance-sheets-gaps", "cash-flows-gaps", "income-statements-gaps", "price-gaps", "ratios-gaps",
            "short-interest-gaps", "short-volume-gaps", "data-inventory", "fundamental-condition-catalog",
            "gap-ack", "symbol-technical-tiers",
        )),
    },
    "market": {
        ("DELETE", "/bars/symbol"),
        ("POST", "/bars/backfill"),
        ("POST", "/bars/fetch"),
        ("POST", "/bars/watchlist/eod-refresh"),
        ("POST", "/bars/watchlist/eod-refresh/preview"),
        ("POST", "/indices/refresh"),
        ("POST", "/market/holidays"),
        ("DELETE", "/market/holidays"),
    },
    "account": {
        ("POST", "/strategies/dims/{dim_type}"),
        ("PUT", "/strategies/dims/by-id/{strategy_dim_id}"),
        ("DELETE", "/strategies/dims/by-id/{strategy_dim_id}"),
        ("GET", "/strategies/dims/{dim_type}/items"),
    },
}

KEPT: Dict[str, Set[Tuple[str, str]]] = {
    "research": {
        ("GET", f"{R}/{k}") for k in (
            "summary", "momentum-distribution", "criteria-stats", "tier-stats", "momentum-grades",
            "momentum-filter", "technical-distribution/symbols", "fundamental-distribution/symbols",
            "symbols-snapshot", "symbol-statements", "symbol-fundamental-raw-data", "technical-filter",
            "tier-filter", "fundamental-conditions", "symbol-technical-conditions", "fundamental-filter",
            "symbol-option-pcr",
        )
    },
    "market": {
        ("GET", "/market/holidays"),
        ("GET", "/market/trading-day"),
        ("GET", "/bars"),
        ("GET", "/bars/coverage"),
        ("GET", "/bars/latest"),
        ("GET", "/bars/benchmark"),
        ("GET", "/bars/stats"),
    },
    "account": {
        ("GET", "/strategies/dims"),
        ("GET", "/strategies/gate-safety/defaults"),
    },
}


def _served(app_name: str) -> Set[Tuple[str, str]]:
    reader = MagicMock()
    reader._config = full_server_config()
    cfg = reader._config
    app: Any = {
        "research": lambda: create_research_app(reader=reader, control_via_db=None, merged_config=cfg),
        "market": lambda: create_market_app(reader=reader, control_via_db=None, merged_config=cfg),
        "account": lambda: create_account_app(reader=reader, control_via_db=None, merged_config=cfg),
        "monitor": lambda: create_monitor_app(
            reader=reader, control_via_db=None, data_lag_threshold_ms=5000, merged_config=cfg
        ),
    }[app_name]()
    return {(m, r.path) for r in app.routes for m in (getattr(r, "methods", None) or ())}


@pytest.mark.parametrize("app_name", sorted(RETIRED))
def test_retired_routes_are_gone(app_name: str) -> None:
    assert not sorted(RETIRED[app_name] & _served(app_name))


@pytest.mark.parametrize("app_name", sorted(KEPT))
def test_their_neighbours_are_served(app_name: str) -> None:
    assert not sorted(KEPT[app_name] - _served(app_name))
