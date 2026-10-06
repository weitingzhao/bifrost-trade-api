"""Routes retired with no caller and no traffic stay retired; their neighbours stay served.

TD-40: every retired path had 0 hits in all retained access logs (25+ days, every
env) and no caller in any sibling repo; the ones marked deprecated first (api 0.2.2 /
0.6.1) had no hit but agents' own checks in Loki before they went in api 0.8.0. TD-58: the dims writes went with core's
writers. TD-64: every process-exit route (lifecycle belongs to Kubernetes), while
each capabilities path stays, now served by one shared function. Each list below
pairs what went with what had to stay.
"""

from __future__ import annotations

from typing import Any, Dict, Set, Tuple

import pytest
from starlette.testclient import TestClient

from bifrost_api.account.app import create_account_app
from bifrost_api.market.app import create_market_app
from bifrost_api.monitor.app import create_app as create_monitor_app
from bifrost_api.research.app import create_research_app
from tests.contract.helpers import full_server_config
from tests.route_listing import served_routes
from tests.reader_mock import reader_mock

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
            # deleted in api 0.5.0 after one release marked deprecated (TD-40/TD-16)
            "momentum-distribution",
            # deleted in api 0.8.0 after one release marked deprecated with no hits (TD-40)
            "fundamental-filter", "technical-filter", "symbols-snapshot",
        )),
        ("GET", "/research/option-expirations"),
        ("GET", "/research/option-oi"),
        ("GET", "/research/option-trades"),
        ("POST", "/research/option-snapshot"),
        ("GET", "/research/iv-term-structure"),
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
        ("POST", "/market/shutdown"),
        # api 0.8.0 (TD-40): marked deprecated, no hits
        ("GET", "/bars/latest"),
        ("GET", "/bars/coverage"),
        ("GET", "/market/trading-day"),
    },
    "account": {
        ("POST", "/strategies/dims/{dim_type}"),
        ("PUT", "/strategies/dims/by-id/{strategy_dim_id}"),
        ("DELETE", "/strategies/dims/by-id/{strategy_dim_id}"),
        ("GET", "/strategies/dims/{dim_type}/items"),
        *(("POST", f"/{d}/shutdown") for d in ("account", "trading", "portfolio", "strategy")),
        # merge PUTs replaced by PATCH, deleted in api 0.6.0 (TD-15)
        ("PUT", "/strategies/templates/{strategy_template_id}"),
        ("PUT", "/strategies/opportunities/{strategy_opportunity_id}"),
        ("PUT", "/strategies/allocations/{strategy_allocation_id}"),
        ("PUT", "/strategies/plans/{strategy_plan_id}"),
        ("PUT", "/strategies/reviews/{strategy_instance_id}"),
        # api 0.8.0 (TD-40): marked deprecated, no hits
        ("GET", "/executions/link-candidates"),
        ("PATCH", "/executions/strategy-attribution"),
        ("DELETE", "/strategies/structures/{strategy_structure_id}"),
        ("GET", "/strategies/instances/{strategy_instance_id}/open-option-legs"),
    },
    "monitor": {
        ("POST", "/api/server/shutdown"),
        ("POST", "/ops/shutdown"),
        ("POST", "/research/docs/shutdown"),
        # api 0.8.0 (TD-40): marked deprecated, no hits. The monitor's IB and daemon
        # control writes, the always-empty operations log, and the ingest control writes.
        *(("POST", f"/control/{k}") for k in (
            "monitor_stop", "monitor_release_ib", "monitor_connect", "stop", "retry_ib", "release_ib",
            "refresh_replay", "refresh_ticker_subscriptions", "release_ticker_subscriptions",
            "init_ticker_subscriptions", "set_heartbeat_interval",
        )),
        ("GET", "/operations"),
        ("POST", "/ops/market-ingest/control"),
        ("POST", "/ops/market-ingest/clear-conflict-leases"),
    },
}
# Naming R4 (api 0.9.0): the 18 routes R1 replaced (decision pack 2026-10-03, D4-A), gone after
# a PROD release cycle with no "replaced route hit" in Loki for 4 days. Their successors stay.
NAMING_R4_RETIRED: Set[Tuple[str, str]] = {
    ("GET", "/strategies/instances"),
    ("POST", "/strategies/instances"),
    ("GET", "/strategies/instances/{strategy_instance_id}"),
    ("PATCH", "/strategies/instances/{strategy_instance_id}"),
    ("DELETE", "/strategies/instances/{strategy_instance_id}"),
    ("GET", "/strategies/win-rate"),
    ("GET", "/strategies/reviews"),
    ("PATCH", "/strategies/reviews/{strategy_instance_id}"),
    ("GET", "/strategies/gate-safety"),
    ("POST", "/strategies/gate-safety"),
    ("GET", "/strategies/gate-safety/defaults"),
    ("GET", "/strategies/gate-safety/{gate_safety_strategy_id}"),
    ("PUT", "/strategies/gate-safety/{gate_safety_strategy_id}"),
    ("PATCH", "/strategies/gate-safety/{gate_safety_strategy_id}"),
    ("DELETE", "/strategies/gate-safety/{gate_safety_strategy_id}"),
    ("GET", "/strategies/saved-searches"),
    ("POST", "/strategies/saved-searches"),
    ("DELETE", "/strategies/saved-searches/{preference_saved_search_id}"),
}
RETIRED["account"] = RETIRED.get("account", set()) | NAMING_R4_RETIRED

KEPT: Dict[str, Set[Tuple[str, str]]] = {
    "research": {
        ("GET", f"{R}/{k}") for k in (
            "summary", "criteria-stats", "tier-stats", "momentum-grades",
            "momentum-filter", "technical-distribution/symbols", "fundamental-distribution/symbols",
            "symbol-statements", "symbol-fundamental-raw-data",
            "tier-filter", "fundamental-conditions", "symbol-technical-conditions",
            "symbol-option-pcr",
        )
    } | {("GET", "/auth/capabilities"), ("GET", "/health")},
    "market": {
        ("GET", "/market/holidays"),
        ("GET", "/bars"),
        ("GET", "/bars/benchmark"),
        ("GET", "/bars/stats"),
        ("GET", "/market/auth/capabilities"),
        ("GET", "/health"),
    },
    "account": {
        ("GET", "/strategies/dims"),
        ("GET", "/gate-sets/defaults"),
        ("GET", "/trades"),
        ("POST", "/trades"),
        ("GET", "/trades/{trade_id:int}"),
        ("PATCH", "/trades/{trade_id:int}"),
        ("DELETE", "/trades/{trade_id:int}"),
        ("GET", "/trades/win-rate"),
        ("GET", "/trade-reviews"),
        ("PATCH", "/trade-reviews/{trade_id:int}"),
        ("GET", "/gate-sets"),
        ("POST", "/gate-sets"),
        ("GET", "/gate-sets/{gate_safety_strategy_id:int}"),
        ("PUT", "/gate-sets/{gate_safety_strategy_id:int}"),
        ("PATCH", "/gate-sets/{gate_safety_strategy_id:int}"),
        ("DELETE", "/gate-sets/{gate_safety_strategy_id:int}"),
        ("GET", "/preferences/saved-searches"),
        ("POST", "/preferences/saved-searches"),
        ("DELETE", "/preferences/saved-searches/{preference_saved_search_id:int}"),
        ("GET", "/strategies/structures/{strategy_structure_id}"),
        ("PUT", "/strategies/structures/{strategy_structure_id}"),
        ("PATCH", "/strategies/structures/{strategy_structure_id}"),
        ("GET", "/executions/stock-link-candidates"),
        *(("GET", f"/{d}/auth/capabilities") for d in ("account", "trading", "portfolio", "strategy")),
        ("GET", "/health"),
    },
    "monitor": {
        ("GET", "/api/server/auth/capabilities"),
        ("GET", "/ops/auth/capabilities"),
        ("GET", "/research/docs/auth/capabilities"),
        ("GET", "/health"),
        ("GET", "/ops/health"),
        ("GET", "/research/docs/health"),
        # the desk's controls stay (TD-40 removed only the uncalled ones)
        *(("POST", f"/control/{k}") for k in ("suspend", "resume", "flatten", "refresh_accounts")),
        ("GET", "/status"),
        ("GET", "/open-orders"),
        ("GET", "/ops/market-ingest/services"),
    },
}


def _app(app_name: str) -> Any:
    reader = reader_mock()
    reader.config = full_server_config()
    cfg = reader.config
    return {
        "research": lambda: create_research_app(reader=reader, control_via_db=None, merged_config=cfg),
        "market": lambda: create_market_app(reader=reader, control_via_db=None, merged_config=cfg),
        "account": lambda: create_account_app(reader=reader, control_via_db=None, merged_config=cfg),
        "monitor": lambda: create_monitor_app(
            reader=reader, control_via_db=None, data_lag_threshold_ms=5000, merged_config=cfg
        ),
    }[app_name]()


def _served(app_name: str) -> Set[Tuple[str, str]]:
    return served_routes(_app(app_name))


@pytest.mark.parametrize("app_name", sorted(RETIRED))
def test_retired_routes_are_gone(app_name: str) -> None:
    assert not sorted(RETIRED[app_name] & _served(app_name))


@pytest.mark.parametrize("app_name", sorted(KEPT))
def test_their_neighbours_are_served(app_name: str) -> None:
    assert not sorted(KEPT[app_name] - _served(app_name))


@pytest.mark.parametrize(
    "app_name, path",
    [
        ("monitor", "/api/server/auth/capabilities"),
        ("monitor", "/ops/auth/capabilities"),
        ("monitor", "/research/docs/auth/capabilities"),
        ("account", "/strategy/auth/capabilities"),
        ("market", "/market/auth/capabilities"),
        ("research", "/auth/capabilities"),
    ],
)
def test_each_capabilities_path_answers_the_same_shape(app_name: str, path: str) -> None:
    body = TestClient(_app(app_name)).get(path).json()
    assert body["ok"] is True
    assert body["identity"]["role"] == "viewer"
    assert body["capabilities"] == {"can_view": True, "can_operate": False, "can_admin": False}


def test_health_reports_only_ports_a_process_listens_on() -> None:
    """docs / ops run in monitor and strategy / portfolio in account (TD-64)."""
    c = TestClient(_app("monitor"))
    body = c.get("/health").json()
    for gone in ("docs_port", "ops_port", "strategy_port", "portfolio_port"):
        assert gone not in body
    assert {"monitor_port", "trading_port", "market_port", "research_port"} <= set(body)
    assert "port" not in c.get("/ops/health").json()
    assert "port" not in c.get("/research/docs/health").json()
