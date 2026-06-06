"""Entry point: start a single API domain service.

Usage:
    python scripts/run_server.py <domain>

Domains: monitor | massive | docs | ops | trading | strategy | portfolio | market | research
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Callable

logging.basicConfig(force=True)

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DOMAIN_RUNNERS: dict[str, Callable[[dict, str | None], None]] = {
    "monitor": lambda cfg, path: __import__(
        "bifrost_api.monitor.app", fromlist=["run_server"]
    ).run_server(cfg, resolved_config_path=path),
    "massive": lambda cfg, path: __import__(
        "bifrost_api.massive.app", fromlist=["run_massive_server"]
    ).run_massive_server(cfg, resolved_config_path=path),
    "docs": lambda cfg, path: __import__(
        "bifrost_api.docs_api.app", fromlist=["run_docs_server"]
    ).run_docs_server(cfg, resolved_config_path=path),
    "ops": lambda cfg, path: __import__(
        "bifrost_api.ops.app", fromlist=["run_ops_server"]
    ).run_ops_server(cfg, resolved_config_path=path),
    "trading": lambda cfg, path: __import__(
        "bifrost_api.trading.app", fromlist=["run_trading_server"]
    ).run_trading_server(cfg, resolved_config_path=path),
    "strategy": lambda cfg, path: __import__(
        "bifrost_api.strategy.app", fromlist=["run_strategy_server"]
    ).run_strategy_server(cfg, resolved_config_path=path),
    "portfolio": lambda cfg, path: __import__(
        "bifrost_api.portfolio.app", fromlist=["run_portfolio_server"]
    ).run_portfolio_server(cfg, resolved_config_path=path),
    "market": lambda cfg, path: __import__(
        "bifrost_api.market.app", fromlist=["run_market_server"]
    ).run_market_server(cfg, resolved_config_path=path),
    "research": lambda cfg, path: __import__(
        "bifrost_api.research.app", fromlist=["run_research_server"]
    ).run_research_server(cfg, resolved_config_path=path),
}


def main() -> None:
    from bifrost_core.config.startup import get_effective_ib_config, read_config, resolve_startup_config_path

    argv = sys.argv[1:]
    domain = "monitor"
    extra_argv: list[str] = []
    if argv and not argv[0].startswith("-"):
        domain = argv[0]
        extra_argv = argv[1:]
    elif argv:
        extra_argv = argv

    if domain not in DOMAIN_RUNNERS:
        print(f"Unknown domain: {domain}. Choose from: {sorted(DOMAIN_RUNNERS)}")
        sys.exit(1)

    config_path, _ = resolve_startup_config_path(_PROJECT_ROOT, extra_argv)
    os.environ["BIFROST_CONFIG"] = config_path
    config, resolved_config_path = read_config(config_path)

    if domain == "monitor" and not (config.get("server") or {}).get("skip_monitor_ib", False):
        try:
            get_effective_ib_config(config)
        except ValueError as exc:
            print(
                "Monitor API: invalid or missing IB config. "
                "Set server.skip_monitor_ib: true for API-only dev hosts. "
                f"Detail: {exc}",
                file=sys.stderr,
            )
            sys.exit(1)

    print(f"bifrost {domain} server: config {resolved_config_path}", file=sys.stderr)
    DOMAIN_RUNNERS[domain](config, resolved_config_path)


if __name__ == "__main__":
    main()
