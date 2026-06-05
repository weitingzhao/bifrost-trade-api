"""Shared helpers for contract / parity tests."""

from __future__ import annotations

_FULL_SERVER = {
    "monitor_port": 8765,
    "massive_port": 8766,
    "docs_port": 8767,
    "ops_port": 8768,
    "trading_port": 8769,
    "strategy_port": 8770,
    "portfolio_port": 8771,
    "market_port": 8772,
    "research_port": 8773,
    "skip_monitor_ib": True,
}


def full_server_config() -> dict:
    return {"server": dict(_FULL_SERVER)}
