"""Wave A closure — D10 freeze precheck for market-ingest control."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from bifrost_api.ops.models.schemas import MarketIngestAction
from bifrost_api.ops.routers.market_ingest import (
    _d10_should_reject_scale_up,
    _daemon_scale_up_blocked_by_guard,
)
from bifrost_api.ops.services.executor_kubernetes import KubernetesExecutor


@pytest.fixture
def freeze_executor(monkeypatch):
    monkeypatch.setattr(
        KubernetesExecutor,
        "_init_clients",
        lambda self: setattr(self, "_k8s_reachable", True) or True,
    )
    ex = KubernetesExecutor(
        namespace="bifrost-stg",
        allowed_units=["bifrost-engine", "bifrost-account-sync-daemon"],
        broker_url="redis://127.0.0.1:6379/0",
        daemon_scale_guard="freeze",
    )
    ex._apps = MagicMock()
    ex._core = MagicMock()
    return ex


def test_freeze_start_replicas_zero_rejected(freeze_executor):
    assert _d10_should_reject_scale_up(
        freeze_executor,
        "trading_engine",
        MarketIngestAction.START,
        0,
    )
    assert _daemon_scale_up_blocked_by_guard(
        freeze_executor,
        "trading_engine",
        MarketIngestAction.START,
        0,
    ) is not None


def test_freeze_start_replicas_positive_allowed(freeze_executor):
    assert not _d10_should_reject_scale_up(
        freeze_executor,
        "trading_engine",
        MarketIngestAction.START,
        2,
    )
    assert (
        _daemon_scale_up_blocked_by_guard(
            freeze_executor,
            "trading_engine",
            MarketIngestAction.START,
            2,
        )
        is None
    )


def test_freeze_account_sync_start_not_blocked(freeze_executor):
    assert not _d10_should_reject_scale_up(
        freeze_executor,
        "account_sync_daemon",
        MarketIngestAction.START,
        0,
    )


def test_observe_guard_start_not_blocked(freeze_executor):
    freeze_executor.set_daemon_scale_guard("observe")
    assert not _d10_should_reject_scale_up(
        freeze_executor,
        "trading_engine",
        MarketIngestAction.START,
        0,
    )


def test_freeze_unknown_replicas_rejected(freeze_executor):
    """Cannot prove already-running → treat as scale-up risk and reject."""
    assert _d10_should_reject_scale_up(
        freeze_executor,
        "trading_engine",
        MarketIngestAction.START,
        None,
    )


def test_non_k8s_executor_never_blocked():
    other = SimpleNamespace(daemon_scale_guard="freeze")
    assert not _d10_should_reject_scale_up(
        other,
        "trading_engine",
        MarketIngestAction.START,
        0,
    )
