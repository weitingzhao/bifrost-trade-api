"""Unit tests for Kubernetes Ops executor (trade-k8s-native W2)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from bifrost_api.ops.services.executor_kubernetes import KubernetesExecutor


def _fake_deployment(replicas: int, ready: int):
    return SimpleNamespace(
        metadata=SimpleNamespace(name="socket", labels={"app.kubernetes.io/name": "socket"}),
        spec=SimpleNamespace(replicas=replicas),
        status=SimpleNamespace(ready_replicas=ready),
    )


@pytest.fixture
def executor(monkeypatch):
    monkeypatch.setattr(
        KubernetesExecutor,
        "_init_clients",
        lambda self: setattr(self, "_k8s_reachable", True) or True,
    )
    ex = KubernetesExecutor(
        namespace="bifrost-stg",
        allowed_units=[
            "bifrost-ib-ingestor",
            "bifrost-engine",
        ],
        daemon_scale_guard="freeze",
    )
    ex._apps = MagicMock()
    ex._core = MagicMock()
    return ex


def _named_deployment(name: str, replicas: int, ready: int):
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name, labels={"app.kubernetes.io/name": name}),
        spec=SimpleNamespace(replicas=replicas),
        status=SimpleNamespace(ready_replicas=ready),
    )


@pytest.mark.asyncio
async def test_workload_status_snapshot_includes_daemon_mode(executor):
    async def _ready(name: str):
        mapping = {
            "daemon": (0, 0, "deployment"),
        }
        return mapping[name]

    executor._workload_ready_replicas = AsyncMock(side_effect=_ready)
    snap = await executor.workload_status_snapshot()
    assert snap["daemon"]["replicas"] == 0
    assert snap["daemon"]["mode"] == "freeze"
    assert snap["daemon"]["scale_guard"] == "freeze"
    # account-sync was deleted (TD-22); the snapshot no longer asks about it.
    assert "account-sync" not in snap


def test_normalize_daemon_scale_guard_defaults_to_freeze():
    assert KubernetesExecutor.normalize_daemon_scale_guard(None) == "freeze"
    assert KubernetesExecutor.normalize_daemon_scale_guard("") == "freeze"
    assert KubernetesExecutor.normalize_daemon_scale_guard("observe") == "observe"
    assert KubernetesExecutor.resolve_daemon_scale_guard({"daemon_scale_guard": "off"}) == "off"


def _fake_statefulset(replicas: int, ready: int):
    return SimpleNamespace(
        spec=SimpleNamespace(replicas=replicas),
        status=SimpleNamespace(ready_replicas=ready),
    )


@pytest.mark.asyncio
async def test_ib_statefulset_is_active(executor):
    from kubernetes.client.rest import ApiException

    executor._read_deployment = AsyncMock(side_effect=ApiException(status=404))
    executor._read_statefulset = AsyncMock(return_value=_fake_statefulset(1, 1))
    state = await executor.systemctl_is_active("bifrost-ib-ingestor.service")
    assert state == "active"


@pytest.mark.asyncio
async def test_systemctl_is_active_running(executor):
    executor._read_deployment = AsyncMock(return_value=_fake_deployment(1, 1))
    state = await executor.systemctl_is_active("bifrost-ib-ingestor.service")
    assert state == "active"


@pytest.mark.asyncio
async def test_systemctl_is_active_scaled_zero(executor):
    executor._read_deployment = AsyncMock(return_value=_fake_deployment(0, 0))
    state = await executor.systemctl_is_active("bifrost-ib-ingestor.service")
    assert state == "inactive"


@pytest.mark.asyncio
async def test_resolve_namespace_from_file(tmp_path, monkeypatch):
    ns_file = tmp_path / "namespace"
    ns_file.write_text("bifrost-dev\n", encoding="utf-8")
    with patch(
        "bifrost_api.ops.services.executor_kubernetes.Path",
    ) as path_cls:
        path_cls.return_value.is_file.return_value = True
        path_cls.return_value.read_text.return_value = "bifrost-dev\n"
        assert KubernetesExecutor.resolve_namespace({}) == "bifrost-dev"


def test_the_executor_cannot_patch_a_workload():
    """TD-40 (api 0.7.6): its only writer, POST /ops/market-ingest/control, is gone, and the
    scale / rollout-restart path went with it. Nothing in api scales the daemon."""
    for name in ("_systemctl", "_systemctl_workload", "_scale_workload", "_rollout_restart_workload",
                 "_patch_deployment", "_patch_statefulset", "set_daemon_scale_guard"):
        assert not hasattr(KubernetesExecutor, name), name


@pytest.mark.asyncio
async def test_a_unit_outside_the_whitelist_reads_unknown(executor):
    executor._read_deployment = AsyncMock(return_value=_fake_deployment(1, 1))
    assert await executor.systemctl_is_active("bifrost-not-listed.service") == "unknown"
    executor._read_deployment.assert_not_awaited()
