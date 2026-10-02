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
async def test_daemon_start_blocked_by_d10_freeze(executor):
    executor._read_deployment = AsyncMock(return_value=_named_deployment("daemon", 0, 0))
    executor._patch_deployment = AsyncMock()

    with pytest.raises(PermissionError, match="BLOCKED \\(D10\\)"):
        await executor._systemctl("start", "bifrost-engine.service")

    executor._patch_deployment.assert_not_awaited()


@pytest.mark.asyncio
async def test_daemon_restart_scale_up_blocked_by_d10_freeze(executor):
    executor._read_deployment = AsyncMock(return_value=_named_deployment("daemon", 0, 0))
    executor._patch_deployment = AsyncMock()

    with pytest.raises(PermissionError, match="BLOCKED \\(D10\\)"):
        await executor._systemctl("restart", "bifrost-engine.service")

    executor._patch_deployment.assert_not_awaited()


@pytest.mark.asyncio
async def test_daemon_rollout_restart_allowed_when_already_running(executor):
    """Freeze blocks scale-up only; rollout of running observe daemon is OK."""
    executor._read_deployment = AsyncMock(return_value=_named_deployment("daemon", 2, 2))
    executor._patch_deployment = AsyncMock()
    # account-sync already up — co-scale start should no-op
    async def _ready(name: str):
        if name == "daemon":
            return 2, 2, "deployment"
        if name == "account-sync":
            return 1, 1, "deployment"
        return 0, 0, "deployment"

    executor._workload_ready_replicas = AsyncMock(side_effect=_ready)

    result = await executor._systemctl("restart", "bifrost-engine.service")
    assert result["action"] == "restart"
    executor._patch_deployment.assert_awaited()


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


@pytest.mark.asyncio
async def test_systemctl_start_scales_deployment(executor):
    executor._read_deployment = AsyncMock(return_value=_fake_deployment(0, 0))
    executor._patch_deployment = AsyncMock()
    result = await executor._systemctl("start", "bifrost-ib-ingestor.service")
    assert result["method"] == "kubernetes"
    assert result["deployment"] == "ib-market-gateway"
    executor._patch_deployment.assert_awaited_once()
    body = executor._patch_deployment.await_args.args[1]
    assert body["spec"]["replicas"] == 1


def _fake_statefulset(replicas: int, ready: int):
    return SimpleNamespace(
        spec=SimpleNamespace(replicas=replicas),
        status=SimpleNamespace(ready_replicas=ready),
    )


@pytest.mark.asyncio
async def test_ib_unit_falls_back_to_statefulset_restart(executor):
    """W5: IB socket is a StatefulSet — Deployment read 404s, control uses STS."""
    from kubernetes.client.rest import ApiException

    executor._read_deployment = AsyncMock(side_effect=ApiException(status=404))
    executor._read_statefulset = AsyncMock(return_value=_fake_statefulset(1, 1))
    executor._patch_statefulset = AsyncMock()
    executor._patch_deployment = AsyncMock()

    result = await executor._systemctl("restart", "bifrost-ib-ingestor.service")

    assert result["kind"] == "statefulset"
    assert result["statefulset"] == "ib-market-gateway"
    executor._patch_statefulset.assert_awaited_once()
    executor._patch_deployment.assert_not_awaited()


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
