"""Unit tests for Kubernetes Ops executor (trade-k8s-native W2)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from bifrost_api.ops.services.executor_kubernetes import KubernetesExecutor


def _fake_deployment(replicas: int, ready: int):
    return SimpleNamespace(
        metadata=SimpleNamespace(name="celery-worker", labels={"app.kubernetes.io/name": "celery-worker"}),
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
            "bifrost-massive-ws",
            "bifrost-celery-worker",
            "bifrost-engine",
            "bifrost-account-sync-daemon",
        ],
        broker_url="redis://127.0.0.1:6379/0",
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
async def test_daemon_stop_co_scales_account_sync(executor):
    patched: list[tuple[str, int]] = []

    async def _ready(name: str):
        if name == "daemon":
            return 2, 2, "deployment"
        if name == "account-sync":
            return 1, 1, "deployment"
        return 0, 0, "deployment"

    async def _scale(kind: str, name: str, replicas: int):
        patched.append((name, replicas))
        return {
            "method": "kubernetes",
            "action": "scale",
            "kind": kind,
            "deployment": name,
            "replicas": replicas,
            "message": f"scaled {name} to {replicas}",
        }

    executor._workload_ready_replicas = AsyncMock(side_effect=_ready)
    executor._scale_workload = AsyncMock(side_effect=_scale)

    result = await executor._systemctl("stop", "bifrost-engine.service")
    assert ("daemon", 0) in patched
    assert ("account-sync", 0) in patched
    assert result["co_scale_account_sync"]["replicas"] == 0


@pytest.mark.asyncio
async def test_daemon_start_co_starts_account_sync(executor):
    executor.set_daemon_scale_guard("observe")
    patched: list[tuple[str, int]] = []

    async def _ready(name: str):
        if name == "daemon":
            return 0, 0, "deployment"
        if name == "account-sync":
            return 0, 0, "deployment"
        return 0, 0, "deployment"

    async def _scale(kind: str, name: str, replicas: int):
        patched.append((name, replicas))
        return {
            "method": "kubernetes",
            "action": "scale",
            "kind": kind,
            "deployment": name,
            "replicas": replicas,
            "message": f"scaled {name} to {replicas}",
        }

    executor._workload_ready_replicas = AsyncMock(side_effect=_ready)
    executor._scale_workload = AsyncMock(side_effect=_scale)

    result = await executor._systemctl("start", "bifrost-engine.service")
    assert ("daemon", 1) in patched
    assert ("account-sync", 1) in patched
    assert result["co_scale_account_sync"]["replicas"] == 1


@pytest.mark.asyncio
async def test_account_sync_start_independent(executor):
    """Account sync is not gated by D10 freeze."""
    executor._read_deployment = AsyncMock(
        return_value=_named_deployment("account-sync", 0, 0)
    )
    executor._patch_deployment = AsyncMock()

    result = await executor._systemctl("start", "bifrost-account-sync-daemon.service")
    assert result["method"] == "kubernetes"
    assert result["replicas"] == 1
    executor._patch_deployment.assert_awaited_once()


@pytest.mark.asyncio
async def test_workload_status_snapshot_includes_daemon_mode(executor):
    async def _ready(name: str):
        mapping = {
            "daemon": (0, 0, "deployment"),
            "account-sync": (1, 1, "deployment"),
            "celery-beat": (1, 1, "deployment"),
        }
        return mapping[name]

    executor._workload_ready_replicas = AsyncMock(side_effect=_ready)
    snap = await executor.workload_status_snapshot()
    assert snap["daemon"]["replicas"] == 0
    assert snap["daemon"]["mode"] == "freeze"
    assert snap["daemon"]["scale_guard"] == "freeze"
    assert snap["account-sync"]["ready"] == 1
    assert snap["celery-beat"]["replicas"] == 1


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
    state = await executor.systemctl_is_active("bifrost-massive-ws.service")
    assert state == "inactive"


@pytest.mark.asyncio
async def test_celery_scale_up(executor):
    executor._read_deployment = AsyncMock(return_value=_fake_deployment(1, 1))
    executor._patch_deployment = AsyncMock()
    unit = "bifrost-celery-worker@stocks_massive-1.service"
    result = await executor._systemctl("start", unit)
    assert result["replicas"] == 2
    assert result["deployment"] == "celery-worker-stocks-massive"
    executor._patch_deployment.assert_awaited_once()


@pytest.mark.asyncio
async def test_celery_scale_falls_back_to_monolithic_deployment(executor):
    from kubernetes.client.rest import ApiException

    executor._read_deployment = AsyncMock(
        side_effect=[ApiException(status=404), _fake_deployment(1, 1)]
    )
    executor._patch_deployment = AsyncMock()

    result = await executor._systemctl("start", "bifrost-celery-worker@stocks_ib-1.service")

    assert result["deployment"] == "celery-worker"
    assert result["replicas"] == 2


@pytest.mark.asyncio
async def test_celery_scale_respects_profile_maximum(executor):
    executor._worker_profile_limits = {"stocks_ib": 1}
    executor._read_deployment = AsyncMock(return_value=_fake_deployment(1, 1))
    executor._patch_deployment = AsyncMock()

    with pytest.raises(PermissionError, match="max_worker_instances=1"):
        await executor._systemctl("start", "bifrost-celery-worker@stocks_ib-1.service")

    executor._patch_deployment.assert_not_awaited()


@pytest.mark.asyncio
async def test_list_instances_returns_deployment_status_not_pods(executor):
    deployment = SimpleNamespace(
        metadata=SimpleNamespace(
            name="celery-worker-stocks-ib",
            labels={"app.kubernetes.io/name": "celery-worker-stocks-ib"},
        ),
        spec=SimpleNamespace(replicas=2),
        status=SimpleNamespace(ready_replicas=1),
    )
    executor._list_celery_deployments = AsyncMock(return_value=[deployment])

    rows = await executor.list_instances()

    assert len(rows) == 1
    assert rows[0]["unit"] == "bifrost-celery-worker@stocks_ib-deployment.service"
    assert rows[0]["deployment"] == "celery-worker-stocks-ib"
    assert rows[0]["replicas"] == 2
    assert rows[0]["ready"] == 1
    assert rows[0]["active"] == "activating"


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
