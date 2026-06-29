"""Kubernetes Deployment executor — scale/restart workloads from api-ops in-cluster."""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from bifrost_api.ops.docker_compose_map import (
    compose_service_for_systemd_unit,
    is_compose_managed_unit,
)
from bifrost_api.ops.services.executor_local import (
    _WORKER_UNIT_BASE,
    RestrictedExecutor,
)

logger = logging.getLogger(__name__)

_CELERY_DEPLOYMENT = "celery-worker"
_CELERY_LABEL = "app.kubernetes.io/name=celery-worker"
_DEPLOY_TIMEOUT_SEC = 120


class KubernetesExecutor:
    """Control whitelisted systemd-unit names via K8s Deployments (same names as compose services)."""

    def __init__(
        self,
        *,
        namespace: str,
        allowed_units: list[str],
        broker_url: str,
        use_redis_stop: bool = True,
    ) -> None:
        self._namespace = namespace.strip() or "default"
        self._allowed: Set[str] = set(allowed_units)
        self._broker_url = broker_url
        self._use_redis_stop = use_redis_stop
        self._redis_delegate = RestrictedExecutor(
            allowed_units=[],
            broker_url=broker_url,
            use_redis_stop=use_redis_stop,
        )
        self._apps = None
        self._core = None
        self._k8s_reachable = self._init_clients()

    @staticmethod
    def resolve_namespace(ops_cfg: dict) -> str:
        k8s_cfg = ops_cfg.get("kubernetes") if isinstance(ops_cfg.get("kubernetes"), dict) else {}
        raw = (
            str(k8s_cfg.get("namespace") or "").strip()
            or os.environ.get("KUBERNETES_NAMESPACE", "").strip()
        )
        if raw:
            return raw
        ns_path = Path("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
        if ns_path.is_file():
            return ns_path.read_text(encoding="utf-8").strip()
        return "default"

    def _init_clients(self) -> bool:
        try:
            from kubernetes import client, config as k8s_config

            try:
                k8s_config.load_incluster_config()
            except Exception:
                k8s_config.load_kube_config()
            self._apps = client.AppsV1Api()
            self._core = client.CoreV1Api()
            return True
        except Exception as exc:
            logger.warning("Kubernetes client unavailable: %s", exc)
            return False

    @property
    def k8s_reachable(self) -> bool:
        return self._k8s_reachable

    @property
    def namespace(self) -> str:
        return self._namespace

    worker_to_unit = staticmethod(RestrictedExecutor.worker_to_unit)
    instance_unit = staticmethod(RestrictedExecutor.instance_unit)

    def _validator(self) -> RestrictedExecutor:
        return RestrictedExecutor(
            allowed_units=list(self._allowed),
            broker_url="",
            use_redis_stop=False,
        )

    def _validate(self, action: str, unit: str) -> None:
        self._validator()._validate(action, unit)  # noqa: SLF001

    async def _run_sync(self, fn, *args, **kwargs):
        return await asyncio.to_thread(fn, *args, **kwargs)

    @staticmethod
    def _is_celery_worker_unit(unit: str) -> bool:
        u = unit.strip()
        return u.startswith(f"{_WORKER_UNIT_BASE}@") and u.endswith(".service")

    @staticmethod
    def _instance_from_worker_unit(unit: str) -> str:
        prefix = f"{_WORKER_UNIT_BASE}@"
        if not unit.startswith(prefix) or not unit.endswith(".service"):
            raise ValueError(f"Not a {_WORKER_UNIT_BASE}@ template unit: {unit!r}")
        return unit[len(prefix) : -len(".service")]

    async def _read_deployment(self, name: str):
        if not self._apps:
            raise RuntimeError("Kubernetes API client is not initialized")
        return await self._run_sync(
            self._apps.read_namespaced_deployment,
            name,
            self._namespace,
        )

    async def _patch_deployment(self, name: str, body: dict):
        if not self._apps:
            raise RuntimeError("Kubernetes API client is not initialized")
        return await self._run_sync(
            self._apps.patch_namespaced_deployment,
            name,
            self._namespace,
            body,
        )

    async def _list_celery_pods(self):
        if not self._core:
            return []
        pod_list = await self._run_sync(
            self._core.list_namespaced_pod,
            self._namespace,
            label_selector=_CELERY_LABEL,
        )
        return list(pod_list.items or [])

    async def _deployment_ready_replicas(self, name: str) -> tuple[int, int]:
        try:
            dep = await self._read_deployment(name)
        except Exception as exc:
            logger.debug("read deployment %s: %s", name, exc)
            return 0, 0
        spec_rep = int(dep.spec.replicas or 0)
        ready = int(dep.status.ready_replicas or 0)
        return spec_rep, ready

    async def _scale_deployment(self, name: str, replicas: int) -> Dict[str, Any]:
        replicas = max(0, replicas)
        body = {"spec": {"replicas": replicas}}
        await self._patch_deployment(name, body)
        return {
            "method": "kubernetes",
            "action": "scale",
            "deployment": name,
            "namespace": self._namespace,
            "replicas": replicas,
            "message": f"scaled {name} to {replicas} in {self._namespace}",
        }

    async def _rollout_restart(self, name: str) -> Dict[str, Any]:
        stamp = datetime.now(timezone.utc).isoformat()
        body = {
            "spec": {
                "template": {
                    "metadata": {
                        "annotations": {
                            "kubectl.kubernetes.io/restartedAt": stamp,
                        }
                    }
                }
            }
        }
        await self._patch_deployment(name, body)
        return {
            "method": "kubernetes",
            "action": "restart",
            "deployment": name,
            "namespace": self._namespace,
            "message": f"rollout restart {name} in {self._namespace}",
        }

    async def _delete_pod(self, name: str) -> Dict[str, Any]:
        if not self._core:
            raise RuntimeError("Kubernetes API client is not initialized")
        await self._run_sync(
            self._core.delete_namespaced_pod,
            name,
            self._namespace,
        )
        return {
            "method": "kubernetes",
            "action": "delete_pod",
            "pod": name,
            "namespace": self._namespace,
            "message": f"deleted pod {name}",
        }

    async def _celery_pod_for_instance(self, instance_id: str):
        pods = await self._list_celery_pods()
        for pod in pods:
            pod_name = str(pod.metadata.name or "")
            if pod_name == instance_id or pod_name.endswith(f"-{instance_id}"):
                return pod
        return None

    async def _systemctl_celery(self, action: str, unit: str) -> Dict[str, Any]:
        instance_id = self._instance_from_worker_unit(unit)
        spec_rep, _ready = await self._deployment_ready_replicas(_CELERY_DEPLOYMENT)

        if action == "start":
            pod = await self._celery_pod_for_instance(instance_id)
            if pod is not None and pod.status.phase == "Running":
                return {
                    "method": "kubernetes",
                    "action": "start",
                    "unit": unit,
                    "message": f"pod already running for {instance_id}",
                }
            return await self._scale_deployment(_CELERY_DEPLOYMENT, spec_rep + 1)

        if action == "stop":
            pod = await self._celery_pod_for_instance(instance_id)
            if pod is not None:
                await self._delete_pod(pod.metadata.name)
                return {
                    "method": "kubernetes",
                    "action": "stop",
                    "unit": unit,
                    "pod": pod.metadata.name,
                    "message": f"deleted pod {pod.metadata.name}",
                }
            if spec_rep > 0:
                return await self._scale_deployment(_CELERY_DEPLOYMENT, spec_rep - 1)
            return {
                "method": "kubernetes",
                "action": "stop",
                "unit": unit,
                "message": "celery-worker already scaled to 0",
            }

        if action == "restart":
            pod = await self._celery_pod_for_instance(instance_id)
            if pod is not None:
                name = pod.metadata.name
                await self._delete_pod(name)
                return {
                    "method": "kubernetes",
                    "action": "restart",
                    "unit": unit,
                    "pod": name,
                    "message": f"deleted pod {name} for restart",
                }
            return await self._rollout_restart(_CELERY_DEPLOYMENT)

        raise PermissionError(f"Action {action!r} not supported for kubernetes celery control")

    async def _systemctl_deployment(
        self,
        action: str,
        deployment: str,
        unit: str,
    ) -> Dict[str, Any]:
        spec_rep, _ready = await self._deployment_ready_replicas(deployment)
        if action == "start":
            if spec_rep > 0:
                return {
                    "method": "kubernetes",
                    "action": "start",
                    "unit": unit,
                    "deployment": deployment,
                    "message": f"{deployment} already has replicas={spec_rep}",
                }
            return await self._scale_deployment(deployment, 1)
        if action == "stop":
            return await self._scale_deployment(deployment, 0)
        if action == "restart":
            if spec_rep == 0:
                return await self._scale_deployment(deployment, 1)
            return await self._rollout_restart(deployment)
        raise PermissionError(f"Action {action!r} not supported")

    async def _systemctl(self, action: str, unit: str, timeout: int | None = None) -> Dict[str, Any]:
        del timeout  # K8s patches are async; callers poll systemctl_is_active
        self._validate(action, unit)
        if self._is_celery_worker_unit(unit):
            return await self._systemctl_celery(action, unit)

        deployment = compose_service_for_systemd_unit(unit)
        if not deployment:
            raise PermissionError(
                f"Unit {unit!r} has no deployment mapping; kubernetes executor cannot control it."
            )
        if deployment == "redis":
            raise PermissionError("redis is external in K8s overlays; not controlled by api-ops")
        return await self._systemctl_deployment(action, deployment, unit)

    async def list_instances(self) -> List[Dict[str, str]]:
        pods = await self._list_celery_pods()
        out: List[Dict[str, str]] = []
        for pod in pods:
            name = str(pod.metadata.name or "")
            if not name:
                continue
            phase = str(pod.status.phase or "")
            if phase not in ("Running", "Pending"):
                continue
            instance_id = name
            unit = f"{_WORKER_UNIT_BASE}@{instance_id}.service"
            active = "active" if phase == "Running" else "activating"
            out.append({
                "unit": unit,
                "load": "loaded",
                "active": active,
                "sub": "running" if phase == "Running" else "start",
                "description": f"k8s pod {name} ({phase})",
            })
        if out:
            return out
        spec_rep, ready = await self._deployment_ready_replicas(_CELERY_DEPLOYMENT)
        if spec_rep > 0 and ready == 0:
            return [{
                "unit": f"{_WORKER_UNIT_BASE}@pending.service",
                "load": "loaded",
                "active": "activating",
                "sub": "start",
                "description": f"deployment {_CELERY_DEPLOYMENT} scaling ({ready}/{spec_rep})",
            }]
        return []

    async def redis_is_local(self) -> bool:
        return False

    async def systemctl_is_active(self, unit: str) -> str:
        if not self._k8s_reachable:
            return "unknown"
        try:
            self._validate("start", unit)
        except PermissionError:
            return "unknown"

        if self._is_celery_worker_unit(unit):
            instance_id = self._instance_from_worker_unit(unit)
            pod = await self._celery_pod_for_instance(instance_id)
            if pod is None:
                return "inactive"
            phase = str(pod.status.phase or "")
            if phase == "Running":
                return "active"
            if phase == "Pending":
                return "activating"
            return "inactive"

        deployment = compose_service_for_systemd_unit(unit)
        if not deployment or deployment == "redis":
            return "unknown"
        spec_rep, ready = await self._deployment_ready_replicas(deployment)
        if spec_rep == 0:
            return "inactive"
        if ready >= spec_rep:
            return "active"
        if ready > 0:
            return "activating"
        return "inactive"

    async def force_stop_worker_unit(self, unit: str) -> Dict[str, Any]:
        self._validate("stop", unit)
        RestrictedExecutor.assert_celery_worker_instance_unit(unit)
        instance_id = self._instance_from_worker_unit(unit)
        pod = await self._celery_pod_for_instance(instance_id)
        if pod is not None:
            return await self._delete_pod(pod.metadata.name)
        spec_rep, _ready = await self._deployment_ready_replicas(_CELERY_DEPLOYMENT)
        if spec_rep > 0:
            return await self._scale_deployment(_CELERY_DEPLOYMENT, spec_rep - 1)
        return {
            "method": "kubernetes",
            "action": "force_stop",
            "unit": unit,
            "message": "no celery-worker replicas to stop",
        }

    def deployment_for_unit(self, unit: str) -> Optional[str]:
        return compose_service_for_systemd_unit(unit)

    def compose_service_for_unit(self, unit: str) -> Optional[str]:
        """Alias for routers that already use compose_service_for_unit."""
        return self.deployment_for_unit(unit)

    def manages_unit(self, unit: str) -> bool:
        dep = is_compose_managed_unit(unit)
        if not dep:
            return False
        svc = compose_service_for_systemd_unit(unit)
        return svc not in (None, "redis")
