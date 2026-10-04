"""Kubernetes workload reader for the daemon and ingest workloads (account-sync deleted, TD-22).

Reads only: replicas, readiness and the daemon's scale guard for the services table and
/ops/health. The write path (scale / rollout restart behind ``POST /ops/market-ingest/control``,
the only caller, guarded for the daemon by D10's freeze) went with that route (TD-40, api 0.7.6);
nothing in api patches a workload any more."""

from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional, Set

from bifrost_api.ops.workload_map import deployment_for_unit, is_managed_unit

logger = logging.getLogger(__name__)

_DAEMON_DEPLOYMENT = "daemon"
_VALID_DAEMON_SCALE_GUARDS = frozenset({"freeze", "observe", "off"})
_HEALTH_WORKLOAD_NAMES = (_DAEMON_DEPLOYMENT,)


class KubernetesExecutor:
    """Read whitelisted systemd-style units as Kubernetes workloads."""

    def __init__(
        self,
        *,
        namespace: str,
        allowed_units: list[str],
        daemon_scale_guard: str = "freeze",
    ) -> None:
        self._namespace = namespace.strip() or "default"
        self._allowed: Set[str] = set(allowed_units)
        self._daemon_scale_guard = self.normalize_daemon_scale_guard(daemon_scale_guard)
        self._apps = None
        self._core = None
        self._k8s_reachable = self._init_clients()

    @staticmethod
    def normalize_daemon_scale_guard(raw: Optional[str]) -> str:
        value = str(raw or "").strip().lower()
        return value if value in _VALID_DAEMON_SCALE_GUARDS else "freeze"

    @staticmethod
    def resolve_daemon_scale_guard(ops_cfg: dict) -> str:
        return KubernetesExecutor.normalize_daemon_scale_guard(
            ops_cfg.get("daemon_scale_guard") if isinstance(ops_cfg, dict) else None
        )

    @staticmethod
    def resolve_namespace(ops_cfg: dict) -> str:
        k8s_cfg = ops_cfg.get("kubernetes") if isinstance(ops_cfg.get("kubernetes"), dict) else {}
        raw = (
            str(k8s_cfg.get("namespace") or "").strip()
            or os.environ.get("KUBERNETES_NAMESPACE", "").strip()
        )
        if raw:
            return raw
        namespace_path = Path("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
        if namespace_path.is_file():
            return namespace_path.read_text(encoding="utf-8").strip()
        return "default"

    @property
    def daemon_scale_guard(self) -> str:
        return self._daemon_scale_guard

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

    def _is_allowed(self, unit: str) -> bool:
        allowed = {item.removesuffix(".service") for item in self._allowed}
        return unit.removesuffix(".service") in allowed

    async def _run_sync(self, fn, *args, **kwargs):
        return await asyncio.to_thread(fn, *args, **kwargs)

    async def _read_deployment(self, name: str):
        if not self._apps:
            raise RuntimeError("Kubernetes API client is not initialized")
        return await self._run_sync(self._apps.read_namespaced_deployment, name, self._namespace)

    async def _read_statefulset(self, name: str):
        if not self._apps:
            raise RuntimeError("Kubernetes API client is not initialized")
        return await self._run_sync(
            self._apps.read_namespaced_stateful_set,
            name,
            self._namespace,
        )

    async def _read_workload(self, name: str) -> tuple[str, Any]:
        from kubernetes.client.rest import ApiException

        try:
            return "deployment", await self._read_deployment(name)
        except ApiException as exc:
            if getattr(exc, "status", None) != 404:
                raise
            return "statefulset", await self._read_statefulset(name)

    async def _workload_ready_replicas(self, name: str) -> tuple[int, int, str]:
        try:
            kind, obj = await self._read_workload(name)
        except Exception as exc:  # noqa: BLE001
            logger.debug("read workload %s: %s", name, exc)
            return 0, 0, "deployment"
        return int(obj.spec.replicas or 0), int(obj.status.ready_replicas or 0), kind

    async def workload_status_snapshot(self) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        if not self._k8s_reachable:
            return out
        for name in _HEALTH_WORKLOAD_NAMES:
            spec_replicas, ready, kind = await self._workload_ready_replicas(name)
            row: Dict[str, Any] = {
                "replicas": spec_replicas,
                "ready": ready,
                "kind": kind,
            }
            if name == _DAEMON_DEPLOYMENT:
                row["mode"] = self._daemon_scale_guard
                row["scale_guard"] = self._daemon_scale_guard
            out[name] = row
        return out

    def scale_guard_for_deployment(self, deployment: Optional[str]) -> Optional[str]:
        return self._daemon_scale_guard if deployment == _DAEMON_DEPLOYMENT else None

    async def deployment_replica_counts(
        self,
        deployment: str,
    ) -> tuple[Optional[int], Optional[int]]:
        if not deployment or not self._k8s_reachable:
            return None, None
        try:
            spec_replicas, ready, _kind = await self._workload_ready_replicas(deployment)
            return spec_replicas, ready
        except Exception:  # noqa: BLE001
            return None, None

    async def systemctl_is_active(self, unit: str) -> str:
        if not self._k8s_reachable:
            return "unknown"
        if not self._is_allowed(unit):
            return "unknown"
        workload = deployment_for_unit(unit)
        if not workload:
            return "unknown"
        spec_replicas, ready, _kind = await self._workload_ready_replicas(workload)
        if spec_replicas == 0:
            return "inactive"
        if ready >= spec_replicas:
            return "active"
        return "activating" if ready > 0 else "inactive"

    def deployment_for_unit(self, unit: str) -> Optional[str]:
        return deployment_for_unit(unit)

    def manages_unit(self, unit: str) -> bool:
        return is_managed_unit(unit)
