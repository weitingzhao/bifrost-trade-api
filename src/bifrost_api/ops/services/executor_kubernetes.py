"""Kubernetes Deployment executor — scale/restart workloads from api-ops in-cluster."""

from __future__ import annotations

import asyncio
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from bifrost_api.ops.workload_map import (
    deployment_for_unit as map_deployment_for_unit,
    is_managed_unit,
)
from bifrost_api.ops.services.executor_common import (
    ActionValidator,
    _WORKER_UNIT_BASE,
    instance_unit as _instance_unit,
    worker_to_unit as _worker_to_unit,
)

logger = logging.getLogger(__name__)

_CELERY_DEPLOYMENT = "celery-worker"
_CELERY_BEAT_DEPLOYMENT = "celery-beat"
_CELERY_WORKER_PREFIX = "celery-worker-"
_DAEMON_DEPLOYMENT = "daemon"
_ACCOUNT_SYNC_DEPLOYMENT = "account-sync"
_DEPLOY_TIMEOUT_SEC = 120
_CELERY_INSTANCE_RE = re.compile(r"^(?P<profile>[A-Za-z0-9_]+)-\d+$")
_VALID_DAEMON_SCALE_GUARDS = frozenset({"freeze", "observe", "off"})
_D10_FREEZE_MESSAGE = (
    "Trading execution is BLOCKED (D10). Daemon scale-up requires Owner unlock."
)
_HEALTH_WORKLOAD_NAMES = (
    _DAEMON_DEPLOYMENT,
    _ACCOUNT_SYNC_DEPLOYMENT,
    _CELERY_BEAT_DEPLOYMENT,
)


class KubernetesExecutor:
    """Control whitelisted systemd-unit names via K8s Deployments (same names as compose services)."""

    def __init__(
        self,
        *,
        namespace: str,
        allowed_units: list[str],
        broker_url: str,
        use_redis_stop: bool = True,
        worker_profile_limits: Optional[Dict[str, int]] = None,
        daemon_scale_guard: str = "freeze",
    ) -> None:
        self._namespace = namespace.strip() or "default"
        self._allowed: Set[str] = set(allowed_units)
        self._broker_url = broker_url
        self._use_redis_stop = use_redis_stop
        self._daemon_scale_guard = self.normalize_daemon_scale_guard(daemon_scale_guard)
        self._worker_profile_limits = {
            key: max(1, int(limit))
            for key, limit in (worker_profile_limits or {}).items()
        }
        self._apps = None
        self._core = None
        self._k8s_reachable = self._init_clients()

    @staticmethod
    def normalize_daemon_scale_guard(raw: Optional[str]) -> str:
        """Return ``freeze`` | ``observe`` | ``off`` (default ``freeze`` for D10)."""
        val = str(raw or "").strip().lower()
        if val in _VALID_DAEMON_SCALE_GUARDS:
            return val
        return "freeze"

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
        ns_path = Path("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
        if ns_path.is_file():
            return ns_path.read_text(encoding="utf-8").strip()
        return "default"

    @property
    def daemon_scale_guard(self) -> str:
        return self._daemon_scale_guard

    def set_daemon_scale_guard(self, guard: str) -> None:
        self._daemon_scale_guard = self.normalize_daemon_scale_guard(guard)

    def _assert_daemon_scale_allowed(self, action: str, *, scaling_up: bool) -> None:
        """D10 freeze: block daemon start / restart that would scale replicas up from 0."""
        if self._daemon_scale_guard != "freeze":
            return
        if not scaling_up:
            return
        if action in ("start", "restart"):
            raise PermissionError(_D10_FREEZE_MESSAGE)

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

    def set_worker_profile_limits(self, limits: Dict[str, int]) -> None:
        """Install validated profile limits after the Ops config registry is built."""
        self._worker_profile_limits = {
            key: max(1, int(limit))
            for key, limit in limits.items()
        }

    worker_to_unit = staticmethod(_worker_to_unit)
    instance_unit = staticmethod(_instance_unit)

    def _validate(self, action: str, unit: str) -> None:
        ActionValidator(list(self._allowed)).validate(action, unit)

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

    @staticmethod
    def celery_deployment_for_profile(profile: str) -> str:
        """Return the W3 per-queue Deployment name for a worker profile."""
        normalized = profile.strip().replace("_", "-")
        if not normalized or not re.fullmatch(r"[A-Za-z0-9-]+", normalized):
            raise ValueError(f"Invalid Celery worker profile {profile!r}")
        return f"{_CELERY_WORKER_PREFIX}{normalized}"

    @staticmethod
    def _profile_from_instance_id(instance_id: str) -> str:
        match = _CELERY_INSTANCE_RE.fullmatch(instance_id)
        if match is None:
            raise ValueError(
                "Kubernetes Celery scaling requires an instance id in "
                "'{profile}-{number}' form."
            )
        return str(match.group("profile"))

    @staticmethod
    def _is_not_found(exc: Exception) -> bool:
        return getattr(exc, "status", None) == 404

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

    async def _read_statefulset(self, name: str):
        if not self._apps:
            raise RuntimeError("Kubernetes API client is not initialized")
        return await self._run_sync(
            self._apps.read_namespaced_stateful_set,
            name,
            self._namespace,
        )

    async def _patch_statefulset(self, name: str, body: dict):
        if not self._apps:
            raise RuntimeError("Kubernetes API client is not initialized")
        return await self._run_sync(
            self._apps.patch_namespaced_stateful_set,
            name,
            self._namespace,
            body,
        )

    async def _read_workload(self, name: str) -> tuple[str, Any]:
        """Resolve a workload to (kind, object).

        W5 trade-k8s-native: IB socket services migrated Deployment → StatefulSet.
        Prefer a Deployment (back-compat: celery + massive-ws); fall back to a
        StatefulSet on 404 so api-ops controls both kinds with one code path.
        """
        from kubernetes.client.rest import ApiException

        try:
            obj = await self._read_deployment(name)
            return "deployment", obj
        except ApiException as exc:
            if getattr(exc, "status", None) != 404:
                raise
            obj = await self._read_statefulset(name)
            return "statefulset", obj

    async def _workload_ready_replicas(self, name: str) -> tuple[int, int, str]:
        try:
            kind, obj = await self._read_workload(name)
        except Exception as exc:  # noqa: BLE001
            logger.debug("read workload %s: %s", name, exc)
            return 0, 0, "deployment"
        spec_rep = int(obj.spec.replicas or 0)
        ready = int(obj.status.ready_replicas or 0)
        return spec_rep, ready, kind

    async def _scale_workload(self, kind: str, name: str, replicas: int) -> Dict[str, Any]:
        replicas = max(0, replicas)
        body = {"spec": {"replicas": replicas}}
        if kind == "statefulset":
            await self._patch_statefulset(name, body)
        else:
            await self._patch_deployment(name, body)
        out: Dict[str, Any] = {
            "method": "kubernetes",
            "action": "scale",
            "namespace": self._namespace,
            "kind": kind,
            "replicas": replicas,
            "message": f"scaled {kind}/{name} to {replicas} in {self._namespace}",
        }
        out["deployment" if kind == "deployment" else "statefulset"] = name
        return out

    async def _rollout_restart_workload(self, kind: str, name: str) -> Dict[str, Any]:
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
        if kind == "statefulset":
            await self._patch_statefulset(name, body)
        else:
            await self._patch_deployment(name, body)
        out: Dict[str, Any] = {
            "method": "kubernetes",
            "action": "restart",
            "namespace": self._namespace,
            "kind": kind,
            "message": f"rollout restart {kind}/{name} in {self._namespace}",
        }
        out["deployment" if kind == "deployment" else "statefulset"] = name
        return out

    async def _list_celery_deployments(self) -> List[Any]:
        """Discover worker Deployments; Pods are controller implementation details."""
        if not self._apps:
            return []
        deployment_list = await self._run_sync(
            self._apps.list_namespaced_deployment,
            self._namespace,
        )
        deployments: List[Any] = []
        for deployment in deployment_list.items or []:
            name = str(getattr(deployment.metadata, "name", "") or "")
            labels = getattr(deployment.metadata, "labels", None) or {}
            app_name = str(labels.get("app.kubernetes.io/name") or "")
            component = str(labels.get("app.kubernetes.io/component") or "")
            if (
                app_name == _CELERY_DEPLOYMENT
                or app_name.startswith(_CELERY_WORKER_PREFIX)
                or (component == "celery" and name.startswith(_CELERY_WORKER_PREFIX))
            ):
                deployments.append(deployment)
        return deployments

    async def _resolve_celery_deployment(self, profile: str) -> tuple[str, int, int]:
        """Prefer W3 profile deployments and fall back to the W1 monolith."""
        profile_deployment = self.celery_deployment_for_profile(profile)
        try:
            dep = await self._read_deployment(profile_deployment)
            return (
                profile_deployment,
                int(dep.spec.replicas or 0),
                int(dep.status.ready_replicas or 0),
            )
        except Exception as exc:
            if not self._is_not_found(exc):
                raise
        dep = await self._read_deployment(_CELERY_DEPLOYMENT)
        return (
            _CELERY_DEPLOYMENT,
            int(dep.spec.replicas or 0),
            int(dep.status.ready_replicas or 0),
        )

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

    async def _systemctl_celery(self, action: str, unit: str) -> Dict[str, Any]:
        instance_id = self._instance_from_worker_unit(unit)
        profile = self._profile_from_instance_id(instance_id)
        deployment, spec_rep, _ready = await self._resolve_celery_deployment(profile)

        if action == "start":
            max_replicas = self._worker_profile_limits.get(profile)
            if max_replicas is not None and spec_rep >= max_replicas:
                raise PermissionError(
                    f"Celery profile {profile!r} is already at max_worker_instances="
                    f"{max_replicas}."
                )
            result = await self._scale_deployment(deployment, spec_rep + 1)
            result.update({"unit": unit, "profile": profile})
            return result

        if action == "stop":
            if spec_rep > 0:
                result = await self._scale_deployment(deployment, spec_rep - 1)
                result.update({"unit": unit, "profile": profile})
                return result
            return {
                "method": "kubernetes",
                "action": "stop",
                "unit": unit,
                "deployment": deployment,
                "profile": profile,
                "message": f"{deployment} already scaled to 0",
            }

        if action == "restart":
            result = await self._rollout_restart(deployment)
            result.update({"unit": unit, "profile": profile})
            return result

        raise PermissionError(f"Action {action!r} not supported for kubernetes celery control")

    async def _co_scale_account_sync(self, daemon_action: str) -> Optional[Dict[str, Any]]:
        """Keep account-sync paired with daemon (Owner D-A: co-scale pair)."""
        try:
            spec_rep, _ready, kind = await self._workload_ready_replicas(_ACCOUNT_SYNC_DEPLOYMENT)
        except Exception as exc:  # noqa: BLE001
            logger.warning("co-scale account-sync: read failed: %s", exc)
            return None
        if daemon_action == "stop":
            if spec_rep > 0:
                return await self._scale_workload(kind, _ACCOUNT_SYNC_DEPLOYMENT, 0)
            return None
        if daemon_action in ("start", "restart"):
            if spec_rep == 0:
                return await self._scale_workload(kind, _ACCOUNT_SYNC_DEPLOYMENT, 1)
            return None
        return None

    async def _systemctl_workload(
        self,
        action: str,
        workload: str,
        unit: str,
    ) -> Dict[str, Any]:
        # W5: workload may be a Deployment (celery/massive-ws) or a StatefulSet (IB edge).
        spec_rep, _ready, kind = await self._workload_ready_replicas(workload)
        is_daemon = workload == _DAEMON_DEPLOYMENT

        if action == "start":
            if is_daemon:
                self._assert_daemon_scale_allowed(action, scaling_up=(spec_rep == 0))
            if spec_rep > 0:
                out = {
                    "method": "kubernetes",
                    "action": "start",
                    "unit": unit,
                    "kind": kind,
                    "message": f"{kind}/{workload} already has replicas={spec_rep}",
                }
                out["deployment" if kind == "deployment" else "statefulset"] = workload
                if is_daemon:
                    co = await self._co_scale_account_sync("start")
                    if co:
                        out["co_scale_account_sync"] = co
                return out
            result = await self._scale_workload(kind, workload, 1)
            result["unit"] = unit
            if is_daemon:
                co = await self._co_scale_account_sync("start")
                if co:
                    result["co_scale_account_sync"] = co
            return result

        if action == "stop":
            result = await self._scale_workload(kind, workload, 0)
            result["unit"] = unit
            if is_daemon:
                co = await self._co_scale_account_sync("stop")
                if co:
                    result["co_scale_account_sync"] = co
            return result

        if action == "restart":
            if is_daemon:
                # Scale-up via restart (replicas==0 → 1) is blocked under D10 freeze.
                # Rollout restart of an already-running observe daemon is allowed.
                self._assert_daemon_scale_allowed(action, scaling_up=(spec_rep == 0))
            if spec_rep == 0:
                result = await self._scale_workload(kind, workload, 1)
                result["unit"] = unit
                if is_daemon:
                    co = await self._co_scale_account_sync("restart")
                    if co:
                        result["co_scale_account_sync"] = co
                return result
            result = await self._rollout_restart_workload(kind, workload)
            result["unit"] = unit
            if is_daemon:
                co = await self._co_scale_account_sync("restart")
                if co:
                    result["co_scale_account_sync"] = co
            return result

        raise PermissionError(f"Action {action!r} not supported")

    async def workload_status_snapshot(self) -> Dict[str, Dict[str, Any]]:
        """Best-effort replicas/ready for daemon, account-sync, celery-beat (Ops health)."""
        out: Dict[str, Dict[str, Any]] = {}
        if not self._k8s_reachable:
            return out
        for name in _HEALTH_WORKLOAD_NAMES:
            try:
                spec_rep, ready, kind = await self._workload_ready_replicas(name)
            except Exception as exc:  # noqa: BLE001
                logger.debug("workload_status_snapshot %s: %s", name, exc)
                continue
            row: Dict[str, Any] = {
                "replicas": spec_rep,
                "ready": ready,
                "kind": kind,
            }
            if name == _DAEMON_DEPLOYMENT:
                row["mode"] = (
                    "freeze"
                    if self._daemon_scale_guard == "freeze"
                    else ("observe" if self._daemon_scale_guard == "observe" else "off")
                )
                row["scale_guard"] = self._daemon_scale_guard
            out[name] = row
        return out

    def scale_guard_for_deployment(self, deployment: Optional[str]) -> Optional[str]:
        """Return daemon_scale_guard for daemon deployment; None for other workloads."""
        if deployment == _DAEMON_DEPLOYMENT:
            return self._daemon_scale_guard
        return None

    async def deployment_replica_counts(
        self, deployment: str
    ) -> tuple[Optional[int], Optional[int]]:
        """Return (replicas, ready) for a named workload, or (None, None) on failure."""
        if not deployment or not self._k8s_reachable:
            return None, None
        try:
            spec_rep, ready, _kind = await self._workload_ready_replicas(deployment)
            return spec_rep, ready
        except Exception:  # noqa: BLE001
            return None, None

    async def _systemctl(self, action: str, unit: str, timeout: int | None = None) -> Dict[str, Any]:
        del timeout  # K8s patches are async; callers poll systemctl_is_active
        self._validate(action, unit)
        if self._is_celery_worker_unit(unit):
            return await self._systemctl_celery(action, unit)

        deployment = map_deployment_for_unit(unit)
        if not deployment:
            raise PermissionError(
                f"Unit {unit!r} has no deployment mapping; kubernetes executor cannot control it."
            )
        if deployment == "redis":
            raise PermissionError("redis is external in K8s overlays; not controlled by api-ops")
        return await self._systemctl_workload(action, deployment, unit)

    async def list_instances(self) -> List[Dict[str, Any]]:
        deployments = await self._list_celery_deployments()
        out: List[Dict[str, Any]] = []
        for deployment in deployments:
            name = str(deployment.metadata.name or "")
            if not name:
                continue
            replicas = int(deployment.spec.replicas or 0)
            ready = int(deployment.status.ready_replicas or 0)
            profile = (
                name[len(_CELERY_WORKER_PREFIX) :].replace("-", "_")
                if name.startswith(_CELERY_WORKER_PREFIX)
                else "all"
            )
            active = "active" if replicas > 0 and ready >= replicas else (
                "activating" if replicas > 0 else "inactive"
            )
            out.append({
                "unit": f"{_WORKER_UNIT_BASE}@{profile}-deployment.service",
                "load": "loaded",
                "active": active,
                "sub": "running" if active == "active" else ("start" if replicas else "dead"),
                "description": f"k8s deployment {name} ({ready}/{replicas} ready)",
                "deployment": name,
                "profile": profile,
                "replicas": replicas,
                "ready": ready,
            })
        return out

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
            try:
                _deployment, replicas, ready = await self._resolve_celery_deployment(
                    self._profile_from_instance_id(instance_id)
                )
            except Exception:
                return "unknown"
            if replicas == 0:
                return "inactive"
            if ready >= replicas:
                return "active"
            if ready > 0:
                return "activating"
            return "inactive"

        deployment = map_deployment_for_unit(unit)
        if not deployment or deployment == "redis":
            return "unknown"
        spec_rep, ready, _kind = await self._workload_ready_replicas(deployment)
        if spec_rep == 0:
            return "inactive"
        if ready >= spec_rep:
            return "active"
        if ready > 0:
            return "activating"
        return "inactive"

    async def force_stop_worker_unit(self, unit: str) -> Dict[str, Any]:
        # A Deployment controller immediately replaces deleted Pods.  Scaling is
        # the only meaningful "force stop" for a Kubernetes Celery worker.
        return await self._systemctl_celery("stop", unit)

    async def celery_runtime_capabilities(self) -> Dict[str, Any]:
        """Best-effort K8s runtime facts for the Celery capabilities endpoint."""
        if not self._k8s_reachable:
            return {"beat_running": None, "worker_profiles": [], "monolithic_worker": False}
        try:
            beat = await self._read_deployment(_CELERY_BEAT_DEPLOYMENT)
            beat_running: Optional[bool] = bool(
                int(beat.spec.replicas or 0) > 0
                and int(beat.status.ready_replicas or 0) > 0
            )
        except Exception:
            beat_running = None
        try:
            deployments = await self._list_celery_deployments()
        except Exception:
            deployments = []
        names = {str(dep.metadata.name or "") for dep in deployments}
        profiles = sorted(
            name[len(_CELERY_WORKER_PREFIX) :].replace("-", "_")
            for name in names
            if name.startswith(_CELERY_WORKER_PREFIX)
        )
        return {
            "beat_running": beat_running,
            "worker_profiles": profiles,
            "monolithic_worker": _CELERY_DEPLOYMENT in names,
        }

    def deployment_for_unit(self, unit: str) -> Optional[str]:
        return map_deployment_for_unit(unit)

    def manages_unit(self, unit: str) -> bool:
        if not is_managed_unit(unit):
            return False
        svc = map_deployment_for_unit(unit)
        return svc not in (None, "redis")
