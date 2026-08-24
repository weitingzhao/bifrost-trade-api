"""Bifrost Ops API — authentication, audit, and market-ingest control.

Independent FastAPI service (same pattern as other bifrost_api domain apps).
Reads config from the shared YAML config system.
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

from bifrost_core.config.startup import (
    config_profile_from_resolved_path,
    normalize_server_config,
)
from bifrost_core.observability.prometheus import instrument_app

logger = logging.getLogger(__name__)


class AccessControlAllowPrivateNetworkMiddleware(BaseHTTPMiddleware):
    """Chrome Private Network Access: public / local pages calling a private IP need this header."""

    async def dispatch(self, request, call_next):
        response = await call_next(request)
        response.headers["Access-Control-Allow-Private-Network"] = "true"
        return response


DEFAULT_ALLOWED_UNITS = [
    "bifrost-ib-operator",
    "bifrost-ib-market-gateway",
    "bifrost-ib-ingestor",
    "bifrost-ib-account-agent",
    "bifrost-engine",
    "bifrost-account-sync-daemon",
]


def _allowed_units_from_config(config: dict) -> List[str]:
    ops_cfg = config.get("ops") or {}
    units = ops_cfg.get("allowed_units")
    if isinstance(units, list) and units:
        return [str(u).strip() for u in units if str(u).strip()]
    return list(DEFAULT_ALLOWED_UNITS)


def wire_ops_control_plane(
    app: FastAPI,
    config: dict,
    resolved_config_path: Optional[str] = None,
    *,
    register_root_health: bool = True,
) -> None:
    """Wire the K8s Ops control plane onto a standalone or merged monitor app."""

    _raw_srv = config.get("server")
    if not isinstance(_raw_srv, dict):
        raise ValueError("wire_ops_control_plane requires config['server'] from read_config() merged YAML.")
    config["server"] = normalize_server_config(dict(_raw_srv))

    _profile = (
        config_profile_from_resolved_path(resolved_config_path)
        if resolved_config_path
        else None
    )
    if _profile is None:
        from bifrost_api.ops.market_ingest_control_env import normalize_control_profile

        ops_cfg = config.get("ops") if isinstance(config.get("ops"), dict) else {}
        _profile = normalize_control_profile(ops_cfg.get("control_profile"))
    # Prefer existing monitor profile if already set
    if getattr(app.state, "bifrost_config_profile", None) is None:
        app.state.bifrost_config_profile = _profile

    allowed_units = _allowed_units_from_config(config)

    ops_cfg = config.get("ops") or {}
    executor_mode = str(ops_cfg.get("executor_mode") or "kubernetes").strip().lower()
    if executor_mode != "kubernetes":
        raise RuntimeError(
            f"ops.executor_mode={executor_mode!r} is no longer supported. "
            "api-ops is Kubernetes-only; set executor_mode: kubernetes "
            "(legacy local/docker/agent executors were removed)."
        )

    from bifrost_api.ops.services.executor_kubernetes import KubernetesExecutor

    namespace = KubernetesExecutor.resolve_namespace(ops_cfg)
    daemon_scale_guard = KubernetesExecutor.resolve_daemon_scale_guard(ops_cfg)
    executor = KubernetesExecutor(
        namespace=namespace,
        allowed_units=allowed_units,
        daemon_scale_guard=daemon_scale_guard,
    )
    logger.info(
        "Executor mode: kubernetes (namespace=%s, reachable=%s, daemon_scale_guard=%s)",
        namespace,
        executor.k8s_reachable,
        daemon_scale_guard,
    )

    app.state.bifrost_config = config
    app.state.executor = executor
    app.state.audit_log: list = []
    app.state.ops_project_root = None

    from bifrost_api.ops.auth import AuthConfig, OpsAuth

    auth_config = AuthConfig.from_config(config)
    app.state.ops_auth = OpsAuth(auth_config)

    from bifrost_api.ops.services.audit_store import AuditStore

    audit_store = AuditStore.from_config(config)
    app.state.audit_store = audit_store

    has_postgres = bool(config.get("postgres") or os.environ.get("PGHOST"))
    use_db_control = has_postgres
    if getattr(app.state, "control_via_db", None) is None:
        app.state.control_via_db = config if use_db_control else None
    if getattr(app.state, "status_cfg_for_read", None) is None:
        app.state.status_cfg_for_read = config if has_postgres else None

    from bifrost_api.ops.routers.workers import router as ops_router
    from bifrost_api.ops.routers.market_ingest import router as market_ingest_router

    app.include_router(market_ingest_router)
    app.include_router(ops_router)

    def _health_payload_sync() -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "status": "ok",
            "service": "bifrost-ops",
            "ts": time.time(),
        }
        profile = getattr(app.state, "bifrost_config_profile", None)
        if profile is not None:
            out["config_profile"] = profile
        out["port"] = int(config["server"]["ops_port"])
        if resolved_config_path:
            out["config_path"] = str(Path(resolved_config_path).resolve())
        out["executor_mode"] = "kubernetes"
        from bifrost_api.ops.services.executor_kubernetes import KubernetesExecutor

        if isinstance(app.state.executor, KubernetesExecutor):
            ex = app.state.executor
            out["k8s_reachable"] = ex.k8s_reachable
            out["k8s_namespace"] = ex.namespace
            out["daemon_scale_guard"] = ex.daemon_scale_guard
        out["auth_required"] = app.state.ops_auth.has_tokens
        out["audit_mode"] = audit_store.stats().get("mode", "memory")
        return out

    async def _health_payload_async() -> Dict[str, Any]:
        out = _health_payload_sync()
        from bifrost_api.ops.services.executor_kubernetes import KubernetesExecutor

        if isinstance(app.state.executor, KubernetesExecutor) and app.state.executor.k8s_reachable:
            try:
                out["k8s_workloads"] = await app.state.executor.workload_status_snapshot()
            except Exception as exc:  # noqa: BLE001
                logger.warning("ops health k8s_workloads failed: %s", exc)
                out["k8s_workloads"] = {}
        return out

    if register_root_health:

        @app.get("/health")
        async def ops_health_root() -> Dict[str, Any]:
            return await _health_payload_async()

    @app.get("/ops/health")
    async def ops_health_prefixed() -> Dict[str, Any]:
        return await _health_payload_async()

    @app.on_event("startup")
    async def ops_startup_event() -> None:
        logger.info(
            "Ops control plane started — allowed units: %s",
            allowed_units,
        )

    @app.on_event("shutdown")
    async def ops_shutdown_event() -> None:
        logger.info("Ops control plane shutting down")


def create_ops_app(
    config: dict,
    resolved_config_path: Optional[str] = None,
) -> FastAPI:
    """Build the Ops control plane FastAPI app."""

    app = FastAPI(
        title="Bifrost Ops API",
        description="Ops authentication, audit, and Kubernetes market-ingest control.",
        docs_url="/ops/docs",
        redoc_url="/ops/redoc",
        openapi_url="/ops/openapi.json",
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.add_middleware(AccessControlAllowPrivateNetworkMiddleware)

    wire_ops_control_plane(
        app,
        config,
        resolved_config_path=resolved_config_path,
        register_root_health=True,
    )

    instrument_app(app, "api-ops")
    return app


def run_ops_server(config: dict, resolved_config_path: Optional[str] = None) -> None:
    """Start the Ops API server."""
    import uvicorn

    port = int(config["server"]["ops_port"])
    app = create_ops_app(config, resolved_config_path=resolved_config_path)
    host = "0.0.0.0"
    logger.info("Ops API server on %s:%s", host, port)
    uvicorn.run(app, host=host, port=port, log_level="info", log_config=None)
