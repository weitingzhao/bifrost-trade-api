"""Bifrost Ops API — unified control plane for Celery workers.

Independent FastAPI service (same pattern as backend.massive).
Reads config from the shared YAML config system.
"""

from __future__ import annotations

import logging
import os
import time
import threading
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
    "bifrost-celery-worker",
    "bifrost-celery-beat",
    # Market ingest (WS Connector); required for systemctl_is_active pgrep + whitelist on subprocess Mac.
    "bifrost-massive-ws",
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


def _project_root_for_subprocess_executor(
    config: dict, resolved_config_path: Optional[str],
) -> Path:
    """Infer repo root for ``run_celery.py`` when ``ops.local_control=subprocess``."""
    ops_cfg = config.get("ops") or {}
    raw = (ops_cfg.get("project_root") or "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    if not resolved_config_path:
        raise ValueError(
            "ops.local_control=subprocess requires ops.project_root or resolved_config_path"
        )
    p = Path(resolved_config_path).resolve()
    if p.parent.name == "config":
        return p.parent.parent
    return p.parent


def _socket_project_root_for_subprocess_executor(
    config: dict, resolved_config_path: Optional[str],
) -> Path:
    """Infer repo root for socket ingest scripts (``run_massive_ws.py``, IB edge)."""
    ops_cfg = config.get("ops") or {}
    raw = (ops_cfg.get("socket_project_root") or "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return _project_root_for_subprocess_executor(config, resolved_config_path)


def create_ops_app(
    config: dict,
    resolved_config_path: Optional[str] = None,
) -> FastAPI:
    """Build the Ops control plane FastAPI app."""

    from bifrost_core.core.redis_url import effective_redis_dict, format_redis_url

    broker_url = format_redis_url(effective_redis_dict(config, default_db=1))

    _raw_srv = config.get("server")
    if not isinstance(_raw_srv, dict):
        raise ValueError("create_ops_app requires config['server'] from read_config() merged YAML.")
    config["server"] = normalize_server_config(dict(_raw_srv))

    app = FastAPI(
        title="Bifrost Ops API",
        description="Unified control plane: Celery worker status, scaling, audit.",
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

    _profile = (
        config_profile_from_resolved_path(resolved_config_path)
        if resolved_config_path
        else None
    )
    if _profile is None:
        from bifrost_api.ops.market_ingest_control_env import normalize_control_profile

        ops_cfg = config.get("ops") if isinstance(config.get("ops"), dict) else {}
        _profile = normalize_control_profile(ops_cfg.get("control_profile"))
    app.state.bifrost_config_profile = _profile

    allowed_units = _allowed_units_from_config(config)

    # ── Wire services ─────────────────────────────────────────────────────────

    from bifrost_worker.celery.celery_app import app as celery_app

    # ``src.workers.celery_app`` resolves broker at import time; ensure Ops ``read_config`` URL wins
    # so ``control.inspect`` hits the same Redis as workers and Flower.
    _prev_broker = celery_app.conf.get("broker_url")
    if _prev_broker != broker_url:
        logger.info(
            "Ops: aligning Celery app broker with ops config (was %r, now %r)",
            _prev_broker,
            broker_url,
        )
    celery_app.conf.broker_url = broker_url
    celery_app.conf.result_backend = broker_url

    from bifrost_api.ops.services.worker_state import WorkerStateService

    worker_svc = WorkerStateService(celery_app, broker_url, config)

    ops_cfg = config.get("ops") or {}
    use_redis_stop = ops_cfg.get("use_redis_stop", True)
    executor_mode = ops_cfg.get("executor_mode", "local")
    local_control_raw = str(ops_cfg.get("local_control") or "systemd").strip().lower()
    local_control = (
        local_control_raw if local_control_raw in ("systemd", "subprocess") else "systemd"
    )

    if executor_mode == "agent":
        from bifrost_api.ops.services.executor_agent import AgentExecutor

        agent_socket = ops_cfg.get(
            "agent_socket",
            "/run/bifrost-agent/bifrost-agent.sock",
        )
        executor = AgentExecutor(
            socket_path=agent_socket,
            allowed_units=allowed_units,
            broker_url=broker_url,
            use_redis_stop=use_redis_stop,
        )
        logger.info("Executor mode: agent (socket=%s)", agent_socket)
    elif executor_mode == "docker":
        from bifrost_api.ops.services.executor_docker import DockerComposeExecutor

        docker_cfg = ops_cfg.get("docker") if isinstance(ops_cfg.get("docker"), dict) else {}
        workdir = (
            str(docker_cfg.get("workdir") or "").strip()
            or os.environ.get("BIFROST_COMPOSE_WORKDIR", "/infra")
        )
        workdir_path = Path(workdir)
        raw_files = docker_cfg.get("compose_files")
        if isinstance(raw_files, list) and raw_files:
            compose_files = [str(f).strip() for f in raw_files if str(f).strip()]
        else:
            compose_files = ["docker-compose.yml"]
        if os.environ.get("BIFROST_BUILD_LOCAL", "").strip().lower() in (
            "1",
            "true",
            "yes",
        ):
            compose_files.append("docker-compose.local.yml")
        compose_files = [
            f
            for f in compose_files
            if (workdir_path / f).is_file()
        ]
        if not compose_files:
            compose_files = ["docker-compose.yml"]
        compose_project = (
            str(docker_cfg.get("compose_project") or "").strip()
            or os.environ.get("COMPOSE_PROJECT_NAME")
            or None
        )
        docker_socket = str(docker_cfg.get("socket_path") or "/var/run/docker.sock")
        host_workdir = (
            str(docker_cfg.get("host_workdir") or "").strip()
            or os.environ.get("BIFROST_COMPOSE_HOST_WORKDIR", "").strip()
            or None
        )
        executor = DockerComposeExecutor(
            workdir=workdir,
            compose_files=compose_files,
            allowed_units=allowed_units,
            broker_url=broker_url,
            use_redis_stop=use_redis_stop,
            compose_project=compose_project,
            docker_socket=docker_socket,
            host_workdir=host_workdir,
        )
        logger.info(
            "Executor mode: docker (workdir=%s, host_workdir=%s, files=%s, project=%s, sock=%s)",
            workdir,
            host_workdir or "(none)",
            compose_files,
            compose_project,
            docker_socket,
        )
    elif executor_mode == "kubernetes":
        from bifrost_api.ops.services.executor_kubernetes import KubernetesExecutor

        namespace = KubernetesExecutor.resolve_namespace(ops_cfg)
        executor = KubernetesExecutor(
            namespace=namespace,
            allowed_units=allowed_units,
            broker_url=broker_url,
            use_redis_stop=use_redis_stop,
        )
        logger.info(
            "Executor mode: kubernetes (namespace=%s, reachable=%s)",
            namespace,
            executor.k8s_reachable,
        )
    elif local_control == "subprocess":
        from bifrost_api.ops.services.executor_local import SubprocessLocalExecutor

        project_root = _project_root_for_subprocess_executor(config, resolved_config_path)
        socket_root = _socket_project_root_for_subprocess_executor(config, resolved_config_path)
        executor = SubprocessLocalExecutor(
            allowed_units=allowed_units,
            broker_url=broker_url,
            use_redis_stop=use_redis_stop,
            project_root=project_root,
            socket_project_root=socket_root,
            resolved_config_path=resolved_config_path,
        )
        logger.info(
            "Executor mode: local subprocess (worker=%s, socket=%s)",
            project_root,
            socket_root,
        )
    else:
        from bifrost_api.ops.services.executor_local import RestrictedExecutor

        executor = RestrictedExecutor(
            allowed_units=allowed_units,
            broker_url=broker_url,
            use_redis_stop=use_redis_stop,
        )
        logger.info("Executor mode: local (systemd)")

    app.state.worker_state_service = worker_svc
    app.state.bifrost_config = config
    app.state.executor = executor
    app.state.agent_socket_path = (
        str(
            ops_cfg.get(
                "agent_socket",
                "/run/bifrost-agent/bifrost-agent.sock",
            ),
        ).strip()
        if executor_mode == "agent"
        else None
    )
    app.state.audit_log: list = []
    try:
        app.state.ops_project_root = _project_root_for_subprocess_executor(
            config, resolved_config_path,
        )
    except ValueError:
        app.state.ops_project_root = None

    # ── Auth ──────────────────────────────────────────────────────────────────

    from bifrost_api.ops.auth import AuthConfig, OpsAuth

    auth_config = AuthConfig.from_config(config)
    app.state.ops_auth = OpsAuth(auth_config)

    # ── Audit store ───────────────────────────────────────────────────────────

    from bifrost_api.ops.services.audit_store import AuditStore

    audit_store = AuditStore.from_config(config)
    app.state.audit_store = audit_store

    has_postgres = bool(config.get("postgres") or os.environ.get("PGHOST"))
    use_db_control = has_postgres
    app.state.control_via_db = config if use_db_control else None
    app.state.status_cfg_for_read = config if has_postgres else None

    app.state.broker_url = broker_url
    app.state.redis_host = effective_redis_dict(config, default_db=1)["host"]

    # Celery worker console SSE (Redis Stream per worker nodename; same as former bifrost-server /api/celery/logs/stream)
    app.state.celery_log_queues: list = []
    app.state.celery_log_lock = threading.Lock()
    app.state._celery_log_loop: Any = None

    # ── Worker profiles (typed scaling) ────────────────────────────────────
    from bifrost_api.ops.worker_profiles import WorkerProfileRegistry

    app.state.worker_profile_registry = WorkerProfileRegistry.from_config(config)
    if hasattr(executor, "set_worker_profile_limits"):
        executor.set_worker_profile_limits({
            key: profile.max_worker_instances
            for key, profile in app.state.worker_profile_registry.profiles.items()
        })

    # ── Router ────────────────────────────────────────────────────────────────

    from bifrost_api.ops.routers.workers import router as ops_router
    from bifrost_api.ops.routers.job_queues import router as job_queues_router
    from bifrost_api.ops.routers.market_ingest import router as market_ingest_router

    app.include_router(job_queues_router)
    app.include_router(market_ingest_router)
    app.include_router(ops_router)

    # ── Health ────────────────────────────────────────────────────────────────

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
        out["executor_mode"] = executor_mode
        if executor_mode == "docker":
            from bifrost_api.ops.services.executor_docker import DockerComposeExecutor

            out["local_control"] = "docker"
            out["market_ingest_script_control"] = False
            if isinstance(app.state.executor, DockerComposeExecutor):
                ex = app.state.executor
                out["docker_reachable"] = ex.docker_reachable
                out["compose_workdir"] = ex.compose_workdir
        elif executor_mode == "kubernetes":
            from bifrost_api.ops.services.executor_kubernetes import KubernetesExecutor

            out["local_control"] = "kubernetes"
            out["market_ingest_script_control"] = False
            if isinstance(app.state.executor, KubernetesExecutor):
                ex = app.state.executor
                out["k8s_reachable"] = ex.k8s_reachable
                out["k8s_namespace"] = ex.namespace
        elif executor_mode == "local":
            out["local_control"] = local_control
            out["market_ingest_script_control"] = local_control == "subprocess"
        else:
            # Agent mode: same effective plane as systemd; omitting local_control confused clients
            # that gate ingest on subprocess vs systemd.
            out["local_control"] = "systemd"
            out["market_ingest_script_control"] = False
        out["auth_required"] = app.state.ops_auth.has_tokens
        out["audit_mode"] = audit_store.stats().get("mode", "memory")
        return out

    async def _health_payload_async() -> Dict[str, Any]:
        out = _health_payload_sync()
        sock = getattr(app.state, "agent_socket_path", None)
        if sock:
            from bifrost_api.ops.agent.health_probe import probe_agent_reachability

            out["agent_socket"] = sock
            ok, err = await probe_agent_reachability(sock)
            out["agent_reachable"] = ok
            if err:
                out["agent_error"] = err
        return out

    @app.get("/health")
    async def ops_health_root() -> Dict[str, Any]:
        return await _health_payload_async()

    @app.get("/ops/health")
    async def ops_health_prefixed() -> Dict[str, Any]:
        return await _health_payload_async()

    @app.on_event("startup")
    async def startup_event() -> None:
        logger.info(
            "Ops API started — allowed units: %s, broker: %s",
            allowed_units,
            broker_url.split("@")[-1] if "@" in broker_url else broker_url,
        )

    @app.on_event("shutdown")
    async def shutdown_event() -> None:
        logger.info("Ops API shutting down")

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
