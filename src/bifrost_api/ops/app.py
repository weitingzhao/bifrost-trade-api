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

    app.state.bifrost_config_profile = (
        config_profile_from_resolved_path(resolved_config_path)
        if resolved_config_path
        else None
    )

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
    elif local_control == "subprocess":
        from bifrost_api.ops.services.executor_local import SubprocessLocalExecutor

        project_root = _project_root_for_subprocess_executor(config, resolved_config_path)
        executor = SubprocessLocalExecutor(
            allowed_units=allowed_units,
            broker_url=broker_url,
            use_redis_stop=use_redis_stop,
            project_root=project_root,
            resolved_config_path=resolved_config_path,
        )
        logger.info(
            "Executor mode: local subprocess (run_celery.py, project=%s)",
            project_root,
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
        if executor_mode == "local":
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

    return app


def run_ops_server(config: dict, resolved_config_path: Optional[str] = None) -> None:
    """Start the Ops API server."""
    import uvicorn

    port = int(config["server"]["ops_port"])
    app = create_ops_app(config, resolved_config_path=resolved_config_path)
    host = "0.0.0.0"
    logger.info("Ops API server on %s:%s", host, port)
    uvicorn.run(app, host=host, port=port, log_level="info", log_config=None)
