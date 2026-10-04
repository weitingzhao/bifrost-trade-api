"""Phase 2: FastAPI app for GET /status, GET /operations, POST /control/*.

When ``frontend/dist`` exists (``npm run build``), GET ``/`` serves the SPA and ``/assets`` is mounted; otherwise GET ``/`` returns a small API stub. Dev hot-reload: ``./scripts/run_frontend.sh dev``.

Monitoring runs on a separate host from the Strategy Trading Daemon (RE-5). Start of the daemon is only on the trading machine (run_engine.py); no subprocess/start on this server.
"""

import logging
import threading
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import asyncio
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from bifrost_core.config.profile import deployment_profile
from bifrost_core.ib_operator.client import IbOperatorClient
from bifrost_core.monitor.reader.common import StatusReader
from bifrost_core.observability.prometheus import instrument_app
from bifrost_api.common.service_endpoints import mount_auth_capabilities
from bifrost_api.deprecations import install_deprecations
from bifrost_api.write_guard import install_write_guard

logger = logging.getLogger(__name__)


_UTILIZED_SERVICE_ORDER = (
    "server",
    "main",
    "api",
    # "massive" retired (P7) — api-massive removed; keep out of default order
    "docs",
    "ops",
    "trading",
    "strategy",
    "portfolio",
    "market",
    "research",
)


def _order_utilized_rows(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    rank = {k: i for i, k in enumerate(_UTILIZED_SERVICE_ORDER)}

    def sort_key(r: Dict[str, str]) -> Tuple[int, str]:
        s = str(r.get("service") or "").lower()
        return (rank.get(s, 1000), s)

    return sorted(rows, key=sort_key)


def _utilized_services_from_config(merged_config: Optional[dict]) -> List[Dict[str, str]]:
    """Parse ``utilized.services`` from YAML into [{"service": "ops", "env": "dev"}, ...].

    Accepts either a mapping (each key = service name, value = ``dev`` or ``prod``) or a legacy
    list of ``{service: env}`` one-key dicts / ``name:env`` strings.
    """
    out: List[Dict[str, str]] = []
    if not merged_config:
        return out
    raw = merged_config.get("utilized") or {}
    services = raw.get("services")
    if isinstance(services, dict):
        for k, v in services.items():
            ks = str(k).strip()
            vs = str(v).strip().strip("\"'")
            if ks and vs:
                out.append({"service": ks, "env": vs})
        return _order_utilized_rows(out)
    if not isinstance(services, list):
        return out
    for x in services:
        if isinstance(x, dict):
            for k, v in x.items():
                ks = str(k).strip()
                vs = str(v).strip().strip("\"'")
                if ks and vs:
                    out.append({"service": ks, "env": vs})
        elif isinstance(x, str):
            part = x.strip()
            if ":" in part:
                left, _, right = part.partition(":")
                name = left.strip()
                env = right.strip().strip("\"'")
                if name and env:
                    out.append({"service": name, "env": env})
    return _order_utilized_rows(out)


def create_app(
    reader: StatusReader,
    control_via_db: Optional[dict],
    data_lag_threshold_ms: Optional[float],
    status_cfg_for_read: Optional[dict] = None,
    resolved_config_path: Optional[str] = None,
    merged_config: Optional[dict] = None,
) -> FastAPI:
    """Build FastAPI app: reader, control channel (stop/flatten/suspend/resume via DB).
    status_cfg_for_read: when set, read paths that use DB without control channel (e.g. only PGHOST or postgres configured without sink=postgres).
    """
    app = FastAPI(
        title="Bifrost Trader API",
        description="Phase 2: status and control API; monitoring UI when frontend/dist is built.",
    )
    # Browser fetch from Vite / another host to this API (e.g. Settings → API Health split probes).
    # Every write needs a role (debt TD-23). Added before CORS so CORS stays the
    # outer layer and a refusal still carries its headers.
    install_write_guard(app, lambda: merged_config or reader.config)
    # Routes no repo calls carry Deprecation: true and log their callers (debt TD-40).
    install_deprecations(app)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    # Phase B Wave B4: Ops Private Network Access header (merged ops control plane).
    try:
        from bifrost_api.ops.app import AccessControlAllowPrivateNetworkMiddleware

        app.add_middleware(AccessControlAllowPrivateNetworkMiddleware)
    except Exception as exc:
        # A monitor without its Ops wiring passes readiness on /health while /ops/* and
        # /research/docs/* answer 404, and the platform's api-ops probe stays green. Fail
        # the start instead, so a rollout keeps the old pods (debt TD-27).
        raise RuntimeError("monitor startup: Ops Private Network middleware failed") from exc
    # The per-service console log-stream queues / locks / threads / loops (12
    # services, 48 attributes) had no reader and are gone (TD-64).

    # System messages (Redis message center -> materialized TTL items -> SSE fan-out).
    app.state.system_message_queues: list = []
    app.state.system_message_queue_lock = threading.Lock()
    app.state._system_message_thread: Optional[threading.Thread] = None
    app.state._system_message_loop: Optional[asyncio.AbstractEventLoop] = None

    # IB access via Redis IB Operator only (no in-process TWS clients).
    app.state.monitor_enabled = True
    app.state.ib_operator_client = None

    # Shared deps for routers (reader, control_via_db, etc.)
    app.state.reader = reader
    app.state.control_via_db = control_via_db
    app.state.data_lag_threshold_ms = data_lag_threshold_ms
    app.state.status_cfg_for_read = status_cfg_for_read
    # One answer to "which env" for every app (debt TD-52): control_profile, then env, then file name.
    app.state.bifrost_config_profile = deployment_profile(merged_config or reader.config, resolved_config_path)
    _fe = (merged_config or {}).get("frontend") or {}

    def _fe_str(key: str) -> Optional[str]:
        v = _fe.get(key)
        if v is None:
            return None
        t = str(v).strip()
        return t or None

    app.state.bifrost_frontend_public_origin = _fe_str("public_origin")
    app.state.bifrost_frontend_dev_path = _fe_str("dev_path")
    app.state.bifrost_frontend_prod_path = _fe_str("prod_path")

    _scfg = (merged_config or {}).get("server") or {}
    if not isinstance(_scfg, dict):
        raise ValueError("create_app (monitor) requires merged_config['server'] from read_config().")
    app.state.bifrost_server_listen_port = int(_scfg["monitor_port"])
    # Only ports a process listens on reach /health: massive / docs / ops /
    # strategy / portfolio stay in the YAML schema but no pod listens on them.
    # read_config normalises the server block (account_port); a raw block may still carry trading_port.
    app.state.bifrost_trading_port = int(_scfg.get("account_port") or _scfg["trading_port"])
    app.state.bifrost_market_port = int(_scfg["market_port"])
    app.state.bifrost_research_port = int(_scfg["research_port"])

    app.state.bifrost_utilized_services = _utilized_services_from_config(merged_config)
    app.state.bifrost_merged_config = merged_config or {}

    from bifrost_api.monitor.routers import (
        config_router,
        core_router,
        daemon_router,
        messages_router,
        status_router,
    )

    app.include_router(core_router)
    app.include_router(messages_router)
    app.include_router(status_router)
    app.include_router(daemon_router)
    app.include_router(config_router)
    mount_auth_capabilities(
        app, ["/api/server/auth/capabilities"], lambda: merged_config or reader.config
    )
    # Phase B: position-categories live on account-service (merged portfolio).

    # Phase B Wave B3: Docs OpenAPI aggregate absorbed into monitor.
    try:
        from bifrost_api.docs_api.app import attach_docs_routes

        attach_docs_routes(
            app,
            config=merged_config or reader.config,
            resolved_config_path=resolved_config_path,
        )
    except Exception as exc:
        raise RuntimeError("monitor startup: attaching the docs routes failed") from exc

    # Phase B Wave B4: Ops control plane absorbed into monitor (Gate PASS).
    try:
        from bifrost_api.ops.app import wire_ops_control_plane

        wire_ops_control_plane(
            app,
            merged_config or reader.config,
            resolved_config_path=resolved_config_path,
            register_root_health=False,
        )
    except Exception as exc:
        raise RuntimeError("monitor startup: wiring the Ops control plane failed") from exc

    # backend/monitor/app.py -> repo root (not backend/)
    _root = Path(__file__).resolve().parent.parent.parent
    _dist_assets = _root / "frontend" / "dist" / "assets"
    if _dist_assets.is_dir():
        app.mount(
            "/assets", StaticFiles(directory=str(_dist_assets)), name="dist_assets"
        )

    @app.on_event("startup")
    async def startup_event() -> None:
        """IB 经 Redis Operator；本进程不连接 TWS。"""
        cfg = merged_config or reader.config
        app.state.ib_operator_client = IbOperatorClient.from_merged_config(cfg)
        if app.state.ib_operator_client is not None:
            logger.info("Monitor IB Operator client enabled (Redis RPC)")
        elif (cfg.get("server") or {}).get("skip_monitor_ib", False):
            logger.info("skip_monitor_ib=true: IB Operator client not used (Management mode)")
        else:
            logger.warning(
                "IB Operator client unavailable (enable Redis and ib_operator.enabled, or set skip_monitor_ib)"
            )

    @app.on_event("shutdown")
    async def shutdown_event() -> None:
        op = getattr(app.state, "ib_operator_client", None)
        if op is not None:
            try:
                op.close()
            except Exception:
                pass

    instrument_app(app, "api-monitor")
    return app


def run_server(config: dict, resolved_config_path: Optional[str] = None) -> None:
    """Start the status server (host 0.0.0.0, port from config). Control channel: Redis STREAM/HASH daemon IPC. No start: daemon is started on trading host only."""
    import os
    import uvicorn

    has_postgres = bool(config.get("postgres") or os.environ.get("PGHOST"))
    use_db_control = has_postgres
    status_cfg_for_read = config if has_postgres else None

    port = int(config["server"]["monitor_port"])
    data_lag_ms = None
    gates = config.get("gates") or {}
    state_cfg = gates.get("state") or {}
    system_cfg = state_cfg.get("system") or {}
    if "data_lag_threshold_ms" in system_cfg:
        data_lag_ms = system_cfg["data_lag_threshold_ms"]

    reader = StatusReader(config)
    control_via_db = config if use_db_control else None
    app = create_app(
        reader,
        control_via_db,
        data_lag_ms,
        status_cfg_for_read=status_cfg_for_read,
        resolved_config_path=resolved_config_path,
        merged_config=config,
    )
    host = "0.0.0.0"
    logger.info(
        "Status server on %s:%s (control=Redis daemon IPC; start only on trading host)",
        host,
        port,
    )
    uvicorn.run(app, host=host, port=int(port), log_level="info", log_config=None)
