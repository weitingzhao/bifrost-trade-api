"""Strategy domain FastAPI app."""

import logging
import os
import threading
import time
from typing import Any, Dict, Optional

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from bifrost_core.config.startup import config_profile_from_resolved_path, normalize_server_config
from bifrost_core.monitor.reader import StatusReader
from bifrost_core.observability.prometheus import instrument_app

logger = logging.getLogger(__name__)

SIDECAR_STOP_EXIT_DELAY_SEC = 2.5


def create_strategy_app(
    reader: StatusReader,
    control_via_db: Optional[dict],
    status_cfg_for_read: Optional[dict] = None,
    resolved_config_path: Optional[str] = None,
    merged_config: Optional[dict] = None,
) -> FastAPI:
    """Build the Strategy domain FastAPI app."""
    app = FastAPI(
        title="Bifrost Strategy API",
        description="Strategy structures, opportunities, instances, and allocations.",
        docs_url="/strategy/docs",
        redoc_url="/strategy/redoc",
        openapi_url="/strategy/openapi.json",
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.state.reader = reader
    app.state.control_via_db = control_via_db
    app.state.status_cfg_for_read = status_cfg_for_read
    app.state.bifrost_config_profile = (
        config_profile_from_resolved_path(resolved_config_path) if resolved_config_path else None
    )

    _cfg_holder = merged_config or reader._config
    _raw_server = _cfg_holder.get("server")
    if not isinstance(_raw_server, dict):
        raise ValueError("create_strategy_app requires config['server'] from read_config() merged YAML.")
    _cfg_holder["server"] = normalize_server_config(dict(_raw_server))
    reader._config["server"] = _cfg_holder["server"]
    app.state.bifrost_strategy_port = int(_cfg_holder["server"]["strategy_port"])

    from bifrost_api.strategy.routers import strategies_router
    app.include_router(strategies_router)

    from bifrost_api.ops.services.audit_store import AuditStore

    app.state.audit_store = AuditStore.from_config(_cfg_holder)

    @app.get("/health")
    def strategy_health() -> Any:
        import time
        out: Any = {"status": "ok", "service": "bifrost-strategy", "ts": time.time()}
        profile = getattr(app.state, "bifrost_config_profile", None)
        if profile is not None:
            out["config_profile"] = profile
        out["port"] = app.state.bifrost_strategy_port
        return out

    @app.get("/strategy/auth/capabilities")
    def strategy_auth_capabilities(request: Request) -> Dict[str, Any]:
        """Same shape as GET /ops/auth/capabilities (shared ops.auth tokens)."""
        from bifrost_api.ops.auth import AuthConfig, OpsAuth

        cfg = merged_config or reader._config
        return OpsAuth(AuthConfig.from_config(cfg)).capabilities(request)

    @app.post("/strategy/shutdown")
    def post_strategy_shutdown(request: Request) -> Any:
        """Terminate the Strategy API process. Requires operator role (same tokens as Ops API)."""
        from bifrost_api.ops.auth import AuthConfig, OpsAuth
        from bifrost_api.ops.models.schemas import AuditEntry

        cfg = merged_config or reader._config
        ops_auth = OpsAuth(AuthConfig.from_config(cfg))
        ident, denied = ops_auth.require_role(request, "operator")
        audit_store = getattr(app.state, "audit_store", None)
        if denied:
            if audit_store is not None:
                audit_store.append(
                    AuditEntry(
                        operator=ident.name,
                        source_ip=request.client.host if request.client else None,
                        action="strategy_shutdown",
                        target="process",
                        outcome="denied",
                        detail=f"role={ident.role}",
                    ),
                )
            return denied
        if audit_store is not None:
            audit_store.append(
                AuditEntry(
                    operator=ident.name,
                    source_ip=request.client.host if request.client else None,
                    action="strategy_shutdown",
                    target="process",
                    outcome="scheduled",
                    detail="process exit",
                ),
            )

        def _exit_after_send() -> None:
            time.sleep(SIDECAR_STOP_EXIT_DELAY_SEC)
            logger.info("Strategy API shutdown: exiting process.")
            os._exit(0)

        threading.Thread(target=_exit_after_send, daemon=True).start()
        return {"ok": True}

    instrument_app(app, "api-strategy")
    return app


def run_strategy_server(config: dict, resolved_config_path: Optional[str] = None) -> None:
    """Start the Strategy API server."""
    import os
    import uvicorn

    has_postgres = bool(config.get("postgres") or os.environ.get("PGHOST"))
    status_cfg_for_read = config if has_postgres else None
    control_via_db = config if has_postgres else None

    port = int(config["server"]["strategy_port"])

    reader = StatusReader(config)
    app = create_strategy_app(
        reader,
        control_via_db,
        status_cfg_for_read=status_cfg_for_read,
        resolved_config_path=resolved_config_path,
        merged_config=config,
    )
    host = "0.0.0.0"
    logger.info("Strategy API server on %s:%s", host, port)
    uvicorn.run(app, host=host, port=int(port), log_level="info", log_config=None)
