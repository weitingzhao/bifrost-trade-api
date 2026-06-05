"""Trading domain FastAPI app — executions, performance, transactions."""

import logging
import os
import threading
import time
from typing import Any, Dict, Optional

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from bifrost_core.config.startup import config_profile_from_resolved_path, normalize_server_config
from bifrost_core.monitor.reader import StatusReader

logger = logging.getLogger(__name__)

# Same delay as Docs/Ops shutdown so the HTTP response can flush before os._exit.
SIDECAR_STOP_EXIT_DELAY_SEC = 2.5


def create_trading_app(
    reader: StatusReader,
    control_via_db: Optional[dict],
    status_cfg_for_read: Optional[dict] = None,
    resolved_config_path: Optional[str] = None,
    merged_config: Optional[dict] = None,
) -> FastAPI:
    """Build the Trading domain FastAPI app (executions, performance, transactions)."""
    app = FastAPI(
        title="Bifrost Trading API",
        description="Executions, performance, and transaction endpoints.",
        docs_url="/trading/docs",
        redoc_url="/trading/redoc",
        openapi_url="/trading/openapi.json",
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
    app.state.monitor_enabled = True
    app.state.ib_operator_client = None
    app.state.bifrost_config_profile = (
        config_profile_from_resolved_path(resolved_config_path) if resolved_config_path else None
    )

    _cfg_holder = merged_config or reader._config
    _raw_server = _cfg_holder.get("server")
    if not isinstance(_raw_server, dict):
        raise ValueError("create_trading_app requires config['server'] from read_config() merged YAML.")
    _cfg_holder["server"] = normalize_server_config(dict(_raw_server))
    reader._config["server"] = _cfg_holder["server"]
    app.state.bifrost_trading_port = int(_cfg_holder["server"]["trading_port"])

    from bifrost_api.ops.services.audit_store import AuditStore

    app.state.audit_store = AuditStore.from_config(_cfg_holder)

    from bifrost_api.trading.routers import executions_router
    app.include_router(executions_router)

    @app.get("/health")
    def trading_health() -> Any:
        import time
        out: Any = {"status": "ok", "service": "bifrost-trading", "ts": time.time()}
        profile = getattr(app.state, "bifrost_config_profile", None)
        if profile is not None:
            out["config_profile"] = profile
        out["port"] = app.state.bifrost_trading_port
        return out

    @app.get("/trading/auth/capabilities")
    def trading_auth_capabilities(request: Request) -> Dict[str, Any]:
        """Same shape as GET /ops/auth/capabilities (shared ops.auth tokens)."""
        from bifrost_api.ops.auth import AuthConfig, OpsAuth

        cfg = merged_config or reader._config
        return OpsAuth(AuthConfig.from_config(cfg)).capabilities(request)

    @app.post("/trading/shutdown")
    def post_trading_shutdown(request: Request) -> Any:
        """Terminate the Trading API process. Requires operator role (same tokens as Ops API)."""
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
                        action="trading_shutdown",
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
                    action="trading_shutdown",
                    target="process",
                    outcome="scheduled",
                    detail="process exit",
                ),
            )

        def _exit_after_send() -> None:
            time.sleep(SIDECAR_STOP_EXIT_DELAY_SEC)
            logger.info("Trading API shutdown: exiting process.")
            os._exit(0)

        threading.Thread(target=_exit_after_send, daemon=True).start()
        return {"ok": True}

    @app.on_event("startup")
    async def startup_event() -> None:
        from bifrost_core.ib_operator.client import IbOperatorClient

        cfg = merged_config or reader._config
        app.state.ib_operator_client = IbOperatorClient.from_merged_config(cfg)

    @app.on_event("shutdown")
    async def shutdown_event() -> None:
        op = getattr(app.state, "ib_operator_client", None)
        if op is not None:
            try:
                op.close()
            except Exception:
                pass

    return app


def run_trading_server(config: dict, resolved_config_path: Optional[str] = None) -> None:
    """Start the Trading API server."""
    import os
    import uvicorn

    has_postgres = bool(config.get("postgres") or os.environ.get("PGHOST"))
    status_cfg_for_read = config if has_postgres else None
    control_via_db = config if has_postgres else None

    port = int(config["server"]["trading_port"])

    reader = StatusReader(config)
    app = create_trading_app(
        reader,
        control_via_db,
        status_cfg_for_read=status_cfg_for_read,
        resolved_config_path=resolved_config_path,
        merged_config=config,
    )
    host = "0.0.0.0"
    logger.info("Trading API server on %s:%s", host, port)
    uvicorn.run(app, host=host, port=int(port), log_level="info", log_config=None)
