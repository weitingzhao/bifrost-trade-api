"""Account domain FastAPI app — merged trading + portfolio (same HTTP paths).

Phase B Wave B2: single process on trading_port (8769) serving:
  - /executions*, /performance, /transactions* (trading)
  - /portfolio/*, /position-categories* (portfolio)
"""

from __future__ import annotations

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


def create_account_app(
    reader: StatusReader,
    control_via_db: Optional[dict],
    status_cfg_for_read: Optional[dict] = None,
    resolved_config_path: Optional[str] = None,
    merged_config: Optional[dict] = None,
) -> FastAPI:
    """Build Account API: executions/transactions + portfolio model/categories."""
    app = FastAPI(
        title="Bifrost Account API",
        description="Account domain: executions, performance, transactions, portfolio model, position categories.",
        docs_url="/account/docs",
        redoc_url="/account/redoc",
        openapi_url="/account/openapi.json",
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
        raise ValueError("create_account_app requires config['server'] from read_config() merged YAML.")
    _cfg_holder["server"] = normalize_server_config(dict(_raw_server))
    reader._config["server"] = _cfg_holder["server"]

    account_port = int(_cfg_holder["server"]["trading_port"])
    app.state.bifrost_account_port = account_port
    app.state.bifrost_trading_port = account_port
    app.state.bifrost_portfolio_port = account_port

    from bifrost_api.ops.services.audit_store import AuditStore

    app.state.audit_store = AuditStore.from_config(_cfg_holder)

    from bifrost_api.trading.routers import executions_router
    from bifrost_api.portfolio.routers import portfolio_config_router, portfolio_model_router
    from bifrost_api.strategy.routers import strategies_router

    app.include_router(executions_router)
    app.include_router(portfolio_model_router)
    app.include_router(portfolio_config_router)
    # Phase B Wave B3: strategy CRUD absorbed into account-service
    app.include_router(strategies_router)

    @app.get("/health")
    def account_health() -> Any:
        out: Any = {"status": "ok", "service": "bifrost-account", "ts": time.time()}
        profile = getattr(app.state, "bifrost_config_profile", None)
        if profile is not None:
            out["config_profile"] = profile
        out["port"] = app.state.bifrost_account_port
        return out

    def _capabilities(request: Request) -> Dict[str, Any]:
        from bifrost_api.ops.auth import AuthConfig, OpsAuth

        cfg = merged_config or reader._config
        return OpsAuth(AuthConfig.from_config(cfg)).capabilities(request)

    @app.get("/account/auth/capabilities")
    def account_auth_capabilities(request: Request) -> Dict[str, Any]:
        return _capabilities(request)

    @app.get("/trading/auth/capabilities")
    def trading_auth_capabilities(request: Request) -> Dict[str, Any]:
        return _capabilities(request)

    @app.get("/portfolio/auth/capabilities")
    def portfolio_auth_capabilities(request: Request) -> Dict[str, Any]:
        return _capabilities(request)

    @app.get("/strategy/auth/capabilities")
    def strategy_auth_capabilities(request: Request) -> Dict[str, Any]:
        return _capabilities(request)

    def _schedule_shutdown(request: Request, *, action: str) -> Any:
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
                        action=action,
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
                    action=action,
                    target="process",
                    outcome="scheduled",
                    detail="process exit",
                ),
            )

        def _exit_after_send() -> None:
            time.sleep(SIDECAR_STOP_EXIT_DELAY_SEC)
            logger.info("Account API shutdown: exiting process.")
            os._exit(0)

        threading.Thread(target=_exit_after_send, daemon=True).start()
        return {"ok": True}

    @app.post("/account/shutdown")
    def post_account_shutdown(request: Request) -> Any:
        return _schedule_shutdown(request, action="account_shutdown")

    @app.post("/trading/shutdown")
    def post_trading_shutdown(request: Request) -> Any:
        return _schedule_shutdown(request, action="trading_shutdown")

    @app.post("/portfolio/shutdown")
    def post_portfolio_shutdown(request: Request) -> Any:
        return _schedule_shutdown(request, action="portfolio_shutdown")

    @app.post("/strategy/shutdown")
    def post_strategy_shutdown(request: Request) -> Any:
        return _schedule_shutdown(request, action="strategy_shutdown")

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

    instrument_app(app, "api-account")
    return app


def run_account_server(config: dict, resolved_config_path: Optional[str] = None) -> None:
    """Start the Account API server on trading_port (8769)."""
    import uvicorn

    has_postgres = bool(config.get("postgres") or os.environ.get("PGHOST"))
    status_cfg_for_read = config if has_postgres else None
    control_via_db = config if has_postgres else None

    port = int(config["server"]["trading_port"])

    reader = StatusReader(config)
    app = create_account_app(
        reader,
        control_via_db,
        status_cfg_for_read=status_cfg_for_read,
        resolved_config_path=resolved_config_path,
        merged_config=config,
    )
    host = "0.0.0.0"
    logger.info("Account API server on %s:%s", host, port)
    uvicorn.run(app, host=host, port=int(port), log_level="info", log_config=None)
