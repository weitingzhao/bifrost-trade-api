"""Account domain FastAPI app — merged trading + portfolio (same HTTP paths).

Phase B Wave B2: single process on account_port (8769; legacy name trading_port) serving:
  - /executions*, /performance, /transactions* (trading)
  - /portfolio/*, /position-categories* (portfolio)
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Optional

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from bifrost_core.config.profile import deployment_profile
from bifrost_core.config.yaml_config import normalize_server_config
from bifrost_core.monitor.reader.common import StatusReader
from bifrost_core.monitor.reader.errors import ReadFailed
from bifrost_core.observability.prometheus import instrument_app
from bifrost_api.common.build_info import core_build_info
from bifrost_api.common.service_endpoints import mount_auth_capabilities
from bifrost_api.common.request_bodies import install_request_field_log
from bifrost_api.common.write_errors import install_write_errors
from bifrost_api.deprecations import install_deprecations
from bifrost_api.write_guard import install_write_guard

logger = logging.getLogger(__name__)


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
    # Every write needs a role (debt TD-23). Added before CORS so CORS stays the
    # outer layer and a refusal still carries its headers.
    install_write_guard(app, lambda: merged_config or reader.config)
    # Routes no repo calls carry Deprecation: true and log their callers (debt TD-40).
    install_deprecations(app)
    # POST / PUT bodies log the unknown fields they ignore, with the route (TD-24).
    install_request_field_log(app)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # A reader that could not read answers 503 with the reason, never 200 with an empty
    # list: the Desk and Rules used to show "no rules" over a full book during a DB
    # hiccup (TD-08). Error contract B: the success envelope is unchanged.
    @app.exception_handler(ReadFailed)
    async def _read_failed(_request: Request, exc: ReadFailed) -> JSONResponse:
        return JSONResponse(status_code=503, content={"detail": str(exc), "reason": "read_failed"})

    # A write that did not happen answers its real status with core's reason:
    # 404 / 409 / 400 / 503 / 500 (TD-15; bifrost_api.common.write_errors).
    install_write_errors(app)

    app.state.reader = reader
    app.state.control_via_db = control_via_db
    app.state.status_cfg_for_read = status_cfg_for_read
    app.state.monitor_enabled = True
    app.state.ib_operator_client = None
    # One answer to "which env" for every app (debt TD-52): control_profile, then env, then file name.
    app.state.bifrost_config_profile = deployment_profile(merged_config or reader.config, resolved_config_path)

    _cfg_holder = merged_config or reader.config
    _raw_server = _cfg_holder.get("server")
    if not isinstance(_raw_server, dict):
        raise ValueError("create_account_app requires config['server'] from read_config() merged YAML.")
    _cfg_holder["server"] = normalize_server_config(dict(_raw_server))
    reader.config["server"] = _cfg_holder["server"]

    account_port = int(_cfg_holder["server"]["account_port"])
    app.state.bifrost_account_port = account_port


    from bifrost_api.trading.routers import executions_router
    from bifrost_api.portfolio.routers import (
        portfolio_config_router,
        portfolio_model_router,
        portfolio_short_legs_router,
    )
    from bifrost_api.strategy.routers import (
        gate_sets_router,
        plans_router,
        preferences_router,
        reviews_router,
        saved_searches_router,
        strategies_router,
        trades_router,
    )

    app.include_router(executions_router)
    # Phase B merged the trading, strategy and portfolio domains in here: this app
    # serves /api/{trading,strategy,portfolio} in every environment, and the
    # per-domain factories that never ran are gone (TD-29). Every router module in
    # those packages must be mounted below --
    # tests/contract/test_account_serves_every_domain_router.py checks each one.
    app.include_router(portfolio_model_router)
    app.include_router(portfolio_config_router)
    app.include_router(portfolio_short_legs_router)
    # Phase B Wave B3: strategy CRUD absorbed into account-service
    app.include_router(strategies_router)
    app.include_router(plans_router)
    app.include_router(saved_searches_router)
    app.include_router(reviews_router)
    # naming R1 (api 0.7.0, D5-A): the trade, its reviews, gate sets and saved searches under
    # their own names; the /strategies/... routes above answer the same until R4.
    app.include_router(trades_router)
    app.include_router(gate_sets_router)
    app.include_router(preferences_router)

    @app.get("/health")
    def account_health() -> Any:
        out: Any = {"status": "ok", "service": "bifrost-account", "ts": time.time()}
        profile = getattr(app.state, "bifrost_config_profile", None)
        if profile is not None:
            out["config_profile"] = profile
        out["port"] = app.state.bifrost_account_port
        out.update(core_build_info())
        return out

    mount_auth_capabilities(
        app,
        [f"/{d}/auth/capabilities" for d in ("account", "trading", "portfolio", "strategy")],
        lambda: merged_config or reader.config,
    )

    @app.on_event("startup")
    async def startup_event() -> None:
        from bifrost_core.ib_operator.client import IbOperatorClient

        cfg = merged_config or reader.config
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
    """Start the Account API server on account_port (8769)."""
    import uvicorn

    has_postgres = bool(config.get("postgres") or os.environ.get("PGHOST"))
    status_cfg_for_read = config if has_postgres else None
    control_via_db = config if has_postgres else None

    port = int(config["server"]["account_port"])

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
