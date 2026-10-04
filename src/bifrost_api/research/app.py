"""Research domain FastAPI app — option discovery, screener, greeks, data readiness."""

import logging
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from bifrost_core.config.profile import deployment_profile
from bifrost_core.config.yaml_config import normalize_server_config
from bifrost_core.monitor.reader.common import StatusReader
from bifrost_core.observability.prometheus import instrument_app
from bifrost_api.common.build_info import core_build_info
from bifrost_api.common.service_endpoints import mount_auth_capabilities
from bifrost_api.deprecations import install_deprecations
from bifrost_api.write_guard import install_write_guard

logger = logging.getLogger(__name__)


def create_research_app(
    reader: StatusReader,
    control_via_db: Optional[dict],
    status_cfg_for_read: Optional[dict] = None,
    resolved_config_path: Optional[str] = None,
    merged_config: Optional[dict] = None,
) -> FastAPI:
    """Build the Research API app (option discovery, screener, greeks, data readiness)."""
    app = FastAPI(
        title="Bifrost Research API",
        description="Option discovery (IB-backed), screener, greeks, and SEPA data readiness.",
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )
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
        raise ValueError("create_research_app requires config['server'] from read_config() merged YAML.")
    _cfg_holder["server"] = normalize_server_config(dict(_raw_server))
    reader.config["server"] = _cfg_holder["server"]
    _scfg = _cfg_holder["server"]
    app.state.bifrost_research_port = int(_scfg["research_port"])

    from bifrost_api.research.routers.option_discovery import router as option_discovery_router
    from bifrost_api.research.routers.screener import router as screener_router
    from bifrost_api.research.routers.greeks import router as greeks_router
    from bifrost_api.research.routers.data_readiness import router as data_readiness_router
    from bifrost_api.research.routers.feedback import router as feedback_router

    app.include_router(option_discovery_router)
    app.include_router(screener_router)
    app.include_router(greeks_router)
    app.include_router(data_readiness_router)
    app.include_router(feedback_router)


    def _health_payload() -> Dict[str, Any]:
        import time
        out: Dict[str, Any] = {"status": "ok", "service": "bifrost-research", "ts": time.time()}
        profile = getattr(app.state, "bifrost_config_profile", None)
        if profile is not None:
            out["config_profile"] = profile
        out["port"] = int(app.state.bifrost_research_port)
        if resolved_config_path:
            out["config_path"] = str(Path(resolved_config_path).resolve())
        out.update(core_build_info())
        return out

    @app.get("/health")
    def research_health() -> Dict[str, Any]:
        return _health_payload()

    mount_auth_capabilities(app, ["/auth/capabilities"], lambda: merged_config or reader.config)

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

    instrument_app(app, "api-research")
    return app


def run_research_server(config: dict, resolved_config_path: Optional[str] = None) -> None:
    """Start the Research API server."""
    import os
    import uvicorn

    has_postgres = bool(config.get("postgres") or os.environ.get("PGHOST"))
    status_cfg_for_read = config if has_postgres else None
    control_via_db = config if has_postgres else None

    port = int(config["server"]["research_port"])

    reader = StatusReader(config)
    app = create_research_app(
        reader,
        control_via_db,
        status_cfg_for_read=status_cfg_for_read,
        resolved_config_path=resolved_config_path,
        merged_config=config,
    )
    host = "0.0.0.0"
    logger.info("Research API server on %s:%s", host, port)
    uvicorn.run(app, host=host, port=int(port), log_level="info", log_config=None)
