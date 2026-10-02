"""Market domain FastAPI app — bars, quotes, watchlist, market calendar."""

import asyncio
import logging
import threading
from typing import Any, Dict, Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from bifrost_core.config.profile import deployment_profile
from bifrost_core.config.startup import normalize_server_config
from bifrost_core.monitor.reader import StatusReader
from bifrost_core.observability.prometheus import instrument_app
from bifrost_core.sse.queue_utils import put_nowait_drop_oldest
from bifrost_api.common.service_endpoints import mount_auth_capabilities
from bifrost_api.common.request_bodies import install_request_field_log
from bifrost_api.common.write_errors import install_write_errors
from bifrost_api.deprecations import install_deprecations
from bifrost_api.write_guard import install_write_guard

logger = logging.getLogger(__name__)

try:
    from bifrost_core.core.realtime import (
        create_reader_from_config as create_redis_quotes,
        run_subscribe_loop as redis_run_subscribe_loop,
    )
except ImportError:
    create_redis_quotes = None
    redis_run_subscribe_loop = None


def create_market_app(
    reader: StatusReader,
    control_via_db: Optional[dict],
    redis_quotes: Optional[Any] = None,
    status_cfg_for_read: Optional[dict] = None,
    resolved_config_path: Optional[str] = None,
    merged_config: Optional[dict] = None,
) -> FastAPI:
    """Build the Market domain FastAPI app (bars, quotes, watchlist)."""
    app = FastAPI(
        title="Bifrost Market API",
        description="Market data, quotes, and watchlist endpoints.",
        docs_url="/market/docs",
        redoc_url="/market/redoc",
        openapi_url="/market/openapi.json",
    )
    # Every write needs a role (debt TD-23). Added before CORS so CORS stays the
    # outer layer and a refusal still carries its headers.
    install_write_guard(app, lambda: merged_config or reader._config)
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
    # Watchlist writes answer their real status with core's reason (TD-15).
    install_write_errors(app)

    app.state.reader = reader
    app.state.control_via_db = control_via_db
    app.state.status_cfg_for_read = status_cfg_for_read
    app.state.monitor_enabled = True
    app.state.redis_quotes = redis_quotes
    app.state.ib_operator_client = None

    # SSE for live quotes
    app.state.sse_queues: list = []
    app.state.sse_lock = threading.Lock()
    app.state._sse_loop: Optional[asyncio.AbstractEventLoop] = None
    app.state._redis_subscriber_stop = threading.Event()
    app.state._redis_subscriber_thread: Optional[threading.Thread] = None

    # One answer to "which env" for every app (debt TD-52): control_profile, then env, then file name.
    app.state.bifrost_config_profile = deployment_profile(merged_config or reader._config, resolved_config_path)

    _cfg_holder = merged_config or reader._config
    _raw_server = _cfg_holder.get("server")
    if not isinstance(_raw_server, dict):
        raise ValueError("create_market_app requires config['server'] from read_config() merged YAML.")
    _cfg_holder["server"] = normalize_server_config(dict(_raw_server))
    reader._config["server"] = _cfg_holder["server"]
    app.state.bifrost_market_port = int(_cfg_holder["server"]["market_port"])

    from bifrost_api.market.routers.market_data import router as market_data_router
    from bifrost_api.market.routers.quotes import router as quotes_router
    from bifrost_api.market.routers.watchlist import router as watchlist_router

    app.include_router(market_data_router)
    app.include_router(quotes_router)
    app.include_router(watchlist_router)


    @app.get("/health")
    def market_health() -> Any:
        import time
        out: Any = {"status": "ok", "service": "bifrost-market", "ts": time.time()}
        profile = getattr(app.state, "bifrost_config_profile", None)
        if profile is not None:
            out["config_profile"] = profile
        out["port"] = app.state.bifrost_market_port
        return out

    mount_auth_capabilities(app, ["/market/auth/capabilities"], lambda: merged_config or reader._config)

    @app.on_event("startup")
    async def startup_event() -> None:
        app.state._sse_loop = asyncio.get_running_loop()

        from bifrost_core.ib_operator.client import IbOperatorClient

        cfg = merged_config or reader._config
        app.state.ib_operator_client = IbOperatorClient.from_merged_config(cfg)

        # Redis quotes subscriber
        rq = getattr(app.state, "redis_quotes", None)
        if (
            redis_run_subscribe_loop
            and rq is not None
            and getattr(rq, "available", False)
        ):
            def _broadcast_quote(quote: Dict[str, Any]) -> None:
                loop = getattr(app.state, "_sse_loop", None)
                if loop is None:
                    return
                with app.state.sse_lock:
                    queues = list(app.state.sse_queues)
                for q in queues:
                    loop.call_soon_threadsafe(put_nowait_drop_oldest, q, quote)

            app.state._redis_subscriber_stop.clear()
            app.state._redis_subscriber_thread = threading.Thread(
                target=redis_run_subscribe_loop,
                args=(rq, _broadcast_quote, app.state._redis_subscriber_stop),
                daemon=True,
                name="redis-quotes-subscriber",
            )
            app.state._redis_subscriber_thread.start()
            logger.info("Redis quotes SSE subscriber thread started")

    @app.on_event("shutdown")
    async def shutdown_event() -> None:
        app.state._redis_subscriber_stop.set()
        if getattr(app.state, "_redis_subscriber_thread", None) is not None:
            app.state._redis_subscriber_thread.join(timeout=2.0)
            app.state._redis_subscriber_thread = None
        op = getattr(app.state, "ib_operator_client", None)
        if op is not None:
            try:
                op.close()
            except Exception:
                pass
        rq = getattr(app.state, "redis_quotes", None)
        if rq is not None and getattr(rq, "close", None):
            try:
                rq.close()
            except Exception:
                pass

    instrument_app(app, "api-market")
    return app


def run_market_server(config: dict, resolved_config_path: Optional[str] = None) -> None:
    """Start the Market API server."""
    import os
    import uvicorn

    has_postgres = bool(config.get("postgres") or os.environ.get("PGHOST"))
    status_cfg_for_read = config if has_postgres else None
    control_via_db = config if has_postgres else None

    port = int(config["server"]["market_port"])

    reader = StatusReader(config)
    redis_quotes = None
    if create_redis_quotes:
        redis_quotes = create_redis_quotes(config)
    app = create_market_app(
        reader,
        control_via_db,
        redis_quotes=redis_quotes,
        status_cfg_for_read=status_cfg_for_read,
        resolved_config_path=resolved_config_path,
        merged_config=config,
    )
    host = "0.0.0.0"
    logger.info("Market API server on %s:%s", host, port)
    uvicorn.run(app, host=host, port=int(port), log_level="info", log_config=None)
