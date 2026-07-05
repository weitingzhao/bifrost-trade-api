"""Bifrost Massive API — independent FastAPI for /research/massive/* endpoints.

No IB client dependencies. Connects to the same PostgreSQL and Redis as the
main Status server.  Hosts the Massive SSE Redis subscriber thread and all
Massive REST/SSE routes previously embedded in the main app.
"""

import asyncio
import logging
import threading
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from bifrost_core.config.startup import config_profile_from_resolved_path, normalize_server_config
from bifrost_core.monitor.reader import StatusReader
from bifrost_core.monitor.redis_url import redis_url_from_config
from bifrost_core.observability.prometheus import instrument_app
from bifrost_core.sse.queue_utils import put_nowait_drop_oldest
from bifrost_api.massive.sse import run_massive_channel_subscribe_loop

logger = logging.getLogger(__name__)


def create_massive_app(
    reader: StatusReader,
    control_via_db: Optional[dict],
    status_cfg_for_read: Optional[dict] = None,
    resolved_config_path: Optional[str] = None,
) -> FastAPI:
    """Build the Massive-only FastAPI app (no IB, no quotes SSE)."""

    app = FastAPI(
        title="Bifrost Massive API",
        description="Massive / Polygon option research endpoints, jobs, and SSE stream.",
        docs_url="/research/massive/docs",
        redoc_url="/research/massive/redoc",
        openapi_url="/research/massive/openapi.json",
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Shared deps expected by routers via request.app.state
    app.state.reader = reader
    app.state.control_via_db = control_via_db
    app.state.status_cfg_for_read = status_cfg_for_read
    app.state.bifrost_config_profile = (
        config_profile_from_resolved_path(resolved_config_path) if resolved_config_path else None
    )

    # Massive SSE fan-out state
    app.state.massive_sse_queues: list = []
    app.state.massive_sse_lock = threading.Lock()
    app.state._massive_sse_subscriber_stop = threading.Event()
    app.state._massive_sse_subscriber_thread: Optional[threading.Thread] = None
    app.state._sse_loop: Optional[asyncio.AbstractEventLoop] = None

    _raw_server = reader._config.get("server")
    if not isinstance(_raw_server, dict):
        raise ValueError("create_massive_app requires reader._config['server'] from read_config() merged YAML.")
    reader._config["server"] = normalize_server_config(dict(_raw_server))

    # Routers
    from bifrost_api.massive.routers.stream import router as massive_stream_router
    from bifrost_api.massive.routers.routes import router as massive_routes_router

    app.include_router(massive_stream_router)
    app.include_router(massive_routes_router)

    def _health_payload() -> Dict[str, Any]:
        import time
        out: Dict[str, Any] = {"status": "ok", "service": "bifrost-massive", "ts": time.time()}
        profile = getattr(app.state, "bifrost_config_profile", None)
        if profile is not None:
            out["config_profile"] = profile
        _srv = reader._config["server"]
        out["port"] = int(_srv["massive_port"])
        if resolved_config_path:
            out["config_path"] = str(Path(resolved_config_path).resolve())
        return out

    @app.get("/health")
    def massive_health_root() -> Dict[str, Any]:
        return _health_payload()

    @app.get("/research/massive/health")
    def massive_health_prefixed() -> Dict[str, Any]:
        return _health_payload()

    @app.on_event("startup")
    async def startup_event() -> None:
        app.state._sse_loop = asyncio.get_running_loop()

        try:
            _massive_url = redis_url_from_config(reader._config)
            if _massive_url:

                def _broadcast_massive(evt: Dict[str, Any]) -> None:
                    loop = getattr(app.state, "_sse_loop", None)
                    if loop is None:
                        return
                    with app.state.massive_sse_lock:
                        queues = list(app.state.massive_sse_queues)
                    for q in queues:
                        loop.call_soon_threadsafe(put_nowait_drop_oldest, q, evt)

                app.state._massive_sse_subscriber_stop.clear()
                app.state._massive_sse_subscriber_thread = threading.Thread(
                    target=run_massive_channel_subscribe_loop,
                    args=(
                        _massive_url,
                        app.state._massive_sse_subscriber_stop,
                        _broadcast_massive,
                    ),
                    daemon=True,
                    name="massive-channel-sse-subscriber",
                )
                app.state._massive_sse_subscriber_thread.start()
                logger.info("Massive SSE Redis subscriber thread started")
        except Exception as exc:
            logger.warning("Massive SSE subscriber not started: %s", exc)

    @app.on_event("shutdown")
    async def shutdown_event() -> None:
        app.state._massive_sse_subscriber_stop.set()
        if getattr(app.state, "_massive_sse_subscriber_thread", None) is not None:
            app.state._massive_sse_subscriber_thread.join(timeout=2.0)
            app.state._massive_sse_subscriber_thread = None

    instrument_app(app, "api-massive")
    return app


def run_massive_server(config: dict, resolved_config_path: Optional[str] = None) -> None:
    """Start the Massive API server."""
    import os
    import uvicorn

    has_postgres = bool(config.get("postgres") or os.environ.get("PGHOST"))
    status_cfg_for_read = config if has_postgres else None
    control_via_db = config if has_postgres else None

    port = int(config["server"]["massive_port"])

    reader = StatusReader(config)
    app = create_massive_app(
        reader,
        control_via_db,
        status_cfg_for_read=status_cfg_for_read,
        resolved_config_path=resolved_config_path,
    )
    host = "0.0.0.0"
    logger.info("Massive API server on %s:%s", host, port)
    uvicorn.run(app, host=host, port=int(port), log_level="info", log_config=None)
