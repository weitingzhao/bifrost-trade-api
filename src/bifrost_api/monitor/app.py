"""Phase 2: FastAPI app for GET /status, GET /operations, POST /control/*.

When ``frontend/dist`` exists (``npm run build``), GET ``/`` serves the SPA and ``/assets`` is mounted; otherwise GET ``/`` returns a small API stub. Dev hot-reload: ``./scripts/run_frontend.sh dev``.

Monitoring runs on a separate host from the Strategy Trading Daemon (RE-5). Start of the daemon is only on the trading machine (run_engine.py); no subprocess/start on this server.
"""

import json
import logging
import os
import threading
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import asyncio
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from bifrost_core.config.startup import (
    config_profile_from_resolved_path,
    docs_api_console_stream_key,
    monitor_api_console_stream_key,
    ops_api_console_stream_key,
    portfolio_api_console_stream_key,
    trading_api_console_stream_key,
)
from bifrost_socket.ib.connector.flex_client import fetch_cash_transactions, fetch_trades
from bifrost_core.ib_operator.client import IbOperatorClient
from bifrost_core.monitor.reader import StatusReader
from bifrost_socket.ib.connector.flex_client import parse_trades_xml
from bifrost_core.monitor.self_check import derive_daemon_self_check

logger = logging.getLogger(__name__)


_UTILIZED_SERVICE_ORDER = (
    "server",
    "main",
    "api",
    "massive",
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
    """Parse ``utilized.services`` from YAML into [{"service": "massive", "env": "dev"}, ...].

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
    status_cfg_for_read: when set, read paths that use DB without control channel (e.g. only PGHOST or postgres configured without sink=postgres). Job queue APIs are on Ops: GET /ops/bars/jobs.
    """
    app = FastAPI(
        title="Bifrost Trader API",
        description="Phase 2: status and control API; monitoring UI when frontend/dist is built.",
    )
    # Browser fetch from Vite / another host to this API (e.g. Settings → API Health split probes).
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    # Strategy Trading Daemon console (run_engine.py → bifrost:console:{dev|prod}:daemon_trading + legacy); reader thread + queues
    app.state.daemon_log_queues: list = []
    app.state.daemon_log_lock = threading.Lock()
    app.state._daemon_log_thread: Optional[threading.Thread] = None
    app.state._daemon_log_loop: Optional[asyncio.AbstractEventLoop] = None

    # Monitor API console log stream (run_server.py → bifrost:console:{dev|prod}:api_monitor)
    app.state.monitor_log_queues: list = []
    app.state.monitor_log_lock = threading.Lock()
    app.state._monitor_log_thread: Optional[threading.Thread] = None
    app.state._monitor_log_loop: Optional[asyncio.AbstractEventLoop] = None

    # Massive API console log stream (run_server_massive.py → bifrost:massive_console)
    app.state.massive_log_queues: list = []
    app.state.massive_log_lock = threading.Lock()
    app.state._massive_log_thread: Optional[threading.Thread] = None
    app.state._massive_log_loop: Optional[asyncio.AbstractEventLoop] = None

    # Massive WS ingest log stream (scripts/systemd/run_massive_ws.py → bifrost:console:ws_massive_option)
    app.state.massive_ws_log_queues: list = []
    app.state.massive_ws_log_lock = threading.Lock()
    app.state._massive_ws_log_thread: Optional[threading.Thread] = None
    app.state._massive_ws_log_loop: Optional[asyncio.AbstractEventLoop] = None

    # IB Operator log stream (scripts/systemd/run_ib_operator.py → bifrost:console:ws_ib_operator)
    app.state.ib_operator_log_queues: list = []
    app.state.ib_operator_log_lock = threading.Lock()
    app.state._ib_operator_log_thread: Optional[threading.Thread] = None
    app.state._ib_operator_log_loop: Optional[asyncio.AbstractEventLoop] = None

    # IB ingestor log stream (scripts/systemd/run_ib_ingestor.py → bifrost:console:ws_ib_ingestor)
    app.state.ib_ingestor_log_queues: list = []
    app.state.ib_ingestor_log_lock = threading.Lock()
    app.state._ib_ingestor_log_thread: Optional[threading.Thread] = None
    app.state._ib_ingestor_log_loop: Optional[asyncio.AbstractEventLoop] = None

    # IB Account Agent log stream (scripts/systemd/run_ib_account_agent.py → bifrost:console:ws_ib_account_agent)
    app.state.ib_account_agent_log_queues: list = []
    app.state.ib_account_agent_log_lock = threading.Lock()
    app.state._ib_account_agent_log_thread: Optional[threading.Thread] = None
    app.state._ib_account_agent_log_loop: Optional[asyncio.AbstractEventLoop] = None

    # Account Sync Daemon console log stream
    app.state.account_sync_daemon_log_queues: list = []
    app.state.account_sync_daemon_log_lock = threading.Lock()
    app.state._account_sync_daemon_log_thread: Optional[threading.Thread] = None
    app.state._account_sync_daemon_log_loop: Optional[asyncio.AbstractEventLoop] = None

    # Docs API console log stream (run_server_docs.py → bifrost:console:{dev|prod}:api_docs)
    app.state.docs_log_queues: list = []
    app.state.docs_log_lock = threading.Lock()
    app.state._docs_log_thread: Optional[threading.Thread] = None
    app.state._docs_log_loop: Optional[asyncio.AbstractEventLoop] = None

    # Ops API console log stream (run_server_ops.py → bifrost:console:{dev|prod}:api_ops)
    app.state.ops_log_queues: list = []
    app.state.ops_log_lock = threading.Lock()
    app.state._ops_log_thread: Optional[threading.Thread] = None
    app.state._ops_log_loop: Optional[asyncio.AbstractEventLoop] = None

    # Trading API console log stream (run_server_trading.py → bifrost:console:{dev|prod}:api_trading)
    app.state.trading_log_queues: list = []
    app.state.trading_log_lock = threading.Lock()
    app.state._trading_log_thread: Optional[threading.Thread] = None
    app.state._trading_log_loop: Optional[asyncio.AbstractEventLoop] = None

    # Portfolio API console log stream (run_server_portfolio.py → bifrost:console:{dev|prod}:api_portfolio)
    app.state.portfolio_log_queues: list = []
    app.state.portfolio_log_lock = threading.Lock()
    app.state._portfolio_log_thread: Optional[threading.Thread] = None
    app.state._portfolio_log_loop: Optional[asyncio.AbstractEventLoop] = None

    # Research API console log stream (run_server_research.py → bifrost:console:{dev|prod}:api_research)
    app.state.research_log_queues: list = []
    app.state.research_log_lock = threading.Lock()
    app.state._research_log_thread: Optional[threading.Thread] = None
    app.state._research_log_loop: Optional[asyncio.AbstractEventLoop] = None

    # Strategy API console log stream (run_server_strategy.py → bifrost:console:{dev|prod}:api_strategy)
    app.state.strategy_log_queues: list = []
    app.state.strategy_log_lock = threading.Lock()
    app.state._strategy_log_thread: Optional[threading.Thread] = None
    app.state._strategy_log_loop: Optional[asyncio.AbstractEventLoop] = None

    # Market API console log stream (run_server_market.py → bifrost:console:{dev|prod}:api_market)
    app.state.market_log_queues: list = []
    app.state.market_log_lock = threading.Lock()
    app.state._market_log_thread: Optional[threading.Thread] = None
    app.state._market_log_loop: Optional[asyncio.AbstractEventLoop] = None

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
    app.state.bifrost_config_profile = (
        config_profile_from_resolved_path(resolved_config_path)
        if resolved_config_path
        else None
    )
    app.state.ops_log_stream_key = ops_api_console_stream_key(app.state.bifrost_config_profile)
    app.state.monitor_log_stream_key = monitor_api_console_stream_key(app.state.bifrost_config_profile)
    app.state.docs_log_stream_key = docs_api_console_stream_key(app.state.bifrost_config_profile)
    app.state.trading_log_stream_key = trading_api_console_stream_key(app.state.bifrost_config_profile)
    app.state.portfolio_log_stream_key = portfolio_api_console_stream_key(app.state.bifrost_config_profile)
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
    app.state.bifrost_massive_port = int(_scfg["massive_port"])
    app.state.bifrost_docs_port = int(_scfg["docs_port"])
    app.state.bifrost_ops_port = int(_scfg["ops_port"])
    app.state.bifrost_trading_port = int(_scfg["trading_port"])
    app.state.bifrost_strategy_port = int(_scfg["strategy_port"])
    app.state.bifrost_portfolio_port = int(_scfg["portfolio_port"])
    app.state.bifrost_market_port = int(_scfg["market_port"])
    app.state.bifrost_research_port = int(_scfg["research_port"])

    app.state.bifrost_utilized_services = _utilized_services_from_config(merged_config)
    app.state.bifrost_merged_config = merged_config or {}
    from bifrost_api.ops.services.audit_store import AuditStore

    app.state.audit_store = AuditStore.from_config(merged_config or {})

    from bifrost_api.monitor.routers import (
        config_router,
        core_router,
        daemon_router,
        logs_router,
        messages_router,
        status_router,
    )
    from bifrost_api.portfolio.routers.config import router as portfolio_position_categories_router

    app.include_router(core_router)
    app.include_router(logs_router)
    app.include_router(messages_router)
    app.include_router(status_router)
    app.include_router(daemon_router)
    app.include_router(config_router)
    # Same StatusReader + DB as Portfolio API: dev often runs Monitor only (run_server.py); Vite proxies here.
    app.include_router(portfolio_position_categories_router)

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
        cfg = merged_config or reader._config
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

    return app


def run_server(config: dict, resolved_config_path: Optional[str] = None) -> None:
    """Start the status server (host 0.0.0.0, port from config). Control channel: PostgreSQL daemon_control + daemon_run_status (RE-5). No start: daemon is started on trading host only."""
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
        "Status server on %s:%s (control=daemon_control + daemon_run_status; start only on trading host)",
        host,
        port,
    )
    uvicorn.run(app, host=host, port=int(port), log_level="info", log_config=None)
