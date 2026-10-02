"""Core endpoints: root and health."""

import logging
import time
from pathlib import Path
from typing import Any, Dict, Optional, Union

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse

logger = logging.getLogger(__name__)

router = APIRouter(tags=["core"])


def _project_root() -> Path:
    # backend/monitor/routers/core.py -> repo root (not backend/)
    return Path(__file__).resolve().parent.parent.parent.parent


def _frontend_dist() -> Path:
    return _project_root() / "frontend" / "dist"


_FALLBACK_INDEX_HTML = """<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><title>Bifrost Trader API</title></head>
<body style="font-family:system-ui;padding:1rem;">
  <p><strong>Bifrost Trader API</strong> — No production build found. Run <code>cd frontend && npm run build</code>, or use dev UI: <code>./scripts/run_frontend.sh dev</code>.</p>
  <p><a href="/docs">/docs</a> · <a href="/status">/status</a> (JSON schema v8, nested) · <a href="/operations">/operations</a></p>
</body></html>"""


@router.get("/", response_model=None)
def get_root() -> Union[FileResponse, HTMLResponse]:
    """Serve SPA from ``frontend/dist`` when present (same port as API after ``npm run build``); else stub."""
    index = _frontend_dist() / "index.html"
    if index.is_file():
        return FileResponse(index, media_type="text/html")
    return HTMLResponse(_FALLBACK_INDEX_HTML)


def _favicon_file_for_profile(profile: Optional[str]) -> str:
    if profile == "dev":
        return "favicon-dev.svg"
    if profile == "prod":
        return "favicon-prod.svg"
    return "favicon.svg"


@router.get("/favicon.svg", response_model=None)
def get_favicon(request: Request) -> FileResponse:
    """Same path as SPA; file picked from env profile (config.dev.yaml / config.prod.yaml) when known."""
    dist = _frontend_dist()
    profile = getattr(request.app.state, "bifrost_config_profile", None)
    name = _favicon_file_for_profile(profile)
    p = dist / name
    if not p.is_file():
        p = dist / "favicon.svg"
    if not p.is_file():
        raise HTTPException(status_code=404, detail="favicon not found (run npm run build)")
    return FileResponse(p, media_type="image/svg+xml")


@router.get("/health")
def get_health(request: Request) -> Dict[str, Any]:
    """Health check: 200 when process is alive; returns server timestamp (Unix s) for client to compute time since last check."""
    out: Dict[str, Any] = {"status": "ok", "service": "bifrost-monitor", "ts": time.time()}
    profile = getattr(request.app.state, "bifrost_config_profile", None)
    if profile is not None:
        out["config_profile"] = profile
    fe_pub = getattr(request.app.state, "bifrost_frontend_public_origin", None)
    if fe_pub:
        out["frontend_public_origin"] = fe_pub
    fe_dev = getattr(request.app.state, "bifrost_frontend_dev_path", None)
    if fe_dev:
        out["frontend_dev_path"] = fe_dev
    fe_prod = getattr(request.app.state, "bifrost_frontend_prod_path", None)
    if fe_prod:
        out["frontend_prod_path"] = fe_prod
    # Ports a process listens on. docs / ops are served by this process and
    # strategy / portfolio by account, so their config ports are not reported (TD-64).
    out["monitor_port"] = int(request.app.state.bifrost_server_listen_port)
    out["trading_port"] = int(request.app.state.bifrost_trading_port)
    out["market_port"] = int(request.app.state.bifrost_market_port)
    out["research_port"] = int(request.app.state.bifrost_research_port)
    out["utilized_services"] = list(getattr(request.app.state, "bifrost_utilized_services", []) or [])
    return out
