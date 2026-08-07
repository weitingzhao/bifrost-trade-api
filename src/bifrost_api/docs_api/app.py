"""Bifrost Docs API — merged OpenAPI (Main + Research)."""

from __future__ import annotations

import logging
import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from bifrost_api.docs_api.merge_openapi import fetch_openapi, merge_openapi_specs
from bifrost_core.config.startup import (
    config_profile_from_resolved_path,
    normalize_server_config,
)
from bifrost_core.observability.prometheus import instrument_app

logger = logging.getLogger(__name__)

DOCS_STOP_EXIT_DELAY_SEC = 2.5
DOCS_PATH_PREFIX = "/research/docs"


def create_docs_app(
    main_openapi_url: str,
    research_openapi_url: str,
    *,
    config: Optional[dict] = None,
    resolved_config_path: Optional[str] = None,
) -> FastAPI:
    """Build the docs-only FastAPI: merged OpenAPI + Swagger/ReDoc.

    Canonical paths::

        /research/docs/openapi.json
        /research/docs/docs
        /research/docs/redoc
        /research/docs/health
    """

    app = FastAPI(
        title="Bifrost Docs API",
        description="Aggregated OpenAPI documentation for Main and Research services.",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    _cfg: Dict[str, Any] = dict(config) if config else {}
    if not isinstance(_cfg.get("server"), dict):
        raise ValueError("create_docs_app requires config['server'] from merged YAML (read_config).")
    _cfg["server"] = normalize_server_config(_cfg["server"])

    _profile = config_profile_from_resolved_path(resolved_config_path) if resolved_config_path else None

    from bifrost_api.ops.services.audit_store import AuditStore

    app.state.audit_store = AuditStore.from_config(_cfg)

    _state: Dict[str, Any] = {
        "main_url": main_openapi_url,
        "research_url": research_openapi_url,
    }

    _legacy_browser_prefix = os.environ.get("BIFROST_DOCS_ROOT_PATH", "").strip().rstrip("/")

    def _health_payload() -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "status": "ok",
            "service": "bifrost-docs",
            "ts": time.time(),
            "main_url": _state["main_url"],
            "research_url": _state["research_url"],
        }
        srv = _cfg["server"]
        out["port"] = int(srv["docs_port"])
        if _profile is not None:
            out["config_profile"] = _profile
        if resolved_config_path:
            out["config_path"] = str(Path(resolved_config_path).resolve())
        return out

    @app.get("/health")
    def docs_health_root() -> Dict[str, Any]:
        return _health_payload()

    @app.get(f"{DOCS_PATH_PREFIX}/health")
    def docs_health_prefixed() -> Dict[str, Any]:
        return _health_payload()

    @app.get(f"{DOCS_PATH_PREFIX}/auth/capabilities")
    def docs_auth_capabilities(request: Request) -> Dict[str, Any]:
        """Same shape as GET /ops/auth/capabilities (shared ops.auth tokens)."""
        from bifrost_api.ops.auth import AuthConfig, OpsAuth

        return OpsAuth(AuthConfig.from_config(_cfg)).capabilities(request)

    @app.post(f"{DOCS_PATH_PREFIX}/shutdown")
    def post_docs_shutdown(request: Request) -> Any:
        """Terminate the Docs API process. Requires operator role (same tokens as Ops API)."""
        from bifrost_api.ops.auth import AuthConfig, OpsAuth
        from bifrost_api.ops.models.schemas import AuditEntry

        ops_auth = OpsAuth(AuthConfig.from_config(_cfg))
        ident, denied = ops_auth.require_role(request, "operator")
        audit_store = getattr(app.state, "audit_store", None)
        if denied:
            if audit_store is not None:
                audit_store.append(
                    AuditEntry(
                        operator=ident.name,
                        source_ip=request.client.host if request.client else None,
                        action="docs_shutdown",
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
                    action="docs_shutdown",
                    target="process",
                    outcome="scheduled",
                    detail="process exit",
                ),
            )

        def _exit_after_send() -> None:
            time.sleep(DOCS_STOP_EXIT_DELAY_SEC)
            logger.info("Docs API shutdown: exiting process.")
            os._exit(0)

        threading.Thread(target=_exit_after_send, daemon=True).start()
        return {"ok": True}

    def _merged_openapi_response() -> JSONResponse:
        try:
            main_spec = fetch_openapi(_state["main_url"])
        except Exception as exc:
            logger.warning("Failed to fetch main OpenAPI from %s: %s", _state["main_url"], exc)
            return JSONResponse(
                status_code=502,
                content={"detail": f"Cannot reach main API: {exc}"},
            )
        try:
            research_spec = fetch_openapi(_state["research_url"])
        except Exception as exc:
            logger.warning("Failed to fetch Research OpenAPI from %s: %s", _state["research_url"], exc)
            return JSONResponse(
                status_code=502,
                content={"detail": f"Cannot reach Research API: {exc}"},
            )
        merged = merge_openapi_specs(main_spec, research_spec, secondary_prefix="Research")
        return JSONResponse(content=merged)

    from fastapi.openapi.docs import get_swagger_ui_html, get_redoc_html

    def _swagger(openapi_url: str) -> Any:
        return get_swagger_ui_html(
            openapi_url=openapi_url,
            title="Bifrost API (merged) — Swagger UI",
        )

    def _redoc(openapi_url: str) -> Any:
        return get_redoc_html(
            openapi_url=openapi_url,
            title="Bifrost API (merged) — ReDoc",
        )

    @app.get(f"{DOCS_PATH_PREFIX}/openapi.json", include_in_schema=False)
    def merged_openapi_prefixed() -> JSONResponse:
        return _merged_openapi_response()

    @app.get(f"{DOCS_PATH_PREFIX}/docs", include_in_schema=False)
    def swagger_ui_prefixed() -> Any:
        return _swagger(f"{DOCS_PATH_PREFIX}/openapi.json")

    @app.get(f"{DOCS_PATH_PREFIX}/redoc", include_in_schema=False)
    def redoc_prefixed() -> Any:
        return _redoc(f"{DOCS_PATH_PREFIX}/openapi.json")

    _root_openapi_browser = f"{_legacy_browser_prefix}/openapi.json" if _legacy_browser_prefix else "/openapi.json"

    @app.get("/openapi.json", include_in_schema=False)
    def merged_openapi_root() -> JSONResponse:
        return _merged_openapi_response()

    @app.get("/docs", include_in_schema=False)
    def swagger_ui_root() -> Any:
        return _swagger(_root_openapi_browser)

    @app.get("/redoc", include_in_schema=False)
    def redoc_root() -> Any:
        return _redoc(_root_openapi_browser)

    instrument_app(app, "api-docs")
    return app


def run_docs_server(
    config: dict,
    resolved_config_path: Optional[str] = None,
    *,
    main_openapi_url: Optional[str] = None,
    research_openapi_url: Optional[str] = None,
) -> None:
    """Start the Docs API server (merged OpenAPI: Main + Research)."""
    import uvicorn

    server_raw = config.get("server")
    if not isinstance(server_raw, dict):
        raise ValueError("run_docs_server requires config['server'] from merged YAML (read_config).")
    server_cfg = normalize_server_config(server_raw)
    main_port = int(server_cfg["monitor_port"])
    research_port = int(server_cfg["research_port"])
    docs_port = int(server_cfg["docs_port"])

    if main_openapi_url is None:
        main_openapi_url = os.environ.get(
            "BIFROST_DOCS_MAIN_OPENAPI",
            f"http://127.0.0.1:{main_port}/openapi.json",
        )
    if research_openapi_url is None:
        research_openapi_url = os.environ.get(
            "BIFROST_DOCS_RESEARCH_OPENAPI",
            f"http://127.0.0.1:{research_port}/openapi.json",
        )

    app = create_docs_app(
        main_openapi_url,
        research_openapi_url,
        config=config,
        resolved_config_path=resolved_config_path,
    )
    host = "0.0.0.0"
    logger.info(
        "Docs API server on %s:%s  (main=%s, research=%s)",
        host,
        docs_port,
        main_openapi_url,
        research_openapi_url,
    )
    uvicorn.run(app, host=host, port=docs_port, log_level="info", log_config=None)
