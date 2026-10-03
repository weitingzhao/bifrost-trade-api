"""Bifrost Docs API — merged OpenAPI (Main + Research)."""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from bifrost_api.common.service_endpoints import mount_auth_capabilities
from bifrost_api.docs_api.merge_openapi import fetch_openapi, merge_openapi_specs
from bifrost_core.config.profile import deployment_profile
from bifrost_core.config.startup import normalize_server_config
from bifrost_core.observability.prometheus import instrument_app

logger = logging.getLogger(__name__)

DOCS_PATH_PREFIX = "/research/docs"


def create_docs_app(
    main_openapi_url: str,
    research_openapi_url: str,
    *,
    extra_openapi_urls: Optional[Dict[str, str]] = None,
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

    _profile = deployment_profile(_cfg, resolved_config_path)


    _state: Dict[str, Any] = {
        "main_url": main_openapi_url,
        "research_url": research_openapi_url,
        # Secondary specs merged after the main one, by component prefix. Account and Market
        # were never in the aggregate, so no env had one document for the Trade API (TD-28).
        "secondaries": {"Research": research_openapi_url, **(extra_openapi_urls or {})},
    }

    _legacy_browser_prefix = os.environ.get("BIFROST_DOCS_ROOT_PATH", "").strip().rstrip("/")

    def _health_payload() -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "status": "ok",
            "service": "bifrost-docs",
            "ts": time.time(),
            "main_url": _state["main_url"],
            "research_url": _state["research_url"],
            "secondary_urls": dict(_state["secondaries"]),
        }
        # No "port": docs_port names a port no pod listens on; monitor serves these routes.
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

    mount_auth_capabilities(app, [f"{DOCS_PATH_PREFIX}/auth/capabilities"], lambda: _cfg)

    def _merged_openapi_response() -> JSONResponse:
        try:
            main_spec = fetch_openapi(_state["main_url"])
        except Exception as exc:
            logger.warning("Failed to fetch main OpenAPI from %s: %s", _state["main_url"], exc)
            return JSONResponse(
                status_code=502,
                content={"detail": f"Cannot reach main API: {exc}"},
            )
        merged = main_spec
        unreachable: Dict[str, str] = {}
        for prefix, url in _state["secondaries"].items():
            try:
                spec = fetch_openapi(url)
            except Exception as exc:
                logger.warning("Failed to fetch %s OpenAPI from %s: %s", prefix, url, exc)
                unreachable[prefix] = f"{url}: {exc}"
                continue
            merged = merge_openapi_specs(merged, spec, secondary_prefix=prefix)
        if unreachable:
            # Serve what answered and say what did not, rather than one 502 for the whole document.
            merged = dict(merged)
            merged["x-bifrost-unreachable"] = unreachable
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


def _sibling_openapi(service: str, port: int, path: str) -> str:
    """Another Trade API's OpenAPI URL as seen from inside the monitor process.

    In K3s each API is its own Deployment behind a Service of the same name in this
    namespace, so 127.0.0.1 only ever reached the monitor itself -- the research spec
    answered 502 in every env (TD-28). Outside Kubernetes (local runs) the APIs share
    a host, and 127.0.0.1 with the configured port is right.
    """
    host = service if os.environ.get("KUBERNETES_SERVICE_HOST") else "127.0.0.1"
    return f"http://{host}:{port}{path}"


def attach_docs_routes(
    host_app: FastAPI,
    *,
    config: dict,
    resolved_config_path: Optional[str] = None,
    main_openapi_url: Optional[str] = None,
    research_openapi_url: Optional[str] = None,
) -> None:
    """Mount docs OpenAPI aggregate routes onto an existing app (Phase B: monitor).

    Does not register root ``/health`` (host app already has one). Registers
    ``/research/docs/*`` and root ``/openapi.json`` / ``/docs`` / ``/redoc``.
    """
    srv = normalize_server_config(dict(config.get("server") or {}))
    docs_app = create_docs_app(
        main_openapi_url
        or os.environ.get("BIFROST_DOCS_MAIN_OPENAPI")
        or f"http://127.0.0.1:{int(srv['monitor_port'])}/openapi.json",
        research_openapi_url
        or os.environ.get("BIFROST_DOCS_RESEARCH_OPENAPI")
        or _sibling_openapi("api-research", int(srv["research_port"]), "/openapi.json"),
        extra_openapi_urls={
            "Account": os.environ.get("BIFROST_DOCS_ACCOUNT_OPENAPI")
            or _sibling_openapi("api-account", int(srv["account_port"]), "/account/openapi.json"),
            "Market": os.environ.get("BIFROST_DOCS_MARKET_OPENAPI")
            or _sibling_openapi("api-market", int(srv["market_port"]), "/market/openapi.json"),
        },
        config=config,
        resolved_config_path=resolved_config_path,
    )
    for route in docs_app.routes:
        path = getattr(route, "path", None)
        # Keep monitor's own OpenAPI UI; only absorb the /research/docs/* aggregate.
        if path in ("/health", "/openapi.json", "/docs", "/redoc"):
            continue
        host_app.router.routes.append(route)
