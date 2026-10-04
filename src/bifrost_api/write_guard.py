"""Every write on a deployed Trade API app needs a role (debt TD-23).

Before this, 11 routes checked a role (process exits and ops actions) and about
100 write routes checked nothing: daemon control, risk gates, executions, bar
deletes, ingest enqueues. The role model was advertised by the capabilities
endpoints and enforced almost nowhere.

The guard is ASGI middleware rather than a router dependency because the docs
routes are appended onto the monitor app as route objects and would slip past a
dependency, and because a route added tomorrow is covered without anyone
remembering to add it. The exceptions are fixed paths, so matching the path the
app sees (after Traefik strips ``/api/<domain>``) is enough:

- ``READ_ONLY_POSTS``: POSTs that only read, open to a viewer.
- everything else that is not GET / HEAD / OPTIONS: operator.

No route needs admin any more: the process-exit routes went in TD-64 (lifecycle
belongs to Kubernetes) and the monitor's IB disconnect / reconnect routes, the last
``ADMIN_PATHS``, in TD-40 (api 0.8.0, no caller and no traffic).

The role comes from ``OpsAuth`` (``ops/auth.py``): an ``Authorization: Bearer``
token, else ``ops.auth.default_role``. Until each environment lowers
``default_role`` to viewer, an anonymous caller is still operator there, so
installing the guard changes nothing on its own.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Optional

from starlette.requests import Request
from starlette.types import ASGIApp, Receive, Scope, Send

from bifrost_api.ops.auth import AuthConfig, OpsAuth

logger = logging.getLogger(__name__)

SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})

READ_ONLY_POSTS = frozenset(
    {
        "/research/screener",
        "/executions/option-stock-links/query",
        # The same on-demand registration GET /quotes makes on every poll.
        "/quotes/refresh-options",
    }
)

def required_role(method: str, path: str) -> Optional[str]:
    """The role a request needs, or None when the guard lets it through."""
    if method.upper() in SAFE_METHODS:
        return None
    p = path.rstrip("/") or "/"
    if p in READ_ONLY_POSTS:
        return None
    return "operator"


class WriteGuard:
    """Refuse a write whose caller lacks the role ``required_role`` names."""

    def __init__(self, app: ASGIApp, *, config: Callable[[], Dict[str, Any]]) -> None:
        self.app = app
        self._config = config
        self._auth: Optional[OpsAuth] = None

    def _ops_auth(self) -> OpsAuth:
        # Tokens come from YAML and env at startup; neither changes while the
        # process runs, so the config is read once (and logged once).
        if self._auth is None:
            self._auth = OpsAuth(AuthConfig.from_config(self._config() or {}))
        return self._auth

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        role = required_role(scope["method"], scope["path"])
        if role is None:
            await self.app(scope, receive, send)
            return
        request = Request(scope)
        ident, denied = self._ops_auth().require_role(request, role)
        if denied is not None:
            logger.warning(
                "write refused: %s %s needs %s, caller %s is %s",
                scope["method"],
                scope["path"],
                role,
                ident.name,
                ident.role,
            )
            await denied(scope, receive, send)
            return
        await self.app(scope, receive, send)


def install_write_guard(app: Any, config: Callable[[], Dict[str, Any]]) -> None:
    """Put the guard in front of every route of ``app``; ``config`` returns the merged YAML."""
    app.add_middleware(WriteGuard, config=config)
