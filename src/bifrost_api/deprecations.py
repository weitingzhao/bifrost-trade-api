"""Routes no repo calls, marked for one release before they go (debt TD-40, decision B).

The 2026-10-01 audit found about 70 deployed routes with no caller in any repo. The
readiness backfills, the market write block and the dims stubs went in wave 3a. What is
left are the monitor daemon / IB control writes and a handful of reads. Loki (4 days to
2026-10-02) saw no POST on any of the control routes, but four of the reads had browser
traffic through the gateway that no frontend source explains — some of it agents'
release checks, the rest a stale tab, a bookmark, or a client nobody listed. So these
are marked rather than deleted: every response carries ``Deprecation: true`` and every
hit is logged with who sent it (behind Traefik the client is the gateway, so read
``forwarded_for``). After a release with no hits a route is deleted; a route with hits
gets its caller found first.

Middleware, like ``write_guard``, so no router body is touched and the docs routes
appended onto monitor would be covered the same way. Paths are the ones the app sees
(Traefik strips ``/api/<domain>``), written as FastAPI serves them.
"""

from __future__ import annotations

import logging
import re
from typing import Any, FrozenSet, List, Optional, Pattern, Tuple

from starlette.types import ASGIApp, Message, Receive, Scope, Send

logger = logging.getLogger(__name__)

DEPRECATED_ROUTES: FrozenSet[Tuple[str, str]] = frozenset(
    {
        # monitor: daemon and IB control nothing calls (the desk uses suspend,
        # resume, flatten and refresh_accounts, which are not here)
        ("POST", "/control/monitor_stop"),
        ("POST", "/control/monitor_release_ib"),
        ("POST", "/control/monitor_connect"),
        ("POST", "/control/stop"),
        ("POST", "/control/retry_ib"),
        ("POST", "/control/release_ib"),
        ("POST", "/control/refresh_replay"),
        ("POST", "/control/refresh_ticker_subscriptions"),
        ("POST", "/control/release_ticker_subscriptions"),
        ("POST", "/control/init_ticker_subscriptions"),
        ("POST", "/control/set_heartbeat_interval"),
        ("GET", "/operations"),
        ("POST", "/ops/market-ingest/control"),
        ("POST", "/ops/market-ingest/clear-conflict-leases"),
        # account
        ("GET", "/executions/link-candidates"),
        ("PATCH", "/executions/strategy-attribution"),
        # GET /instrument-classes is not here: the audit listed it, but it serves
        # the instrument-class (Shares registration) feature and Loki shows it in use.
        ("DELETE", "/strategies/structures/{structure_id}"),
        ("GET", "/strategies/instances/{strategy_instance_id}/open-option-legs"),
        # market
        ("GET", "/bars/latest"),
        ("GET", "/market/trading-day"),
        ("GET", "/bars/coverage"),
        # research (the Trade research app, not the Research engine)
        ("GET", "/research/option-expirations"),
        ("GET", "/research/option-oi"),
        ("GET", "/research/option-trades"),
        ("POST", "/research/option-snapshot"),
        ("GET", "/research/iv-term-structure"),
        ("GET", "/research/data/readiness/momentum-distribution"),
    }
)


def _compile(template: str) -> Pattern[str]:
    out, pos = [], 0
    for m in re.finditer(r"\{[^}]+\}", template):
        out.append(re.escape(template[pos : m.start()]))
        out.append(r"[^/]+")
        pos = m.end()
    out.append(re.escape(template[pos:]))
    return re.compile("^" + "".join(out) + "/?$")


_MATCHERS: List[Tuple[str, str, Pattern[str]]] = sorted(
    (method, template, _compile(template)) for method, template in DEPRECATED_ROUTES
)


def deprecated_route(method: str, path: str) -> Optional[str]:
    """The deprecated route template a request hits, or None."""
    m = method.upper()
    for method_, template, pattern in _MATCHERS:
        if method_ == m and pattern.match(path):
            return template
    return None


def _header(scope: Scope, name: bytes) -> str:
    for k, v in scope.get("headers") or ():
        if k.lower() == name:
            return v.decode("latin-1")
    return ""


class DeprecationMarker:
    """Add ``Deprecation: true`` to a deprecated route's response and log who called it."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        template = deprecated_route(scope["method"], scope["path"])
        if template is None:
            await self.app(scope, receive, send)
            return
        client = scope.get("client")
        logger.warning(
            "deprecated route hit: %s %s (route %s) client=%s forwarded_for=%s user_agent=%s",
            scope["method"],
            scope["path"],
            template,
            client[0] if client else "-",
            _header(scope, b"x-forwarded-for") or "-",
            _header(scope, b"user-agent") or "-",
        )

        async def send_marked(message: Message) -> None:
            if message["type"] == "http.response.start":
                message = {**message, "headers": [*message.get("headers", []), (b"deprecation", b"true")]}
            await send(message)

        await self.app(scope, receive, send_marked)


def install_deprecations(app: Any) -> None:
    """Mark the deprecated routes on ``app``."""
    app.add_middleware(DeprecationMarker)
