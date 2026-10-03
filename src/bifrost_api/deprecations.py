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

``REPLACED_ROUTES`` (debt TD-15, decision B) is the second list: routes that still
work but have a successor -- the merge-style PUTs, replaced by PATCH. Their
response carries ``Deprecation: true`` and ``Link: <successor>; rel="successor-version"``
(the successor's path with this request's ids, behind the prefix the gateway
stripped when it says so in ``X-Forwarded-Prefix``), and each hit is logged as
"replaced route hit ... use <successor>". A route is on one list or the other,
never both.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, FrozenSet, List, Optional, Pattern, Tuple
from urllib.parse import quote

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from bifrost_api.common.query_vocab import install_query_aliases

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
        ("DELETE", "/strategies/structures/{strategy_structure_id}"),
        ("GET", "/strategies/instances/{strategy_instance_id}/open-option-legs"),
        # market
        ("GET", "/bars/latest"),
        ("GET", "/market/trading-day"),
        ("GET", "/bars/coverage"),
        # research: the six marked here in 3a (10-02) were deleted in api 0.5.0. These three
        # lost their last caller in fe 066bb81e (handed over 2026-10-03; Loki 6 days: none).
        ("GET", "/research/data/readiness/fundamental-filter"),
        ("GET", "/research/data/readiness/technical-filter"),
        ("GET", "/research/data/readiness/symbols-snapshot"),
    }
)


# (method, path template) -> "METHOD successor template", the successor written as it is
# served. A hit's ids carry over by position: the successor's n-th path parameter takes
# the replaced route's n-th (so {strategy_instance_id} fills {trade_id:int}).
#
# The TD-15 merge PUTs went in api 0.6.0. PUT /executions/{account_executions_id} is not
# here: ExecutionFormModal edits the fill columns through it and those have no PATCH yet.
#
# Naming program R1 (api 0.7.0, decision pack 2026-10-03 D4/D5): the Trade, its reviews,
# gate sets and saved searches move out of /strategies. The old routes answer the same
# through the same functions until R4, which deletes them once Loki has shown no
# "replaced route hit" for 4 days after a PROD release cycle (D4-A).
# GET /strategies/instances/{id}/open-option-legs stays in DEPRECATED_ROUTES (no successor).
_TRADE = "/trades/{trade_id:int}"
_GATE_SET = "/gate-sets/{gate_safety_strategy_id:int}"
REPLACED_ROUTES: Dict[Tuple[str, str], str] = {
    ("GET", "/strategies/instances"): "GET /trades",
    ("POST", "/strategies/instances"): "POST /trades",
    ("GET", "/strategies/instances/{strategy_instance_id}"): f"GET {_TRADE}",
    ("PATCH", "/strategies/instances/{strategy_instance_id}"): f"PATCH {_TRADE}",
    ("DELETE", "/strategies/instances/{strategy_instance_id}"): f"DELETE {_TRADE}",
    ("GET", "/strategies/win-rate"): "GET /trades/win-rate",
    ("GET", "/strategies/reviews"): "GET /trade-reviews",
    ("PATCH", "/strategies/reviews/{strategy_instance_id}"): "PATCH /trade-reviews/{trade_id:int}",
    ("GET", "/strategies/gate-safety"): "GET /gate-sets",
    ("POST", "/strategies/gate-safety"): "POST /gate-sets",
    ("GET", "/strategies/gate-safety/defaults"): "GET /gate-sets/defaults",
    ("GET", "/strategies/gate-safety/{gate_safety_strategy_id}"): f"GET {_GATE_SET}",
    ("PUT", "/strategies/gate-safety/{gate_safety_strategy_id}"): f"PUT {_GATE_SET}",
    ("PATCH", "/strategies/gate-safety/{gate_safety_strategy_id}"): f"PATCH {_GATE_SET}",
    ("DELETE", "/strategies/gate-safety/{gate_safety_strategy_id}"): f"DELETE {_GATE_SET}",
    ("GET", "/strategies/saved-searches"): "GET /preferences/saved-searches",
    ("POST", "/strategies/saved-searches"): "POST /preferences/saved-searches",
    ("DELETE", "/strategies/saved-searches/{preference_saved_search_id}"): (
        "DELETE /preferences/saved-searches/{preference_saved_search_id:int}"
    ),
}


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


_PARAM = re.compile(r"\{([^}:]+)(?::[^}]*)?\}")


def _compile_named(template: str) -> Pattern[str]:
    """Like ``_compile``, with each path parameter captured under its name."""
    out, pos = [], 0
    for m in _PARAM.finditer(template):
        out.append(re.escape(template[pos : m.start()]))
        out.append(f"(?P<{m.group(1)}>[^/]+)")
        pos = m.end()
    out.append(re.escape(template[pos:]))
    return re.compile("^" + "".join(out) + "/?$")


# Literal templates first: GET /strategies/gate-safety/defaults is not the id route.
_REPLACED_MATCHERS: List[Tuple[str, str, str, Pattern[str]]] = sorted(
    ((method, template, successor, _compile_named(template)) for (method, template), successor in REPLACED_ROUTES.items()),
    key=lambda m: (m[0], "{" in m[1], m[1]),
)


def replaced_route(method: str, path: str) -> Optional[Tuple[str, str, str]]:
    """``(template, successor, successor path for this request)`` for a replaced route, or None."""
    m = method.upper()
    for method_, template, successor, pattern in _REPLACED_MATCHERS:
        hit = pattern.match(path) if method_ == m else None
        if hit:
            successor_path = successor.split(" ", 1)[1]
            ids = iter(hit.groups())
            link = _PARAM.sub(lambda _p: quote(next(ids, ""), safe=""), successor_path)
            return template, successor, link
    return None


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


def _without_deprecation(message: Message) -> List[Tuple[bytes, bytes]]:
    """The response headers less any ``Deprecation`` a handler set itself (the TD-73 body
    fields did until api 0.7.1): a replaced route says so once, not twice."""
    return [(k, v) for k, v in message.get("headers", []) if k.lower() != b"deprecation"]


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
            replaced = replaced_route(scope["method"], scope["path"])
            if replaced is None:
                await self.app(scope, receive, send)
            else:
                await self._replaced(scope, receive, send, *replaced)
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
                message = {**message, "headers": [*_without_deprecation(message), (b"deprecation", b"true")]}
            await send(message)

        await self.app(scope, receive, send_marked)

    async def _replaced(
        self, scope: Scope, receive: Receive, send: Send, template: str, successor: str, link_path: str
    ) -> None:
        client = scope.get("client")
        logger.warning(
            "replaced route hit: %s %s (route %s) use %s client=%s forwarded_for=%s user_agent=%s",
            scope["method"],
            scope["path"],
            template,
            successor,
            client[0] if client else "-",
            _header(scope, b"x-forwarded-for") or "-",
            _header(scope, b"user-agent") or "-",
        )
        prefix = _header(scope, b"x-forwarded-prefix").rstrip("/")
        link = f'<{prefix}{link_path}>; rel="successor-version"'.encode("latin-1")

        async def send_marked(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = [*_without_deprecation(message), (b"deprecation", b"true"), (b"link", link)]
                message = {**message, "headers": headers}
            await send(message)

        await self.app(scope, receive, send_marked)


def install_deprecations(app: Any) -> None:
    """Mark the deprecated and the replaced routes on ``app``, and accept the old query
    names (``common.query_vocab``, TD-51) for one more release."""
    app.add_middleware(DeprecationMarker)
    install_query_aliases(app)
