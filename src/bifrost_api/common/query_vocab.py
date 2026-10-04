"""One query-parameter vocabulary for every Trade API route (debt TD-51).

Before api 0.6.6 one idea had several spellings: an option expiry was ``expiry`` or
``expiration`` and YYYYMMDD on one route, YYYY-MM-DD on another; the option side was
``option_right`` or ``right``; a time range was ``since_ts``/``until_ts``,
``opened_at_from``/``opened_at_until`` or ``trade_date_from``/``trade_date_to``. A
caller had to know which route spelled it which way, and sent the wrong one.

The canonical names, for every route that takes the idea:

==============  =====================================================================
``expiry``      option expiry, ``YYYY-MM-DD`` (``YYYYMMDD`` is accepted and read the same)
``option_right``  ``C`` or ``P``
``from_ts``     Unix seconds, inclusive lower bound (the route's description names the column)
``to_ts``       Unix seconds, inclusive upper bound
``from_date``   ``YYYY-MM-DD``, inclusive lower bound
``to_date``     ``YYYY-MM-DD``, inclusive upper bound
``limit``       the most rows to return (a route's own default and cap are unchanged)
``trade_id``    one Trade (table ``trade``; ``strategy_instance`` before naming R3, core 0.45.0)
``trade_ids``   comma-separated Trade ids
==============  =====================================================================

The unit is in the name: ``_ts`` is always Unix seconds and ``_date`` always a
calendar date, so ``from`` and ``to`` never need a description to be read.

**Old names keep working for one release** (Owner decision, additive first, as for
TD-16/TD-19). :data:`QUERY_ALIASES` lists them per route; :class:`QueryAliasRewriter`
renames an old name to its canonical one before routing, so a route declares only
the canonical names (and OpenAPI shows only them). When a request sends both, the
canonical one wins and the old one is dropped. Each request that used an old name is
logged once as ``deprecated query params: <METHOD> <path> <old>-><new> ...`` with who
sent it, like a deprecated route (behind Traefik read ``forwarded_for``). After a
release with no such line for a route its old names come off the table.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, FrozenSet, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode

from fastapi import Query
from starlette.types import ASGIApp, Receive, Scope, Send

logger = logging.getLogger(__name__)

EXPIRY = "expiry"
OPTION_RIGHT = "option_right"
FROM_TS = "from_ts"
TO_TS = "to_ts"
FROM_DATE = "from_date"
TO_DATE = "to_date"
LIMIT = "limit"
TRADE_ID = "trade_id"
TRADE_IDS = "trade_ids"

CANONICAL_NAMES: FrozenSet[str] = frozenset(
    {EXPIRY, OPTION_RIGHT, FROM_TS, TO_TS, FROM_DATE, TO_DATE, LIMIT, TRADE_ID, TRADE_IDS}
)

_TS_RANGE = {"since_ts": FROM_TS, "until_ts": TO_TS}
# naming R1 (api 0.7.0): the Trade's id under its name; the instance names go in R4.
_TRADE = {"strategy_instance_id": TRADE_ID}
_TRADES_LIST = {"opened_at_from": FROM_TS, "opened_at_until": TO_TS, "strategy_instance_ids": TRADE_IDS}

# (method, path as the app sees it) -> {old name: canonical name}. Paths are the ones
# the app serves (Traefik strips ``/api/<domain>``). Removed next release, route by
# route, once a release has gone by with no "deprecated query params" line for it.
QUERY_ALIASES: Dict[Tuple[str, str], Dict[str, str]] = {
    # account app (trading + strategy routers)
    ("GET", "/executions"): {**_TS_RANGE, **_TRADE},
    ("GET", "/performance"): {**_TS_RANGE, **_TRADE},
    ("GET", "/transactions"): dict(_TS_RANGE),
    ("GET", "/strategies/win-rate"): dict(_TS_RANGE),
    ("GET", "/trades/win-rate"): dict(_TS_RANGE),
    ("GET", "/strategies/instances"): dict(_TRADES_LIST),
    ("GET", "/trades"): dict(_TRADES_LIST),
    ("GET", "/executions/stock-link-candidates"): {"trade_date_from": FROM_DATE, "trade_date_to": TO_DATE},
    # research app
    ("GET", "/research/greeks"): {"right": OPTION_RIGHT},
    ("GET", "/research/option-snapshots"): {"expiration": EXPIRY},
    ("GET", "/research/option-contract/liquidity-summary"): {"expiration": EXPIRY, "right": OPTION_RIGHT},
    ("GET", "/research/option-contract/relative-value"): {"expiration": EXPIRY, "right": OPTION_RIGHT},
}


# --- Query definitions --------------------------------------------------------


# Each factory names its parameter on the wire (``alias``), so a route's Python argument
# may keep an older name without the old name leaking into the URL or OpenAPI.


def expiry_query(default: Any = None, *, note: str = "") -> Any:
    """``expiry``: option expiry ``YYYY-MM-DD`` (``YYYYMMDD`` read the same)."""
    return Query(default, alias=EXPIRY, description=_join("Option expiry YYYY-MM-DD (YYYYMMDD is read the same).", note))


def option_right_query(default: Any = None, *, note: str = "") -> Any:
    """``option_right``: ``C`` or ``P``."""
    return Query(default, alias=OPTION_RIGHT, description=_join("Option right: C or P.", note))


def from_ts_query(column: str, *, note: str = "") -> Any:
    """``from_ts``: Unix seconds, ``column >= from_ts``."""
    return Query(None, alias=FROM_TS, description=_join(f"Unix seconds, inclusive: {column} >= from_ts.", note))


def to_ts_query(column: str, *, note: str = "") -> Any:
    """``to_ts``: Unix seconds, ``column <= to_ts``."""
    return Query(None, alias=TO_TS, description=_join(f"Unix seconds, inclusive: {column} <= to_ts.", note))


def from_date_query(column: str, *, note: str = "") -> Any:
    """``from_date``: ``YYYY-MM-DD``, ``column >= from_date``."""
    return Query(None, alias=FROM_DATE, description=_join(f"YYYY-MM-DD, inclusive: {column} >= from_date.", note))


def to_date_query(column: str, *, note: str = "") -> Any:
    """``to_date``: ``YYYY-MM-DD``, ``column <= to_date``."""
    return Query(None, alias=TO_DATE, description=_join(f"YYYY-MM-DD, inclusive: {column} <= to_date.", note))


def trade_id_query(*, note: str = "") -> Any:
    """``trade_id``: one Trade (``strategy_instance_id`` before api 0.7.0)."""
    return Query(None, alias=TRADE_ID, description=_join("Filter by trade id.", note))


def trade_ids_query(*, note: str = "") -> Any:
    """``trade_ids``: comma-separated Trade ids (``strategy_instance_ids`` before api 0.7.0)."""
    return Query(None, alias=TRADE_IDS, description=_join("Comma-separated trade ids (e.g. 1,2,3).", note))


def _join(text: str, note: str) -> str:
    return f"{text} {note}".strip()


_ISO_DAY = re.compile(r"^(\d{4})-(\d{2})-(\d{2})")
_COMPACT_DAY = re.compile(r"^(\d{4})(\d{2})(\d{2})$")


def normalize_expiry(value: Optional[str]) -> Optional[str]:
    """An expiry as ``YYYY-MM-DD``: ``YYYYMMDD`` and ``YYYY-MM-DD…`` are rewritten, anything
    else is returned stripped and unchanged (the route answers it as it always did)."""
    if value is None:
        return None
    s = str(value).strip()
    m = _ISO_DAY.match(s) or _COMPACT_DAY.match(s)
    if not m:
        return s
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"


# --- Old names -> canonical, before routing ------------------------------------


def _header(scope: Scope, name: bytes) -> str:
    for k, v in scope.get("headers") or ():
        if k.lower() == name:
            return v.decode("latin-1")
    return ""


def aliases_for(method: str, path: str) -> Optional[Dict[str, str]]:
    """The old -> canonical names a route still accepts, or None."""
    p = path.rstrip("/") or "/"
    return QUERY_ALIASES.get((method.upper(), p))


def rewrite_query(query: str, aliases: Dict[str, str]) -> Tuple[str, List[str]]:
    """``query`` with old names renamed; and what was renamed (``old->new``) or dropped
    (``old->new (ignored)``, when the canonical name was sent too)."""
    pairs = parse_qsl(query, keep_blank_values=True)
    sent = {k for k, _ in pairs}
    out: List[Tuple[str, str]] = []
    used: List[str] = []
    for key, value in pairs:
        new = aliases.get(key)
        if new is None:
            out.append((key, value))
        elif new in sent:
            used.append(f"{key}->{new} (ignored)")
        else:
            out.append((new, value))
            used.append(f"{key}->{new}")
    return urlencode(out, doseq=True), sorted(set(used))


class QueryAliasRewriter:
    """Rename the old query names in :data:`QUERY_ALIASES` to the canonical ones, and log who sent them."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http" and scope.get("query_string"):
            aliases = aliases_for(scope["method"], scope["path"])
            if aliases:
                query = scope["query_string"].decode("latin-1")
                rewritten, used = rewrite_query(query, aliases)
                if used:
                    client = scope.get("client")
                    logger.warning(
                        "deprecated query params: %s %s %s client=%s forwarded_for=%s user_agent=%s",
                        scope["method"],
                        scope["path"],
                        " ".join(used),
                        client[0] if client else "-",
                        _header(scope, b"x-forwarded-for") or "-",
                        _header(scope, b"user-agent") or "-",
                    )
                    scope = {**scope, "query_string": rewritten.encode("latin-1")}
        await self.app(scope, receive, send)


def install_query_aliases(app: Any) -> None:
    """Accept the old query names on ``app`` for one more release."""
    app.add_middleware(QueryAliasRewriter)
