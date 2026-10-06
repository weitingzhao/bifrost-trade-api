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

**The old names are refused** (api 0.10.0, TD-51). api 0.6.6 renamed them before
routing for one release and logged every caller; Research moved to ``from_ts`` in 0.161.0,
the frontend has sent only the canonical names since 0.6.6, and Loki showed no caller
from 2026-10-05 13:30 UTC through the 10-05 nightly batch to 2026-10-06 16:10 UTC
(infra ``scripts/release/loki_gate.py td51-query-aliases``). The rename went with them.

A route ignores a query name it does not declare, so an old name would now drop the
caller's filter without a word: ``/executions?since_ts=…`` would answer every
execution. :data:`RETIRED_QUERY_NAMES` keeps, per route that used to take them, the old
names and their successors; :class:`RetiredQueryNames` answers a request that sends one
with a 422 of type ``retired_query_param`` naming the successor (FastAPI's validation
shape, as a retired body field is, ``common.write_errors.RetiredFields``) and logs it
as ``retired query params: <METHOD> <path> <old> (use <new>) ...`` with who sent it.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, FrozenSet, List, Optional, Tuple
from urllib.parse import parse_qsl

from fastapi import Query
from fastapi.responses import JSONResponse
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
_TRADES_LIST = {"opened_at_from": FROM_TS, "opened_at_until": TO_TS}

# (method, path as the app sees it) -> {old name: canonical name}: the TD-51 spellings,
# renamed before routing in api 0.6.6 .. 0.9.0 and refused since 0.10.0. Paths are the
# ones the app serves (Traefik strips ``/api/<domain>``). The naming-R1 Trade ids
# (strategy_instance_id(s)) went in api 0.9.0 with their routes and are not here.
RETIRED_QUERY_NAMES: Dict[Tuple[str, str], Dict[str, str]] = {
    # account app (trading + strategy routers)
    ("GET", "/executions"): dict(_TS_RANGE),
    ("GET", "/performance"): dict(_TS_RANGE),
    ("GET", "/transactions"): dict(_TS_RANGE),
    ("GET", "/trades/win-rate"): dict(_TS_RANGE),
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


# --- Old names: refused --------------------------------------------------------------


def _header(scope: Scope, name: bytes) -> str:
    for k, v in scope.get("headers") or ():
        if k.lower() == name:
            return v.decode("latin-1")
    return ""


def retired_for(method: str, path: str) -> Optional[Dict[str, str]]:
    """The retired old -> canonical names of a route, or None."""
    p = path.rstrip("/") or "/"
    return RETIRED_QUERY_NAMES.get((method.upper(), p))


def retired_sent(query: str, retired: Dict[str, str]) -> List[Dict[str, Any]]:
    """One FastAPI-shaped 422 error per retired name in ``query`` (first value of each)."""
    errors: List[Dict[str, Any]] = []
    seen: set = set()
    for key, value in parse_qsl(query, keep_blank_values=True):
        new = retired.get(key)
        if new is None or key in seen:
            continue
        seen.add(key)
        errors.append({
            "type": "retired_query_param",
            "loc": ["query", key],
            "msg": f"Query parameter {key} was retired in api 0.10.0 (TD-51); use {new}.",
            "input": value,
        })
    return errors


class RetiredQueryNames:
    """Answer 422 to a request that sends an old query name in :data:`RETIRED_QUERY_NAMES`, and log who sent it."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http" and scope.get("query_string"):
            retired = retired_for(scope["method"], scope["path"])
            if retired:
                errors = retired_sent(scope["query_string"].decode("latin-1"), retired)
                if errors:
                    client = scope.get("client")
                    logger.warning(
                        "retired query params: %s %s %s client=%s forwarded_for=%s user_agent=%s",
                        scope["method"],
                        scope["path"],
                        " ".join(f"{e['loc'][1]} (use {retired[e['loc'][1]]})" for e in errors),
                        client[0] if client else "-",
                        _header(scope, b"x-forwarded-for") or "-",
                        _header(scope, b"user-agent") or "-",
                    )
                    await JSONResponse({"detail": errors}, status_code=422)(scope, receive, send)
                    return
        await self.app(scope, receive, send)


def install_retired_query_names(app: Any) -> None:
    """Refuse the retired query names on ``app`` (422, never a silently dropped filter)."""
    app.add_middleware(RetiredQueryNames)
