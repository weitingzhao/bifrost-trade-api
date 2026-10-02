"""Watchlist: CRUD for watchlist items.

Failures answer a real status with ``{"detail", "ok": false, "error"}``; the list
answers ``{"items", "count"}`` (``bifrost_api.common.envelopes``, TD-16/17).

Writes (TD-15, batch 3b-2) call core's TD-15 writers; their Write* outcomes are
mapped once in ``bifrost_api.common.write_errors`` (400 bad input, 404 not on the
list, 503 Postgres unavailable, 500 write failed). The POST body is
``market.schemas.requests.WatchlistBody`` (TD-24: strict types, unknown fields
ignored and logged this release):

    POST   /watchlist                      add, or change the fields sent on a watched contract
    PATCH  /watchlist/{contract_key}       change the fields sent; 404 when not on the list
    DELETE /watchlist?contract_key=        take it off; 404 when not on the list
"""

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Body, Query, Request
from pydantic import StrictBool, StrictFloat, StrictInt, StrictStr

from bifrost_api.common.envelopes import list_body
from bifrost_api.common.write_errors import PatchBody, deleted_body, write_target
from bifrost_api.market.schemas.requests import WatchlistBody
from bifrost_core.monitor.reader import watchlist as watchlist_module

logger = logging.getLogger(__name__)

router = APIRouter(tags=["watchlist"])


class WatchlistItemPatch(PatchBody):
    symbol: Optional[StrictStr] = None
    sec_type: Optional[StrictStr] = None
    expiry: Optional[StrictStr] = None
    strike: Optional[StrictFloat] = None
    option_right: Optional[StrictStr] = None
    display_label: Optional[StrictStr] = None
    source: Optional[StrictStr] = None
    category_id: Optional[StrictInt] = None
    optionable: Optional[StrictBool] = None


def _post_fields(body: WatchlistBody) -> Dict[str, Any]:
    """What POST hands core's upsert: the fields the client sent, contract_key aside.

    An explicit null clears (the Watchlist page's "None" category sends
    ``category_id: null``); a field left out keeps the stored value -- a re-add from
    the Omnibar, the Symbol Dock or a drop no longer moves the row out of its list
    or drops its label. Two inputs POST always took and core's field rules refuse
    mean "not sent" here, as they did before: ``optionable: null`` (it was kept) and
    a blank string (stored as ''; Add from position sends one for a stock's expiry).
    """
    fields = body.declared(exclude_unset=True)
    fields.pop("contract_key", None)
    if fields.get("optionable", False) is None:
        del fields["optionable"]
    return {k: v for k, v in fields.items() if not (isinstance(v, str) and not v.strip())}


@router.get("/watchlist")
def get_watchlist(request: Request) -> Dict[str, Any]:
    """R-A3: Return Watchlist (user symbols / contracts)."""
    reader = request.app.state.reader
    items = reader.get_watchlist()
    return list_body(items)


@router.post("/watchlist")
def post_watchlist(request: Request, body: WatchlistBody = Body(...)) -> Any:
    """Add a contract, or change the fields sent on one already watched (see ``_post_fields``).

    A new row's source is 'manual' when none is sent. Answers the row (GET /watchlist
    item shape) plus ``ok`` / ``message``, kept one release for older callers."""
    config = write_target(request, "the watchlist")
    row = watchlist_module.upsert_watchlist(config, body.contract_key, _post_fields(body))
    return {**row, "ok": True, "message": "Watchlist item added or updated."}


@router.patch("/watchlist/{contract_key:path}")
def patch_watchlist_item(request: Request, contract_key: str, body: WatchlistItemPatch) -> Any:
    """Change the fields sent on a watched contract (`null` clears; `category_id: null` takes
    it out of its list). Never inserts: 404 when it is not on the list. Answers the row."""
    config = write_target(request, f"watchlist item {contract_key}")
    return watchlist_module.patch_watchlist_item(config, contract_key, body.patch_fields())


@router.delete("/watchlist")
def delete_watchlist(
    request: Request,
    contract_key: Optional[str] = Query(None, description="Delete by contract_key"),
) -> Any:
    """Take one contract off the watchlist (hard delete). 400 without a key, 404 when not on it."""
    config = write_target(request, f"watchlist item {contract_key}")
    return deleted_body(watchlist_module.delete_watchlist_strict(config, contract_key or ""))
