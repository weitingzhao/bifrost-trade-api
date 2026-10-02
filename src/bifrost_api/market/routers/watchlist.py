"""Watchlist: CRUD for watchlist items.

Failures answer a real status with ``{"detail", "ok": false, "error"}``; the list
answers ``{"items", "count"}`` (``bifrost_api.common.envelopes``, TD-16/17).
"""

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Body, Query, Request
from pydantic import BaseModel

from bifrost_api.common.envelopes import error_response, list_body

logger = logging.getLogger(__name__)

router = APIRouter(tags=["watchlist"])


class WatchlistBody(BaseModel):
    contract_key: str
    symbol: Optional[str] = None
    sec_type: Optional[str] = None
    expiry: Optional[str] = None
    strike: Optional[float] = None
    option_right: Optional[str] = None
    display_label: Optional[str] = None
    source: Optional[str] = None
    category_id: Optional[int] = None
    optionable: Optional[bool] = None

    class Config:
        extra = "ignore"


@router.get("/watchlist")
def get_watchlist(request: Request) -> Dict[str, Any]:
    """R-A3: Return Watchlist (user symbols / contracts)."""
    reader = request.app.state.reader
    items = reader.get_watchlist()
    return list_body(items)


@router.post("/watchlist")
def post_watchlist(request: Request, body: WatchlistBody = Body(...)) -> Any:
    """R-A3: Add or update a Watchlist item (by contract_key)."""
    reader = request.app.state.reader
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        logger.info("POST /watchlist rejected: need postgres config")
        return error_response(503, "Postgres config required to write watchlist.")
    if not body.contract_key.strip():
        # Core's writer refuses a blank key without writing; say so as input.
        return error_response(400, "contract_key is required.")
    ok = reader.add_watchlist(
        contract_key=body.contract_key,
        symbol=body.symbol,
        sec_type=body.sec_type,
        expiry=body.expiry,
        strike=body.strike,
        option_right=body.option_right,
        display_label=body.display_label,
        source=body.source or "manual",
        category_id=body.category_id,
        optionable=body.optionable,
    )
    if ok:
        return {"ok": True, "message": "Watchlist item added or updated."}
    return error_response(500, "Failed to write watchlist.")


@router.delete("/watchlist")
def delete_watchlist(
    request: Request,
    contract_key: Optional[str] = Query(None, description="Delete by contract_key"),
) -> Any:
    """R-A3: Delete one Watchlist item by contract_key."""
    reader = request.app.state.reader
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return error_response(503, "Postgres config required to modify watchlist.")
    if not contract_key or not contract_key.strip():
        return error_response(400, "Provide contract_key query parameter.")
    if reader.delete_watchlist(contract_key=contract_key):
        return {"ok": True, "message": "Deleted."}
    # Core's delete does not check the row count, so a False here is a database error
    # (a key that is not on the list deletes nothing and answers ok).
    return error_response(500, "Delete failed (database error).")
