"""Watchlist: CRUD for watchlist items."""

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Body, Query, Request
from pydantic import BaseModel

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
    return {"items": items}


@router.post("/watchlist")
def post_watchlist(request: Request, body: WatchlistBody = Body(...)) -> Dict[str, Any]:
    """R-A3: Add or update a Watchlist item (by contract_key)."""
    reader = request.app.state.reader
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        logger.info("POST /watchlist rejected: need postgres config")
        return {"ok": False, "error": "Postgres config required to write watchlist."}
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
    logger.warning("POST /watchlist write failed")
    return {"ok": False, "error": "Failed to write watchlist."}


@router.delete("/watchlist")
def delete_watchlist(
    request: Request,
    contract_key: Optional[str] = Query(None, description="Delete by contract_key"),
) -> Dict[str, Any]:
    """R-A3: Delete one Watchlist item by contract_key."""
    reader = request.app.state.reader
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return {"ok": False, "error": "Postgres config required to modify watchlist."}
    if not contract_key or not contract_key.strip():
        return {"ok": False, "error": "Provide contract_key query parameter."}
    if reader.delete_watchlist(contract_key=contract_key):
        return {"ok": True, "message": "Deleted."}
    return {"ok": False, "error": "Delete failed (not found or database error)."}
