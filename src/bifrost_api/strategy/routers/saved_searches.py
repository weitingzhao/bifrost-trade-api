"""`/strategies/saved-searches` — a page's scope kept under a name.

Trade design Rev .139: Plans' Save as list writes one, the sidebar lists them
on every page. Stored server-side for the one operator (Owner 2026-10-01); the
rules and the table are core's (`saved_search`, core 0.28.0).

    400  a route, label or state the table would refuse (with the reason)
    404  no such saved search
    503  Postgres is not configured for writes
"""

from __future__ import annotations

import logging
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request

from bifrost_api.common.write_errors import deleted_body, write_target
from bifrost_api.strategy.deps import db_not_configured, read_config, write_config
from bifrost_api.strategy.schemas.requests import SavedSearchBody
from bifrost_core.monitor.reader import saved_search as saved_search_module
from bifrost_core.monitor.reader.saved_search import SavedSearchError

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/strategies", tags=["saved-searches"])


@router.get("/saved-searches")
def list_saved_searches_endpoint(request: Request) -> Dict[str, Any]:
    """Every saved search, oldest first. A read that fails is a 500, never an empty list."""
    config = read_config(request)
    try:
        items = saved_search_module.list_saved_searches(config)
    except Exception as e:
        logger.warning("list_saved_searches failed: %s", e)
        raise HTTPException(status_code=500, detail="Failed to read saved searches") from e
    return {"items": items, "count": len(items)}


@router.post("/saved-searches")
def create_saved_search_endpoint(request: Request, body: SavedSearchBody) -> Dict[str, Any]:
    """Keep a scope: body {route, label, state}. Saving a label again on a page replaces it."""
    config = write_config(request)
    try:
        new_id = saved_search_module.create_saved_search(config, body.route or "", body.label or "", body.state or {})
    except SavedSearchError as e:
        raise HTTPException(status_code=400, detail=e.reason) from e
    except Exception as e:
        logger.warning("create_saved_search failed: %s", e)
        raise HTTPException(status_code=500, detail="Failed to save the search") from e
    if new_id is None:
        raise db_not_configured()
    return {"preference_saved_search_id": new_id}


@router.delete("/saved-searches/{saved_search_id}")
def delete_saved_search_endpoint(request: Request, saved_search_id: int) -> Dict[str, Any]:
    """Forget one saved search (hard delete). Failures are core's Write* outcomes
    (``bifrost_api.common.write_errors``): 404 missing, 503 Postgres unavailable."""
    config = write_target(request, f"saved search {saved_search_id}")
    return deleted_body(saved_search_module.delete_saved_search_strict(config, saved_search_id))
