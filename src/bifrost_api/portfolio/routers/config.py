"""Portfolio config: position-categories management."""

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Body, Request

logger = logging.getLogger(__name__)

router = APIRouter(tags=["portfolio-config"])


def _coerce_optional_int(value: Any) -> Optional[int]:
    """Accept JSON int/float/str for sort_order; invalid values become None (omit from insert)."""
    if value is None:
        return None
    try:
        if isinstance(value, bool):
            return None
        if isinstance(value, float):
            if not value.is_integer():
                return None
            return int(value)
        return int(value)
    except (TypeError, ValueError):
        return None


@router.get("/position-categories")
def get_position_categories(request: Request) -> Dict[str, Any]:
    """Return all position_categories rows (for dropdown and manage UI)."""
    reader = request.app.state.reader
    items = reader.get_position_categories()
    return {"ok": True, "items": items}


@router.post("/position-categories")
def post_position_category(request: Request, body: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    """Create one position category. body: name (required), description, sort_order."""
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return {"ok": False, "error": "Postgres required.", "id": None}
    reader = request.app.state.reader
    b = body or {}
    name = (b.get("name") or "").strip()
    if not name:
        return {"ok": False, "error": "name is required.", "id": None}
    gid, err = reader.create_position_category(
        name=name,
        description=b.get("description"),
        sort_order=_coerce_optional_int(b.get("sort_order")),
    )
    if gid is not None:
        return {"ok": True, "id": gid, "name": name}
    return {"ok": False, "error": err or "Failed to create category.", "id": None}


@router.patch("/position-categories/{category_id:int}")
def patch_position_category(request: Request, category_id: int, body: Dict[str, Any] = Body(default=None)) -> Dict[str, Any]:
    """Update one position category by id. body: name, description, sort_order (optional)."""
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return {"ok": False, "error": "Postgres required."}
    reader = request.app.state.reader
    b = body or {}
    if reader.update_position_category(
        category_id,
        name=b.get("name"),
        description=b.get("description"),
        sort_order=_coerce_optional_int(b.get("sort_order")),
    ):
        return {"ok": True, "id": category_id}
    return {"ok": False, "error": "Failed to update category."}


@router.delete("/position-categories/{category_id:int}")
def delete_position_category(request: Request, category_id: int) -> Dict[str, Any]:
    """Delete one position category by id (tags removed by CASCADE)."""
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return {"ok": False, "error": "Postgres required."}
    reader = request.app.state.reader
    if reader.delete_position_category(category_id):
        return {"ok": True, "id": category_id}
    return {"ok": False, "error": "Failed to delete category."}


@router.patch("/executions/strategy-attribution")
def patch_execution_strategy_attribution(request: Request, body: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    """Batch update strategy attribution on executions.
    body: account_id (required), contract_key OR execution_ids[], strategy_opportunity_id, strategy_instance_id."""
    from fastapi import HTTPException
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return {"ok": False, "error": "Postgres required."}
    reader = request.app.state.reader
    b = body or {}
    account_id = (b.get("account_id") or "").strip()
    if not account_id:
        return {"ok": False, "error": "account_id is required."}
    contract_key = (b.get("contract_key") or "").strip() or None
    execution_ids = b.get("execution_ids")
    if isinstance(execution_ids, list):
        execution_ids = [int(x) for x in execution_ids if x is not None]
    else:
        execution_ids = None
    if not contract_key and not execution_ids:
        return {"ok": False, "error": "contract_key or execution_ids is required."}
    so_id = b.get("strategy_opportunity_id")
    si_id = b.get("strategy_instance_id")
    if so_id is not None:
        try:
            so_id = int(so_id)
        except (TypeError, ValueError):
            so_id = None
    if si_id is not None:
        try:
            si_id = int(si_id)
        except (TypeError, ValueError):
            si_id = None
    count = reader.batch_update_execution_strategy(account_id, contract_key, execution_ids, so_id, si_id)
    if count < 0:
        raise HTTPException(
            status_code=409,
            detail="One or more executions have instance_allocations; clear or edit splits before batch attribution.",
        )
    if count > 0:
        return {"ok": True, "updated": count}
    return {"ok": False, "error": "No matching executions found or update failed.", "updated": 0}


@router.put("/position-categories/tag")
def put_position_category_tag(request: Request, body: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    """Tag a position with a category (STK). Pass category_id null to clear tag. body: account_id, contract_key, category_id."""
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return {"ok": False, "error": "Postgres required."}
    reader = request.app.state.reader
    b = body or {}
    account_id = (b.get("account_id") or "").strip()
    contract_key = (b.get("contract_key") or "").strip()
    category_id = b.get("category_id")
    if not account_id:
        return {"ok": False, "error": "account_id is required."}
    if not contract_key:
        return {"ok": False, "error": "contract_key is required."}
    if category_id is not None:
        try:
            category_id = int(category_id)
        except (TypeError, ValueError):
            category_id = None
    if reader.set_position_category_tag(account_id, contract_key, category_id):
        return {"ok": True}
    return {"ok": False, "error": "Failed to set tag."}


@router.get("/position-categories/symbol-order")
def get_market_streams_symbol_order(request: Request) -> Dict[str, Any]:
    """Return category_name -> ordered list of symbols (Market Streams custom symbol order)."""
    reader = request.app.state.reader
    order = reader.get_market_streams_symbol_order()
    return {"ok": True, "order": order}


@router.put("/position-categories/symbol-order")
def put_market_streams_symbol_order(request: Request, body: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    """Save symbol order for one category. body: category_name (required), symbols (array of symbol strings)."""
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return {"ok": False, "error": "Postgres required."}
    reader = request.app.state.reader
    b = body or {}
    category_name = (b.get("category_name") or "").strip()
    symbols = b.get("symbols")
    if not category_name:
        return {"ok": False, "error": "category_name is required."}
    if not isinstance(symbols, list):
        return {"ok": False, "error": "symbols must be an array."}
    if reader.set_market_streams_symbol_order(category_name, symbols):
        return {"ok": True}
    return {"ok": False, "error": "Failed to save symbol order."}
