"""Portfolio config: position categories, tags, symbol order, instrument classes.

Failures answer a real status with ``{"detail", "ok": false, "error", ...}`` and
lists answer ``{"items", "count", ...}`` (``bifrost_api.common.envelopes``, TD-16/17).
"""

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Body, Request

from bifrost_api.common.envelopes import error_response, list_body
from bifrost_core.portfolio.reader.instrument_class import INSTRUMENT_CLASSES, normalize_instrument_class

logger = logging.getLogger(__name__)

router = APIRouter(tags=["portfolio-config"])

POSTGRES_REQUIRED = "Postgres required."
# The reader's answers when it had no connection (core monitor/reader/common.py,
# portfolio/reader/position_categories.py, instrument_class.py); input was checked before the call.
_NO_CONNECTION = frozenset(
    {"Database connection failed.", "No database connection.", "Invalid name or no database connection."}
)


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


def _write_failed(err: Optional[str], fallback: str, legacy: Optional[Dict[str, Any]] = None) -> Any:
    """A writer's refusal after input was checked: 503 when it had no connection, else 500."""
    if err in _NO_CONNECTION:
        return error_response(503, err, legacy)
    return error_response(500, err or fallback, legacy)


@router.get("/position-categories")
def get_position_categories(request: Request) -> Dict[str, Any]:
    """Return all position_categories rows (for dropdown and manage UI)."""
    reader = request.app.state.reader
    items = reader.get_position_categories()
    return list_body(items, ok=True)


@router.post("/position-categories")
def post_position_category(request: Request, body: Dict[str, Any] = Body(...)) -> Any:
    """Create one position category. body: name (required), description, sort_order."""
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return error_response(503, POSTGRES_REQUIRED, {"id": None})
    reader = request.app.state.reader
    b = body or {}
    name = (b.get("name") or "").strip()
    if not name:
        return error_response(400, "name is required.", {"id": None})
    gid, err = reader.create_position_category(
        name=name,
        description=b.get("description"),
        sort_order=_coerce_optional_int(b.get("sort_order")),
    )
    if gid is not None:
        return {"ok": True, "id": gid, "name": name}
    return _write_failed(err, "Failed to create category.", {"id": None})


@router.patch("/position-categories/{category_id:int}")
def patch_position_category(request: Request, category_id: int, body: Dict[str, Any] = Body(default=None)) -> Any:
    """Update one position category by id. body: name, description, sort_order (optional)."""
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return error_response(503, POSTGRES_REQUIRED)
    reader = request.app.state.reader
    b = body or {}
    if reader.update_position_category(
        category_id,
        name=b.get("name"),
        description=b.get("description"),
        sort_order=_coerce_optional_int(b.get("sort_order")),
    ):
        return {"ok": True, "id": category_id}
    return error_response(500, "Failed to update category.")


@router.delete("/position-categories/{category_id:int}")
def delete_position_category(request: Request, category_id: int) -> Any:
    """Delete one position category by id (tags removed by CASCADE)."""
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return error_response(503, POSTGRES_REQUIRED)
    reader = request.app.state.reader
    if reader.delete_position_category(category_id):
        return {"ok": True, "id": category_id}
    return error_response(500, "Failed to delete category.")


@router.patch("/executions/strategy-attribution")
def patch_execution_strategy_attribution(request: Request, body: Dict[str, Any] = Body(...)) -> Any:
    """Batch update strategy attribution on executions.
    body: account_id (required), contract_key OR execution_ids[], strategy_opportunity_id, strategy_instance_id."""
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return error_response(503, POSTGRES_REQUIRED)
    reader = request.app.state.reader
    b = body or {}
    account_id = (b.get("account_id") or "").strip()
    if not account_id:
        return error_response(400, "account_id is required.")
    contract_key = (b.get("contract_key") or "").strip() or None
    execution_ids = b.get("execution_ids")
    if isinstance(execution_ids, list):
        execution_ids = [int(x) for x in execution_ids if x is not None]
    else:
        execution_ids = None
    if not contract_key and not execution_ids:
        return error_response(400, "contract_key or execution_ids is required.")
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
        return error_response(
            409,
            "One or more executions have instance_allocations; clear or edit splits before batch attribution.",
            {"updated": 0},
        )
    if count > 0:
        return {"ok": True, "updated": count}
    # Core answers 0 both when nothing matched and when the UPDATE raised (it logs the
    # latter); nothing matching is the case a caller can reach, so 404.
    return error_response(404, "No matching executions found or update failed.", {"updated": 0})


@router.put("/position-categories/tag")
def put_position_category_tag(request: Request, body: Dict[str, Any] = Body(...)) -> Any:
    """Tag a position with a category (STK). Pass category_id null to clear tag. body: account_id, contract_key, category_id."""
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return error_response(503, POSTGRES_REQUIRED)
    reader = request.app.state.reader
    b = body or {}
    account_id = (b.get("account_id") or "").strip()
    contract_key = (b.get("contract_key") or "").strip()
    category_id = b.get("category_id")
    if not account_id:
        return error_response(400, "account_id is required.")
    if not contract_key:
        return error_response(400, "contract_key is required.")
    if category_id is not None:
        try:
            category_id = int(category_id)
        except (TypeError, ValueError):
            category_id = None
    if reader.set_position_category_tag(account_id, contract_key, category_id):
        return {"ok": True}
    return error_response(500, "Failed to set tag.")


@router.get("/position-categories/symbol-order")
def get_market_streams_symbol_order(request: Request) -> Dict[str, Any]:
    """Return category_name -> ordered list of symbols (Market Streams custom symbol order)."""
    reader = request.app.state.reader
    order = reader.get_market_streams_symbol_order()
    return {"ok": True, "order": order}


@router.put("/position-categories/symbol-order")
def put_market_streams_symbol_order(request: Request, body: Dict[str, Any] = Body(...)) -> Any:
    """Save symbol order for one category. body: category_name (required), symbols (array of symbol strings)."""
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return error_response(503, POSTGRES_REQUIRED)
    reader = request.app.state.reader
    b = body or {}
    category_name = (b.get("category_name") or "").strip()
    symbols = b.get("symbols")
    if not category_name:
        return error_response(400, "category_name is required.")
    if not isinstance(symbols, list):
        return error_response(400, "symbols must be an array.")
    if reader.set_market_streams_symbol_order(category_name, symbols):
        return {"ok": True}
    return error_response(500, "Failed to save symbol order.")


# --- Instrument class (core 0.27.0, trade design Rev .119) --------------------
# What kind of security a stock-like holding is: stock / fixed_income /
# cash_like, registered by the Owner once per instrument. Positions carry it as
# `instrument_class`; an unregistered instrument has none and reads as a stock.


@router.get("/instrument-classes")
def get_instrument_classes(request: Request) -> Dict[str, Any]:
    """Every registered instrument and its class."""
    reader = request.app.state.reader
    items = reader.list_instrument_classes()
    return list_body(items, ok=True)


@router.put("/instrument-classes/{contract_key}")
def put_instrument_class(request: Request, contract_key: str, body: Dict[str, Any] = Body(...)) -> Any:
    """Register or change one instrument's class. body: instrument_class, note (optional)."""
    if not request.app.state.control_via_db:
        return error_response(503, POSTGRES_REQUIRED)
    b = body or {}
    instrument_class = str(b.get("instrument_class") or "")
    # The same rule core's writer applies, checked first so a bad class is a 400
    # and anything the writer still refuses is a write failure.
    if normalize_instrument_class(instrument_class) is None:
        return error_response(400, f"instrument_class must be one of {', '.join(INSTRUMENT_CLASSES)}.")
    ok, err = request.app.state.reader.set_instrument_class(contract_key, instrument_class, note=b.get("note"))
    if ok:
        return {"ok": True}
    return _write_failed(err, "Failed to save the instrument class.")


@router.delete("/instrument-classes/{contract_key}")
def delete_instrument_class(request: Request, contract_key: str) -> Any:
    """Drop the registration; the instrument reads as a stock again."""
    if not request.app.state.control_via_db:
        return error_response(503, POSTGRES_REQUIRED)
    if request.app.state.reader.delete_instrument_class(contract_key):
        return {"ok": True}
    return error_response(500, "Failed to clear the instrument class.")
