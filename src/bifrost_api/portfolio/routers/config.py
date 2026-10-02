"""Portfolio config: position categories, tags, symbol order, instrument classes.

Failures answer a real status with ``{"detail", "ok": false, "error", ...}`` and
lists answer ``{"items", "count", ...}`` (``bifrost_api.common.envelopes``, TD-16/17).

PATCH and DELETE (TD-15, batch 3b-2) call core's TD-15 writers, whose Write*
outcomes are mapped once in ``bifrost_api.common.write_errors``: PATCH changes
only the fields sent (null clears a nullable column) and answers the row; DELETE
is strict (404 for a missing row) and answers ``{"deleted": "hard", ..., "ok": true}``.

POST / PUT bodies (TD-24, batch 3c-1) are ``portfolio.schemas.requests`` models: a
wrong type is 422 and writes nothing (a malformed ``category_id`` no longer clears a
tag); unknown fields are ignored and logged this release.
"""

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Request
from pydantic import StrictInt, StrictStr

from bifrost_api.common.envelopes import error_response, list_body
from bifrost_api.common.write_errors import PatchBody, deleted_body, write_target
from bifrost_api.portfolio.schemas.requests import (
    InstrumentClassBody,
    PositionCategoryBody,
    PositionTagBody,
    StrategyAttributionBatchBody,
    SymbolOrderBody,
)
from bifrost_core.portfolio.reader import instrument_class as instrument_class_module
from bifrost_core.portfolio.reader import position_categories as position_categories_module
from bifrost_core.portfolio.reader.instrument_class import INSTRUMENT_CLASSES, normalize_instrument_class

logger = logging.getLogger(__name__)

router = APIRouter(tags=["portfolio-config"])


class PositionCategoryPatch(PatchBody):
    name: Optional[StrictStr] = None
    description: Optional[StrictStr] = None
    sort_order: Optional[StrictInt] = None


class InstrumentClassPatch(PatchBody):
    instrument_class: Optional[StrictStr] = None
    note: Optional[StrictStr] = None

POSTGRES_REQUIRED = "Postgres required."
# The reader's answers when it had no connection (core monitor/reader/common.py,
# portfolio/reader/position_categories.py, instrument_class.py); input was checked before the call.
_NO_CONNECTION = frozenset(
    {"Database connection failed.", "No database connection.", "Invalid name or no database connection."}
)


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
def post_position_category(request: Request, body: PositionCategoryBody) -> Any:
    """Create one position category. body: name (required), description, sort_order (an integer)."""
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return error_response(503, POSTGRES_REQUIRED, {"id": None})
    reader = request.app.state.reader
    name = (body.name or "").strip()
    if not name:
        return error_response(400, "name is required.", {"id": None})
    gid, err = reader.create_position_category(
        name=name,
        description=body.description,
        sort_order=body.sort_order,
    )
    if gid is not None:
        return {"ok": True, "id": gid, "name": name}
    return _write_failed(err, "Failed to create category.", {"id": None})


@router.patch("/position-categories/{category_id:int}")
def patch_position_category(request: Request, category_id: int, body: PositionCategoryPatch) -> Any:
    """Change name / description / sort_order; `null` clears description or sort_order.
    Answers the category row plus `ok: true` (SharesBand reads `ok`; it goes next release)."""
    config = write_target(request, f"position category {category_id}")
    row = position_categories_module.patch_position_category(config, category_id, body.patch_fields())
    return {**row, "ok": True}


@router.delete("/position-categories/{category_id:int}")
def delete_position_category(request: Request, category_id: int) -> Any:
    """Hard delete; its tags go with it (CASCADE) and watchlist rows in it become uncategorized."""
    config = write_target(request, f"position category {category_id}")
    return deleted_body(position_categories_module.delete_position_category_strict(config, category_id))


@router.patch("/executions/strategy-attribution")
def patch_execution_strategy_attribution(request: Request, body: StrategyAttributionBatchBody) -> Any:
    """Batch update strategy attribution on executions.
    body: account_id (required), contract_key OR execution_ids[], strategy_opportunity_id, strategy_instance_id
    (null or absent clears; a non-integer is 422)."""
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return error_response(503, POSTGRES_REQUIRED)
    reader = request.app.state.reader
    account_id = (body.account_id or "").strip()
    if not account_id:
        return error_response(400, "account_id is required.")
    contract_key = (body.contract_key or "").strip() or None
    execution_ids = list(body.execution_ids) if body.execution_ids else None
    if not contract_key and not execution_ids:
        return error_response(400, "contract_key or execution_ids is required.")
    count = reader.batch_update_execution_strategy(
        account_id, contract_key, execution_ids, body.strategy_opportunity_id, body.strategy_instance_id
    )
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
def put_position_category_tag(request: Request, body: PositionTagBody) -> Any:
    """Tag a position with a category (STK). body: account_id, contract_key, category_id --
    an integer tags, an explicit null clears the tag; a non-integer is 422 and left out is 400,
    so neither can clear a tag by accident."""
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return error_response(503, POSTGRES_REQUIRED)
    reader = request.app.state.reader
    account_id = (body.account_id or "").strip()
    contract_key = (body.contract_key or "").strip()
    if not account_id:
        return error_response(400, "account_id is required.")
    if not contract_key:
        return error_response(400, "contract_key is required.")
    if "category_id" not in body.model_fields_set:
        return error_response(400, "category_id is required: an id tags the position, null clears its tag.")
    if reader.set_position_category_tag(account_id, contract_key, body.category_id):
        return {"ok": True}
    return error_response(500, "Failed to set tag.")


@router.get("/position-categories/symbol-order")
def get_market_streams_symbol_order(request: Request) -> Dict[str, Any]:
    """Return category_name -> ordered list of symbols (Market Streams custom symbol order)."""
    reader = request.app.state.reader
    order = reader.get_market_streams_symbol_order()
    return {"ok": True, "order": order}


@router.put("/position-categories/symbol-order")
def put_market_streams_symbol_order(request: Request, body: SymbolOrderBody) -> Any:
    """Save symbol order for one category. body: category_name (required), symbols (array of symbol strings)."""
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return error_response(503, POSTGRES_REQUIRED)
    reader = request.app.state.reader
    category_name = (body.category_name or "").strip()
    if not category_name:
        return error_response(400, "category_name is required.")
    if body.symbols is None:
        return error_response(400, "symbols must be an array.")
    if reader.set_market_streams_symbol_order(category_name, list(body.symbols)):
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
def put_instrument_class(request: Request, contract_key: str, body: InstrumentClassBody) -> Any:
    """Register or change one instrument's class. body: instrument_class, note (optional)."""
    if not request.app.state.control_via_db:
        return error_response(503, POSTGRES_REQUIRED)
    instrument_class = body.instrument_class or ""
    # The same rule core's writer applies, checked first so a bad class is a 400
    # and anything the writer still refuses is a write failure.
    if normalize_instrument_class(instrument_class) is None:
        return error_response(400, f"instrument_class must be one of {', '.join(INSTRUMENT_CLASSES)}.")
    ok, err = request.app.state.reader.set_instrument_class(contract_key, instrument_class, note=body.note)
    if ok:
        return {"ok": True}
    return _write_failed(err, "Failed to save the instrument class.")


@router.patch("/instrument-classes/{contract_key}")
def patch_instrument_class(request: Request, contract_key: str, body: InstrumentClassPatch) -> Any:
    """Change a registered instrument's class or note (`note: null` clears it, which PUT
    cannot). 404 when the instrument has no registration -- PUT registers one."""
    config = write_target(request, f"the instrument class of {contract_key}")
    return instrument_class_module.patch_instrument_class(config, contract_key, body.patch_fields())


@router.delete("/instrument-classes/{contract_key}")
def delete_instrument_class(request: Request, contract_key: str) -> Any:
    """Drop the registration; the instrument reads as a stock again. 404 when none."""
    config = write_target(request, f"the instrument class of {contract_key}")
    return deleted_body(instrument_class_module.delete_instrument_class_strict(config, contract_key))
