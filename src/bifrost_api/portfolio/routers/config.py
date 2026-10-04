"""Portfolio config: position categories, tags, symbol order, instrument classes.

Failures answer a real status with ``{"detail"}`` and
lists answer ``{"items", "count", ...}`` (``bifrost_api.common.envelopes``, TD-16/17).

PATCH and DELETE (TD-15, batch 3b-2) call core's TD-15 writers, whose Write*
outcomes are mapped once in ``bifrost_api.common.write_errors``: PATCH changes
only the fields sent (null clears a nullable column) and answers the row; DELETE
is strict (404 for a missing row) and answers ``{"deleted": "hard", ..., "ok": true}``.
POST and PUT call core's strict twins too since api 0.9.0 (core 0.47.0, TD-80 C2):
input core refuses is 400, a name in use 409, no Postgres 503, a failed statement
500 -- the same answers PATCH / DELETE give -- and ``ok: true`` stays beside what
was written.

POST / PUT bodies (TD-24, batch 3c-1) are ``portfolio.schemas.requests`` models: a
wrong type is 422 and writes nothing (a malformed ``category_id`` no longer clears a
tag); an unknown field is a 422 too (api 0.9.0).
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
    SymbolOrderBody,
)
from bifrost_core.portfolio.reader import instrument_class as instrument_class_module
from bifrost_core.portfolio.reader import position_categories as position_categories_module

logger = logging.getLogger(__name__)

router = APIRouter(tags=["portfolio-config"])


class PositionCategoryPatch(PatchBody):
    name: Optional[StrictStr] = None
    description: Optional[StrictStr] = None
    sort_order: Optional[StrictInt] = None


class InstrumentClassPatch(PatchBody):
    instrument_class: Optional[StrictStr] = None
    note: Optional[StrictStr] = None


def _with_category_id(row: Any) -> Any:
    """A category row with its key as ``category_id`` (TD-57; ``id`` dropped in api 0.6.12, TD-56).

    The table's key is ``id``, but the path (``/position-categories/{category_id}``), the
    tag body and every referencing column call it ``category_id``, so the API says that one
    name everywhere. api 0.6.7 sent both for one release; the frontend reads only ``category_id``."""
    if isinstance(row, dict) and "id" in row:
        out = {k: v for k, v in row.items() if k != "id"}
        out.setdefault("category_id", row["id"])
        return out
    return row


@router.get("/position-categories")
def get_position_categories(request: Request) -> Dict[str, Any]:
    """Return all position_categories rows (for dropdown and manage UI), keyed ``category_id``."""
    reader = request.app.state.reader
    items = reader.get_position_categories()
    return list_body([_with_category_id(r) for r in items or []])


@router.post("/position-categories")
def post_position_category(request: Request, body: PositionCategoryBody) -> Any:
    """Create one position category. body: name (required), description, sort_order (an integer).
    A name already in use is 409; ``Uncategorized`` (any case) is reserved for positions without
    a category, 400 (core 0.41.0, TD-56); a blank description is 400 (leave it out). Answers the
    category row (keyed ``category_id``) plus ``ok: true``."""
    config = write_target(request, "a position category")
    row = position_categories_module.create_position_category_strict(
        config, body.name, description=body.description, sort_order=body.sort_order
    )
    return {**_with_category_id(row), "ok": True}


@router.patch("/position-categories/{category_id:int}")
def patch_position_category(request: Request, category_id: int, body: PositionCategoryPatch) -> Any:
    """Change name / description / sort_order; `null` clears description or sort_order.
    Answers the category row plus `ok: true` (SharesBand reads `ok`; it goes next release).
    A new name carries the category's Market Streams symbol order with it; a name in use is
    409, ``Uncategorized`` is 400 (core 0.41.0, TD-56)."""
    config = write_target(request, f"position category {category_id}")
    row = position_categories_module.patch_position_category(config, category_id, body.patch_fields())
    return {**_with_category_id(row), "ok": True}


@router.delete("/position-categories/{category_id:int}")
def delete_position_category(request: Request, category_id: int) -> Any:
    """Hard delete; its tags go with it (CASCADE), watchlist rows in it become uncategorized and its
    Market Streams symbol order is removed (``symbol_order_removed``, core 0.41.0)."""
    config = write_target(request, f"position category {category_id}")
    return deleted_body(position_categories_module.delete_position_category_strict(config, category_id))


@router.put("/position-categories/tag")
def put_position_category_tag(request: Request, body: PositionTagBody) -> Any:
    """Tag a position with a category (STK). body: account_id, contract_key, category_id --
    an integer tags, an explicit null clears the tag; a non-integer is 422 and left out is 400,
    so neither can clear a tag by accident. A category that does not exist is 400. Answers
    ``{account_id, contract_key, category_id, cleared, ok: true}``."""
    config = write_target(request, "a position category tag")
    if "category_id" not in body.model_fields_set:
        return error_response(400, "category_id is required: an id tags the position, null clears its tag.")
    result = position_categories_module.set_position_category_tag_strict(
        config, body.account_id, body.contract_key, body.category_id
    )
    return {**result, "ok": True}


@router.get("/position-categories/symbol-order")
def get_market_streams_symbol_order(request: Request) -> Dict[str, Any]:
    """Return category_name -> ordered list of symbols (Market Streams custom symbol order)."""
    reader = request.app.state.reader
    order = reader.get_market_streams_symbol_order()
    return {"ok": True, "order": order}


@router.put("/position-categories/symbol-order")
def put_market_streams_symbol_order(request: Request, body: SymbolOrderBody) -> Any:
    """Save symbol order for one category. body: category_name (required), symbols (array of symbol
    strings, ``[]`` empties it). A blank or repeated symbol is 400 and nothing changes. Answers
    ``{category_name, symbols, ok: true}``."""
    config = write_target(request, "a symbol order")
    result = position_categories_module.set_market_streams_symbol_order_strict(
        config, body.category_name, body.symbols
    )
    return {**result, "ok": True}


# --- Instrument class (core 0.27.0, trade design Rev .119) --------------------
# What kind of security a stock-like holding is: stock / fixed_income /
# cash_like, registered by the Owner once per instrument. Positions carry it as
# `instrument_class`; an unregistered instrument has none and reads as a stock.


@router.get("/instrument-classes")
def get_instrument_classes(request: Request) -> Dict[str, Any]:
    """Every registered instrument and its class."""
    reader = request.app.state.reader
    items = reader.list_instrument_classes()
    return list_body(items)


@router.put("/instrument-classes/{contract_key}")
def put_instrument_class(request: Request, contract_key: str, body: InstrumentClassBody) -> Any:
    """Register or replace one instrument's class: a full replace since api 0.6.0 (TD-15),
    so the row becomes exactly what is sent and no ``note`` clears a stored one.
    PATCH changes the fields sent and keeps the rest. body: instrument_class, note (optional; blank
    is 400). A class other than stock / fixed_income / cash_like is 400. Answers the row plus
    ``ok: true``."""
    config = write_target(request, f"the instrument class of {contract_key}")
    row = instrument_class_module.set_instrument_class_strict(
        config, contract_key, body.instrument_class, note=body.note
    )
    return {**row, "ok": True}


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
