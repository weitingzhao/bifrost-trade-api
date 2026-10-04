"""POST / PUT bodies for the portfolio config router (TD-24, batch 3c-1).

Strict types, unknown fields ignored and logged this release, ``extra="forbid"``
next (:class:`~bifrost_api.common.request_bodies.LenientBody`). Fields are optional
at the model so the route's own 400s (``account_id is required.`` …) keep their
messages; a wrong type is 422. The PATCH bodies stay in the router (TD-15).
"""

from __future__ import annotations

from typing import List, Optional

from pydantic import StrictInt, StrictStr

from bifrost_api.common.request_bodies import LenientBody


class PositionCategoryBody(LenientBody):
    """POST /position-categories. ``name`` is required (400)."""

    name: Optional[StrictStr] = None
    description: Optional[StrictStr] = None
    sort_order: Optional[StrictInt] = None


class PositionTagBody(LenientBody):
    """PUT /position-categories/tag.

    ``category_id`` must be sent: an integer tags the position, ``null`` clears its tag
    (SharesBand's None). A string or a fraction is 422 and leaves the tag alone -- before
    0.3.1 it was read as null and deleted the tag. Left out it is 400, not a delete."""

    account_id: Optional[StrictStr] = None
    contract_key: Optional[StrictStr] = None
    category_id: Optional[StrictInt] = None


class SymbolOrderBody(LenientBody):
    """PUT /position-categories/symbol-order: replaces one category's order."""

    category_name: Optional[StrictStr] = None
    symbols: Optional[List[StrictStr]] = None


class InstrumentClassBody(LenientBody):
    """PUT /instrument-classes/{contract_key}. ``instrument_class`` is checked by the
    route (400 naming the classes)."""

    instrument_class: Optional[StrictStr] = None
    note: Optional[StrictStr] = None
