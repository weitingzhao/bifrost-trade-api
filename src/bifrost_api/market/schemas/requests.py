"""POST body for the watchlist router (TD-24, batch 3c-1).

Strict types (it took lax ones: ``"450"`` was a strike) and no unknown fields
(:class:`~bifrost_api.common.request_bodies.StrictBody`, ``extra="forbid"`` since api
0.9.0: an undeclared field is a 422). The PATCH body stays in the router (TD-15).
"""

from __future__ import annotations

from typing import Optional

from pydantic import StrictBool, StrictFloat, StrictInt, StrictStr

from bifrost_api.common.request_bodies import StrictBody


class WatchlistBody(StrictBody):
    """POST /watchlist: add a contract, or change the fields sent on one already watched.

    An explicit null clears (``category_id: null`` is the None list); ``optionable:
    null`` and a blank string count as not sent (see the router's ``_post_fields``)."""

    contract_key: StrictStr
    symbol: Optional[StrictStr] = None
    sec_type: Optional[StrictStr] = None
    expiry: Optional[StrictStr] = None
    strike: Optional[StrictFloat] = None
    option_right: Optional[StrictStr] = None
    display_label: Optional[StrictStr] = None
    source: Optional[StrictStr] = None
    category_id: Optional[StrictInt] = None
    optionable: Optional[StrictBool] = None
