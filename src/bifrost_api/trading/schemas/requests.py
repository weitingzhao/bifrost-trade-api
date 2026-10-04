"""POST / PUT bodies for the executions router (TD-24, batch 3c-1).

Strict types and no unknown fields (:class:`~bifrost_api.common.request_bodies.StrictBody`,
``extra="forbid"`` since api 0.9.0: an undeclared field is a 422). Fields are optional
at the model so the route's and core's own 400s keep their messages; a wrong type is
422 and nothing is written -- before 0.3.1 a strategy id that was not a number was
stored as null, clearing the fill's attribution. Prices, quantities and amounts are
numbers (an integer is a number); ids are integers; ``time`` / ``exec_time`` are Unix
seconds. ``PATCH /executions/{id}/attribution`` keeps its TD-15 body in the router.
"""

from __future__ import annotations

from typing import Any, List, Optional

from pydantic import StrictFloat, StrictInt, StrictStr

from bifrost_api.common.request_bodies import StrictBody, StrictItem


class FillSplitItem(StrictItem):
    """One split of a fill across trades; the splits must sum to the fill's quantity (core, 400)."""

    trade_id: Optional[StrictInt] = None
    quantity: Optional[StrictFloat] = None


class _ExecutionFields(StrictBody):
    account_id: Optional[StrictStr] = None
    symbol: Optional[StrictStr] = None
    sec_type: Optional[StrictStr] = None
    side: Optional[StrictStr] = None
    # Signed: sells negative.
    quantity: Optional[StrictFloat] = None
    price: Optional[StrictFloat] = None
    # manual (default) | journal_closed (writes the journal table: the performance book).
    source: Optional[StrictStr] = None
    expiry: Optional[StrictStr] = None
    strike: Optional[StrictFloat] = None
    option_right: Optional[StrictStr] = None
    contract_key: Optional[StrictStr] = None
    exchange: Optional[StrictStr] = None
    order_id: Optional[StrictInt] = None
    cum_qty: Optional[StrictFloat] = None
    commission: Optional[StrictFloat] = None
    realized_pnl: Optional[StrictFloat] = None
    currency: Optional[StrictStr] = None
    # Direct attribution (trade_id), or fill_splits -- not both. The old names
    # strategy_instance_id / instance_allocations went in naming R4 (api 0.9.0): sent now they
    # are unknown fields, a 422 like any other (TD-24).
    strategy_opportunity_id: Optional[StrictInt] = None
    trade_id: Optional[StrictInt] = None
    fill_splits: Optional[List[FillSplitItem]] = None

    def sends_splits(self) -> bool:
        """Whether non-empty splits were sent."""
        return bool(self.fill_splits)


class ExecutionCreateBody(_ExecutionFields):
    """POST /executions: one fill written by hand. ``quantity`` and ``price`` are required (400)."""

    # Unix seconds; now when absent.
    time: Optional[StrictFloat] = None
    # Generated (manual_<uuid>) when absent.
    exec_id: Optional[StrictStr] = None
    # Stored as JSON text.
    raw_extra: Optional[Any] = None


class ExecutionUpdateBody(_ExecutionFields):
    """PUT /executions/{id}: changes the fields sent (a merge; only the attribution has a PATCH)."""

    # Unix seconds. ``time`` is read when ``exec_time`` is absent.
    exec_time: Optional[StrictFloat] = None
    time: Optional[StrictFloat] = None


class OptionStockLinkBody(StrictBody):
    """POST /executions/option-stock-links. ``account_id`` and both ids are required (core, 400)."""

    account_id: Optional[StrictStr] = None
    option_account_executions_id: Optional[StrictInt] = None
    stock_account_executions_id: Optional[StrictInt] = None
    # exercise | assignment | omitted.
    role: Optional[StrictStr] = None
    note: Optional[StrictStr] = None


class OptionStockLinkBatch(StrictItem):
    account_id: Optional[StrictStr] = None
    option_account_executions_ids: Optional[List[StrictInt]] = None


class OptionStockLinksQueryBody(StrictBody):
    """POST /executions/option-stock-links/query. ``batches`` is required (400); a batch
    without an account or ids is skipped."""

    batches: Optional[List[OptionStockLinkBatch]] = None
