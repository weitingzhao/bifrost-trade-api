"""`/trades` and `/trade-reviews` -- the Trade under its own name (naming program R1, api 0.7.0).

A Trade is a position opened under the rules (table ``trade`` since naming R3, core
0.45.0). Decision D5-A: the Trade does not nest under ``/strategies``, so that
``/strategies/*`` holds only the rule chain (dims, templates, structures, opportunities,
allocations, plans).

    GET    /trades                    the trades (filters: account_id, strategy_opportunity_id,
                                      trade_ids, from_ts / to_ts on opened_at)
    GET    /trades/win-rate           per-structure win rate over trades
    GET    /trades/{trade_id}         one trade (404)
    POST   /trades                    open one: {strategy_opportunity_id, account_id, opened_at, label?}
    PATCH  /trades/{trade_id}         label / opened_at / created_at
    DELETE /trades/{trade_id}         strict delete (409 while fills are attributed or split to it)
    GET    /trade-reviews             every review
    PATCH  /trade-reviews/{trade_id}  upsert one review

A trade's notes live in the Research journal (TD-73): ``notes`` on a trade and ``note`` on a
review are a 422 since api 0.7.1 (core 0.43.0 no longer has the columns' readers or writers).

Rows carry ``trade_id`` (and reviews ``tags_added_json`` / ``tags_dropped_json``) only. Naming
R4 (api 0.9.0, core 0.47.0) removed what R1 kept one version: ``strategy_instance_id`` beside
``trade_id``, the old routes ``/strategies/instances…``, ``/strategies/win-rate`` and
``/strategies/reviews…`` (their handlers live here), and the query name ``strategy_instance_ids``.

A review is a record, never an instruction: nothing downstream reads it to act (D10).
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from bifrost_core.monitor.reader import strategy_instance as strategy_instance_module
from bifrost_core.monitor.reader import trade_review as trade_review_module
from bifrost_core.monitor.services.strategy_parsing import (
    parse_opened_at_to_unix,
    parse_strategy_instance_ids_csv,
)
from fastapi import APIRouter, HTTPException, Query, Request

from bifrost_api.common.envelopes import list_body
from bifrost_api.common.query_vocab import from_ts_query, to_ts_query, trade_ids_query
from bifrost_api.common.write_errors import deleted_body, write_target
from bifrost_api.strategy.deps import read_config, write_config
from bifrost_api.strategy.patch_bodies import ReviewPatch, TradeCreate, TradePatch
from bifrost_api.strategy.schemas.responses import TradeList, TradeRow

logger = logging.getLogger(__name__)

router = APIRouter(tags=["trades"])


# --- the work -------------------------------------------------------------------------------


def list_trades(
    request: Request,
    account_id: Optional[str],
    strategy_opportunity_id: Optional[int],
    trade_ids: Optional[str],
    from_ts: Optional[float],
    to_ts: Optional[float],
) -> Dict[str, Any]:
    reader = request.app.state.reader
    try:
        ids = parse_strategy_instance_ids_csv(trade_ids)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    items: List[Dict[str, Any]] = reader.list_trades(
        account_id=account_id,
        strategy_opportunity_id=strategy_opportunity_id,
        trade_ids=ids,
        opened_at_from=from_ts,
        opened_at_until=to_ts,
    )
    return list_body(items)


def get_trade(request: Request, trade_id: int) -> Dict[str, Any]:
    row = request.app.state.reader.get_trade_by_id(trade_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Trade not found")
    return row


def create_trade(request: Request, body: TradeCreate) -> Dict[str, Any]:
    reader = request.app.state.reader
    write_config(request)  # 503 without Postgres; the reader does the write
    try:
        opened_at_val = parse_opened_at_to_unix(body.opened_at)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    sid = reader.create_trade(
        strategy_opportunity_id=body.strategy_opportunity_id,
        account_id=body.account_id.strip(),
        opened_at=opened_at_val,
        label=body.label.strip() if body.label else None,
    )
    if sid is None:
        raise HTTPException(status_code=500, detail="Failed to create trade")
    return {"trade_id": sid}


def patch_trade(request: Request, trade_id: int, body: TradePatch) -> Dict[str, Any]:
    config = write_target(request, f"trade {trade_id}")
    return strategy_instance_module.patch_instance(config, trade_id, body.patch_fields())


def delete_trade(request: Request, trade_id: int) -> Dict[str, Any]:
    config = write_target(request, f"trade {trade_id}")
    return deleted_body(strategy_instance_module.delete_instance_strict(config, trade_id))


def trade_win_rate(request: Request, from_ts: Optional[float], to_ts: Optional[float]) -> Dict[str, Any]:
    return request.app.state.reader.get_trade_win_rate(since_ts=from_ts, until_ts=to_ts)


def patch_trade_review(request: Request, trade_id: int, body: ReviewPatch) -> Dict[str, Any]:
    config = write_target(request, f"the review of trade {trade_id}")
    return trade_review_module.patch_review(config, trade_id, body.patch_fields())


# --- routes ---------------------------------------------------------------------------------

_OPENED = "the trade's opened_at"
_COUNTED = "the time of the fills counted toward each trade"


@router.get("/trades", response_model=TradeList, response_model_exclude_unset=True)
def list_trades_endpoint(
    request: Request,
    account_id: Optional[str] = Query(None, description="Filter by account ID"),
    strategy_opportunity_id: Optional[int] = Query(None, description="Filter by strategy opportunity ID"),
    trade_ids: Optional[str] = trade_ids_query(),
    from_ts: Optional[float] = from_ts_query(_OPENED),
    to_ts: Optional[float] = to_ts_query(_OPENED),
) -> Dict[str, Any]:
    """The trades, newest opened first, each with ``executions_count`` (fills attributed or split to it)."""
    return list_trades(request, account_id, strategy_opportunity_id, trade_ids, from_ts, to_ts)


# Declared before /trades/{trade_id}: the path converter would refuse "win-rate" anyway.
@router.get("/trades/win-rate")
def trade_win_rate_endpoint(
    request: Request,
    from_ts: Optional[float] = from_ts_query(_COUNTED),
    to_ts: Optional[float] = to_ts_query(_COUNTED),
) -> Dict[str, Any]:
    """Per-structure win-rate rows and ``totals_all`` (all trades combined); ``total_trades``
    counts trades. ``total_profit`` / ``total_loss`` sum the execution-derived Net PnL of the
    trades with a positive / negative net."""
    return trade_win_rate(request, from_ts, to_ts)


@router.get("/trades/{trade_id:int}", response_model=TradeRow, response_model_exclude_unset=True)
def get_trade_endpoint(request: Request, trade_id: int) -> Dict[str, Any]:
    """One trade. 404 if there is none."""
    return get_trade(request, trade_id)


@router.post("/trades")
def create_trade_endpoint(request: Request, body: TradeCreate) -> Dict[str, Any]:
    """Open a trade: strategy_opportunity_id, account_id, opened_at (ISO 8601 or Unix seconds),
    label?. Answers ``{trade_id}``. ``notes`` is a 422
    (TD-73): a trade's notes live in the Research journal."""
    return create_trade(request, body)


@router.patch("/trades/{trade_id:int}", response_model=TradeRow, response_model_exclude_unset=True)
def patch_trade_endpoint(request: Request, trade_id: int, body: TradePatch) -> Dict[str, Any]:
    """Change label / opened_at / created_at; ``null`` clears label. Answers the trade as
    GET /trades/{trade_id} does. ``notes`` is a 422 (TD-73, Research journal)."""
    return patch_trade(request, trade_id, body)


@router.delete("/trades/{trade_id:int}")
def delete_trade_endpoint(request: Request, trade_id: int) -> Dict[str, Any]:
    """Hard delete. 409 while fills are split to it or attributed to it, while a plan was
    filled by it or while it has a review (both RESTRICT since core 0.41.0, TD-43);
    503 / 500 when that cannot be checked -- nothing is deleted blind."""
    return delete_trade(request, trade_id)


@router.get("/trade-reviews")
def list_trade_reviews_endpoint(request: Request) -> Dict[str, Any]:
    """Every review; rows carry ``trade_id`` and ``tags_added_json`` / ``tags_dropped_json``.
    A failed read is a 500 -- an empty list would say nothing has been reviewed, which is a
    statement about the book, not the query."""
    try:
        items = trade_review_module.list_reviews(read_config(request))
    except Exception as e:
        logger.warning("list_reviews failed: %s", e)
        raise HTTPException(status_code=500, detail="Failed to read trade reviews") from e
    return {"items": items, "count": len(items)}


@router.patch("/trade-reviews/{trade_id:int}")
def patch_trade_review_endpoint(request: Request, trade_id: int, body: ReviewPatch) -> Dict[str, Any]:
    """Write the fields sent; creates the review when the trade has none. ``reviewed: true``
    stamps it (the first stamp stays), ``false`` reopens it. 404 when there is no such trade.
    ``note`` is a 422 (TD-73): a trade's notes live in the Research journal."""
    return patch_trade_review(request, trade_id, body)
