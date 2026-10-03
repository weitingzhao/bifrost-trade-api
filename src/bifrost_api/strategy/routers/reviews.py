"""`/strategies/reviews` — the trader's review of each strategy instance.

Review › Queue and Review › Single trade read the same rows (design Rev .110).
A review is a record, never an instruction: nothing downstream reads it to act
(D10).

    GET    /strategies/reviews                 every review
    PATCH  /strategies/reviews/{instance_id}   upsert one; fields left out are kept,
                                               `note: null` clears the note (TD-15)
    (PUT went in api 0.6.0 after a release marked replaced by PATCH; TD-15.)

    404  no such instance
    400  a bad tag, note or flag (PATCH; core's reason)
    503  Postgres is not configured for writes
"""

from __future__ import annotations

import logging
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request

from bifrost_api.common.write_errors import write_target
from bifrost_api.strategy.deps import read_config
from bifrost_api.strategy.patch_bodies import ReviewPatch
from bifrost_core.monitor.reader import trade_review as trade_review_module

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/strategies", tags=["trade-reviews"])


@router.get("/reviews")
def list_reviews_endpoint(request: Request) -> Dict[str, Any]:
    """Every review. A failed read is a 500 -- an empty list would say nothing
    has been reviewed, which is a statement about the book, not the query."""
    try:
        items = trade_review_module.list_reviews(read_config(request))
    except Exception as e:
        logger.warning("list_reviews failed: %s", e)
        raise HTTPException(status_code=500, detail="Failed to read trade reviews") from e
    return {"items": items, "count": len(items)}


@router.patch("/reviews/{strategy_instance_id}")
def patch_review_endpoint(request: Request, strategy_instance_id: int, body: ReviewPatch) -> Dict[str, Any]:
    """Write the fields sent; creates the review when the instance has none. Answers the
    review row. `reviewed: true` stamps it (the first stamp stays), `false` reopens it."""
    config = write_target(request, f"the review of strategy instance {strategy_instance_id}")
    return trade_review_module.patch_review(config, strategy_instance_id, body.patch_fields())
