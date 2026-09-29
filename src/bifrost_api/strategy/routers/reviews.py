"""`/strategies/reviews` — the trader's review of each strategy instance.

Review › Queue and Review › Single trade read the same rows (design Rev .110).
A review is a record, never an instruction: nothing downstream reads it to act
(D10).

    GET  /strategies/reviews                 every review
    PUT  /strategies/reviews/{instance_id}   upsert one; fields left out are kept

    404  no such instance
    503  Postgres is not configured for writes
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import psycopg2
from fastapi import APIRouter, HTTPException, Request

from bifrost_core.monitor.reader import trade_review as trade_review_module
from bifrost_core.monitor.schemas.trade_reviews import TradeReviewBody

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/strategies", tags=["trade-reviews"])


def _read_config(request: Request) -> Optional[dict]:
    return getattr(request.app.state, "status_cfg_for_read", None)


def _write_config(request: Request) -> dict:
    control_via_db = getattr(request.app.state, "control_via_db", None)
    if not control_via_db:
        raise HTTPException(status_code=503, detail="Database control not configured")
    return control_via_db


@router.get("/reviews")
def list_reviews_endpoint(request: Request) -> Dict[str, Any]:
    """Every review. A failed read is a 500 -- an empty list would say nothing
    has been reviewed, which is a statement about the book, not the query."""
    try:
        items = trade_review_module.list_reviews(_read_config(request))
    except Exception as e:
        logger.warning("list_reviews failed: %s", e)
        raise HTTPException(status_code=500, detail="Failed to read trade reviews") from e
    return {"items": items, "count": len(items)}


@router.put("/reviews/{strategy_instance_id}")
def save_review_endpoint(
    request: Request, strategy_instance_id: int, body: TradeReviewBody
) -> Dict[str, Any]:
    """Write one instance's review. `reviewed: true` confirms it, `false` reopens it."""
    config = _write_config(request)
    try:
        row = trade_review_module.save_review(
            config, strategy_instance_id, body.model_dump(exclude_unset=True)
        )
    except psycopg2.errors.ForeignKeyViolation as e:
        raise HTTPException(status_code=404, detail="Strategy instance not found") from e
    except Exception as e:
        logger.warning("save_review failed: %s", e)
        raise HTTPException(status_code=500, detail="Failed to save trade review") from e
    if row is None:
        raise HTTPException(status_code=503, detail="Database control not configured")
    return row
