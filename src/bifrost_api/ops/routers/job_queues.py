"""Ops job queue summary (Trade Celery retired — use Market Data Plugin ops_jobs)."""

from __future__ import annotations

import logging
from typing import Any, Dict

from fastapi import APIRouter, Request

logger = logging.getLogger(__name__)

router = APIRouter(tags=["ops-job-queues"])

_RETIRED_NOTE = (
    "Trade Celery (stocks_ib bars backfill) retired. "
    "Stock OHLC ingest: Market Data Plugin ops_jobs.job_ingest."
)


@router.get("/ops/jobs/queues/summary")
def ops_aggregated_job_queues_summary(request: Request) -> Dict[str, Any]:
    """Celery queue summary — empty after stocks_ib retirement."""
    del request
    return {"ok": True, "rows": [], "retired": True, "note": _RETIRED_NOTE}
