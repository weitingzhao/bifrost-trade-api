"""Enumerate Celery task names — Trade Celery retired (stocks_ib bars backfill)."""

from __future__ import annotations

from typing import Any, Dict


def build_supported_tasks_payload(celery_app: Any) -> Dict[str, Any]:
    """Return empty task list after Trade Celery retirement."""
    del celery_app
    return {
        "ok": True,
        "tasks": [],
        "count": 0,
        "retired": True,
        "note": "Trade Celery retired; stock OHLC via Market Data Plugin ops_jobs.",
    }
