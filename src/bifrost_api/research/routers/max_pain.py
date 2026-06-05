"""Report endpoints (Max Pain and related) — separate router per FASTAPI_PLAN FA-3."""

from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, Query, Request

from bifrost_api.research.routers.option_discovery import _db_config

router = APIRouter(prefix="/research", tags=["reports"])


@router.get("/max-pain")
def get_max_pain_report(
    request: Request,
    symbol: Optional[str] = Query(None, description="Underlying symbol filter"),
    expiry: Optional[str] = Query(None, description="Expiration YYYYMMDD filter"),
    trade_date_gte: Optional[str] = Query(None, description="Min trade_date YYYY-MM-DD"),
    trade_date_lte: Optional[str] = Query(None, description="Max trade_date YYYY-MM-DD"),
    limit: int = Query(100, ge=1, le=500),
) -> Dict[str, Any]:
    """Query report_option_max_pain_daily (Massive EOD batch)."""
    from bifrost_worker.data.massive.vendor.reader import get_report_max_pain_rows

    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured", "rows": []}
    rows = get_report_max_pain_rows(
        db,
        symbol=symbol,
        expiry=expiry,
        trade_date_gte=trade_date_gte,
        trade_date_lte=trade_date_lte,
        limit=limit,
    )
    return {"ok": True, "count": len(rows), "rows": rows}


@router.get("/max-pain/latest")
def get_max_pain_latest(
    request: Request,
    symbol: Optional[str] = Query(None, description="Optional underlying symbol filter"),
    limit: int = Query(80, ge=1, le=500),
) -> Dict[str, Any]:
    """Latest trade_date batch in report_option_max_pain_daily."""
    from bifrost_worker.data.massive.vendor.reader import get_report_max_pain_latest_batch

    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured", "trade_date": None, "rows": []}
    rows, td = get_report_max_pain_latest_batch(db, symbol=symbol, limit=limit)
    return {"ok": True, "trade_date": td, "count": len(rows), "rows": rows}


@router.get("/max-pain/compute")
def get_max_pain_compute(
    request: Request,
    symbol: str = Query(..., description="Underlying symbol"),
    expiry: str = Query(..., description="Expiration YYYYMMDD or YYYY-MM-DD"),
    trade_date: Optional[str] = Query(None, description="OI as-of date YYYY-MM-DD (default: latest in PG)"),
) -> Dict[str, Any]:
    """Real-time Max Pain from EOD OI (not from report_option_max_pain_daily)."""
    from bifrost_worker.data.massive.vendor.reader import compute_max_pain_live_from_db

    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}
    return compute_max_pain_live_from_db(db, symbol=symbol, expiry=expiry, trade_date=trade_date)


@router.get("/max-pain/compute/history")
def get_max_pain_compute_history(
    request: Request,
    symbol: str = Query(..., description="Underlying symbol"),
    expiry: str = Query(..., description="Expiration YYYYMMDD or YYYY-MM-DD"),
    lookback_days: int = Query(90, ge=7, le=365, description="Calendar days back from latest OI date"),
) -> Dict[str, Any]:
    """Per-trade-date max pain series recomputed from OI (no stored report rows)."""
    from bifrost_worker.data.massive.vendor.reader import compute_max_pain_history_from_db

    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured", "series": []}
    return compute_max_pain_history_from_db(db, symbol=symbol, expiry=expiry, lookback_days=lookback_days)
