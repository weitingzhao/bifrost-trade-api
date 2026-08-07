"""Max Pain report endpoints — report tables retired (Wave 7-C / D19=A).

Use Plugin ``/market/analytics/max-pain*`` instead.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, Query, Request

router = APIRouter(prefix="/research", tags=["reports"])

_GONE = (
    "report_option_max_pain_daily retired — use market-data plugin "
    "/market/analytics/max-pain* endpoints"
)


def _deprecated_payload(**extra: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "ok": False,
        "error": _GONE,
        "reason": "massive_report_retired",
        "message": _GONE,
    }
    out.update(extra)
    return out


@router.get("/max-pain")
def get_max_pain_report(
    request: Request,
    symbol: Optional[str] = Query(None, description="Underlying symbol filter"),
    expiry: Optional[str] = Query(None, description="Expiration YYYYMMDD filter"),
    trade_date_gte: Optional[str] = Query(None, description="Min trade_date YYYY-MM-DD"),
    trade_date_lte: Optional[str] = Query(None, description="Max trade_date YYYY-MM-DD"),
    limit: int = Query(100, ge=1, le=500),
) -> Dict[str, Any]:
    """Deprecated: report_option_max_pain_daily dropped."""
    _ = (request, symbol, expiry, trade_date_gte, trade_date_lte, limit)
    return _deprecated_payload(rows=[], count=0)


@router.get("/max-pain/latest")
def get_max_pain_latest(
    request: Request,
    symbol: Optional[str] = Query(None, description="Optional underlying symbol filter"),
    limit: int = Query(80, ge=1, le=500),
) -> Dict[str, Any]:
    """Deprecated: report_option_max_pain_daily dropped."""
    _ = (request, symbol, limit)
    return _deprecated_payload(trade_date=None, rows=[], count=0)


@router.get("/max-pain/compute")
def get_max_pain_compute(
    request: Request,
    symbol: str = Query(..., description="Underlying symbol"),
    expiry: str = Query(..., description="Expiration YYYYMMDD or YYYY-MM-DD"),
    trade_date: Optional[str] = Query(None, description="OI as-of date YYYY-MM-DD"),
) -> Dict[str, Any]:
    """Deprecated: use Plugin max-pain compute."""
    _ = (request, symbol, expiry, trade_date)
    return _deprecated_payload()


@router.get("/max-pain/compute/history")
def get_max_pain_compute_history(
    request: Request,
    symbol: str = Query(..., description="Underlying symbol"),
    expiry: str = Query(..., description="Expiration YYYYMMDD or YYYY-MM-DD"),
    lookback_days: int = Query(90, ge=7, le=365),
) -> Dict[str, Any]:
    """Deprecated: use Plugin max-pain history."""
    _ = (request, symbol, expiry, lookback_days)
    return _deprecated_payload(series=[])
