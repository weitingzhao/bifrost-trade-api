from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field

from bifrost_api.research.sepa.crs_engine import compute_crs_scores
from bifrost_api.research.market_pg import get_stock_day_close_series_for_crs

router = APIRouter(tags=["research"])


class SepaCrsRequest(BaseModel):
    symbols: List[str] = Field(default_factory=list)
    as_of_date: Optional[str] = None
    source: str = "massive"
    lookback_days: int = 420
    min_crs: Optional[float] = None


def _db_config(request: Request) -> Optional[dict]:
    return request.app.state.control_via_db or getattr(request.app.state, "status_cfg_for_read", None)


@router.post("/research/screening/sepa/crs")
def run_sepa_crs(body: SepaCrsRequest, request: Request) -> Dict[str, Any]:
    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured", "results": [], "summary": {}}

    symbols = sorted({str(s or "").strip().upper() for s in body.symbols if str(s or "").strip()})
    if not symbols:
        return {"ok": False, "error": "symbols is required", "results": [], "summary": {}}
    if len(symbols) > 300:
        return {"ok": False, "error": "Too many symbols (max 300).", "results": [], "summary": {}}

    rows_by_symbol = get_stock_day_close_series_for_crs(
        db,
        symbols,
        lookback_days=body.lookback_days,
        source=body.source,
    )
    out = compute_crs_scores(
        rows_by_symbol,
        as_of_date=body.as_of_date,
        lookback=252,
        min_crs=body.min_crs,
    )
    return {
        "ok": True,
        "as_of_date": body.as_of_date or date.today().isoformat(),
        "source": body.source,
        "crs_version": out.get("crs_version", "crs_v1_ret252_pct"),
        "results": out.get("results", []),
        "summary": out.get("summary", {}),
        "warnings": out.get("warnings", {}),
    }

