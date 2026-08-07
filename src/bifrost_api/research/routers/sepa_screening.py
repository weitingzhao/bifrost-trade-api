from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field

from bifrost_api.research.sepa.phase1_engine import Phase1Config, evaluate_phase1_batch
from bifrost_api.research.market_pg import get_stock_day_series_for_sepa

router = APIRouter(tags=["research"])


class SepaPhase1Request(BaseModel):
    symbols: List[str] = Field(default_factory=list)
    as_of_date: Optional[str] = None
    volume_threshold: float = 100000.0
    strict_sma200_rising: bool = False
    source: str = "massive"
    lookback_days: int = 400


def _db_config(request: Request) -> Optional[dict]:
    return request.app.state.control_via_db or getattr(request.app.state, "status_cfg_for_read", None)


@router.post("/research/screening/sepa/phase1")
def run_sepa_phase1(body: SepaPhase1Request, request: Request) -> Dict[str, Any]:
    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured", "results": [], "summary": {}}

    symbols = sorted({str(s or "").strip().upper() for s in body.symbols if str(s or "").strip()})
    if not symbols:
        return {"ok": False, "error": "symbols is required", "results": [], "summary": {}}
    if len(symbols) > 300:
        return {
            "ok": False,
            "error": "Too many symbols (max 300 per request for phase1).",
            "results": [],
            "summary": {},
        }

    rows_by_symbol = get_stock_day_series_for_sepa(
        db,
        symbols,
        lookback_days=body.lookback_days,
        source=body.source,
    )

    cfg = Phase1Config(
        volume_threshold=float(body.volume_threshold),
        strict_sma200_rising=bool(body.strict_sma200_rising),
    )
    out = evaluate_phase1_batch(rows_by_symbol, cfg=cfg)

    # Phase-1 currently evaluates on latest available bar per symbol; keep client-supplied
    # as_of_date for traceability but do not backdate computation yet.
    as_of = body.as_of_date
    if not as_of:
        as_of = date.today().isoformat()

    return {
        "ok": True,
        "as_of_date": as_of,
        "source": body.source,
        "results": out.get("results", []),
        "summary": out.get("summary", {}),
        "warnings": out.get("warnings", {}),
        "rule_version": out.get("rule_version", "sepa_phase1_v1"),
    }

