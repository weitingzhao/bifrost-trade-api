from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field

from bifrost_api.research.sepa.fundamentals_engine import (
    FUNDAMENTALS_RULE_VERSION,
    FundamentalsConfig,
    fetch_and_evaluate_fundamentals_batch,
)

router = APIRouter(tags=["research"])


class SepaFundamentalsRequest(BaseModel):
    symbols: List[str] = Field(default_factory=list)
    as_of_date: Optional[str] = None
    eps_q2q_threshold: float = 0.25
    rev_q2q_threshold: float = 0.25
    eps_3y_threshold: float = 0.15
    rev_3y_threshold: float = 0.15


@router.post("/research/screening/sepa/fundamentals")
def run_sepa_fundamentals(body: SepaFundamentalsRequest, request: Request) -> Dict[str, Any]:
    symbols = sorted({str(s or "").strip().upper() for s in body.symbols if str(s or "").strip()})
    if not symbols:
        return {"ok": False, "error": "symbols is required", "results": [], "summary": {}}
    if len(symbols) > 200:
        return {"ok": False, "error": "Too many symbols (max 200 per request).", "results": [], "summary": {}}

    db = request.app.state.control_via_db or getattr(request.app.state, "status_cfg_for_read", None)
    out = fetch_and_evaluate_fundamentals_batch(
        symbols,
        cfg=FundamentalsConfig(
            eps_q2q_threshold=float(body.eps_q2q_threshold),
            rev_q2q_threshold=float(body.rev_q2q_threshold),
            eps_3y_threshold=float(body.eps_3y_threshold),
            rev_3y_threshold=float(body.rev_3y_threshold),
        ),
        status_config=db,
    )

    return {
        "ok": True,
        "as_of_date": body.as_of_date or date.today().isoformat(),
        "results": out.get("results", []),
        "summary": out.get("summary", {}),
        "warnings": out.get("warnings", {}),
        "rule_version": out.get("rule_version", FUNDAMENTALS_RULE_VERSION),
    }

