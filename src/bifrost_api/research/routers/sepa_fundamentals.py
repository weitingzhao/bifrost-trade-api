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
from bifrost_worker.data.massive.vendor.client import MassiveClient
from bifrost_worker.data.massive.vendor.config import get_massive_settings

router = APIRouter(tags=["research"])


class SepaFundamentalsRequest(BaseModel):
    symbols: List[str] = Field(default_factory=list)
    as_of_date: Optional[str] = None
    eps_q2q_threshold: float = 0.25
    rev_q2q_threshold: float = 0.25
    eps_3y_threshold: float = 0.15
    rev_3y_threshold: float = 0.15
    throttle_sec: float = 0.2


@router.post("/research/screening/sepa/fundamentals")
def run_sepa_fundamentals(body: SepaFundamentalsRequest, request: Request) -> Dict[str, Any]:
    reader = getattr(request.app.state, "reader", None)
    cfg = reader._config if reader else {}
    ms = get_massive_settings(cfg)
    if not ms.get("api_key"):
        return {"ok": False, "error": "Massive API key not configured", "results": [], "summary": {}}

    symbols = sorted({str(s or "").strip().upper() for s in body.symbols if str(s or "").strip()})
    if not symbols:
        return {"ok": False, "error": "symbols is required", "results": [], "summary": {}}
    if len(symbols) > 200:
        return {"ok": False, "error": "Too many symbols (max 200 per request).", "results": [], "summary": {}}

    client = MassiveClient(api_key=ms["api_key"])
    out = fetch_and_evaluate_fundamentals_batch(
        client,
        symbols,
        cfg=FundamentalsConfig(
            eps_q2q_threshold=float(body.eps_q2q_threshold),
            rev_q2q_threshold=float(body.rev_q2q_threshold),
            eps_3y_threshold=float(body.eps_3y_threshold),
            rev_3y_threshold=float(body.rev_3y_threshold),
        ),
        throttle_sec=max(0.0, float(body.throttle_sec)),
    )

    return {
        "ok": True,
        "as_of_date": body.as_of_date or date.today().isoformat(),
        "results": out.get("results", []),
        "summary": out.get("summary", {}),
        "warnings": out.get("warnings", {}),
        "rule_version": out.get("rule_version", FUNDAMENTALS_RULE_VERSION),
    }

