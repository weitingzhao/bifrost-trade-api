"""R-M8: GET /portfolio/model-analysis — portfolio-level model analysis (payoff, CAR, Greeks, stress)."""

from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Query, Request

from bifrost_core.portfolio.services.portfolio import run_model_analysis_for_account

router = APIRouter(tags=["portfolio-model"])


@router.get("/portfolio/model-analysis")
def get_portfolio_model_analysis(
    request: Request,
    account_id: str = Query(..., description="IB account ID to analyse"),
) -> Dict[str, Any]:
    """Compute and return model analysis for one account's current positions (R-M8 V1)."""
    reader = request.app.state.reader
    result = run_model_analysis_for_account(reader, account_id)
    if result is None:
        raise HTTPException(status_code=503, detail="Database unavailable")
    return result
