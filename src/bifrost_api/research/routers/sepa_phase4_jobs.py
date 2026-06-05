from __future__ import annotations

import threading
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query, Request
from pydantic import BaseModel, Field

from bifrost_api.research.sepa.phase4_engine import (
    Phase4JobConfig,
    create_phase4_job,
    delete_phase4_job,
    get_phase4_job,
    get_phase4_job_result,
    list_phase4_jobs,
    run_sepa_phase4_job,
)
from bifrost_worker.data.massive.vendor.client import MassiveClient
from bifrost_worker.data.massive.vendor.config import get_massive_settings

router = APIRouter(tags=["research"])


class SepaPhase4SubmitRequest(BaseModel):
    symbols: List[str] = Field(default_factory=list)
    source: str = "massive"
    lookback_days: int = 420
    volume_threshold: float = 100000.0
    strict_sma200_rising: bool = False
    min_crs: Optional[float] = 70.0
    max_workers: int = 4
    max_retries: int = 3
    rate_limit_rps: float = 4.0
    retry_base_sec: float = 0.6
    cache_ttl_sec: int = 21600
    use_parallel: bool = True


def _db_config(request: Request) -> Optional[dict]:
    return request.app.state.control_via_db or getattr(request.app.state, "status_cfg_for_read", None)


@router.post("/research/screening/sepa/phase4/jobs")
def submit_sepa_phase4_job(body: SepaPhase4SubmitRequest, request: Request) -> Dict[str, Any]:
    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}

    symbols = sorted({str(s or "").strip().upper() for s in body.symbols if str(s or "").strip()})
    if not symbols:
        return {"ok": False, "error": "symbols is required"}
    if len(symbols) > 5000:
        return {"ok": False, "error": "Too many symbols (max 5000)."}

    reader = getattr(request.app.state, "reader", None)
    merged_config = reader._config if reader else {}
    ms = get_massive_settings(merged_config)
    if not ms.get("api_key"):
        return {"ok": False, "error": "Massive API key not configured"}
    client = MassiveClient(api_key=ms["api_key"], base_url=ms["rest_base"])

    payload = {
        "source": body.source,
        "lookback_days": body.lookback_days,
        "volume_threshold": body.volume_threshold,
        "strict_sma200_rising": body.strict_sma200_rising,
        "min_crs": body.min_crs,
        "max_workers": body.max_workers,
        "max_retries": body.max_retries,
        "rate_limit_rps": body.rate_limit_rps,
        "retry_base_sec": body.retry_base_sec,
        "cache_ttl_sec": body.cache_ttl_sec,
        "use_parallel": body.use_parallel,
    }
    job_id = create_phase4_job(db, symbols, payload=payload)
    cfg = Phase4JobConfig(**payload)

    t = threading.Thread(
        target=run_sepa_phase4_job,
        kwargs={
            "job_id": job_id,
            "symbols": symbols,
            "status_config": db,
            "merged_config": merged_config,
            "massive_client": client,
            "cfg": cfg,
        },
        daemon=True,
    )
    t.start()
    return {"ok": True, "job_id": job_id}


@router.get("/research/screening/sepa/phase4/jobs/{job_id}")
def get_sepa_phase4_job(job_id: str, request: Request) -> Dict[str, Any]:
    db = _db_config(request)
    row = get_phase4_job(db, job_id) if db else None
    if row is None:
        return {"ok": False, "error": "job not found"}
    return {"ok": True, **row}


@router.get("/research/screening/sepa/phase4/jobs/{job_id}/result")
def get_sepa_phase4_job_result(
    job_id: str,
    request: Request,
    offset: int = Query(0, ge=0),
    limit: int = Query(200, ge=1, le=1000),
) -> Dict[str, Any]:
    db = _db_config(request)
    row = get_phase4_job_result(db, job_id, offset=offset, limit=limit) if db else None
    if row is None:
        return {"ok": False, "error": "job not found"}
    return {"ok": True, **row}


@router.get("/research/screening/sepa/phase4/jobs")
def list_sepa_phase4_jobs(
    request: Request,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    status: Optional[str] = Query(None),
    created_from: Optional[str] = Query(None, description="ISO datetime lower bound on created_at"),
    created_to: Optional[str] = Query(None, description="ISO datetime upper bound on created_at"),
) -> Dict[str, Any]:
    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured", "jobs": []}
    rows = list_phase4_jobs(
        db,
        limit=limit,
        offset=offset,
        status_filter=status,
        created_from=created_from,
        created_to=created_to,
    )
    return {
        "ok": True,
        "jobs": rows,
        "limit": limit,
        "offset": offset,
        "filters": {
            "status": status,
            "created_from": created_from,
            "created_to": created_to,
        },
    }


@router.delete("/research/screening/sepa/phase4/jobs/{job_id}")
def delete_sepa_phase4_job(job_id: str, request: Request) -> Dict[str, Any]:
    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}
    ok = delete_phase4_job(db, job_id)
    return {"ok": bool(ok)}

