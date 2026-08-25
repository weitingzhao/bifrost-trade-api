from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from bifrost_api.research.market_pg import (
    delete_job_sepa_phase4,
    get_job_sepa_phase4,
    get_job_sepa_phase4_result,
    list_job_sepa_phase4,
)
from bifrost_api.research.reference_cache_keys import CACHE_TTL_SEPA_FUNDAMENTALS_SEC

PHASE4_VERSION = "sepa_phase4_v1"
PHASE4_RETIRED_MSG = (
    "SEPA Phase4 PG job queue retired; use dw_stock.mart_sepa_screener_wide / Research screener APIs."
)


@dataclass
class Phase4JobConfig:
    source: str = "massive"
    lookback_days: int = 420
    volume_threshold: float = 100000.0
    strict_sma200_rising: bool = False
    min_crs: Optional[float] = 70.0
    max_workers: int = 4
    max_retries: int = 3
    rate_limit_rps: float = 4.0
    retry_base_sec: float = 0.6
    cache_ttl_sec: int = CACHE_TTL_SEPA_FUNDAMENTALS_SEC
    use_parallel: bool = True


def create_phase4_job(
    status_config: dict,
    symbols: List[str],
    payload: Optional[Dict[str, Any]] = None,
) -> str:
    raise RuntimeError(PHASE4_RETIRED_MSG)


def get_phase4_job(status_config: dict, job_id: str) -> Optional[Dict[str, Any]]:
    row = get_job_sepa_phase4(status_config, job_id)
    if row is None:
        return None
    out = dict(row)
    for k in ("progress", "request", "summary"):
        if not isinstance(out.get(k), dict):
            out[k] = {}
    if not isinstance(out.get("errors"), list):
        out["errors"] = []
    return out


def get_phase4_job_result(
    status_config: dict,
    job_id: str,
    *,
    offset: int = 0,
    limit: int = 200,
) -> Optional[Dict[str, Any]]:
    return get_job_sepa_phase4_result(status_config, job_id, offset=offset, limit=limit)


def list_phase4_jobs(
    status_config: dict,
    *,
    limit: int = 50,
    offset: int = 0,
    status_filter: Optional[str] = None,
    created_from: Optional[str] = None,
    created_to: Optional[str] = None,
) -> List[Dict[str, Any]]:
    return list_job_sepa_phase4(
        status_config,
        limit=limit,
        offset=offset,
        status_filter=status_filter,
        created_from=created_from,
        created_to=created_to,
    )


def delete_phase4_job(status_config: dict, job_id: str) -> bool:
    return delete_job_sepa_phase4(status_config, job_id)


def run_sepa_phase4_job(
    job_id: str,
    *,
    symbols: List[str],
    status_config: dict,
    merged_config: dict,
    cfg: Optional[Phase4JobConfig] = None,
) -> None:
    """No-op — job_sepa_phase4 retired (bifrost-core 0.10.6)."""
    return
