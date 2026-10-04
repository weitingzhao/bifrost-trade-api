"""Golden Source analytics reads — thin HTTP proxy to Research API (:8795).

The SEPA mart reads (criteria stats, per-name evals, filters, distributions,
screener-wide) go to Research only. A Research failure raises
``ResearchUnavailable`` and the route answers 503 naming Research; nothing
falls back to direct SQL (TD-49 step 1, Owner 2026-10-03).

The tier marts (``dw_stock.mart_sepa_tier_*``), the momentum grades
(``features.stock_signal_momentum_daily``) and the criteria-stats pass-count
distributions go to Research too (0.157.0 endpoints; TD-49 step 3).

This module holds no database connection. The feedback store, the research
app's only Golden Source writer, owns its own (``feedback_store.get_conn``,
role ``feedback_writer``, api 0.8.1); the analytics connection env and the
shared ``analytics_writer`` role it named are no longer read anywhere here.

Env:
  RESEARCH_API_URL   — default ``http://research-api.research.svc.cluster.local:8795``
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import httpx

_DEFAULT_RESEARCH_URL = "http://research-api.research.svc.cluster.local:8795"
# 12 s: Research answers every proxied route in < 0.5 s (DEV, 2026-10-03; screener-wide 5000 rows
# is the slowest at 0.48 s), so a stuck Research gives a named 503 after 12 s instead of 30 s.
_RESEARCH_TIMEOUT = float(os.environ.get("RESEARCH_API_TIMEOUT", "12"))

FUND_CONDITION_COLUMNS = [
    "eps_q2q_ge_25pct",
    "rev_q2q_ge_25pct",
    "eps_acc_2q",
    "rev_acc_2q",
    "eps_3y_ge_15pct",
    "rev_3y_ge_15pct",
    "eps_acc_fy",
    "rev_acc_fy",
]

# The mart's "Raw metrics for inspector" columns: the numbers the conditions above test.
FUND_METRIC_COLUMNS = [
    "eps_q0",
    "eps_q0_yoy_base",
    "eps_g0",
    "eps_g1",
    "eps_g2",
    "rev_q0",
    "rev_q0_yoy_base",
    "rev_g0",
    "rev_g1",
    "rev_g2",
    "eps_fy0",
    "eps_fy3",
    "rev_fy0",
    "rev_fy3",
    "eps_fy_g0",
    "eps_fy_g1",
    "rev_fy_g0",
    "rev_fy_g1",
]

TECH_CONDITION_COLUMNS = [
    "avg_volume_50_gt_threshold",
    "close_ge_low52_x_1_3",
    "close_ge_high52_x_0_75",
    "sma50_gt_sma150",
    "sma50_gt_sma200",
    "sma150_gt_sma200",
    "sma200_rising_1m",
    "price_gt_sma50",
    "price_gt_sma150",
    "price_gt_sma200",
    "crs_ge_70",
]


def research_api_base() -> str:
    return (
        os.environ.get("RESEARCH_API_URL")
        or os.environ.get("VITE_RESEARCH_API")
        or _DEFAULT_RESEARCH_URL
    ).rstrip("/")


def _proxy_get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    base = research_api_base()
    url = f"{base}{path}"
    if params:
        clean = {k: v for k, v in params.items() if v is not None and v != ""}
        if clean:
            url = f"{url}?{urlencode(clean, doseq=True)}"
    with httpx.Client(timeout=_RESEARCH_TIMEOUT) as client:
        resp = client.get(url)
        resp.raise_for_status()
        data = resp.json()
    if not isinstance(data, dict):
        raise RuntimeError(f"Research API {path} returned non-object JSON")
    return data


class ResearchUnavailable(RuntimeError):
    """A Research API read failed. The routes answer 503 with this message (TD-49).

    There is no direct-SQL fallback: the SEPA marts are Research's, read through
    Research's own endpoints, and a fallback that never ran (none in six days on
    any environment) would only drift from them unseen.
    """


def _research_get(
    path: str,
    params: Optional[Dict[str, Any]] = None,
    *,
    missing_is_none: bool = False,
) -> Optional[Dict[str, Any]]:
    """GET a Research endpoint; any failure raises ``ResearchUnavailable`` naming Research.

    With ``missing_is_none`` a 404 (Research holds no row) answers None instead.
    """
    try:
        return _proxy_get(path, params)
    except httpx.HTTPStatusError as exc:
        code = exc.response.status_code if exc.response is not None else None
        if missing_is_none and code == 404:
            return None
        detail = ""
        try:
            body = exc.response.json() if exc.response is not None else None
            if isinstance(body, dict) and body.get("detail"):
                detail = f" — {body['detail']}"
        except ValueError:
            pass
        raise ResearchUnavailable(f"Research API {path}: HTTP {code}{detail}") from exc
    except Exception as exc:  # transport failure, timeout, non-JSON or non-object body
        raise ResearchUnavailable(f"Research API {path}: {exc}") from exc


# ---------------------------------------------------------------------------
# SEPA mart reads — Research API only
# ---------------------------------------------------------------------------


def fetch_criteria_stats() -> Dict[str, Any]:
    """Pre-aggregated criteria pass/fail per domain (``dw_stock.mart_sepa_criteria_stats``)."""
    data = _research_get("/analytics/sepa/criteria-stats") or {}
    return {k: v for k, v in data.items() if k != "ok"}


def fetch_fundamental_eval_single(symbol: str) -> Optional[Dict[str, Any]]:
    """One name's latest fundamental eval row; None when Research has none (404)."""
    data = _research_get(f"/analytics/sepa/fundamental-eval/{symbol.upper()}", missing_is_none=True)
    row = (data or {}).get("row")
    return dict(row) if isinstance(row, dict) else None


def fetch_technical_eval_single(symbol: str) -> Optional[Dict[str, Any]]:
    """One name's latest technical eval row; None when Research has none (404)."""
    data = _research_get(f"/analytics/sepa/technical-eval/{symbol.upper()}", missing_is_none=True)
    row = (data or {}).get("row")
    return dict(row) if isinstance(row, dict) else None


def fetch_fundamental_distribution_symbols(conditions_passed: int) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """Names passing exactly N fundamental conditions, and the eval date Research read them on."""
    data = _research_get("/analytics/sepa/fundamental-distribution", {"conditions_passed": conditions_passed}) or {}
    return list(data.get("symbols") or []), data.get("as_of")


def fetch_technical_distribution_symbols(conditions_passed: int) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """Names passing exactly N technical conditions, and the eval date Research read them on."""
    data = _research_get("/analytics/sepa/technical-distribution", {"conditions_passed": conditions_passed}) or {}
    return list(data.get("symbols") or []), data.get("as_of")


def fetch_tier_stats(tier: str) -> Dict[str, Any]:
    """Per-signal pass counts and the signals-passed histogram for one tier mart (latest eval_date).

    Research also answers ``signals`` (the tier's vocabulary); it is passed through.
    """
    return dict(_research_get("/analytics/sepa/tier-stats", {"tier": tier}) or {})


def fetch_tier_filter(
    tier: str,
    cond_ids: List[str],
    min_score: int,
    match: str,
    limit: int,
) -> Dict[str, Any]:
    """Names on a tier mart's latest eval_date passing the picked signals (all / any) and ``min_score``.

    ``count`` is the whole match and ``truncated`` says ``symbols`` stops short of it.
    """
    params: Dict[str, Any] = {
        "tier": tier,
        "include": ",".join(cond_ids),
        "min_score": min_score,
        "match": match,
        "limit": limit,
    }
    return dict(_research_get("/analytics/sepa/tier-filter", params) or {})


def fetch_momentum_grades(grades: str, limit: int) -> Dict[str, Any]:
    """The momentum radar's grades on its latest session, and the names in ``grades``."""
    return dict(_research_get("/research/momentum/grades", {"grades": grades, "limit": limit}) or {})


def fetch_iv_percentile_latest(symbol: str) -> Optional[Dict[str, Any]]:
    """One name's newest ``features.option_metric_iv_percentile_daily`` row, from Research.

    The row ranks the name's IV30 against its last 252 sessions (``iv_percentile_1y``,
    withheld under Research's floor, with ``lookback_days`` saying how many it had).
    None when Research holds no row for the name (404); any other failure raises,
    so a caller can tell an outage from a name with no IV history. Research only:
    there is no direct-PG path for a ``features.*`` read.
    """
    try:
        data = _proxy_get("/analytics/options/iv-percentile", {"symbol": symbol.strip().upper()})
    except httpx.HTTPStatusError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            return None
        raise
    rows = data.get("rows") or []
    return dict(rows[0]) if rows and isinstance(rows[0], dict) else None
