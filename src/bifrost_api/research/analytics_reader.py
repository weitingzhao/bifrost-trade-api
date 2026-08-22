"""Golden Source analytics reads — thin HTTP proxy to Research API (:8795).

Wave 2.3: fetch helpers no longer query ``analytics.*`` directly when the
Research proxy is enabled. Trade research domain (:8773) keeps the same
function signatures so ``data_readiness`` / FE paths stay stable.

Env:
  RESEARCH_API_URL   — default ``http://research-api.research.svc.cluster.local:8795``
  RESEARCH_PROXY     — ``true`` (default) to prefer HTTP; ``false`` uses direct PG
  SEPA_USE_ANALYTICS — master flag (unchanged)
  ANALYTICS_PG_*     — direct PG fallback / readiness_snapshot get_conn
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from datetime import date, datetime
from typing import Any, Dict, Generator, List, Optional
from urllib.parse import urlencode

import httpx
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

logger = logging.getLogger(__name__)

_ANALYTICS_PG_HOST = os.environ.get("ANALYTICS_PG_HOST", "192.168.10.73")
_ANALYTICS_PG_PORT = int(os.environ.get("ANALYTICS_PG_PORT", "30432"))
_ANALYTICS_PG_DATABASE = os.environ.get("ANALYTICS_PG_DATABASE", "bifrost_golden_source")
_ANALYTICS_PG_USER = os.environ.get("ANALYTICS_PG_USER", "bifrost_readonly")
_ANALYTICS_PG_PASSWORD = os.environ.get("ANALYTICS_PG_PASSWORD", "")

_DEFAULT_RESEARCH_URL = "http://research-api.research.svc.cluster.local:8795"
_RESEARCH_TIMEOUT = float(os.environ.get("RESEARCH_API_TIMEOUT", "30"))

_pool: Optional[ThreadedConnectionPool] = None

# dbt SEPA marts are single-day snapshots — prefer MAX(eval_date), not CURRENT_DATE.
_FUND_EVAL_TABLE = "analytics.mart_sepa_fundamental_eval"
_TECH_EVAL_TABLE = "analytics.mart_sepa_technical_eval"
_ALLOWED_EVAL_TABLES = frozenset({_FUND_EVAL_TABLE, _TECH_EVAL_TABLE})

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


def latest_eval_date(cur: Any, table: str) -> Optional[date]:
    """Return MAX(eval_date) for a known mart table, or None if empty."""
    if table not in _ALLOWED_EVAL_TABLES:
        raise ValueError(f"unsupported eval table: {table}")
    cur.execute(f"SELECT MAX(eval_date) AS d FROM {table}")
    row = cur.fetchone()
    if not row:
        return None
    raw = row["d"] if isinstance(row, dict) else row[0]
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw.date()
    if isinstance(raw, date):
        return raw
    return date.fromisoformat(str(raw)[:10])


def use_analytics() -> bool:
    """Return True when the analytics path is enabled (feature flag)."""
    return os.environ.get("SEPA_USE_ANALYTICS", "true").lower() in ("1", "true", "yes")


def use_research_proxy() -> bool:
    """Prefer Research API HTTP over direct Golden Source SQL."""
    if not use_analytics():
        return False
    return os.environ.get("RESEARCH_PROXY", "true").lower() in ("1", "true", "yes")


def research_api_base() -> str:
    return (
        os.environ.get("RESEARCH_API_URL")
        or os.environ.get("VITE_RESEARCH_API")
        or _DEFAULT_RESEARCH_URL
    ).rstrip("/")


def _get_pool() -> ThreadedConnectionPool:
    global _pool
    if _pool is None or _pool.closed:
        _pool = ThreadedConnectionPool(
            minconn=1,
            maxconn=5,
            host=_ANALYTICS_PG_HOST,
            port=_ANALYTICS_PG_PORT,
            dbname=_ANALYTICS_PG_DATABASE,
            user=_ANALYTICS_PG_USER,
            password=_ANALYTICS_PG_PASSWORD,
            connect_timeout=10,
            options="-c statement_timeout=30000",
        )
    return _pool


@contextmanager
def get_conn() -> Generator:
    """Yield a pooled Golden Source connection (readiness_snapshot / fallback)."""
    pool = _get_pool()
    conn = pool.getconn()
    try:
        yield conn
    finally:
        pool.putconn(conn)


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


# ---------------------------------------------------------------------------
# Query helpers (proxy → Research; fallback → direct SQL)
# ---------------------------------------------------------------------------


def fetch_criteria_stats() -> Dict[str, Any]:
    """Read pre-aggregated criteria pass/fail (Research or direct mart)."""
    if use_research_proxy():
        try:
            data = _proxy_get("/analytics/sepa/criteria-stats")
            out = {k: v for k, v in data.items() if k != "ok"}
            return out
        except Exception as exc:
            logger.warning("Research proxy criteria-stats failed, falling back to PG: %s", exc)
    return _fetch_criteria_stats_direct()


def _fetch_criteria_stats_direct() -> Dict[str, Any]:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute("SELECT domain, stats FROM analytics.mart_sepa_criteria_stats")
            except Exception:
                cur.execute("SELECT domain, stats FROM analytics.sepa_criteria_stats")
            rows = cur.fetchall() or []
    result: Dict[str, Any] = {}
    for row in rows:
        domain = row.get("domain", "unknown")
        stats = row.get("stats")
        if isinstance(stats, dict):
            result[domain] = stats
        else:
            result[domain] = dict(row)
    return result


def fetch_fundamental_eval_single(symbol: str) -> Optional[Dict[str, Any]]:
    if use_research_proxy():
        try:
            data = _proxy_get(f"/analytics/sepa/fundamental-eval/{symbol.upper()}")
            row = data.get("row")
            return dict(row) if isinstance(row, dict) else None
        except httpx.HTTPStatusError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                return None
            logger.warning("Research proxy fundamental-eval failed, PG fallback: %s", exc)
        except Exception as exc:
            logger.warning("Research proxy fundamental-eval failed, PG fallback: %s", exc)
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT *
                FROM analytics.mart_sepa_fundamental_eval
                WHERE symbol = %s
                ORDER BY eval_date DESC
                LIMIT 1
                """,
                (symbol.upper(),),
            )
            row = cur.fetchone()
    return dict(row) if row else None


def fetch_technical_eval_single(symbol: str) -> Optional[Dict[str, Any]]:
    if use_research_proxy():
        try:
            data = _proxy_get(f"/analytics/sepa/technical-eval/{symbol.upper()}")
            row = data.get("row")
            return dict(row) if isinstance(row, dict) else None
        except httpx.HTTPStatusError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                return None
            logger.warning("Research proxy technical-eval failed, PG fallback: %s", exc)
        except Exception as exc:
            logger.warning("Research proxy technical-eval failed, PG fallback: %s", exc)
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT *
                FROM analytics.mart_sepa_technical_eval
                WHERE symbol = %s
                ORDER BY eval_date DESC
                LIMIT 1
                """,
                (symbol.upper(),),
            )
            row = cur.fetchone()
    return dict(row) if row else None


def fetch_fundamental_filter(condition_ids: List[str], *, limit: int = 500) -> List[Dict[str, Any]]:
    valid = [c for c in condition_ids if c in FUND_CONDITION_COLUMNS]
    if not valid:
        return []
    if use_research_proxy():
        try:
            data = _proxy_get(
                "/analytics/sepa/fundamental-filter",
                {"conditions": ",".join(valid), "limit": limit},
            )
            symbols = data.get("symbols") or []
            return [
                {"symbol": s["symbol"], "pass_count": int(s.get("pass_count") or 0)}
                for s in symbols
                if isinstance(s, dict) and s.get("symbol")
            ]
        except Exception as exc:
            logger.warning("Research proxy fundamental-filter failed, PG fallback: %s", exc)
    where_parts = [f"{col} = true" for col in valid]
    where_parts.append("insufficient_data = false")
    sql = (
        f"SELECT symbol, pass_count FROM {_FUND_EVAL_TABLE} "
        f"WHERE eval_date = %s AND {' AND '.join(where_parts)} "
        "ORDER BY pass_count DESC, symbol ASC "
        f"LIMIT {int(limit)}"
    )
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            as_of = latest_eval_date(cur, _FUND_EVAL_TABLE)
            if as_of is None:
                return []
            cur.execute(sql, (as_of,))
            return [dict(r) for r in (cur.fetchall() or [])]


def fetch_technical_filter(condition_ids: List[str], *, limit: int = 500) -> List[Dict[str, Any]]:
    valid = [c for c in condition_ids if c in TECH_CONDITION_COLUMNS]
    if not valid:
        return []
    if use_research_proxy():
        try:
            data = _proxy_get(
                "/analytics/sepa/technical-filter",
                {"conditions": ",".join(valid), "limit": limit},
            )
            symbols = data.get("symbols") or []
            return [
                {"symbol": s["symbol"], "pass_count": int(s.get("pass_count") or 0)}
                for s in symbols
                if isinstance(s, dict) and s.get("symbol")
            ]
        except Exception as exc:
            logger.warning("Research proxy technical-filter failed, PG fallback: %s", exc)
    where_parts = [f"{col} = true" for col in valid]
    sql = (
        f"SELECT symbol, pass_count FROM {_TECH_EVAL_TABLE} "
        f"WHERE eval_date = %s AND {' AND '.join(where_parts)} "
        "ORDER BY pass_count DESC, symbol ASC "
        f"LIMIT {int(limit)}"
    )
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            as_of = latest_eval_date(cur, _TECH_EVAL_TABLE)
            if as_of is None:
                return []
            cur.execute(sql, (as_of,))
            return [dict(r) for r in (cur.fetchall() or [])]


def fetch_fundamental_distribution_symbols(conditions_passed: int) -> List[Dict[str, Any]]:
    if use_research_proxy():
        try:
            data = _proxy_get(
                "/analytics/sepa/fundamental-distribution",
                {"conditions_passed": conditions_passed},
            )
            return list(data.get("symbols") or [])
        except Exception as exc:
            logger.warning("Research proxy fund-distribution failed, PG fallback: %s", exc)
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            as_of = latest_eval_date(cur, _FUND_EVAL_TABLE)
            if as_of is None:
                return []
            cur.execute(
                f"""
                SELECT symbol, pass_count,
                       eps_q2q_ge_25pct, rev_q2q_ge_25pct,
                       eps_acc_2q, rev_acc_2q,
                       eps_3y_ge_15pct, rev_3y_ge_15pct,
                       eps_acc_fy, rev_acc_fy
                FROM {_FUND_EVAL_TABLE}
                WHERE eval_date = %s
                  AND insufficient_data = false
                  AND pass_count = %s
                ORDER BY symbol
                """,
                (as_of, conditions_passed),
            )
            rows = cur.fetchall() or []
    result = []
    for r in rows:
        passed_conditions = [col for col in FUND_CONDITION_COLUMNS if r.get(col) is True]
        result.append({
            "symbol": r["symbol"],
            "pass_count": int(r.get("pass_count") or 0),
            "passed_conditions": passed_conditions,
        })
    return result


def fetch_technical_distribution_symbols(conditions_passed: int) -> List[Dict[str, Any]]:
    if use_research_proxy():
        try:
            data = _proxy_get(
                "/analytics/sepa/technical-distribution",
                {"conditions_passed": conditions_passed},
            )
            return list(data.get("symbols") or [])
        except Exception as exc:
            logger.warning("Research proxy tech-distribution failed, PG fallback: %s", exc)
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            as_of = latest_eval_date(cur, _TECH_EVAL_TABLE)
            if as_of is None:
                return []
            cur.execute(
                f"""
                SELECT symbol, pass_count,
                       avg_volume_50_gt_threshold, close_ge_low52_x_1_3,
                       close_ge_high52_x_0_75, sma50_gt_sma150,
                       sma50_gt_sma200, sma150_gt_sma200,
                       sma200_rising_1m, price_gt_sma50,
                       price_gt_sma150, price_gt_sma200,
                       crs_ge_70
                FROM {_TECH_EVAL_TABLE}
                WHERE eval_date = %s
                  AND pass_count = %s
                ORDER BY symbol
                """,
                (as_of, conditions_passed),
            )
            rows = cur.fetchall() or []
    result = []
    for r in rows:
        passed_conditions = [col for col in TECH_CONDITION_COLUMNS if r.get(col) is True]
        result.append({
            "symbol": r["symbol"],
            "pass_count": int(r.get("pass_count") or 0),
            "passed_conditions": passed_conditions,
        })
    return result


def fetch_screener_wide(
    symbols: Optional[List[str]] = None,
    *,
    limit: int = 500,
) -> List[Dict[str, Any]]:
    if use_research_proxy():
        try:
            params: Dict[str, Any] = {"limit": limit}
            if symbols:
                params["symbols"] = ",".join(s.upper() for s in symbols[:500])
            data = _proxy_get("/analytics/sepa/screener-wide", params)
            return [dict(r) for r in (data.get("rows") or []) if isinstance(r, dict)]
        except Exception as exc:
            logger.warning("Research proxy screener-wide failed, PG fallback: %s", exc)
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if symbols:
                syms = [s.upper() for s in symbols[:500]]
                cur.execute(
                    """
                    SELECT *
                    FROM analytics.mart_sepa_screener_wide
                    WHERE symbol = ANY(%s)
                    ORDER BY symbol
                    """,
                    (syms,),
                )
            else:
                cur.execute(
                    f"""
                    SELECT *
                    FROM analytics.mart_sepa_screener_wide
                    ORDER BY overall_rank ASC NULLS LAST
                    LIMIT {int(limit)}
                    """
                )
            return [dict(r) for r in (cur.fetchall() or [])]


def fetch_screening_ranked(*, limit: int = 500) -> List[Dict[str, Any]]:
    if use_research_proxy():
        try:
            data = _proxy_get("/analytics/sepa/screening-ranked", {"limit": limit})
            return [dict(r) for r in (data.get("rows") or []) if isinstance(r, dict)]
        except Exception as exc:
            logger.warning("Research proxy screening-ranked failed, PG fallback: %s", exc)
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT symbol, composite_score, overall_rank, decile, percentile
                FROM analytics.mart_sepa_screening_ranked
                ORDER BY overall_rank ASC NULLS LAST
                LIMIT {int(limit)}
                """
            )
            return [dict(r) for r in (cur.fetchall() or [])]
