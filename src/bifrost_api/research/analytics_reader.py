"""Read-only connection to the Golden Source `analytics` schema (dbt models).

Wave 5-A/B: SEPA evaluation results are now produced by dbt CronJob and
materialized as wide boolean-column tables in `analytics.*` on
`bifrost_golden_source`. This module provides a pooled read-only
connection and typed query helpers that replace the legacy jsonb
aggregation over `public.stock_readiness_daily`.

Feature flag: set ``SEPA_USE_ANALYTICS=true`` (default) to route SEPA
criteria/screener endpoints here. When ``false``, endpoints fall back to
the legacy jsonb path in ``readiness_snapshot.py``.
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import Any, Dict, Generator, List, Optional

from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

logger = logging.getLogger(__name__)

_ANALYTICS_PG_HOST = os.environ.get("ANALYTICS_PG_HOST", "192.168.10.73")
_ANALYTICS_PG_PORT = int(os.environ.get("ANALYTICS_PG_PORT", "30432"))
_ANALYTICS_PG_DATABASE = os.environ.get("ANALYTICS_PG_DATABASE", "bifrost_golden_source")
_ANALYTICS_PG_USER = os.environ.get("ANALYTICS_PG_USER", "bifrost_readonly")
_ANALYTICS_PG_PASSWORD = os.environ.get("ANALYTICS_PG_PASSWORD", "")

_pool: Optional[ThreadedConnectionPool] = None


def use_analytics() -> bool:
    """Return True when the analytics path is enabled (feature flag)."""
    return os.environ.get("SEPA_USE_ANALYTICS", "true").lower() in ("1", "true", "yes")


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
    """Yield a pooled connection; return it on exit."""
    pool = _get_pool()
    conn = pool.getconn()
    try:
        yield conn
    finally:
        pool.putconn(conn)


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

# Fundamental condition column names in analytics.sepa_fundamental_eval
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

# Technical condition column names in analytics.sepa_technical_eval
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


def fetch_criteria_stats() -> Dict[str, Any]:
    """Read pre-aggregated criteria pass/fail from analytics.sepa_criteria_stats.

    Table schema: domain (text) + stats (jsonb).  We unpack the jsonb
    so callers see ``{"fundamental": {<counters>}, "technical": {<counters>}}``.
    """
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
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
    """Return latest fundamental eval row for a single symbol."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT *
                FROM analytics.sepa_fundamental_eval
                WHERE symbol = %s
                ORDER BY eval_date DESC
                LIMIT 1
                """,
                (symbol.upper(),),
            )
            row = cur.fetchone()
    return dict(row) if row else None


def fetch_technical_eval_single(symbol: str) -> Optional[Dict[str, Any]]:
    """Return latest technical eval row for a single symbol."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT *
                FROM analytics.sepa_technical_eval
                WHERE symbol = %s
                ORDER BY eval_date DESC
                LIMIT 1
                """,
                (symbol.upper(),),
            )
            row = cur.fetchone()
    return dict(row) if row else None


def fetch_fundamental_filter(condition_ids: List[str], *, limit: int = 500) -> List[Dict[str, Any]]:
    """Return symbols that pass ALL given fundamental conditions.

    Uses simple boolean WHERE clauses instead of jsonb containment.
    """
    valid = [c for c in condition_ids if c in FUND_CONDITION_COLUMNS]
    if not valid:
        return []
    where_parts = [f"{col} = true" for col in valid]
    where_parts.append("insufficient_data = false")
    sql = (
        "SELECT symbol, pass_count "
        "FROM analytics.sepa_fundamental_eval "
        f"WHERE eval_date = CURRENT_DATE AND {' AND '.join(where_parts)} "
        "ORDER BY pass_count DESC, symbol ASC "
        f"LIMIT {int(limit)}"
    )
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            return [dict(r) for r in (cur.fetchall() or [])]


def fetch_technical_filter(condition_ids: List[str], *, limit: int = 500) -> List[Dict[str, Any]]:
    """Return symbols that pass ALL given technical conditions."""
    valid = [c for c in condition_ids if c in TECH_CONDITION_COLUMNS]
    if not valid:
        return []
    where_parts = [f"{col} = true" for col in valid]
    sql = (
        "SELECT symbol, pass_count "
        "FROM analytics.sepa_technical_eval "
        f"WHERE eval_date = CURRENT_DATE AND {' AND '.join(where_parts)} "
        "ORDER BY pass_count DESC, symbol ASC "
        f"LIMIT {int(limit)}"
    )
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            return [dict(r) for r in (cur.fetchall() or [])]


def fetch_fundamental_distribution_symbols(conditions_passed: int) -> List[Dict[str, Any]]:
    """Return symbols with exactly N fundamental conditions passed."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT symbol, pass_count,
                       eps_q2q_ge_25pct, rev_q2q_ge_25pct,
                       eps_acc_2q, rev_acc_2q,
                       eps_3y_ge_15pct, rev_3y_ge_15pct,
                       eps_acc_fy, rev_acc_fy
                FROM analytics.sepa_fundamental_eval
                WHERE eval_date = CURRENT_DATE
                  AND insufficient_data = false
                  AND pass_count = %s
                ORDER BY symbol
                """,
                (conditions_passed,),
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
    """Return symbols with exactly N technical conditions passed."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT symbol, pass_count,
                       avg_volume_50_gt_threshold, close_ge_low52_x_1_3,
                       close_ge_high52_x_0_75, sma50_gt_sma150,
                       sma50_gt_sma200, sma150_gt_sma200,
                       sma200_rising_1m, price_gt_sma50,
                       price_gt_sma150, price_gt_sma200,
                       crs_ge_70
                FROM analytics.sepa_technical_eval
                WHERE eval_date = CURRENT_DATE
                  AND pass_count = %s
                ORDER BY symbol
                """,
                (conditions_passed,),
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
    """Read from analytics.sepa_screener_wide (all conditions + scores joined).

    If symbols is None, returns top ranked symbols up to limit.
    If symbols is provided, returns rows for those symbols.
    """
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if symbols:
                syms = [s.upper() for s in symbols[:500]]
                cur.execute(
                    """
                    SELECT *
                    FROM analytics.sepa_screener_wide
                    WHERE symbol = ANY(%s)
                    ORDER BY symbol
                    """,
                    (syms,),
                )
            else:
                cur.execute(
                    f"""
                    SELECT *
                    FROM analytics.sepa_screener_wide
                    ORDER BY overall_rank ASC NULLS LAST
                    LIMIT {int(limit)}
                    """
                )
            return [dict(r) for r in (cur.fetchall() or [])]


def fetch_screening_ranked(*, limit: int = 500) -> List[Dict[str, Any]]:
    """Read composite scores and rankings from analytics.sepa_screening_ranked."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT symbol, composite_score, overall_rank, decile, percentile
                FROM analytics.sepa_screening_ranked
                ORDER BY overall_rank ASC NULLS LAST
                LIMIT {int(limit)}
                """
            )
            return [dict(r) for r in (cur.fetchall() or [])]
