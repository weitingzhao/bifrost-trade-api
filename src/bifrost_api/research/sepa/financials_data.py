"""SEPA fundamentals raw tables: gap detection, upserts, and Celery feed helpers.

Massive Stocks REST v1 financials (flat) + short interest / short volume.
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from psycopg2.extras import Json

logger = logging.getLogger(__name__)

SOURCE_DEFAULT = "massive"

_FQ_TO_PERIOD = {0: "FY", 1: "Q1", 2: "Q2", 3: "Q3", 4: "Q4"}

# Instrument types treated as Supported or Partial for Massive financial statements coverage
# (income, balance sheet, cash flow, ratios). SEPA Data Ready counts gaps and selects
# backfill targets only within this universe; not_supported types do not contribute gap counts.
_FINANCIAL_STATEMENTS_INSTRUMENT_TYPES = (
    "CS",
    "ADRC",
    "PFD",
)


def _symbol_from_gap_sql_row(r: Any) -> Optional[str]:
    """Extract symbol from a gap-query row.

    ``RealDictCursor`` rows are mapping-like and do **not** support ``r[0]`` (raises ``KeyError``).
    """
    if not r:
        return None
    v: Any
    if isinstance(r, dict):
        v = r.get("symbol")
    else:
        try:
            v = r[0]
        except (TypeError, KeyError, IndexError):
            return None
    if v is None or v == "":
        return None
    s = str(v).strip().upper()
    return s or None


def fetch_income_rows_for_sepa_from_pg(
    status_config: dict,
    symbol: str,
    *,
    min_quarterly: int = 5,
    min_annual: int = 4,
) -> Optional[Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]]:
    """Build quarterly/annual row dicts for ``evaluate_fundamentals`` from ``stock_income_statements``.

    Returns None if the table is missing or coverage is insufficient.
    """
    sym = (symbol or "").strip().upper()
    if not sym or not status_config:
        return None
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor

        from bifrost_core.persistence.postgres.connection import _get_conn_params

        params = _get_conn_params(status_config)
        params["connect_timeout"] = 15
        conn = psycopg2.connect(**params)
    except Exception as e:
        logger.debug("fetch_income_rows_for_sepa_from_pg connect failed: %s", e)
        return None
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT to_regclass('public.stock_income_statements') IS NOT NULL AS texists"
            )
            if not bool((cur.fetchone() or {}).get("texists")):
                return None
            cur.execute(
                """
                SELECT timeframe, fiscal_year, fiscal_quarter, period_end, filing_date,
                       basic_earnings_per_share, revenue, diluted_earnings_per_share
                FROM public.stock_income_statements
                WHERE symbol = %s AND source = %s AND timeframe = 'quarterly'
                ORDER BY fiscal_year ASC, fiscal_quarter ASC
                """,
                (sym, SOURCE_DEFAULT),
            )
            q_db = cur.fetchall() or []
            cur.execute(
                """
                SELECT timeframe, fiscal_year, fiscal_quarter, period_end, filing_date,
                       basic_earnings_per_share, revenue, diluted_earnings_per_share
                FROM public.stock_income_statements
                WHERE symbol = %s AND source = %s AND timeframe = 'annual'
                ORDER BY fiscal_year ASC
                """,
                (sym, SOURCE_DEFAULT),
            )
            a_db = cur.fetchall() or []
    finally:
        conn.close()
    if len(q_db) < min_quarterly or len(a_db) < min_annual:
        return None

    def _map_q(r: Any) -> Dict[str, Any]:
        fq = int(r.get("fiscal_quarter") or 0)
        fp = _FQ_TO_PERIOD.get(fq, f"Q{fq}" if fq else "FY")
        fd = r.get("filing_date")
        fd_s = fd.isoformat() if hasattr(fd, "isoformat") else (str(fd)[:10] if fd else None)
        pe = r.get("period_end")
        pe_s = pe.isoformat() if hasattr(pe, "isoformat") else (str(pe)[:10] if pe else None)
        return {
            "fiscal_year": int(r.get("fiscal_year") or 0),
            "fiscal_period": fp,
            "filing_date": fd_s,
            "timeframe": "quarterly",
            "start_date": pe_s,
            "end_date": pe_s,
            "basic_earnings_per_share": r.get("basic_earnings_per_share"),
            "diluted_earnings_per_share": r.get("diluted_earnings_per_share"),
            "revenues": r.get("revenue"),
        }

    def _map_a(r: Any) -> Dict[str, Any]:
        fd = r.get("filing_date")
        fd_s = fd.isoformat() if hasattr(fd, "isoformat") else (str(fd)[:10] if fd else None)
        pe = r.get("period_end")
        pe_s = pe.isoformat() if hasattr(pe, "isoformat") else (str(pe)[:10] if pe else None)
        return {
            "fiscal_year": int(r.get("fiscal_year") or 0),
            "fiscal_period": "FY",
            "filing_date": fd_s,
            "timeframe": "annual",
            "start_date": pe_s,
            "end_date": pe_s,
            "basic_earnings_per_share": r.get("basic_earnings_per_share"),
            "diluted_earnings_per_share": r.get("diluted_earnings_per_share"),
            "revenues": r.get("revenue"),
        }

    return ([_map_q(r) for r in q_db], [_map_a(r) for r in a_db])


# Celery feed upserts + job runners (worker-owned; re-exported for API/tests).
from bifrost_worker.data.massive.financials_feed import (  # noqa: E402
    SOURCE_DEFAULT as FEED_SOURCE_DEFAULT,
    run_feed_stocks_balance_sheets_job,
    run_feed_stocks_cash_flows_job,
    run_feed_stocks_income_statements_job,
    run_feed_stocks_ratios_job,
    run_feed_stocks_short_interest_job,
    run_feed_stocks_short_volume_job,
    upsert_balance_sheet_rows,
    upsert_cash_flow_rows,
    upsert_income_statement_rows,
    upsert_ratios_rows,
    upsert_short_interest_rows,
    upsert_short_volume_rows,
)

# Keep module-level SOURCE_DEFAULT for gap/readiness SQL below.
SOURCE_DEFAULT = FEED_SOURCE_DEFAULT

_INCOME_GAP_DETAIL_SQL = f"""
WITH u AS (
    SELECT u.symbol
    FROM public.v_us_equity_universe u
    WHERE upper(coalesce(u.instrument_type, '')) IN {_FINANCIAL_STATEMENTS_INSTRUMENT_TYPES}
),
q AS (
    SELECT symbol,
           count(*)::integer AS n,
           count(*) FILTER (WHERE basic_earnings_per_share IS NOT NULL)::integer AS eps_n,
           count(*) FILTER (WHERE revenue IS NOT NULL)::integer AS rev_n,
           max(period_end) AS max_pe
    FROM public.stock_income_statements
    WHERE source = 'massive' AND timeframe = 'quarterly'
    GROUP BY symbol
),
a AS (
    SELECT symbol, count(*)::integer AS n, max(period_end) AS max_pe
    FROM public.stock_income_statements
    WHERE source = 'massive' AND timeframe = 'annual'
    GROUP BY symbol
)
SELECT
    u.symbol,
    COALESCE(q.n, 0) AS quarterly_rows,
    COALESCE(a.n, 0) AS annual_rows,
    q.max_pe::text AS quarterly_max_period_end,
    a.max_pe::text AS annual_max_period_end,
    CASE
        WHEN q.symbol IS NULL THEN 'missing_quarterly'
        WHEN q.n < 5 THEN 'insufficient_quarterly'
        WHEN q.n > 0 AND (q.eps_n::float / q.n) < 0.8 THEN 'eps_null_ratio_high'
        WHEN q.n > 0 AND (q.rev_n::float / q.n) < 0.8 THEN 'revenue_null_ratio_high'
        WHEN a.symbol IS NULL OR a.n < 4 THEN 'insufficient_annual'
        ELSE NULL
    END AS gap_reason
FROM u
LEFT JOIN q ON q.symbol = u.symbol
LEFT JOIN a ON a.symbol = u.symbol
WHERE q.symbol IS NULL OR q.n < 5 OR a.symbol IS NULL OR a.n < 4
   OR (q.n > 0 AND (q.eps_n::float / q.n) < 0.8)
   OR (q.n > 0 AND (q.rev_n::float / q.n) < 0.8)
ORDER BY u.symbol
LIMIT %s
"""

_INCOME_GAP_COUNT_SQL = f"""
WITH u AS (
    SELECT u.symbol
    FROM public.v_us_equity_universe u
    WHERE upper(coalesce(u.instrument_type, '')) IN {_FINANCIAL_STATEMENTS_INSTRUMENT_TYPES}
),
q AS (
    SELECT symbol,
           count(*)::integer AS n,
           count(*) FILTER (WHERE basic_earnings_per_share IS NOT NULL)::integer AS eps_n,
           count(*) FILTER (WHERE revenue IS NOT NULL)::integer AS rev_n
    FROM public.stock_income_statements
    WHERE source = 'massive' AND timeframe = 'quarterly'
    GROUP BY symbol
),
a AS (
    SELECT symbol, count(*)::integer AS n
    FROM public.stock_income_statements
    WHERE source = 'massive' AND timeframe = 'annual'
    GROUP BY symbol
)
SELECT count(*)::bigint AS n
FROM u
LEFT JOIN q ON q.symbol = u.symbol
LEFT JOIN a ON a.symbol = u.symbol
WHERE q.symbol IS NULL OR q.n < 5 OR a.symbol IS NULL OR a.n < 4
   OR (q.n > 0 AND (q.eps_n::float / q.n) < 0.8)
   OR (q.n > 0 AND (q.rev_n::float / q.n) < 0.8)
"""


def count_income_statements_gaps(cur: Any) -> int:
    cur.execute(_INCOME_GAP_COUNT_SQL)
    row = cur.fetchone()
    return int(row["n"] or 0) if row else 0


def get_income_statements_gap_details(cur: Any, *, limit: int = 2000) -> Tuple[List[Dict[str, Any]], int]:
    cur.execute(_INCOME_GAP_COUNT_SQL)
    total = int((cur.fetchone() or {}).get("n") or 0)
    cur.execute(_INCOME_GAP_DETAIL_SQL, (max(1, min(int(limit), 5000)),))
    rows = cur.fetchall() or []
    out = [dict(r) for r in rows]
    return out, total


_BALANCE_GAP_COUNT = f"""
WITH u AS (
    SELECT u.symbol
    FROM public.v_us_equity_universe u
    WHERE upper(coalesce(u.instrument_type, '')) IN {_FINANCIAL_STATEMENTS_INSTRUMENT_TYPES}
),
q AS (
    SELECT symbol, count(*)::integer AS n,
           count(*) FILTER (WHERE total_assets IS NOT NULL)::integer AS ta_n
    FROM public.stock_balance_sheets
    WHERE source='massive' AND timeframe='quarterly'
    GROUP BY symbol
)
SELECT count(*)::bigint AS n FROM u
LEFT JOIN q ON q.symbol=u.symbol
WHERE q.symbol IS NULL OR q.n < 4 OR (q.n > 0 AND (q.ta_n::float/q.n) < 0.9)
"""

_BALANCE_GAP_DETAIL = f"""
WITH u AS (
    SELECT u.symbol
    FROM public.v_us_equity_universe u
    WHERE upper(coalesce(u.instrument_type, '')) IN {_FINANCIAL_STATEMENTS_INSTRUMENT_TYPES}
),
q AS (
    SELECT symbol, count(*)::integer AS n,
           count(*) FILTER (WHERE total_assets IS NOT NULL)::integer AS ta_n
    FROM public.stock_balance_sheets
    WHERE source='massive' AND timeframe='quarterly'
    GROUP BY symbol
)
SELECT u.symbol, COALESCE(q.n,0) AS quarterly_rows, NULL::text AS annual_max_period_end,
    CASE WHEN q.symbol IS NULL THEN 'missing'
         WHEN q.n < 4 THEN 'insufficient_quarterly'
         WHEN q.n > 0 AND (q.ta_n::float/q.n) < 0.9 THEN 'total_assets_null_ratio_high'
         ELSE NULL END AS gap_reason
FROM u LEFT JOIN q ON q.symbol=u.symbol
WHERE q.symbol IS NULL OR q.n < 4 OR (q.n > 0 AND (q.ta_n::float/q.n) < 0.9)
ORDER BY u.symbol LIMIT %s
"""


def count_balance_sheet_gaps(cur: Any) -> int:
    cur.execute(_BALANCE_GAP_COUNT)
    return int((cur.fetchone() or {}).get("n") or 0)


def get_balance_sheet_gap_details(cur: Any, *, limit: int = 2000) -> Tuple[List[Dict[str, Any]], int]:
    cur.execute(_BALANCE_GAP_COUNT)
    total = int((cur.fetchone() or {}).get("n") or 0)
    cur.execute(_BALANCE_GAP_DETAIL, (max(1, min(int(limit), 5000)),))
    return [dict(r) for r in (cur.fetchall() or [])], total


_CF_GAP_COUNT = f"""
WITH u AS (
    SELECT u.symbol
    FROM public.v_us_equity_universe u
    WHERE upper(coalesce(u.instrument_type, '')) IN {_FINANCIAL_STATEMENTS_INSTRUMENT_TYPES}
),
q AS (
    SELECT symbol, count(*)::integer AS n,
           count(*) FILTER (WHERE net_cash_from_operating_activities IS NOT NULL)::integer AS op_n
    FROM public.stock_cash_flows
    WHERE source='massive' AND timeframe='quarterly'
    GROUP BY symbol
)
SELECT count(*)::bigint AS n FROM u
LEFT JOIN q ON q.symbol=u.symbol
WHERE q.symbol IS NULL OR q.n < 4 OR (q.n > 0 AND (op_n::float/q.n) < 0.8)
"""

_CF_GAP_DETAIL = f"""
WITH u AS (
    SELECT u.symbol
    FROM public.v_us_equity_universe u
    WHERE upper(coalesce(u.instrument_type, '')) IN {_FINANCIAL_STATEMENTS_INSTRUMENT_TYPES}
),
q AS (
    SELECT symbol, count(*)::integer AS n,
           count(*) FILTER (WHERE net_cash_from_operating_activities IS NOT NULL)::integer AS op_n
    FROM public.stock_cash_flows
    WHERE source='massive' AND timeframe='quarterly'
    GROUP BY symbol
)
SELECT u.symbol, COALESCE(q.n,0) AS quarterly_rows, NULL::text AS annual_max_period_end,
    CASE WHEN q.symbol IS NULL THEN 'missing'
         WHEN q.n < 4 THEN 'insufficient_quarterly'
         WHEN q.n > 0 AND (op_n::float/q.n) < 0.8 THEN 'operating_cf_null_ratio_high'
         ELSE NULL END AS gap_reason
FROM u LEFT JOIN q ON q.symbol=u.symbol
WHERE q.symbol IS NULL OR q.n < 4 OR (q.n > 0 AND (op_n::float/q.n) < 0.8)
ORDER BY u.symbol LIMIT %s
"""


def count_cash_flow_gaps(cur: Any) -> int:
    cur.execute(_CF_GAP_COUNT)
    return int((cur.fetchone() or {}).get("n") or 0)


def get_cash_flow_gap_details(cur: Any, *, limit: int = 2000) -> Tuple[List[Dict[str, Any]], int]:
    cur.execute(_CF_GAP_COUNT)
    total = int((cur.fetchone() or {}).get("n") or 0)
    cur.execute(_CF_GAP_DETAIL, (max(1, min(int(limit), 5000)),))
    return [dict(r) for r in (cur.fetchall() or [])], total


_RAT_GAP_COUNT = f"""
WITH u AS (
    SELECT u.symbol
    FROM public.v_us_equity_universe u
    WHERE upper(coalesce(u.instrument_type, '')) IN {_FINANCIAL_STATEMENTS_INSTRUMENT_TYPES}
),
q AS (
    SELECT symbol, count(*)::integer AS n,
           max(date) AS mx
    FROM public.stock_ratios
    WHERE source='massive'
    GROUP BY symbol
)
SELECT count(*)::bigint AS n FROM u
LEFT JOIN q ON q.symbol=u.symbol
WHERE q.symbol IS NULL OR q.n < 1 OR q.mx < (CURRENT_DATE - integer '45')
"""

_RAT_GAP_DETAIL = f"""
WITH u AS (
    SELECT u.symbol
    FROM public.v_us_equity_universe u
    WHERE upper(coalesce(u.instrument_type, '')) IN {_FINANCIAL_STATEMENTS_INSTRUMENT_TYPES}
),
q AS (
    SELECT symbol, count(*)::integer AS n,
           max(date) AS mx
    FROM public.stock_ratios
    WHERE source='massive'
    GROUP BY symbol
)
SELECT u.symbol, COALESCE(q.n,0) AS quarterly_rows,
    to_char(q.mx, 'YYYY-MM-DD') AS annual_max_period_end,
    CASE WHEN q.symbol IS NULL OR q.n < 1 OR q.mx < (CURRENT_DATE - integer '45') THEN 'stale_or_missing'
         ELSE NULL END AS gap_reason
FROM u LEFT JOIN q ON q.symbol=u.symbol
WHERE q.symbol IS NULL OR q.n < 1 OR q.mx < (CURRENT_DATE - integer '45')
ORDER BY u.symbol LIMIT %s
"""


def count_ratios_gaps(cur: Any) -> int:
    cur.execute(_RAT_GAP_COUNT)
    return int((cur.fetchone() or {}).get("n") or 0)


def get_ratios_gap_details(cur: Any, *, limit: int = 2000) -> Tuple[List[Dict[str, Any]], int]:
    cur.execute(_RAT_GAP_COUNT)
    total = int((cur.fetchone() or {}).get("n") or 0)
    cur.execute(_RAT_GAP_DETAIL, (max(1, min(int(limit), 5000)),))
    return [dict(r) for r in (cur.fetchall() or [])], total


_SI_GAP_COUNT = """
WITH u AS (SELECT symbol FROM public.v_us_equity_universe),
h AS (
    SELECT symbol, count(*)::integer AS n,
           max(settlement_date) AS mx
    FROM public.stock_short_interest
    WHERE source='massive'
    GROUP BY symbol
)
SELECT count(*)::bigint AS n FROM u
LEFT JOIN h ON h.symbol=u.symbol
WHERE h.symbol IS NULL OR h.n < 1 OR h.mx < (CURRENT_DATE - integer '45')
"""

_SI_GAP_DETAIL = """
WITH u AS (SELECT symbol FROM public.v_us_equity_universe),
h AS (
    SELECT symbol, count(*)::integer AS n,
           max(settlement_date) AS mx
    FROM public.stock_short_interest
    WHERE source='massive'
    GROUP BY symbol
)
SELECT u.symbol, COALESCE(h.n,0) AS quarterly_rows, h.mx::text AS annual_max_period_end,
    CASE WHEN h.symbol IS NULL OR h.n < 1 THEN 'missing'
         WHEN h.mx < (CURRENT_DATE - integer '45') THEN 'stale_settlement'
         ELSE NULL END AS gap_reason
FROM u LEFT JOIN h ON h.symbol=u.symbol
WHERE h.symbol IS NULL OR h.n < 1 OR h.mx < (CURRENT_DATE - integer '45')
ORDER BY u.symbol LIMIT %s
"""


def count_short_interest_gaps(cur: Any) -> int:
    cur.execute(_SI_GAP_COUNT)
    return int((cur.fetchone() or {}).get("n") or 0)


def get_short_interest_gap_details(cur: Any, *, limit: int = 2000) -> Tuple[List[Dict[str, Any]], int]:
    cur.execute(_SI_GAP_COUNT)
    total = int((cur.fetchone() or {}).get("n") or 0)
    cur.execute(_SI_GAP_DETAIL, (max(1, min(int(limit), 5000)),))
    return [dict(r) for r in (cur.fetchall() or [])], total


_SV_GAP_COUNT = """
WITH u AS (SELECT symbol FROM public.v_us_equity_universe),
d AS (
    SELECT symbol, count(*)::integer AS n, max(trade_date) AS mx
    FROM public.stock_short_volume
    WHERE source='massive'
    GROUP BY symbol
)
SELECT count(*)::bigint AS n FROM u
LEFT JOIN d ON d.symbol=u.symbol
WHERE d.symbol IS NULL OR d.n < 5 OR d.mx < (CURRENT_DATE - integer '14')
"""

_SV_GAP_DETAIL = """
WITH u AS (SELECT symbol FROM public.v_us_equity_universe),
d AS (
    SELECT symbol, count(*)::integer AS n, max(trade_date) AS mx
    FROM public.stock_short_volume
    WHERE source='massive'
    GROUP BY symbol
)
SELECT u.symbol, COALESCE(d.n,0) AS quarterly_rows, d.mx::text AS annual_max_period_end,
    CASE WHEN d.symbol IS NULL OR d.n < 5 THEN 'insufficient_rows'
         WHEN d.mx < (CURRENT_DATE - integer '14') THEN 'stale_trade_dates'
         ELSE NULL END AS gap_reason
FROM u LEFT JOIN d ON d.symbol=u.symbol
WHERE d.symbol IS NULL OR d.n < 5 OR d.mx < (CURRENT_DATE - integer '14')
ORDER BY u.symbol LIMIT %s
"""


def count_short_volume_gaps(cur: Any) -> int:
    cur.execute(_SV_GAP_COUNT)
    return int((cur.fetchone() or {}).get("n") or 0)


def get_short_volume_gap_details(cur: Any, *, limit: int = 2000) -> Tuple[List[Dict[str, Any]], int]:
    cur.execute(_SV_GAP_COUNT)
    total = int((cur.fetchone() or {}).get("n") or 0)
    cur.execute(_SV_GAP_DETAIL, (max(1, min(int(limit), 5000)),))
    return [dict(r) for r in (cur.fetchall() or [])], total



def financials_gap_symbols_from_db(cur: Any, kind: str, *, batch_size: int = 50) -> Dict[str, Any]:
    """Return gap symbol batches for a fundamentals feed kind (DB-only)."""
    k = (kind or "").strip().lower()
    if k == "feed_stocks_income_statements":
        cur.execute(
            f"""
            WITH u AS (
                SELECT u.symbol
                FROM public.v_us_equity_universe u
                WHERE upper(coalesce(u.instrument_type, '')) IN {_FINANCIAL_STATEMENTS_INSTRUMENT_TYPES}
            ),
            q AS (
                SELECT symbol,
                       count(*)::integer AS n,
                       count(*) FILTER (WHERE basic_earnings_per_share IS NOT NULL)::integer AS eps_n,
                       count(*) FILTER (WHERE revenue IS NOT NULL)::integer AS rev_n
                FROM public.stock_income_statements
                WHERE source = 'massive' AND timeframe = 'quarterly'
                GROUP BY symbol
            ),
            a AS (
                SELECT symbol, count(*)::integer AS n
                FROM public.stock_income_statements
                WHERE source = 'massive' AND timeframe = 'annual'
                GROUP BY symbol
            )
            SELECT u.symbol FROM u
            LEFT JOIN q ON q.symbol = u.symbol
            LEFT JOIN a ON a.symbol = u.symbol
            WHERE q.symbol IS NULL OR q.n < 5 OR a.symbol IS NULL OR a.n < 4
               OR (q.n > 0 AND (q.eps_n::float / q.n) < 0.8)
               OR (q.n > 0 AND (q.rev_n::float / q.n) < 0.8)
            ORDER BY u.symbol
            """
        )
    elif k == "feed_stocks_balance_sheets":
        cur.execute(
            f"""
            WITH u AS (
                SELECT u.symbol
                FROM public.v_us_equity_universe u
                WHERE upper(coalesce(u.instrument_type, '')) IN {_FINANCIAL_STATEMENTS_INSTRUMENT_TYPES}
            ),
            q AS (
                SELECT symbol, count(*)::integer AS n,
                       count(*) FILTER (WHERE total_assets IS NOT NULL)::integer AS ta_n
                FROM public.stock_balance_sheets
                WHERE source='massive' AND timeframe='quarterly'
                GROUP BY symbol
            )
            SELECT u.symbol FROM u
            LEFT JOIN q ON q.symbol=u.symbol
            WHERE q.symbol IS NULL OR q.n < 4 OR (q.n > 0 AND (q.ta_n::float/q.n) < 0.9)
            ORDER BY u.symbol
            """
        )
    elif k == "feed_stocks_cash_flows":
        cur.execute(
            f"""
            WITH u AS (
                SELECT u.symbol
                FROM public.v_us_equity_universe u
                WHERE upper(coalesce(u.instrument_type, '')) IN {_FINANCIAL_STATEMENTS_INSTRUMENT_TYPES}
            ),
            q AS (
                SELECT symbol, count(*)::integer AS n,
                       count(*) FILTER (WHERE net_cash_from_operating_activities IS NOT NULL)::integer AS op_n
                FROM public.stock_cash_flows
                WHERE source='massive' AND timeframe='quarterly'
                GROUP BY symbol
            )
            SELECT u.symbol FROM u
            LEFT JOIN q ON q.symbol=u.symbol
            WHERE q.symbol IS NULL OR q.n < 4 OR (q.n > 0 AND (op_n::float/q.n) < 0.8)
            ORDER BY u.symbol
            """
        )
    elif k == "feed_stocks_ratios":
        cur.execute(
            f"""
            WITH u AS (
                SELECT u.symbol
                FROM public.v_us_equity_universe u
                WHERE upper(coalesce(u.instrument_type, '')) IN {_FINANCIAL_STATEMENTS_INSTRUMENT_TYPES}
            ),
            q AS (
                SELECT symbol, count(*)::integer AS n,
                       max(date) AS mx
                FROM public.stock_ratios
                WHERE source='massive'
                GROUP BY symbol
            )
            SELECT u.symbol FROM u
            LEFT JOIN q ON q.symbol=u.symbol
            WHERE q.symbol IS NULL OR q.n < 1 OR q.mx < (CURRENT_DATE - integer '45')
            ORDER BY u.symbol
            """
        )
    elif k == "feed_stocks_short_interest":
        cur.execute(
            """
            WITH u AS (SELECT symbol FROM public.v_us_equity_universe),
            h AS (
                SELECT symbol, count(*)::integer AS n, max(settlement_date) AS mx
                FROM public.stock_short_interest
                WHERE source='massive'
                GROUP BY symbol
            )
            SELECT u.symbol FROM u
            LEFT JOIN h ON h.symbol=u.symbol
            WHERE h.symbol IS NULL OR h.n < 1 OR h.mx < (CURRENT_DATE - integer '45')
            ORDER BY u.symbol
            """
        )
    elif k == "feed_stocks_short_volume":
        cur.execute(
            """
            WITH u AS (SELECT symbol FROM public.v_us_equity_universe),
            d AS (
                SELECT symbol, count(*)::integer AS n, max(trade_date) AS mx
                FROM public.stock_short_volume
                WHERE source='massive'
                GROUP BY symbol
            )
            SELECT u.symbol FROM u
            LEFT JOIN d ON d.symbol=u.symbol
            WHERE d.symbol IS NULL OR d.n < 5 OR d.mx < (CURRENT_DATE - integer '14')
            ORDER BY u.symbol
            """
        )
    else:
        return {"ok": False, "error": f"unknown fundamentals kind: {kind}"}

    syms = [s for s in (_symbol_from_gap_sql_row(r) for r in (cur.fetchall() or [])) if s]
    bs = max(1, min(int(batch_size), 200))
    batches = [syms[i : i + bs] for i in range(0, len(syms), bs)]
    return {"ok": True, "gap_count": len(syms), "batches": batches}


# ── Batch readers for fundamentals extension evaluators ──────────────────────

def fetch_income_ext_rows_batch(
    cur: Any,
    symbols: List[str],
) -> Dict[str, List[Dict[str, Any]]]:
    """Batch-read quarterly income-statement rows with extra columns needed by ext evaluators.

    Returns symbol -> list of dicts (ascending period_end).
    Columns: revenue, gross_profit, operating_income, ebitda, cost_of_revenue,
    consolidated_net_income_loss, interest_expense, diluted_shares_outstanding.
    """
    from collections import defaultdict

    out: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    if not symbols:
        return dict(out)
    cur.execute(
        """
        SELECT symbol, fiscal_year, fiscal_quarter, period_end,
               revenue, gross_profit, operating_income, ebitda, cost_of_revenue,
               consolidated_net_income_loss, interest_expense,
               diluted_shares_outstanding
        FROM public.stock_income_statements
        WHERE symbol = ANY(%s)
          AND source = 'massive'
          AND timeframe = 'quarterly'
        ORDER BY symbol, period_end ASC
        """,
        (symbols,),
    )
    for r in cur.fetchall() or []:
        out[r["symbol"]].append(dict(r))
    return dict(out)


def fetch_balance_sheet_rows_for_ext_batch(
    cur: Any,
    symbols: List[str],
    *,
    max_quarters: int = 6,
) -> Dict[str, List[Dict[str, Any]]]:
    """Batch-read latest N quarterly balance-sheet rows for each symbol.

    Returns symbol -> list of dicts (ascending period_end).
    """
    from collections import defaultdict

    out: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    if not symbols:
        return dict(out)
    cur.execute(
        """
        SELECT * FROM (
            SELECT symbol, period_end, fiscal_year, fiscal_quarter,
                   cash_and_equivalents, short_term_investments,
                   receivables, inventories,
                   total_current_assets, total_current_liabilities,
                   total_assets, total_liabilities, total_equity,
                   debt_current, long_term_debt_and_capital_lease_obligations,
                   goodwill, intangible_assets_net,
                   property_plant_equipment_net, retained_earnings_deficit,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY period_end DESC) AS rn
            FROM public.stock_balance_sheets
            WHERE symbol = ANY(%s)
              AND source = 'massive'
              AND timeframe = 'quarterly'
        ) ranked
        WHERE rn <= %s
        ORDER BY symbol, period_end ASC
        """,
        (symbols, max_quarters),
    )
    for r in cur.fetchall() or []:
        d = dict(r)
        d.pop("rn", None)
        out[d["symbol"]].append(d)
    return dict(out)


def fetch_cash_flow_rows_for_ext_batch(
    cur: Any,
    symbols: List[str],
    *,
    max_quarters: int = 6,
) -> Dict[str, List[Dict[str, Any]]]:
    """Batch-read latest N quarterly cash-flow rows for each symbol.

    Returns symbol -> list of dicts (ascending period_end).
    """
    from collections import defaultdict

    out: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    if not symbols:
        return dict(out)
    cur.execute(
        """
        SELECT * FROM (
            SELECT symbol, period_end, fiscal_year, fiscal_quarter,
                   net_income,
                   net_cash_from_operating_activities,
                   purchase_of_property_plant_and_equipment,
                   depreciation_depletion_and_amortization,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY period_end DESC) AS rn
            FROM public.stock_cash_flows
            WHERE symbol = ANY(%s)
              AND source = 'massive'
              AND timeframe = 'quarterly'
        ) ranked
        WHERE rn <= %s
        ORDER BY symbol, period_end ASC
        """,
        (symbols, max_quarters),
    )
    for r in cur.fetchall() or []:
        d = dict(r)
        d.pop("rn", None)
        out[d["symbol"]].append(d)
    return dict(out)


def fetch_ratios_latest_for_ext_batch(
    cur: Any,
    symbols: List[str],
) -> Dict[str, Dict[str, Any]]:
    """Batch-read the latest ratios row per symbol (DISTINCT ON).

    Returns symbol -> single dict.
    """
    out: Dict[str, Dict[str, Any]] = {}
    if not symbols:
        return out
    cur.execute(
        """
        SELECT DISTINCT ON (symbol)
            symbol, date,
            price_to_earnings, price_to_sales, price_to_book,
            price_to_free_cash_flow, price_to_cash_flow,
            debt_to_equity, return_on_equity, return_on_assets,
            market_cap, free_cash_flow, earnings_per_share,
            average_volume, dividend_yield,
            enterprise_value, ev_to_ebitda, ev_to_sales,
            "current" AS current_ratio_from_ratios,
            quick AS quick_ratio_from_ratios
        FROM public.stock_ratios
        WHERE symbol = ANY(%s)
          AND source = 'massive'
        ORDER BY symbol, date DESC
        """,
        (symbols,),
    )
    for r in cur.fetchall() or []:
        out[r["symbol"]] = dict(r)
    return out


def fetch_short_interest_latest_batch(
    cur: Any,
    symbols: List[str],
    *,
    max_rows: int = 2,
) -> Dict[str, List[Dict[str, Any]]]:
    """Batch-read latest N short-interest rows per symbol.

    Returns symbol -> list of dicts (ascending settlement_date).
    """
    from collections import defaultdict

    out: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    if not symbols:
        return dict(out)
    cur.execute(
        """
        SELECT * FROM (
            SELECT symbol, settlement_date, short_interest, avg_daily_volume, days_to_cover,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY settlement_date DESC) AS rn
            FROM public.stock_short_interest
            WHERE symbol = ANY(%s)
              AND source = 'massive'
        ) ranked
        WHERE rn <= %s
        ORDER BY symbol, settlement_date ASC
        """,
        (symbols, max_rows),
    )
    for r in cur.fetchall() or []:
        d = dict(r)
        d.pop("rn", None)
        out[d["symbol"]].append(d)
    return dict(out)


def fetch_short_volume_recent_batch(
    cur: Any,
    symbols: List[str],
    *,
    max_days: int = 10,
) -> Dict[str, List[Dict[str, Any]]]:
    """Batch-read latest N short-volume rows per symbol.

    Returns symbol -> list of dicts (ascending trade_date).
    """
    from collections import defaultdict

    out: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    if not symbols:
        return dict(out)
    cur.execute(
        """
        SELECT * FROM (
            SELECT symbol, trade_date, short_volume, short_volume_ratio, total_volume,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date DESC) AS rn
            FROM public.stock_short_volume
            WHERE symbol = ANY(%s)
              AND source = 'massive'
        ) ranked
        WHERE rn <= %s
        ORDER BY symbol, trade_date ASC
        """,
        (symbols, max_days),
    )
    for r in cur.fetchall() or []:
        d = dict(r)
        d.pop("rn", None)
        out[d["symbol"]].append(d)
    return dict(out)
