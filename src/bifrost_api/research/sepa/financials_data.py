"""SEPA fundamentals readers against ``market.stock_financials`` (jsonb).

Replaces six flat ``public.stock_*`` tables. Writers/ops feeds remain in the worker
until P9; this module only *reads* ``market.stock_financials``.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

SOURCE_DEFAULT = "massive"

# report_type values written by plugin ingest/financials.py (+ SCHEMA.md for ratios/short).
REPORT_INCOME = "income_statement"
REPORT_BALANCE = "balance_sheet"
REPORT_CASH_FLOW = "cash_flow_statement"
REPORT_RATIOS = "ratios"
REPORT_SHORT_INTEREST = "short_interest"
REPORT_SHORT_VOLUME = "short_volume"

_FQ_TO_PERIOD = {0: "FY", 1: "Q1", 2: "Q2", 3: "Q3", 4: "Q4"}

_FINANCIAL_STATEMENTS_INSTRUMENT_TYPES = (
    "CS",
    "ADRC",
    "PFD",
)

# dest_field -> candidate keys inside jsonb ``data`` (Polygon vx nested or flat).
_INCOME_FIELDS: Dict[str, Tuple[str, ...]] = {
    "basic_earnings_per_share": ("basic_earnings_per_share",),
    "diluted_earnings_per_share": ("diluted_earnings_per_share",),
    "revenue": ("revenues", "revenue"),
    "revenues": ("revenues", "revenue"),
    "gross_profit": ("gross_profit",),
    "operating_income": ("operating_income_loss", "operating_income"),
    "ebitda": ("ebitda", "earnings_before_interest_taxes_depreciation_amortization"),
    "cost_of_revenue": ("cost_of_revenue",),
    "consolidated_net_income_loss": ("net_income_loss", "consolidated_net_income_loss", "net_income"),
    "interest_expense": ("interest_expense",),
    "diluted_shares_outstanding": ("diluted_average_shares", "diluted_shares_outstanding"),
    "basic_shares_outstanding": ("basic_average_shares", "basic_shares_outstanding"),
}

_BALANCE_FIELDS: Dict[str, Tuple[str, ...]] = {
    "cash_and_equivalents": ("cash", "cash_and_equivalents", "cash_and_cash_equivalents"),
    "short_term_investments": ("short_term_investments",),
    "receivables": ("accounts_receivable", "receivables"),
    "inventories": ("inventory", "inventories"),
    "total_current_assets": ("current_assets", "total_current_assets"),
    "total_current_liabilities": ("current_liabilities", "total_current_liabilities"),
    "total_assets": ("assets", "total_assets"),
    "total_liabilities": ("liabilities", "total_liabilities"),
    "total_equity": ("equity", "total_equity"),
    "debt_current": ("debt_current", "current_debt"),
    "long_term_debt_and_capital_lease_obligations": (
        "long_term_debt",
        "long_term_debt_and_capital_lease_obligations",
    ),
    "goodwill": ("goodwill",),
    "intangible_assets_net": ("intangible_assets", "intangible_assets_net"),
    "property_plant_equipment_net": (
        "property_plant_equipment_net",
        "fixed_assets",
    ),
    "retained_earnings_deficit": ("retained_earnings", "retained_earnings_deficit"),
}

_CASH_FLOW_FIELDS: Dict[str, Tuple[str, ...]] = {
    "net_income": ("net_income_loss", "net_income"),
    "net_cash_from_operating_activities": ("net_cash_from_operating_activities",),
    "net_cash_from_investing_activities": ("net_cash_from_investing_activities",),
    "net_cash_from_financing_activities": ("net_cash_from_financing_activities",),
    "purchase_of_property_plant_and_equipment": (
        "purchase_of_property_plant_and_equipment",
    ),
    "depreciation_depletion_and_amortization": (
        "depreciation_depletion_and_amortization",
        "depreciation_and_amortization",
    ),
    "change_in_cash_and_equivalents": ("change_in_cash_and_equivalents",),
}

_RATIOS_FIELDS: Dict[str, Tuple[str, ...]] = {
    "price_to_earnings": ("price_to_earnings",),
    "price_to_sales": ("price_to_sales",),
    "price_to_book": ("price_to_book",),
    "price_to_free_cash_flow": ("price_to_free_cash_flow",),
    "price_to_cash_flow": ("price_to_cash_flow",),
    "debt_to_equity": ("debt_to_equity",),
    "return_on_equity": ("return_on_equity",),
    "return_on_assets": ("return_on_assets",),
    "market_cap": ("market_cap",),
    "free_cash_flow": ("free_cash_flow",),
    "earnings_per_share": ("earnings_per_share",),
    "average_volume": ("average_volume",),
    "dividend_yield": ("dividend_yield",),
    "enterprise_value": ("enterprise_value",),
    "ev_to_ebitda": ("ev_to_ebitda",),
    "ev_to_sales": ("ev_to_sales",),
    "current_ratio_from_ratios": ("current", "current_ratio"),
    "quick_ratio_from_ratios": ("quick", "quick_ratio"),
}

_SHORT_INTEREST_FIELDS: Dict[str, Tuple[str, ...]] = {
    "short_interest": ("short_interest",),
    "avg_daily_volume": ("avg_daily_volume",),
    "days_to_cover": ("days_to_cover",),
}

_SHORT_VOLUME_FIELDS: Dict[str, Tuple[str, ...]] = {
    "short_volume": ("short_volume",),
    "short_volume_ratio": ("short_volume_ratio",),
    "total_volume": ("total_volume",),
}


def _json_scalar(v: Any) -> Any:
    if isinstance(v, dict) and "value" in v:
        return v.get("value")
    return v


def unpack_financial_data(
    data: Any,
    field_map: Dict[str, Tuple[str, ...]],
) -> Dict[str, Any]:
    """Flatten jsonb ``data`` into legacy column names expected by SEPA callers."""
    raw = data if isinstance(data, dict) else {}
    out: Dict[str, Any] = {}
    for dest, keys in field_map.items():
        val = None
        for k in keys:
            if k in raw:
                val = _json_scalar(raw[k])
                break
        out[dest] = val
    return out


def _jf(*keys: str) -> str:
    """SQL expression: first non-null text from jsonb leaf or nested ``.value``."""
    parts: List[str] = []
    for k in keys:
        parts.append(f"(data->'{k}'->>'value')")
        parts.append(f"(NULLIF(data->>'{k}', ''))")
    return f"COALESCE({', '.join(parts)})"


def _jf_not_null(*keys: str) -> str:
    return f"({_jf(*keys)}) IS NOT NULL"


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
    """Build quarterly/annual row dicts for ``evaluate_fundamentals`` from ``market.stock_financials``.

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
                "SELECT to_regclass('market.stock_financials') IS NOT NULL AS texists"
            )
            if not bool((cur.fetchone() or {}).get("texists")):
                return None
            cur.execute(
                """
                SELECT period_type AS timeframe, fiscal_year, fiscal_quarter,
                       period_date AS period_end, data
                FROM market.stock_financials
                WHERE symbol = %s
                  AND report_type = %s
                  AND lower(period_type) = 'quarterly'
                ORDER BY fiscal_year ASC NULLS LAST, fiscal_quarter ASC NULLS LAST, period_date ASC
                """,
                (sym, REPORT_INCOME),
            )
            q_db = cur.fetchall() or []
            cur.execute(
                """
                SELECT period_type AS timeframe, fiscal_year, fiscal_quarter,
                       period_date AS period_end, data
                FROM market.stock_financials
                WHERE symbol = %s
                  AND report_type = %s
                  AND lower(period_type) = 'annual'
                ORDER BY fiscal_year ASC NULLS LAST, period_date ASC
                """,
                (sym, REPORT_INCOME),
            )
            a_db = cur.fetchall() or []
    finally:
        conn.close()
    if len(q_db) < min_quarterly or len(a_db) < min_annual:
        return None

    def _enrich(r: Any) -> Dict[str, Any]:
        flat = unpack_financial_data(r.get("data"), _INCOME_FIELDS)
        return {**dict(r), **flat}

    def _map_q(r: Any) -> Dict[str, Any]:
        r = _enrich(r)
        fq = int(r.get("fiscal_quarter") or 0)
        fp = _FQ_TO_PERIOD.get(fq, f"Q{fq}" if fq else "FY")
        pe = r.get("period_end")
        pe_s = pe.isoformat() if hasattr(pe, "isoformat") else (str(pe)[:10] if pe else None)
        return {
            "fiscal_year": int(r.get("fiscal_year") or 0),
            "fiscal_period": fp,
            "filing_date": None,
            "timeframe": "quarterly",
            "start_date": pe_s,
            "end_date": pe_s,
            "basic_earnings_per_share": r.get("basic_earnings_per_share"),
            "diluted_earnings_per_share": r.get("diluted_earnings_per_share"),
            "revenues": r.get("revenue"),
        }

    def _map_a(r: Any) -> Dict[str, Any]:
        r = _enrich(r)
        pe = r.get("period_end")
        pe_s = pe.isoformat() if hasattr(pe, "isoformat") else (str(pe)[:10] if pe else None)
        return {
            "fiscal_year": int(r.get("fiscal_year") or 0),
            "fiscal_period": "FY",
            "filing_date": None,
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

_EPS_NN = _jf_not_null("basic_earnings_per_share")
_REV_NN = _jf_not_null("revenues", "revenue")
_TA_NN = _jf_not_null("assets", "total_assets")
_OCF_NN = _jf_not_null("net_cash_from_operating_activities")

_INCOME_GAP_DETAIL_SQL = f"""
WITH u AS (
    SELECT u.symbol
    FROM public.v_us_equity_universe u
    WHERE upper(coalesce(u.instrument_type, '')) IN {_FINANCIAL_STATEMENTS_INSTRUMENT_TYPES}
),
q AS (
    SELECT symbol,
           count(*)::integer AS n,
           count(*) FILTER (WHERE {_EPS_NN})::integer AS eps_n,
           count(*) FILTER (WHERE {_REV_NN})::integer AS rev_n,
           max(period_date) AS max_pe
    FROM market.stock_financials
    WHERE report_type = '{REPORT_INCOME}' AND lower(period_type) = 'quarterly'
    GROUP BY symbol
),
a AS (
    SELECT symbol, count(*)::integer AS n, max(period_date) AS max_pe
    FROM market.stock_financials
    WHERE report_type = '{REPORT_INCOME}' AND lower(period_type) = 'annual'
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
           count(*) FILTER (WHERE {_EPS_NN})::integer AS eps_n,
           count(*) FILTER (WHERE {_REV_NN})::integer AS rev_n
    FROM market.stock_financials
    WHERE report_type = '{REPORT_INCOME}' AND lower(period_type) = 'quarterly'
    GROUP BY symbol
),
a AS (
    SELECT symbol, count(*)::integer AS n
    FROM market.stock_financials
    WHERE report_type = '{REPORT_INCOME}' AND lower(period_type) = 'annual'
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
           count(*) FILTER (WHERE {_TA_NN})::integer AS ta_n
    FROM market.stock_financials
    WHERE report_type = '{REPORT_BALANCE}' AND lower(period_type) = 'quarterly'
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
           count(*) FILTER (WHERE {_TA_NN})::integer AS ta_n
    FROM market.stock_financials
    WHERE report_type = '{REPORT_BALANCE}' AND lower(period_type) = 'quarterly'
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
           count(*) FILTER (WHERE {_OCF_NN})::integer AS op_n
    FROM market.stock_financials
    WHERE report_type = '{REPORT_CASH_FLOW}' AND lower(period_type) = 'quarterly'
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
           count(*) FILTER (WHERE {_OCF_NN})::integer AS op_n
    FROM market.stock_financials
    WHERE report_type = '{REPORT_CASH_FLOW}' AND lower(period_type) = 'quarterly'
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
           max(period_date) AS mx
    FROM market.stock_financials
    WHERE report_type = '{REPORT_RATIOS}'
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
           max(period_date) AS mx
    FROM market.stock_financials
    WHERE report_type = '{REPORT_RATIOS}'
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


_SI_GAP_COUNT = f"""
WITH u AS (SELECT symbol FROM public.v_us_equity_universe),
h AS (
    SELECT symbol, count(*)::integer AS n,
           max(period_date) AS mx
    FROM market.stock_financials
    WHERE report_type = '{REPORT_SHORT_INTEREST}'
    GROUP BY symbol
)
SELECT count(*)::bigint AS n FROM u
LEFT JOIN h ON h.symbol=u.symbol
WHERE h.symbol IS NULL OR h.n < 1 OR h.mx < (CURRENT_DATE - integer '45')
"""

_SI_GAP_DETAIL = f"""
WITH u AS (SELECT symbol FROM public.v_us_equity_universe),
h AS (
    SELECT symbol, count(*)::integer AS n,
           max(period_date) AS mx
    FROM market.stock_financials
    WHERE report_type = '{REPORT_SHORT_INTEREST}'
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


_SV_GAP_COUNT = f"""
WITH u AS (SELECT symbol FROM public.v_us_equity_universe),
d AS (
    SELECT symbol, count(*)::integer AS n, max(period_date) AS mx
    FROM market.stock_financials
    WHERE report_type = '{REPORT_SHORT_VOLUME}'
    GROUP BY symbol
)
SELECT count(*)::bigint AS n FROM u
LEFT JOIN d ON d.symbol=u.symbol
WHERE d.symbol IS NULL OR d.n < 5 OR d.mx < (CURRENT_DATE - integer '14')
"""

_SV_GAP_DETAIL = f"""
WITH u AS (SELECT symbol FROM public.v_us_equity_universe),
d AS (
    SELECT symbol, count(*)::integer AS n, max(period_date) AS mx
    FROM market.stock_financials
    WHERE report_type = '{REPORT_SHORT_VOLUME}'
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
                       count(*) FILTER (WHERE {_EPS_NN})::integer AS eps_n,
                       count(*) FILTER (WHERE {_REV_NN})::integer AS rev_n
                FROM market.stock_financials
                WHERE report_type = '{REPORT_INCOME}' AND lower(period_type) = 'quarterly'
                GROUP BY symbol
            ),
            a AS (
                SELECT symbol, count(*)::integer AS n
                FROM market.stock_financials
                WHERE report_type = '{REPORT_INCOME}' AND lower(period_type) = 'annual'
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
                       count(*) FILTER (WHERE {_TA_NN})::integer AS ta_n
                FROM market.stock_financials
                WHERE report_type = '{REPORT_BALANCE}' AND lower(period_type) = 'quarterly'
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
                       count(*) FILTER (WHERE {_OCF_NN})::integer AS op_n
                FROM market.stock_financials
                WHERE report_type = '{REPORT_CASH_FLOW}' AND lower(period_type) = 'quarterly'
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
                       max(period_date) AS mx
                FROM market.stock_financials
                WHERE report_type = '{REPORT_RATIOS}'
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
            f"""
            WITH u AS (SELECT symbol FROM public.v_us_equity_universe),
            h AS (
                SELECT symbol, count(*)::integer AS n, max(period_date) AS mx
                FROM market.stock_financials
                WHERE report_type = '{REPORT_SHORT_INTEREST}'
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
            f"""
            WITH u AS (SELECT symbol FROM public.v_us_equity_universe),
            d AS (
                SELECT symbol, count(*)::integer AS n, max(period_date) AS mx
                FROM market.stock_financials
                WHERE report_type = '{REPORT_SHORT_VOLUME}'
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
    """
    from collections import defaultdict

    out: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    if not symbols:
        return dict(out)
    cur.execute(
        """
        SELECT symbol, fiscal_year, fiscal_quarter,
               period_date AS period_end, data
        FROM market.stock_financials
        WHERE symbol = ANY(%s)
          AND report_type = %s
          AND lower(period_type) = 'quarterly'
        ORDER BY symbol, period_date ASC
        """,
        (symbols, REPORT_INCOME),
    )
    for r in cur.fetchall() or []:
        row = dict(r)
        data = row.pop("data", None)
        flat = unpack_financial_data(data, _INCOME_FIELDS)
        out[row["symbol"]].append({**row, **flat})
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
            SELECT symbol, period_date AS period_end, fiscal_year, fiscal_quarter, data,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY period_date DESC) AS rn
            FROM market.stock_financials
            WHERE symbol = ANY(%s)
              AND report_type = %s
              AND lower(period_type) = 'quarterly'
        ) ranked
        WHERE rn <= %s
        ORDER BY symbol, period_end ASC
        """,
        (symbols, REPORT_BALANCE, max_quarters),
    )
    for r in cur.fetchall() or []:
        d = dict(r)
        d.pop("rn", None)
        data = d.pop("data", None)
        flat = unpack_financial_data(data, _BALANCE_FIELDS)
        out[d["symbol"]].append({**d, **flat})
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
            SELECT symbol, period_date AS period_end, fiscal_year, fiscal_quarter, data,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY period_date DESC) AS rn
            FROM market.stock_financials
            WHERE symbol = ANY(%s)
              AND report_type = %s
              AND lower(period_type) = 'quarterly'
        ) ranked
        WHERE rn <= %s
        ORDER BY symbol, period_end ASC
        """,
        (symbols, REPORT_CASH_FLOW, max_quarters),
    )
    for r in cur.fetchall() or []:
        d = dict(r)
        d.pop("rn", None)
        data = d.pop("data", None)
        flat = unpack_financial_data(data, _CASH_FLOW_FIELDS)
        out[d["symbol"]].append({**d, **flat})
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
            symbol, period_date AS date, data
        FROM market.stock_financials
        WHERE symbol = ANY(%s)
          AND report_type = %s
        ORDER BY symbol, period_date DESC
        """,
        (symbols, REPORT_RATIOS),
    )
    for r in cur.fetchall() or []:
        row = dict(r)
        data = row.pop("data", None)
        flat = unpack_financial_data(data, _RATIOS_FIELDS)
        out[row["symbol"]] = {**row, **flat}
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
            SELECT symbol, period_date AS settlement_date, data,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY period_date DESC) AS rn
            FROM market.stock_financials
            WHERE symbol = ANY(%s)
              AND report_type = %s
        ) ranked
        WHERE rn <= %s
        ORDER BY symbol, settlement_date ASC
        """,
        (symbols, REPORT_SHORT_INTEREST, max_rows),
    )
    for r in cur.fetchall() or []:
        d = dict(r)
        d.pop("rn", None)
        data = d.pop("data", None)
        flat = unpack_financial_data(data, _SHORT_INTEREST_FIELDS)
        out[d["symbol"]].append({**d, **flat})
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
            SELECT symbol, period_date AS trade_date, data,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY period_date DESC) AS rn
            FROM market.stock_financials
            WHERE symbol = ANY(%s)
              AND report_type = %s
        ) ranked
        WHERE rn <= %s
        ORDER BY symbol, trade_date ASC
        """,
        (symbols, REPORT_SHORT_VOLUME, max_days),
    )
    for r in cur.fetchall() or []:
        d = dict(r)
        d.pop("rn", None)
        data = d.pop("data", None)
        flat = unpack_financial_data(data, _SHORT_VOLUME_FIELDS)
        out[d["symbol"]].append({**d, **flat})
    return dict(out)
