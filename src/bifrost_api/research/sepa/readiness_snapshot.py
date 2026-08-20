"""SEPA universe + stock_day readiness snapshot (shared by Research API and scripts)."""

from __future__ import annotations

from copy import deepcopy
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor

from bifrost_core.persistence.postgres.connection import _get_conn_params

logger = logging.getLogger(__name__)


# Step 3 vendor gap detection is now delegated to the Market Data Plugin via
# GET /readiness/vendor-gap. The CTE _STOCK_DAY_VENDOR_GAP_CANDIDATE_SQL and
# related constants (_STOCK_DAY_GAP_CLOSE_MATCH_ABS_EPS, _STOCK_DAY_GAP_LATEST_BAR_LOOKBACK_DAYS)
# have been retired.

# Shipped with GET /readiness/summary for UI: raw DB objects vs derived views/tables (English labels).
READINESS_DATA_CATALOG: Dict[str, Any] = {
    "raw_sources": [
        {
            "id": "tickers",
            "object": "public.v_us_equity_universe",
            "role": "US equity universe via FDW from Golden Source market.ticker.",
            "typical_ingest": "FDW (postgres_fdw → market.ticker)",
            "data_points": [
                "symbol",
                "name",
                "market",
                "locale",
                "primary_exchange",
                "instrument_type",
                "active",
                "delisted_utc",
                "list_date",
                "sector",
                "industry",
            ],
        },
        {
            "id": "ticker_overview",
            "object": "public.ticker_overview",
            "role": "Per-ticker detail joined to tickers (sector, list_date, …).",
            "typical_ingest": "Plugin ticker ingest (POST /market/reference/ticker/upsert-overview)",
            "data_points": [
                "tickers_id (FK)",
                "sector",
                "industry",
                "exchange",
                "list_date",
                "ticker_root",
                "market_cap",
                "description",
                "overview_updated_at",
            ],
        },
        {
            "id": "stock_day",
            "object": "market.stock_daily",
            "role": "Daily OHLCV bars; SEPA Phase1/CRS read source=massive.",
            "typical_ingest": "Plugin ingest kind stock_daily_grouped (Polygon grouped daily → market.stock_daily)",
            "data_points": [
                "symbol",
                "bar_time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "source",
                "vwap (optional)",
                "trade_count (optional)",
                "adjusted (optional)",
            ],
        },
        {
            "id": "cache_stock_snapshot",
            "object": "market.stock_snapshot (Plugin)",
            "role": "Plugin-managed stock snapshot — coverage and vendor gap via /readiness/snapshot-coverage and /readiness/vendor-gap.",
            "typical_ingest": "Plugin CronJob stock-snapshot (Golden Source)",
            "data_points": [
                "row_count",
                "last_fetched_at",
                "session_date",
                "by_instrument_type",
            ],
        },
    ],
    "computed_layers": [
        {
            "id": "v_us_equity_universe",
            "object": "public.v_us_equity_universe",
            "role": "Plugin-synced US common-stock universe (Golden Source).",
            "depends_on": ["tickers"],
            "data_points": [
                "tickers_id",
                "symbol",
                "name",
                "market",
                "locale",
                "primary_exchange",
                "instrument_type",
                "active",
                "delisted_utc",
                "list_date",
                "sector",
                "industry",
            ],
        },
        {
            "id": "v_sepa_symbol_price_readiness",
            "object": "Plugin API /readiness/bar-aggregate",
            "role": "Per-symbol bar counts and price_ready computed at query time via Plugin API.",
            "depends_on": ["stock_day"],
            "data_points": [
                "symbol",
                "bar_rows",
                "first_bar_date",
                "last_bar_date",
                "null_close_rows",
                "null_volume_rows",
                "price_ready (derived)",
            ],
        },
        {
            "id": "research_sepa_fundamentals_cache",
            "object": "public.research_sepa_fundamentals_cache",
            "role": "Cached income-statement payload for SEPA fundamentals / Phase4.",
            "typical_ingest": "Written by SEPA Phase4 or fundamentals batch jobs",
            "data_points": [
                "symbol",
                "rule_version",
                "payload (jsonb: evaluation + rows)",
                "source",
                "fetched_at",
                "expire_at",
                "updated_at",
            ],
        },
        {
            "id": "stock_readiness_daily",
            "object": "public.stock_readiness_daily",
            "role": "Materialized daily snapshot (UPSERT) combining universe + bars + financial coverage + SEPA fundamental results written directly by run_fundamentals_local_backfill.",
            "depends_on": [
                "v_us_equity_universe",
                "stock_day",
                "stock_income_statements",
            ],
            "data_points": [
                "as_of_date",
                "symbol",
                "tickers_id",
                "universe_rule_version",
                "price_source",
                "included_in_universe",
                "bar_count_lookback",
                "first_bar_date",
                "last_bar_date",
                "null_close_rows",
                "null_volume_rows",
                "price_ready",
                "fund_cache_present",
                "fund_cache_expire_at",
                "notes",
                "computed_at",
            ],
        },
        {
            "id": "v_sepa_symbol_fund_cache_readiness",
            "object": "public.v_sepa_symbol_fund_cache_readiness",
            "role": "Symbols with non-expired fundamentals cache (optional view).",
            "depends_on": ["research_sepa_fundamentals_cache"],
            "data_points": [
                "symbol",
                "rule_version",
                "fund_cache_valid",
                "expire_at",
                "fetched_at",
            ],
        },
    ],
}


def _split_qualified_object_name(obj: str) -> Tuple[str, str]:
    s = (obj or "").strip()
    if not s:
        return ("public", "")
    if "." not in s:
        return ("public", s)
    schema, name = s.split(".", 1)
    return ((schema or "public").strip(), name.strip())


def _read_object_columns(
    cur: Any,
    *,
    schema: str,
    name: str,
) -> List[str]:
    if not schema or not name:
        return []
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema, name),
    )
    rows = cur.fetchall() or []
    cols: List[str] = []
    for r in rows:
        c = (r or {}).get("column_name")
        if isinstance(c, str) and c.strip():
            cols.append(c.strip())
    return cols


def _read_view_query(
    cur: Any,
    *,
    schema: str,
    name: str,
) -> Optional[str]:
    if not schema or not name:
        return None
    fq_name = f"{schema}.{name}"
    try:
        cur.execute(
            """
            SELECT pg_get_viewdef(to_regclass(%s), true) AS view_sql
            """,
            (fq_name,),
        )
        row = cur.fetchone() or {}
        sql = row.get("view_sql")
        if isinstance(sql, str) and sql.strip():
            return sql.strip()
    except Exception:
        pass
    # Fallback: information_schema.views (works when pg_get_viewdef path is restricted).
    try:
        cur.execute(
            """
            SELECT view_definition
            FROM information_schema.views
            WHERE table_schema = %s
              AND table_name = %s
            """,
            (schema, name),
        )
        row = cur.fetchone() or {}
        sql = row.get("view_definition")
        if isinstance(sql, str) and sql.strip():
            return sql.strip()
    except Exception:
        pass
    return None


def _build_runtime_data_catalog(cur: Any) -> Dict[str, Any]:
    """Return catalog with dynamic data_points from current DB object columns."""
    catalog = deepcopy(READINESS_DATA_CATALOG)
    for bucket in ("raw_sources", "computed_layers"):
        entries = catalog.get(bucket) or []
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            schema, name = _split_qualified_object_name(str(entry.get("object") or ""))
            try:
                dynamic_cols = _read_object_columns(cur, schema=schema, name=name)
                if dynamic_cols:
                    entry["data_points"] = dynamic_cols
            except Exception as e:
                logger.debug("read object columns failed for %s.%s: %s", schema, name, e)
            try:
                view_query = _read_view_query(cur, schema=schema, name=name)
                if view_query:
                    entry["view_query"] = view_query
            except Exception as e:
                logger.debug("read view query failed for %s.%s: %s", schema, name, e)
    return catalog

_ENSURE_FUND_CACHE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS public.research_sepa_fundamentals_cache (
    symbol text NOT NULL,
    rule_version text NOT NULL,
    payload jsonb NOT NULL,
    source text DEFAULT 'massive',
    fetched_at timestamptz NOT NULL DEFAULT now(),
    expire_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, rule_version)
)
"""

_ENSURE_FUND_CACHE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_research_sepa_fund_cache_expire
ON public.research_sepa_fundamentals_cache (expire_at)
"""

def _populate_market_temp_tables(cur: Any) -> None:
    """Create and populate temp tables from Plugin API data.

    Replaces direct market.stock_daily and market.stock_financials SQL
    with Plugin API HTTP calls + local temp tables.
    """
    from psycopg2.extras import execute_values

    from bifrost_api.research.market_data_client import (
        fetch_readiness_bar_aggregate,
        fetch_readiness_financials_coverage,
        fetch_readiness_latest_bar,
    )

    cur.execute("CREATE TEMP TABLE IF NOT EXISTS _tmp_bar_agg ("
                "symbol text PRIMARY KEY, bar_rows integer, first_bar_date date, "
                "last_bar_date date, null_close_rows integer, null_volume_rows integer"
                ") ON COMMIT DROP")
    cur.execute("CREATE TEMP TABLE IF NOT EXISTS _tmp_latest_bar ("
                "symbol text PRIMARY KEY, bar_date date, close double precision"
                ") ON COMMIT DROP")
    cur.execute("CREATE TEMP TABLE IF NOT EXISTS _tmp_latest_bar_full ("
                "symbol text PRIMARY KEY, bar_date date, close double precision"
                ") ON COMMIT DROP")
    cur.execute("CREATE TEMP TABLE IF NOT EXISTS _tmp_fin_income ("
                "symbol text PRIMARY KEY, q_count integer, a_count integer"
                ") ON COMMIT DROP")
    for tbl in ("_tmp_fin_balance_sheet", "_tmp_fin_cash_flow", "_tmp_fin_ratios",
                "_tmp_fin_short_interest", "_tmp_fin_short_volume"):
        cur.execute(f"CREATE TEMP TABLE IF NOT EXISTS {tbl} (symbol text PRIMARY KEY) ON COMMIT DROP")

    bar_data = fetch_readiness_bar_aggregate(window_days=420)
    if bar_data:
        rows = [
            (sym, d.get("bar_rows", 0), d.get("first_bar_date"), d.get("last_bar_date"),
             d.get("null_close_rows", 0), d.get("null_volume_rows", 0))
            for sym, d in bar_data.items()
        ]
        execute_values(cur,
            "INSERT INTO _tmp_bar_agg (symbol, bar_rows, first_bar_date, last_bar_date, "
            "null_close_rows, null_volume_rows) VALUES %s ON CONFLICT DO NOTHING",
            rows, page_size=500)

    latest_bar = fetch_readiness_latest_bar(lookback_days=90)
    if latest_bar:
        rows = [(sym, d.get("bar_date"), d.get("close")) for sym, d in latest_bar.items()]
        execute_values(cur,
            "INSERT INTO _tmp_latest_bar (symbol, bar_date, close) VALUES %s ON CONFLICT DO NOTHING",
            rows, page_size=500)

    cur.execute(
        "INSERT INTO _tmp_latest_bar_full (symbol, bar_date, close) "
        "SELECT symbol, last_bar_date, NULL FROM _tmp_bar_agg "
        "WHERE symbol NOT IN (SELECT symbol FROM _tmp_latest_bar) "
        "ON CONFLICT DO NOTHING"
    )

    fin_cov = fetch_readiness_financials_coverage()
    inc = fin_cov.get("income_statement")
    if inc and isinstance(inc, dict):
        rows = [(sym, d.get("q_count", 0), d.get("a_count", 0)) for sym, d in inc.items()]
        if rows:
            execute_values(cur,
                "INSERT INTO _tmp_fin_income (symbol, q_count, a_count) VALUES %s ON CONFLICT DO NOTHING",
                rows, page_size=500)

    for rtype, tbl in (
        ("balance_sheet", "_tmp_fin_balance_sheet"),
        ("cash_flow_statement", "_tmp_fin_cash_flow"),
        ("ratios", "_tmp_fin_ratios"),
        ("short_interest", "_tmp_fin_short_interest"),
        ("short_volume", "_tmp_fin_short_volume"),
    ):
        syms = fin_cov.get(rtype) or []
        if syms:
            rows = [(s,) for s in syms]
            execute_values(cur,
                f"INSERT INTO {tbl} (symbol) VALUES %s ON CONFLICT DO NOTHING",
                rows, page_size=500)


_SNAPSHOT_INSERT_SQL = """
INSERT INTO public.stock_readiness_daily (
    as_of_date,
    symbol,
    tickers_id,
    universe_rule_version,
    price_source,
    included_in_universe,
    bar_count_lookback,
    first_bar_date,
    last_bar_date,
    null_close_rows,
    null_volume_rows,
    price_ready,
    fund_cache_present,
    fund_cache_expire_at,
    notes,
    computed_at,
    income_stmt_q_count,
    income_stmt_a_count,
    income_stmt_ready,
    balance_sheet_present,
    cash_flow_present,
    ratios_present,
    short_interest_present,
    short_volume_present,
    fundamental_pass,
    fundamental_pass_count,
    fundamental_insufficient,
    fundamental_eval,
    technical_pass,
    technical_pass_count,
    technical_insufficient,
    technical_eval
)
WITH params AS (
    SELECT
        CURRENT_DATE AS as_of_date,
        'v1'::text AS universe_rule_version,
        'massive'::text AS price_source,
        (CURRENT_DATE - integer '420') AS window_start,
        240::integer AS min_bar_rows,
        7::integer AS max_stale_calendar_days
),
u AS (
    SELECT v.tickers_id, v.symbol
    FROM public.v_us_equity_universe v
),
bars AS (
    SELECT
        p.as_of_date,
        sd.symbol,
        p.price_source,
        sd.bar_rows,
        sd.first_bar_date::date AS first_bar_date,
        sd.last_bar_date::date AS last_bar_date,
        sd.null_close_rows,
        sd.null_volume_rows
    FROM params p
    CROSS JOIN _tmp_bar_agg sd
),
symbols AS (
    SELECT symbol FROM u
    UNION
    SELECT symbol FROM bars
),
-- Stage 2 financial coverage aggregates (from Plugin temp tables)
inc_agg AS (SELECT symbol, q_count, a_count FROM _tmp_fin_income),
bs_agg AS (SELECT symbol FROM _tmp_fin_balance_sheet),
cf_agg AS (SELECT symbol FROM _tmp_fin_cash_flow),
rat_agg AS (SELECT symbol FROM _tmp_fin_ratios),
-- Stage 3 short data coverage aggregates (from Plugin temp tables)
si_agg AS (SELECT symbol FROM _tmp_fin_short_interest),
sv_agg AS (SELECT symbol FROM _tmp_fin_short_volume)
SELECT
    p.as_of_date,
    s.symbol,
    u.tickers_id,
    p.universe_rule_version,
    p.price_source,
    (u.tickers_id IS NOT NULL) AS included_in_universe,
    coalesce(b.bar_rows, 0) AS bar_count_lookback,
    b.first_bar_date,
    b.last_bar_date,
    coalesce(b.null_close_rows, 0) AS null_close_rows,
    coalesce(b.null_volume_rows, 0) AS null_volume_rows,
    (
        coalesce(b.bar_rows, 0) >= p.min_bar_rows
        AND b.last_bar_date IS NOT NULL
        AND b.last_bar_date >= (
            p.as_of_date - (p.max_stale_calendar_days || ' days')::interval
        )::date
        AND coalesce(b.null_close_rows, 0) = 0
        AND coalesce(b.null_volume_rows, 0) = 0
    ) AS price_ready,
    false AS fund_cache_present,
    NULL::timestamptz AS fund_cache_expire_at,
    CASE
        WHEN u.tickers_id IS NULL THEN 'symbol not in v_us_equity_universe'
        WHEN coalesce(b.bar_rows, 0) < p.min_bar_rows THEN 'insufficient stock_day rows in lookback window'
        WHEN b.last_bar_date IS NULL THEN 'no stock_day rows in window'
        WHEN b.last_bar_date < (
            p.as_of_date - (p.max_stale_calendar_days || ' days')::interval
        )::date THEN 'stale last bar_time'
        WHEN coalesce(b.null_close_rows, 0) > 0 OR coalesce(b.null_volume_rows, 0) > 0
            THEN 'null close or volume in window'
        ELSE NULL
    END AS notes,
    now() AS computed_at,
    -- Stage 2 financial coverage columns
    coalesce(inc.q_count, 0) AS income_stmt_q_count,
    coalesce(inc.a_count, 0) AS income_stmt_a_count,
    (coalesce(inc.q_count, 0) >= 5 AND coalesce(inc.a_count, 0) >= 4) AS income_stmt_ready,
    (bs.symbol IS NOT NULL)  AS balance_sheet_present,
    (cf.symbol IS NOT NULL)  AS cash_flow_present,
    (rat.symbol IS NOT NULL) AS ratios_present,
    -- Stage 3 short data coverage columns
    (si.symbol IS NOT NULL)  AS short_interest_present,
    (sv.symbol IS NOT NULL)  AS short_volume_present,
    -- Stage 4 SEPA fundamental result columns (written by run_fundamentals_local_backfill, preserved on conflict)
    false       AS fundamental_pass,
    0           AS fundamental_pass_count,
    false       AS fundamental_insufficient,
    NULL::jsonb AS fundamental_eval,
    -- Stage 5 SEPA technical result columns (written by run_technical_local_backfill, preserved on conflict)
    false       AS technical_pass,
    0           AS technical_pass_count,
    false       AS technical_insufficient,
    NULL::jsonb AS technical_eval
FROM params p
CROSS JOIN symbols s
LEFT JOIN u ON u.symbol = s.symbol
LEFT JOIN bars b
    ON b.symbol = s.symbol
   AND b.as_of_date = p.as_of_date
   AND b.price_source = p.price_source
LEFT JOIN inc_agg   inc ON inc.symbol = s.symbol
LEFT JOIN bs_agg    bs  ON bs.symbol  = s.symbol
LEFT JOIN cf_agg    cf  ON cf.symbol  = s.symbol
LEFT JOIN rat_agg   rat ON rat.symbol = s.symbol
LEFT JOIN si_agg    si  ON si.symbol  = s.symbol
LEFT JOIN sv_agg    sv  ON sv.symbol  = s.symbol
ON CONFLICT (as_of_date, symbol, universe_rule_version, price_source)
DO UPDATE SET
    tickers_id              = EXCLUDED.tickers_id,
    included_in_universe    = EXCLUDED.included_in_universe,
    bar_count_lookback      = EXCLUDED.bar_count_lookback,
    first_bar_date          = EXCLUDED.first_bar_date,
    last_bar_date           = EXCLUDED.last_bar_date,
    null_close_rows         = EXCLUDED.null_close_rows,
    null_volume_rows        = EXCLUDED.null_volume_rows,
    price_ready             = EXCLUDED.price_ready,
    fund_cache_present      = CASE WHEN EXCLUDED.fundamental_eval IS NOT NULL THEN EXCLUDED.fund_cache_present ELSE stock_readiness_daily.fund_cache_present END,
    fund_cache_expire_at    = CASE WHEN EXCLUDED.fundamental_eval IS NOT NULL THEN EXCLUDED.fund_cache_expire_at ELSE stock_readiness_daily.fund_cache_expire_at END,
    notes                   = EXCLUDED.notes,
    computed_at             = EXCLUDED.computed_at,
    income_stmt_q_count     = EXCLUDED.income_stmt_q_count,
    income_stmt_a_count     = EXCLUDED.income_stmt_a_count,
    income_stmt_ready       = EXCLUDED.income_stmt_ready,
    balance_sheet_present   = EXCLUDED.balance_sheet_present,
    cash_flow_present       = EXCLUDED.cash_flow_present,
    ratios_present          = EXCLUDED.ratios_present,
    short_interest_present  = EXCLUDED.short_interest_present,
    short_volume_present    = EXCLUDED.short_volume_present,
    fundamental_pass        = CASE WHEN EXCLUDED.fundamental_eval IS NOT NULL THEN EXCLUDED.fundamental_pass ELSE stock_readiness_daily.fundamental_pass END,
    fundamental_pass_count  = CASE WHEN EXCLUDED.fundamental_eval IS NOT NULL THEN EXCLUDED.fundamental_pass_count ELSE stock_readiness_daily.fundamental_pass_count END,
    fundamental_insufficient = CASE WHEN EXCLUDED.fundamental_eval IS NOT NULL THEN EXCLUDED.fundamental_insufficient ELSE stock_readiness_daily.fundamental_insufficient END,
    fundamental_eval        = COALESCE(EXCLUDED.fundamental_eval, stock_readiness_daily.fundamental_eval),
    technical_pass          = CASE WHEN EXCLUDED.technical_eval IS NOT NULL THEN EXCLUDED.technical_pass ELSE stock_readiness_daily.technical_pass END,
    technical_pass_count    = CASE WHEN EXCLUDED.technical_eval IS NOT NULL THEN EXCLUDED.technical_pass_count ELSE stock_readiness_daily.technical_pass_count END,
    technical_insufficient  = CASE WHEN EXCLUDED.technical_eval IS NOT NULL THEN EXCLUDED.technical_insufficient ELSE stock_readiness_daily.technical_insufficient END,
    technical_eval          = COALESCE(EXCLUDED.technical_eval, stock_readiness_daily.technical_eval);
"""


def _db_ok(status_config: Optional[dict]) -> bool:
    if not status_config:
        return False
    return status_config.get("sink") == "postgres" or bool(status_config.get("postgres"))




def run_sepa_universe_readiness_snapshot(
    status_config: dict,
    *,
    statement_timeout_ms: int = 120_000,
) -> Dict[str, Any]:
    """Ensure fund cache table, then upsert today's stock_readiness_daily rows."""
    if not _db_ok(status_config):
        return {"ok": False, "error": "PostgreSQL not configured"}
    params = _get_conn_params(status_config)
    params["connect_timeout"] = 15
    t0 = time.monotonic()
    try:
        conn = psycopg2.connect(**params)
    except Exception as e:
        logger.warning("readiness snapshot connect failed: %s", e)
        return {"ok": False, "error": str(e)}
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET statement_timeout = {int(max(5_000, statement_timeout_ms))}")
            _populate_market_temp_tables(cur)
            cur.execute(
                "DELETE FROM public.stock_readiness_daily WHERE as_of_date < CURRENT_DATE"
            )
            cur.execute(_SNAPSHOT_INSERT_SQL)
            n = cur.rowcount
        conn.commit()
        elapsed_ms = int((time.monotonic() - t0) * 1000)
        return {"ok": True, "rows_affected": n, "elapsed_ms": elapsed_ms}
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.warning("readiness snapshot failed: %s", e)
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


def _view_exists(conn, schema: str, name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.views
                WHERE table_schema = %s AND table_name = %s
            )
            """,
            (schema, name),
        )
        row = cur.fetchone()
    return bool(row and row[0])


def _pg_rel_exists(cur: Any, rel: str) -> bool:
    cur.execute("SELECT to_regclass(%s) IS NOT NULL AS ex", (rel,))
    return bool((cur.fetchone() or {}).get("ex"))


def _fetch_fundamentals_symbol_counts_by_instrument_type(cur: Any) -> Optional[List[Dict[str, Any]]]:
    """Distinct symbols per instrument_type via Plugin API + v_us_equity_universe.

    The Plugin provides total counts per report_type; instrument_type breakdown
    uses the Plugin-synced universe table.
    """
    from bifrost_api.research.market_data_client import fetch_readiness_financials_by_instrument_type

    by_code: Dict[str, Dict[str, Any]] = {}
    try:
        cur.execute(
            """
            SELECT DISTINCT COALESCE(NULLIF(instrument_type, ''), '(unknown)') AS code
            FROM public.v_us_equity_universe
            WHERE COALESCE(active, true)
            """
        )
        for r in cur.fetchall() or []:
            code = str(r.get("code") or "(unknown)")
            by_code[code] = {
                "code": code,
                "income_statement_symbols": 0,
                "balance_sheet_symbols": 0,
                "cash_flow_symbols": 0,
                "ratio_symbols": 0,
            }

        resp = fetch_readiness_financials_by_instrument_type()
        counts = resp.get("counts", {})
        if by_code:
            first_code = next(iter(by_code))
            for col, val in counts.items():
                if first_code in by_code:
                    by_code[first_code][col] = int(val or 0)
    except Exception as e:
        logger.debug("fundamentals_symbol_count_by_type failed: %s", e)
        return None

    rows = list(by_code.values())
    rows.sort(key=lambda x: str(x.get("code") or ""))
    return rows


def fetch_sepa_readiness_summary(status_config: dict) -> Dict[str, Any]:
    """Aggregate counts for UI (live views + today's snapshot table)."""
    if not _db_ok(status_config):
        return {"ok": False, "error": "PostgreSQL not configured"}
    params = _get_conn_params(status_config)
    params["connect_timeout"] = 15
    out: Dict[str, Any] = {"ok": True}
    try:
        conn = psycopg2.connect(**params)
    except Exception as e:
        return {"ok": False, "error": str(e)}
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # JIT helps long analytical queries but adds latency to short summary queries; disable per-session.
            try:
                cur.execute("SET LOCAL jit = off")
            except Exception:
                pass
            cur.execute("SELECT count(*)::bigint AS n FROM public.v_us_equity_universe")
            out["universe_count"] = int((cur.fetchone() or {}).get("n") or 0)

            # Universe counts (Step 1). FDW-backed: no synced_at column.
            out["tickers_active_count"] = out["universe_count"]
            out["tickers_last_synced_at"] = None

            # Price readiness from Plugin API (no local sepa_symbol_price_readiness table)
            try:
                from datetime import date as _date, timedelta as _td

                from bifrost_api.research.market_data_client import (
                    fetch_readiness_bar_aggregate,
                )

                bar_agg = fetch_readiness_bar_aggregate(window_days=420)
                _min_bar_rows = 240
                _stale_cutoff = _date.today() - _td(days=7)
                _total = len(bar_agg)
                _ready = 0
                for _stats in bar_agg.values():
                    _br = int(_stats.get("bar_rows") or 0)
                    _lb = _stats.get("last_bar_date")
                    if _lb and not isinstance(_lb, _date):
                        try:
                            _lb = _date.fromisoformat(str(_lb)[:10])
                        except ValueError:
                            _lb = None
                    _nc = int(_stats.get("null_close_rows") or 0)
                    _nv = int(_stats.get("null_volume_rows") or 0)
                    if _br >= _min_bar_rows and _lb and _lb >= _stale_cutoff and _nc == 0 and _nv == 0:
                        _ready += 1
                out["price_readiness_live"] = {
                    "total_symbols": _total,
                    "price_ready": _ready,
                }
            except Exception as _pr_err:
                logger.warning("price readiness Plugin API call failed: %s", _pr_err)
                out["price_readiness_live"] = {
                    "total_symbols": 0,
                    "price_ready": 0,
                }

            out["fund_cache_view_exists"] = True
            try:
                cur.execute(
                    """
                    SELECT count(*)::bigint AS n
                    FROM public.stock_readiness_daily
                    WHERE as_of_date = CURRENT_DATE
                      AND universe_rule_version = 'v1'
                      AND price_source = 'massive'
                      AND fundamental_eval IS NOT NULL
                      AND fund_cache_expire_at > now()
                    """
                )
                out["fund_cache_valid_count"] = int((cur.fetchone() or {}).get("n") or 0)
            except Exception:
                out["fund_cache_valid_count"] = None

            # Step 2: snapshot coverage via Plugin HTTP (replaces cache_stock_snapshot SQL).
            try:
                from bifrost_api.research.market_data_client import _get_json as _plugin_get
                snap_cov = _plugin_get("/readiness/snapshot-coverage")
                out["stock_unified_snapshot_row_count"] = snap_cov.get("row_count")
                out["stock_unified_snapshot_last_fetched_at"] = snap_cov.get("last_fetched_at")
                by_type = snap_cov.get("by_instrument_type") or []
                out["stock_unified_snapshot_by_type"] = [
                    {
                        "code": r.get("code", ""),
                        "description": r.get("code", ""),
                        "snapshot_row_count": r.get("snapshot_row_count", 0),
                        "universe_ticker_count": r.get("universe_ticker_count", 0),
                    }
                    for r in by_type
                ] if by_type else None
            except Exception as e:
                logger.debug("snapshot-coverage plugin fetch failed: %s", e)
                out["stock_unified_snapshot_row_count"] = None
                out["stock_unified_snapshot_last_fetched_at"] = None
                out["stock_unified_snapshot_by_type"] = None

            try:
                out["fundamentals_symbol_count_by_type"] = (
                    _fetch_fundamentals_symbol_counts_by_instrument_type(cur)
                )
            except Exception as e:
                logger.debug("fundamentals_symbol_count_by_type failed: %s", e)
                out["fundamentals_symbol_count_by_type"] = None

            # Step 3: vendor gap count via Plugin HTTP (replaces CTE on cache_stock_snapshot).
            try:
                from bifrost_api.research.market_data_client import _get_json as _plugin_get
                vg = _plugin_get("/readiness/vendor-gap")
                out["stock_day_vendor_fill_gap_count"] = vg.get("gap_count")
            except Exception as e:
                logger.debug("vendor-gap plugin fetch failed: %s", e)
                out["stock_day_vendor_fill_gap_count"] = None

            # Fundamentals gap counts via Plugin HTTP (SEPA Data Ready Steps 4–9).
            try:
                from bifrost_api.research.sepa import financials_data as _fd

                out["income_statements_gap_count"] = _fd.count_income_statements_gaps()
                out["balance_sheets_gap_count"] = _fd.count_balance_sheet_gaps()
                out["cash_flows_gap_count"] = _fd.count_cash_flow_gaps()
                out["ratios_gap_count"] = _fd.count_ratios_gaps()
                out["short_interest_gap_count"] = _fd.count_short_interest_gaps()
                out["short_volume_gap_count"] = _fd.count_short_volume_gaps()
            except Exception as e:
                logger.debug("fundamentals gap counts failed: %s", e)
                out["income_statements_gap_count"] = None
                out["balance_sheets_gap_count"] = None
                out["cash_flows_gap_count"] = None
                out["ratios_gap_count"] = None
                out["short_interest_gap_count"] = None
                out["short_volume_gap_count"] = None

            # Source-void acknowledgment flags + actionable gap counts (preference_data_gap_ack)
            _GAP_ACK_TYPES = (
                "income_statements", "balance_sheets", "cash_flows",
                "ratios", "short_interest", "short_volume",
            )
            try:
                cur.execute(
                    "SELECT (to_regclass('public.preference_data_gap_ack') IS NOT NULL) AS texists"
                )
                if bool((cur.fetchone() or {}).get("texists")):
                    cur.execute(
                        "SELECT data_type, is_void, acked_gap_count, void_reason "
                        "FROM public.preference_data_gap_ack"
                    )
                    ack_map = {r["data_type"]: r for r in (cur.fetchall() or [])}
                    for dt in _GAP_ACK_TYPES:
                        row = ack_map.get(dt) or {}
                        is_void = bool(row.get("is_void"))
                        acked_n = int(row.get("acked_gap_count") or 0)
                        total_n = out.get(f"{dt}_gap_count")
                        if is_void and total_n is not None:
                            actionable = max(0, total_n - acked_n)
                        else:
                            actionable = total_n
                        out[f"{dt}_source_void"] = is_void
                        out[f"{dt}_acked_gap_count"] = acked_n if is_void else None
                        out[f"{dt}_actionable_gap_count"] = actionable
                        out[f"{dt}_void_reason"] = row.get("void_reason")
                else:
                    for dt in _GAP_ACK_TYPES:
                        out[f"{dt}_source_void"] = False
                        out[f"{dt}_acked_gap_count"] = None
                        out[f"{dt}_actionable_gap_count"] = out.get(f"{dt}_gap_count")
                        out[f"{dt}_void_reason"] = None
            except Exception as e:
                logger.debug("gap_ack fetch failed: %s", e)
                for dt in _GAP_ACK_TYPES:
                    out[f"{dt}_source_void"] = False
                    out[f"{dt}_acked_gap_count"] = None
                    out[f"{dt}_actionable_gap_count"] = out.get(f"{dt}_gap_count")
                    out[f"{dt}_void_reason"] = None

            cur.execute(
                """
                SELECT count(*)::bigint AS n
                FROM public.stock_readiness_daily
                WHERE as_of_date = CURRENT_DATE
                  AND universe_rule_version = 'v1'
                  AND price_source = 'massive'
                """
            )
            snap_total = int((cur.fetchone() or {}).get("n") or 0)
            out["snapshot_populated"] = snap_total > 0

            cur.execute(
                """
                SELECT count(*)::bigint AS n
                FROM public.stock_readiness_daily
                WHERE as_of_date = CURRENT_DATE
                  AND universe_rule_version = 'v1'
                  AND price_source = 'massive'
                  AND included_in_universe
                """
            )
            snap_included = int((cur.fetchone() or {}).get("n") or 0)

            cur.execute(
                """
                SELECT count(*)::bigint AS n
                FROM public.stock_readiness_daily
                WHERE as_of_date = CURRENT_DATE
                  AND universe_rule_version = 'v1'
                  AND price_source = 'massive'
                  AND included_in_universe
                  AND price_ready
                """
            )
            snap_ready = int((cur.fetchone() or {}).get("n") or 0)

            out["snapshot_today"] = {
                "rows_total": snap_total,
                "included_in_universe": snap_included,
                "price_ready": snap_ready,
            }

            cur.execute(
                """
                SELECT coalesce(notes, '(ready)') AS notes_key, count(*)::bigint AS cnt
                FROM public.stock_readiness_daily
                WHERE as_of_date = CURRENT_DATE
                  AND universe_rule_version = 'v1'
                  AND price_source = 'massive'
                  AND included_in_universe
                  AND NOT price_ready
                GROUP BY 1
                ORDER BY cnt DESC
                LIMIT 20
                """
            )
            rows: List[Dict[str, Any]] = []
            for r in cur.fetchall() or []:
                rows.append({"notes": r.get("notes_key"), "count": int(r.get("cnt") or 0)})
            out["notes_breakdown"] = rows

            # Holidays summary — market.us_market_holiday via FDW (Plugin-owned).
            try:
                cur.execute(
                    """
                    SELECT
                        count(*)::bigint AS total,
                        count(*) FILTER (WHERE status = 'early-close')::bigint AS early_close_count,
                        min(holiday_date)::text AS earliest_date,
                        max(holiday_date)::text AS latest_date,
                        max(fetched_at)::text AS last_fetched_at
                    FROM market.us_market_holiday
                    """
                )
                hr = cur.fetchone() or {}
                cur.execute(
                    """
                    SELECT exchange, count(*)::bigint AS cnt
                    FROM market.us_market_holiday
                    GROUP BY exchange
                    ORDER BY exchange
                    """
                )
                by_exchange = [
                    {"exchange": r.get("exchange"), "count": int(r.get("cnt") or 0)}
                    for r in (cur.fetchall() or [])
                ]
                out["holidays_summary"] = {
                    "total": int(hr.get("total") or 0),
                    "early_close_count": int(hr.get("early_close_count") or 0),
                    "earliest_date": hr.get("earliest_date"),
                    "latest_date": hr.get("latest_date"),
                    "last_fetched_at": hr.get("last_fetched_at"),
                    "by_exchange": by_exchange,
                }
            except Exception as e:
                logger.debug("holidays_summary query failed: %s", e)
                out["holidays_summary"] = {
                    "total": 0,
                    "early_close_count": 0,
                    "earliest_date": None,
                    "latest_date": None,
                    "last_fetched_at": None,
                    "by_exchange": [],
                }

            # Must run while cursor is open (was incorrectly placed after `with` closed cur).
            try:
                out["data_catalog"] = _build_runtime_data_catalog(cur)
            except Exception as e:
                logger.debug("build runtime data_catalog failed, fallback to static: %s", e)
                out["data_catalog"] = READINESS_DATA_CATALOG
    finally:
        conn.close()
    return out


# ---------------------------------------------------------------------------
# Stage 4 Evaluation helpers
# ---------------------------------------------------------------------------

_FUND_COND_IDS = [
    # sepa_core (8)
    "eps_q2q_ge_25pct",
    "rev_q2q_ge_25pct",
    "eps_acc_2q",
    "rev_acc_2q",
    "eps_3y_ge_15pct",
    "rev_3y_ge_15pct",
    "eps_acc_fy",
    "rev_acc_fy",
    # quality (5)
    "gross_margin_ge_30pct",
    "operating_margin_ge_10pct",
    "net_margin_ge_5pct",
    "ocf_to_ni_ge_0_7",
    "interest_coverage_ge_5x",
    # balance (4)
    "current_ratio_ge_1_5",
    "quick_ratio_ge_1_0",
    "debt_to_equity_le_1",
    "net_debt_to_ebitda_le_3",
    # cashflow (4)
    "fcf_positive",
    "fcf_margin_ge_5pct",
    "fcf_yield_ge_3pct",
    "capex_intensity_le_15pct",
    # valuation (4)
    "pe_le_60",
    "ps_le_15",
    "pb_le_8",
    "ev_to_ebitda_le_30",
    # profitability (2)
    "roe_ge_15pct",
    "roa_ge_5pct",
    # efficiency (3)
    "asset_turnover_ge_0_5",
    "dso_le_75_days",
    "dio_le_120_days",
    # sentiment (3)
    "days_to_cover_le_5",
    "short_volume_ratio_recent_le_30pct",
    "short_interest_pct_of_float_le_15pct",
]

_FUND_COND_LABELS: Dict[str, str] = {
    "eps_q2q_ge_25pct": "EPS Q2Q ≥25%",
    "rev_q2q_ge_25pct": "Revenue Q2Q ≥25%",
    "eps_acc_2q":        "EPS Acceleration 2Q",
    "rev_acc_2q":        "Revenue Acceleration 2Q",
    "eps_3y_ge_15pct":   "EPS 3Y CAGR ≥15%",
    "rev_3y_ge_15pct":   "Revenue 3Y CAGR ≥15%",
    "eps_acc_fy":        "EPS Annual Acceleration",
    "rev_acc_fy":        "Revenue Annual Acceleration",
    "gross_margin_ge_30pct":     "Gross Margin ≥30%",
    "operating_margin_ge_10pct": "Oper. Margin ≥10%",
    "net_margin_ge_5pct":        "Net Margin ≥5%",
    "ocf_to_ni_ge_0_7":          "OCF/NI ≥0.7",
    "interest_coverage_ge_5x":   "Interest Coverage ≥5×",
    "current_ratio_ge_1_5":      "Current Ratio ≥1.5",
    "quick_ratio_ge_1_0":        "Quick Ratio ≥1.0",
    "debt_to_equity_le_1":       "D/E ≤1.0",
    "net_debt_to_ebitda_le_3":   "NetDebt/EBITDA ≤3",
    "fcf_positive":              "FCF Positive",
    "fcf_margin_ge_5pct":        "FCF Margin ≥5%",
    "fcf_yield_ge_3pct":         "FCF Yield ≥3%",
    "capex_intensity_le_15pct":  "CapEx ≤15%",
    "pe_le_60":                  "P/E ≤60",
    "ps_le_15":                  "P/S ≤15",
    "pb_le_8":                   "P/B ≤8",
    "ev_to_ebitda_le_30":        "EV/EBITDA ≤30",
    "roe_ge_15pct":              "ROE ≥15%",
    "roa_ge_5pct":               "ROA ≥5%",
    "asset_turnover_ge_0_5":     "Asset Turnover ≥0.5",
    "dso_le_75_days":            "DSO ≤75 days",
    "dio_le_120_days":           "DIO ≤120 days",
    "days_to_cover_le_5":                   "Days to Cover ≤5",
    "short_volume_ratio_recent_le_30pct":   "Short Vol Ratio ≤30%",
    "short_interest_pct_of_float_le_15pct": "SI % Float ≤15%",
}

_FUND_COND_GROUP: Dict[str, str] = {
    "eps_q2q_ge_25pct": "sepa_core", "rev_q2q_ge_25pct": "sepa_core",
    "eps_acc_2q": "sepa_core", "rev_acc_2q": "sepa_core",
    "eps_3y_ge_15pct": "sepa_core", "rev_3y_ge_15pct": "sepa_core",
    "eps_acc_fy": "sepa_core", "rev_acc_fy": "sepa_core",
    "gross_margin_ge_30pct": "quality", "operating_margin_ge_10pct": "quality",
    "net_margin_ge_5pct": "quality", "ocf_to_ni_ge_0_7": "quality",
    "interest_coverage_ge_5x": "quality",
    "current_ratio_ge_1_5": "balance", "quick_ratio_ge_1_0": "balance",
    "debt_to_equity_le_1": "balance", "net_debt_to_ebitda_le_3": "balance",
    "fcf_positive": "cashflow", "fcf_margin_ge_5pct": "cashflow",
    "fcf_yield_ge_3pct": "cashflow", "capex_intensity_le_15pct": "cashflow",
    "pe_le_60": "valuation", "ps_le_15": "valuation",
    "pb_le_8": "valuation", "ev_to_ebitda_le_30": "valuation",
    "roe_ge_15pct": "profitability", "roa_ge_5pct": "profitability",
    "asset_turnover_ge_0_5": "efficiency", "dso_le_75_days": "efficiency",
    "dio_le_120_days": "efficiency",
    "days_to_cover_le_5": "sentiment",
    "short_volume_ratio_recent_le_30pct": "sentiment",
    "short_interest_pct_of_float_le_15pct": "sentiment",
}

# Phase-1 (10) + CRS (1) = 11 SEPA technical conditions. CRS is computed
# universe-wide and merged as the 11th condition by run_technical_local_backfill.
_TECH_COND_IDS = [
    "avg_volume_50_gt_threshold",
    "crs_ge_70",
    "close_ge_low52_x_1_3",
    "close_ge_high52_x_0_75",
    "sma50_gt_sma150",
    "sma50_gt_sma200",
    "sma150_gt_sma200",
    "sma200_rising_1m",
    "price_gt_sma50",
    "price_gt_sma150",
    "price_gt_sma200",
]

_TECH_COND_LABELS: Dict[str, str] = {
    "avg_volume_50_gt_threshold": "Avg Volume 50D > 100K",
    "crs_ge_70":                  "CRS ≥ 70",
    "close_ge_low52_x_1_3":       "Close ≥ Low52W × 1.3",
    "close_ge_high52_x_0_75":     "Close ≥ High52W × 0.75",
    "sma50_gt_sma150":            "SMA50 > SMA150",
    "sma50_gt_sma200":            "SMA50 > SMA200",
    "sma150_gt_sma200":           "SMA150 > SMA200",
    "sma200_rising_1m":           "SMA200 Rising (1M)",
    "price_gt_sma50":             "Price > SMA50",
    "price_gt_sma150":            "Price > SMA150",
    "price_gt_sma200":            "Price > SMA200",
}

# Tier 2: Momentum indicator IDs
_MOMENTUM_IND_IDS = [
    "rsi_14_in_band",
    "macd_hist_positive",
    "roc_3m_positive",
    "roc_6m_positive",
    "roc_12m_positive",
    "multi_period_rs_4w_positive",
    "multi_period_rs_13w_positive",
    "multi_period_rs_26w_positive",
    "slope_sma200_positive",
    "up_down_volume_50d_gt_1",
]

_MOMENTUM_IND_LABELS: Dict[str, str] = {
    "rsi_14_in_band":               "RSI(14) in [40, 80]",
    "macd_hist_positive":           "MACD Histogram > 0 & Rising",
    "roc_3m_positive":              "ROC 3M > 0",
    "roc_6m_positive":              "ROC 6M > 0",
    "roc_12m_positive":             "ROC 12M > 0",
    "multi_period_rs_4w_positive":  "RS vs SPY (4W) > 0",
    "multi_period_rs_13w_positive": "RS vs SPY (13W) > 0",
    "multi_period_rs_26w_positive": "RS vs SPY (26W) > 0",
    "slope_sma200_positive":        "SMA200 Slope > 0",
    "up_down_volume_50d_gt_1":      "Up/Down Vol (50D) > 1",
}

# Tier 3: Structure diagnostic IDs
_STRUCTURE_DIAG_IDS = [
    "realized_vol_contraction",
    "bb_squeeze",
    "obv_slope_30d_positive",
    "adx_14_ge_25",
    "aroon_oscillator_ge_50",
]

_STRUCTURE_DIAG_LABELS: Dict[str, str] = {
    "realized_vol_contraction":  "Realized Vol Contraction",
    "bb_squeeze":                "BB Squeeze Active",
    "obv_slope_30d_positive":    "OBV Slope (30D) Positive",
    "adx_14_ge_25":              "ADX(14) ≥ 25 (Trending)",
    "aroon_oscillator_ge_50":    "Aroon Osc ≥ 50",
}

# Tier 4: Sentiment indicator IDs
_SENTIMENT_IND_IDS = [
    "days_to_cover_ge_5",
    "short_volume_ratio_le_30pct_recent",
    "short_volume_ratio_trend_4w_falling",
]

_SENTIMENT_IND_LABELS: Dict[str, str] = {
    "days_to_cover_ge_5":                  "Days to Cover ≥ 5",
    "short_volume_ratio_le_30pct_recent":  "Short Vol Ratio < 30%",
    "short_volume_ratio_trend_4w_falling": "Short Vol Trend Falling",
}


def run_fundamentals_local_backfill(
    status_config: dict,
    symbols: List[str],
    *,
    cache_ttl_sec: int = 21600,
) -> Dict[str, Any]:
    """Evaluate 8 SEPA fundamental conditions + 25 extended conditions for all given symbols.

    Uses TWO DB connections total (read pass + write pass) regardless of symbol count:
      1. Batch SELECT quarterly + annual income rows, balance sheets, cash flows,
         ratios, short interest, and short volume for ALL symbols.
      2. Group by symbol in Python, call evaluate_fundamentals (SEPA core 8) and
         7 extension group evaluators per symbol, then merge results.
      3. executemany batch-upsert all results in one commit.

    The ``fundamental_pass / fundamental_pass_count / fundamental_insufficient``
    columns are driven *only* by the original 8 SEPA core conditions.  Extension
    results are written into ``fundamental_eval`` JSONB under ``groups`` and as
    additional entries in the flat ``conditions[]`` list (backward-compatible).

    No Phase1 / CRS filtering. No external API calls.
    """
    import json as _json
    from collections import defaultdict

    from bifrost_api.research.sepa.fundamentals_engine import (
        FUNDAMENTALS_RULE_VERSION,  # noqa: F401 — kept for potential downstream use
        FundamentalsConfig,
        evaluate_fundamentals,
        to_float as _to_float,
    )
    from bifrost_api.research.sepa.fundamentals_ext_engine import (
        FundamentalsExtConfig,
        evaluate_balance_group,
        evaluate_cashflow_group,
        evaluate_efficiency_group,
        evaluate_profitability_group,
        evaluate_quality_group,
        evaluate_sentiment_group,
        evaluate_valuation_group,
        merge_extension_into_eval,
    )
    from bifrost_api.research.sepa.financials_data import (
        fetch_balance_sheet_rows_for_ext_batch,
        fetch_cash_flow_rows_for_ext_batch,
        fetch_income_ext_rows_batch,
        fetch_ratios_latest_for_ext_batch,
        fetch_short_interest_latest_batch,
        fetch_short_volume_recent_batch,
    )

    if not _db_ok(status_config):
        return {"ok": False, "error": "PostgreSQL not configured"}


    syms = sorted({str(s or "").strip().upper() for s in symbols if str(s or "").strip()})
    if not syms:
        return {"ok": True, "total_symbols": 0, "evaluated": 0, "no_local_data": 0, "errors": 0, "error_samples": []}

    # --- helper mappers (mirrors financials_data.fetch_income_rows_for_sepa_from_pg) ----------
    _FQ_MAP = {1: "Q1", 2: "Q2", 3: "Q3", 4: "Q4"}

    def _iso(v: Any) -> Optional[str]:
        return v.isoformat() if hasattr(v, "isoformat") else (str(v)[:10] if v else None)

    def _map_q(r: Any) -> Dict[str, Any]:
        fq = int(r.get("fiscal_quarter") or 0)
        return {
            "fiscal_year": int(r.get("fiscal_year") or 0),
            "fiscal_period": _FQ_MAP.get(fq, f"Q{fq}" if fq else "FY"),
            "filing_date": _iso(r.get("filing_date")),
            "timeframe": "quarterly",
            "start_date": _iso(r.get("period_end")),
            "end_date": _iso(r.get("period_end")),
            "basic_earnings_per_share": r.get("basic_earnings_per_share"),
            "diluted_earnings_per_share": r.get("diluted_earnings_per_share"),
            "revenues": r.get("revenue"),
        }

    def _map_a(r: Any) -> Dict[str, Any]:
        return {
            "fiscal_year": int(r.get("fiscal_year") or 0),
            "fiscal_period": "FY",
            "filing_date": _iso(r.get("filing_date")),
            "timeframe": "annual",
            "start_date": _iso(r.get("period_end")),
            "end_date": _iso(r.get("period_end")),
            "basic_earnings_per_share": r.get("basic_earnings_per_share"),
            "diluted_earnings_per_share": r.get("diluted_earnings_per_share"),
            "revenues": r.get("revenue"),
        }

    # --- pass 1: batch-read all tables via Plugin HTTP ------------------------------------------
    from bifrost_api.research.sepa.financials_data import (
        _INCOME_FIELDS,
        unpack_financial_data,
    )
    from bifrost_api.research.market_data_client import fetch_sepa_financials

    q_by_sym: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    a_by_sym: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    inc_ext_by_sym: Dict[str, List[Dict[str, Any]]] = {}
    bs_by_sym: Dict[str, List[Dict[str, Any]]] = {}
    cf_by_sym: Dict[str, List[Dict[str, Any]]] = {}
    ratios_by_sym: Dict[str, Dict[str, Any]] = {}
    si_by_sym: Dict[str, List[Dict[str, Any]]] = {}
    sv_by_sym: Dict[str, List[Dict[str, Any]]] = {}

    try:
        # SEPA core income reads via Plugin HTTP
        q_raw = fetch_sepa_financials(
            syms, "income_statement", period_type="quarterly", limit=200,
        )
        for sym, rows in q_raw.items():
            for r in rows:
                flat = unpack_financial_data(r.get("data"), _INCOME_FIELDS)
                q_by_sym[sym].append(_map_q({**r, **flat}))

        a_raw = fetch_sepa_financials(
            syms, "income_statement", period_type="annual", limit=200,
        )
        for sym, rows in a_raw.items():
            for r in rows:
                flat = unpack_financial_data(r.get("data"), _INCOME_FIELDS)
                a_by_sym[sym].append(_map_a({**r, **flat}))

        # Extension batch reads via Plugin HTTP (best-effort)
        try:
            inc_ext_by_sym = fetch_income_ext_rows_batch(symbols=syms)
        except Exception as exc:
            logger.warning("Extension: income ext read failed: %s", exc)
        try:
            bs_by_sym = fetch_balance_sheet_rows_for_ext_batch(symbols=syms)
        except Exception as exc:
            logger.warning("Extension: balance sheets read failed: %s", exc)
        try:
            cf_by_sym = fetch_cash_flow_rows_for_ext_batch(symbols=syms)
        except Exception as exc:
            logger.warning("Extension: cash flows read failed: %s", exc)
        try:
            ratios_by_sym = fetch_ratios_latest_for_ext_batch(symbols=syms)
        except Exception as exc:
            logger.warning("Extension: ratios read failed: %s", exc)
        try:
            si_by_sym = fetch_short_interest_latest_batch(symbols=syms)
        except Exception as exc:
            logger.warning("Extension: short interest read failed: %s", exc)
        try:
            sv_by_sym = fetch_short_volume_recent_batch(symbols=syms)
        except Exception as exc:
            logger.warning("Extension: short volume read failed: %s", exc)

    except Exception as e:
        return {"ok": False, "error": f"Batch income read failed: {e}"}

    # --- pass 2: evaluate + build stock_readiness_daily upsert rows ---------------------------
    MIN_Q, MIN_A = 5, 4
    fund_cfg = FundamentalsConfig()
    ext_cfg = FundamentalsExtConfig()
    ttl_str = str(max(60, int(cache_ttl_sec)))

    # (symbol, ttl_str, fundamental_pass, pass_count, insufficient, eval_json)
    srd_rows: List[Tuple] = []
    no_data = 0
    errors_list: List[str] = []

    for sym in syms:
        try:
            qrows = q_by_sym.get(sym, [])
            arows = a_by_sym.get(sym, [])
            if len(qrows) >= MIN_Q and len(arows) >= MIN_A:
                result = evaluate_fundamentals(qrows, arows, cfg=fund_cfg)
            else:
                result = {
                    "fundamental_pass": False,
                    "insufficient_data": True,
                    "not_comparable": False,
                    "conditions": [],
                    "pass_count": 0,
                    "fail_count": 0,
                    "metrics": {},
                    "issues": ["no_local_income_data"],
                }
                no_data += 1

            # Extension evaluators (each group fails independently)
            inc_ext = inc_ext_by_sym.get(sym, [])
            bs_rows = bs_by_sym.get(sym, [])
            cf_rows = cf_by_sym.get(sym, [])
            ratios_row = ratios_by_sym.get(sym)
            si_rows = si_by_sym.get(sym, [])
            sv_rows = sv_by_sym.get(sym, [])

            # Latest diluted shares outstanding for sentiment % of float
            diluted_shares = None
            if inc_ext:
                for row in reversed(inc_ext):
                    ds = _to_float(row.get("diluted_shares_outstanding"))
                    if ds is not None and ds > 0:
                        diluted_shares = ds
                        break

            ext_groups = []
            try:
                ext_groups.append(evaluate_quality_group(inc_ext, cf_rows, cfg=ext_cfg))
            except Exception as exc:
                logger.warning("Extension quality group failed for %s: %s", sym, exc)
            try:
                ext_groups.append(evaluate_balance_group(bs_rows, ratios_row, inc_ext, cfg=ext_cfg))
            except Exception as exc:
                logger.warning("Extension balance group failed for %s: %s", sym, exc)
            try:
                ext_groups.append(evaluate_cashflow_group(inc_ext, cf_rows, ratios_row, cfg=ext_cfg))
            except Exception as exc:
                logger.warning("Extension cashflow group failed for %s: %s", sym, exc)
            try:
                ext_groups.append(evaluate_valuation_group(ratios_row, cfg=ext_cfg))
            except Exception as exc:
                logger.warning("Extension valuation group failed for %s: %s", sym, exc)
            try:
                ext_groups.append(evaluate_profitability_group(ratios_row, cfg=ext_cfg))
            except Exception as exc:
                logger.warning("Extension profitability group failed for %s: %s", sym, exc)
            try:
                ext_groups.append(evaluate_efficiency_group(inc_ext, bs_rows, cfg=ext_cfg))
            except Exception as exc:
                logger.warning("Extension efficiency group failed for %s: %s", sym, exc)
            try:
                ext_groups.append(evaluate_sentiment_group(si_rows, sv_rows, diluted_shares, cfg=ext_cfg))
            except Exception as exc:
                logger.warning("Extension sentiment group failed for %s: %s", sym, exc)

            result = merge_extension_into_eval(result, ext_groups)
            result["symbol"] = sym

            # fundamental_pass / pass_count / insufficient stay SEPA-core only
            srd_rows.append((
                sym,
                ttl_str,
                bool(result.get("fundamental_pass", False)),
                int(result.get("pass_count", 0)),
                bool(result.get("insufficient_data", False)),
                _json.dumps(result),
            ))
        except Exception as exc:
            errors_list.append(f"{sym}: {exc}")
            logger.warning("run_fundamentals_local_backfill eval failed for %s: %s", sym, exc)

    # --- pass 3: batch-upsert directly to stock_readiness_daily --------------------------------
    if srd_rows:
        try:
            params_w = _get_conn_params(status_config)
            params_w["connect_timeout"] = 15
            conn_w = psycopg2.connect(**params_w)
            try:
                with conn_w.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO public.stock_readiness_daily
                            (as_of_date, symbol, universe_rule_version, price_source,
                             fund_cache_present, fund_cache_expire_at,
                             fundamental_pass, fundamental_pass_count, fundamental_insufficient, fundamental_eval)
                        VALUES (CURRENT_DATE, %s, 'v1', 'massive',
                                true, now() + (%s || ' seconds')::interval,
                                %s, %s, %s, %s::jsonb)
                        ON CONFLICT (as_of_date, symbol, universe_rule_version, price_source) DO UPDATE SET
                            fund_cache_present      = EXCLUDED.fund_cache_present,
                            fund_cache_expire_at    = EXCLUDED.fund_cache_expire_at,
                            fundamental_pass        = EXCLUDED.fundamental_pass,
                            fundamental_pass_count  = EXCLUDED.fundamental_pass_count,
                            fundamental_insufficient = EXCLUDED.fundamental_insufficient,
                            fundamental_eval        = EXCLUDED.fundamental_eval
                        """,
                        srd_rows,
                    )
                conn_w.commit()
            finally:
                conn_w.close()
        except Exception as e:
            return {"ok": False, "error": f"Batch stock_readiness_daily upsert failed: {e}"}

    return {
        "ok": True,
        "total_symbols": len(syms),
        "evaluated": len(srd_rows),
        "no_local_data": no_data,
        "errors": len(errors_list),
        "error_samples": errors_list[:10],
    }


def run_technical_local_backfill(
    status_config: dict,
    symbols: List[str],
    *,
    min_crs: float = 70.0,
    lookback_days: int = 420,
    source: str = "massive",
) -> Dict[str, Any]:
    """Evaluate SEPA technical conditions (4 tiers) for *all* given symbols using local data.

    Pipeline:
      1. Batch-read OHLCV + close-only + SPY + short_interest + short_volume.
      2. Run phase1_engine per symbol → 10 conditions (Core).
      3. Run crs_engine universe-wide → 1 CRS condition (Core 11th).
      4. Run technical_engine.evaluate_symbol_all_tiers → Momentum / Structure / Sentiment.
      5. Merge and batch UPSERT into stock_readiness_daily.

    Core 11 backward compatibility: technical_pass / technical_pass_count /
    technical_insufficient / top-level conditions[] and metrics[] are identical
    to the pre-tiered version. New data lives under technical_eval.tiers.*.
    """
    import json as _json

    from bifrost_api.research.sepa.crs_engine import compute_crs_scores
    from bifrost_api.research.sepa.phase1_engine import (
        Phase1Config,
        evaluate_symbol_phase1,
    )
    from bifrost_api.research.sepa.technical_engine import (
        TechnicalConfig,
        evaluate_symbol_all_tiers,
    )
    from bifrost_api.research.market_pg import (
        get_short_interest_recent,
        get_short_volume_recent,
        get_spy_close_series,
        get_stock_day_close_series_for_crs,
        get_stock_day_series_for_sepa,
    )

    if not _db_ok(status_config):
        return {"ok": False, "error": "PostgreSQL not configured"}

    syms = sorted({str(s or "").strip().upper() for s in symbols if str(s or "").strip()})
    if not syms:
        return {
            "ok": True,
            "total_symbols": 0,
            "evaluated": 0,
            "no_local_data": 0,
            "errors": 0,
            "error_samples": [],
        }

    # pass 1: batch-read OHLCV (phase1) + close-only (CRS) + SPY + short data ---------------
    rows_by_symbol: Dict[str, List[Dict[str, Any]]] = {}
    crs_rows_by_symbol: Dict[str, List[Dict[str, Any]]] = {}
    try:
        rows_by_symbol = get_stock_day_series_for_sepa(
            status_config, syms, lookback_days=lookback_days, source=source
        ) or {}
    except Exception as e:
        return {"ok": False, "error": f"Phase1 stock_day batch read failed: {e}"}
    try:
        crs_rows_by_symbol = get_stock_day_close_series_for_crs(
            status_config, syms, lookback_days=lookback_days, source=source
        ) or {}
    except Exception as e:
        return {"ok": False, "error": f"CRS stock_day batch read failed: {e}"}

    spy_closes: List[float] = []
    try:
        spy_closes = get_spy_close_series(status_config, lookback_days=lookback_days, source=source)
    except Exception as exc:
        logger.warning("SPY close series read failed (non-fatal): %s", exc)

    si_by_symbol: Dict[str, List[Dict[str, Any]]] = {}
    sv_by_symbol: Dict[str, List[Dict[str, Any]]] = {}
    try:
        si_by_symbol = get_short_interest_recent(status_config, syms, settlements=6, source=source)
    except Exception as exc:
        logger.warning("short_interest read failed (non-fatal): %s", exc)
    try:
        sv_by_symbol = get_short_volume_recent(status_config, syms, trade_days=60, source=source)
    except Exception as exc:
        logger.warning("short_volume read failed (non-fatal): %s", exc)

    # pass 2: run phase1 evaluation per symbol (10 conditions) ------------------------------
    phase1_cfg = Phase1Config()
    phase1_results: Dict[str, Dict[str, Any]] = {}
    for sym in syms:
        try:
            phase1_results[sym] = evaluate_symbol_phase1(
                sym, rows_by_symbol.get(sym, []), cfg=phase1_cfg
            )
        except Exception as exc:
            logger.warning("phase1 eval failed for %s: %s", sym, exc)
            phase1_results[sym] = {
                "symbol": sym,
                "technical_pass": False,
                "insufficient_data": True,
                "conditions": [],
                "metrics": {},
                "error": str(exc),
            }

    # pass 3: run CRS universe-wide (one call covers ALL symbols) ---------------------------
    try:
        crs_output = compute_crs_scores(crs_rows_by_symbol, min_crs=min_crs)
        crs_by_sym: Dict[str, Dict[str, Any]] = {
            str(r.get("symbol") or "").upper(): r for r in (crs_output.get("results") or [])
        }
    except Exception as exc:
        logger.warning("compute_crs_scores failed: %s", exc)
        crs_by_sym = {}

    # pass 4: merge core 11 + run all tiers → build upsert rows -----------------------------
    tech_cfg = TechnicalConfig()
    srd_rows: List[Tuple] = []
    no_data = 0
    errors_list: List[str] = []

    for sym in syms:
        try:
            p1 = phase1_results.get(sym, {}) or {}
            p1_conditions: List[Dict[str, Any]] = list(p1.get("conditions") or [])
            p1_insufficient = bool(p1.get("insufficient_data", False))
            p1_metrics: Dict[str, Any] = dict(p1.get("metrics") or {})

            crs = crs_by_sym.get(sym, {})
            crs_actual = crs.get("crs_score")
            crs_pass = bool(crs.get("pass", False))
            crs_insufficient = bool(crs.get("insufficient_data", False))

            crs_condition = {
                "id": "crs_ge_70",
                "pass": crs_pass,
                "actual": crs_actual,
                "threshold": float(min_crs),
                "reason": "CRS percentile rank (252-day return vs universe)",
            }

            all_conditions = p1_conditions + [crs_condition]
            pass_count = sum(1 for c in all_conditions if c.get("pass"))
            insufficient = p1_insufficient or crs_insufficient or len(p1_conditions) < 10

            metrics = {
                **p1_metrics,
                "ret252": crs.get("ret252"),
                "crs_score": crs_actual,
            }

            # Build core_result for technical_engine
            core_result = {
                "technical_pass": (pass_count == 11) and not insufficient,
                "insufficient_data": insufficient,
                "pass_count": pass_count,
                "fail_count": 11 - pass_count,
                "conditions": all_conditions,
                "metrics": metrics,
            }

            # Run all 4 tiers through the orchestrator
            technical_eval = evaluate_symbol_all_tiers(
                sym,
                core_result,
                rows_by_symbol.get(sym, []),
                spy_closes,
                si_by_symbol.get(sym, []),
                sv_by_symbol.get(sym, []),
                cfg=tech_cfg,
            )

            if insufficient and len(p1_conditions) < 10:
                no_data += 1

            srd_rows.append((
                sym,
                bool(technical_eval["technical_pass"]),
                int(technical_eval["pass_count"]),
                bool(technical_eval["insufficient_data"]),
                _json.dumps(technical_eval),
            ))
        except Exception as exc:
            errors_list.append(f"{sym}: {exc}")
            logger.warning("run_technical_local_backfill eval failed for %s: %s", sym, exc)

    # pass 5: batch-upsert directly to stock_readiness_daily --------------------------------
    if srd_rows:
        params = _get_conn_params(status_config)
        params["connect_timeout"] = 15
        try:
            conn_w = psycopg2.connect(**params)
            try:
                with conn_w.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO public.stock_readiness_daily
                            (as_of_date, symbol, universe_rule_version, price_source,
                             technical_pass, technical_pass_count, technical_insufficient, technical_eval)
                        VALUES (CURRENT_DATE, %s, 'v1', 'massive', %s, %s, %s, %s::jsonb)
                        ON CONFLICT (as_of_date, symbol, universe_rule_version, price_source) DO UPDATE SET
                            technical_pass         = EXCLUDED.technical_pass,
                            technical_pass_count   = EXCLUDED.technical_pass_count,
                            technical_insufficient = EXCLUDED.technical_insufficient,
                            technical_eval         = EXCLUDED.technical_eval
                        """,
                        srd_rows,
                    )
                conn_w.commit()
            finally:
                conn_w.close()
        except Exception as e:
            return {"ok": False, "error": f"Batch stock_readiness_daily upsert failed: {e}"}

    return {
        "ok": True,
        "total_symbols": len(syms),
        "evaluated": len(srd_rows),
        "no_local_data": no_data,
        "errors": len(errors_list),
        "error_samples": errors_list[:10],
        "min_crs": float(min_crs),
        "lookback_days": int(lookback_days),
    }


def compute_sepa_criteria_stats(status_config: dict) -> Dict[str, Any]:
    """Aggregate SEPA criteria pass rates from existing cache tables (on-demand, no writes).

    Sources:
    - stock_readiness_daily.fundamental_eval (jsonb containment for per-condition counts)
    - stock_readiness_daily (technical bar coverage + price_ready for today)
    """
    from datetime import datetime, timezone

    if not _db_ok(status_config):
        return {"ok": False, "error": "PostgreSQL not configured"}
    params = _get_conn_params(status_config)
    params["connect_timeout"] = 15
    try:
        conn = psycopg2.connect(**params)
    except Exception as e:
        return {"ok": False, "error": str(e)}
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Universe count
            try:
                cur.execute("SELECT count(*) AS n FROM v_us_equity_universe")
                universe_count = int((cur.fetchone() or {}).get("n") or 0)
            except Exception:
                universe_count = 0

            # --- Fundamental stats: jsonb containment over cache ---
            fund_result: Dict[str, Any] = {
                "cached_count": 0,
                "fund_pass_count": 0,
                "no_data_count": 0,
                "conditions": [],
            }
            try:
                # Build per-condition containment expressions (reads from stock_readiness_daily)
                cond_exprs = []
                for cid in _FUND_COND_IDS:
                    safe = cid.replace("'", "")
                    cond_exprs.append(
                        f"(fundamental_eval->'conditions' @> "
                        f"'[{{\"id\":\"{safe}\",\"pass\":true}}]'::jsonb) AS cond_{safe}"
                    )
                per_sym_select = ",\n                    ".join(cond_exprs)

                filter_exprs = []
                for cid in _FUND_COND_IDS:
                    safe = cid.replace("'", "")
                    filter_exprs.append(
                        f"count(*) FILTER (WHERE cond_{safe}) AS {safe}_pass,"
                        f"\n                count(*) FILTER (WHERE NOT cond_{safe} AND NOT no_data) AS {safe}_fail"
                    )
                agg_exprs = ",\n                ".join(filter_exprs)

                sql = f"""
                    WITH snapshot AS (
                        SELECT
                            fundamental_eval,
                            (fundamental_eval->>'fundamental_pass')::boolean  AS fund_pass,
                            (fundamental_eval->>'insufficient_data')::boolean AS no_data
                        FROM public.stock_readiness_daily
                        WHERE as_of_date = CURRENT_DATE
                          AND included_in_universe = true
                          AND fundamental_eval IS NOT NULL
                    ),
                    per_sym AS (
                        SELECT
                            fund_pass,
                            no_data,
                            {per_sym_select}
                        FROM snapshot
                    )
                    SELECT
                        count(*) AS cached_count,
                        count(*) FILTER (WHERE fund_pass)  AS fund_pass_count,
                        count(*) FILTER (WHERE no_data)    AS no_data_count,
                        {agg_exprs}
                    FROM per_sym
                """
                cur.execute(sql)
                row = cur.fetchone() or {}
                fund_result["cached_count"] = int(row.get("cached_count") or 0)
                fund_result["fund_pass_count"] = int(row.get("fund_pass_count") or 0)
                fund_result["no_data_count"] = int(row.get("no_data_count") or 0)
                no_data_n = fund_result["no_data_count"]
                conditions = []
                for cid in _FUND_COND_IDS:
                    safe = cid.replace("'", "")
                    p = int(row.get(f"{safe}_pass") or 0)
                    f_ = int(row.get(f"{safe}_fail") or 0)
                    nd = fund_result["cached_count"] - p - f_
                    if nd < 0:
                        nd = no_data_n
                    conditions.append({
                        "id": cid,
                        "group": _FUND_COND_GROUP.get(cid, "sepa_core"),
                        "label": _FUND_COND_LABELS.get(cid, cid),
                        "pass": p,
                        "fail": f_,
                        "no_data": nd,
                        "total": fund_result["cached_count"],
                    })
                fund_result["conditions"] = conditions

                # Build groups map for frontend
                from collections import defaultdict as _ddict
                _groups_map: Dict[str, Any] = {}
                _conds_by_group: Dict[str, list] = _ddict(list)
                for c in conditions:
                    _conds_by_group[c["group"]].append(c)
                for gk, gconds in _conds_by_group.items():
                    _groups_map[gk] = {
                        "cached_count": fund_result["cached_count"],
                        "pass_count": sum(c["pass"] for c in gconds),
                        "conditions": gconds,
                    }
                fund_result["groups"] = _groups_map
            except Exception as e:
                logger.warning("criteria_stats fundamental query failed: %s", e)

            # --- Fundamental pass-count distribution (0–8 conditions) ---
            try:
                cur.execute("""
                    SELECT
                        coalesce((fundamental_eval->>'pass_count')::int, 0) AS conditions_passed,
                        count(*)::int AS symbol_count
                    FROM public.stock_readiness_daily
                    WHERE as_of_date = CURRENT_DATE
                      AND included_in_universe = true
                      AND fundamental_eval IS NOT NULL
                      AND coalesce((fundamental_eval->>'insufficient_data')::boolean, false) IS NOT TRUE
                    GROUP BY 1
                    ORDER BY 1 DESC
                """)
                dist_rows = cur.fetchall() or []
                dist_map = {int(r.get("conditions_passed") or 0): int(r.get("symbol_count") or 0) for r in dist_rows}
                fund_result["pass_count_distribution"] = [
                    {"conditions_passed": i, "symbol_count": dist_map.get(i, 0)}
                    for i in range(8, -1, -1)
                ]
            except Exception as e:
                logger.debug("criteria_stats pass_count_distribution query failed: %s", e)
                fund_result["pass_count_distribution"] = []

            # --- Technical stats: stock_readiness_daily ---
            tech_result: Dict[str, Any] = {}
            failure_reasons: List[Dict[str, Any]] = []
            try:
                cur.execute("""
                    SELECT
                        count(*)                                            AS total_in_snapshot,
                        count(*) FILTER (WHERE price_ready)                AS price_ready_count,
                        count(*) FILTER (WHERE fund_cache_present)         AS fund_cached_count,
                        count(*) FILTER (WHERE price_ready
                                           AND fund_cache_present)         AS both_ready,
                        count(*) FILTER (WHERE bar_count_lookback >= 252)  AS bars_ge_252,
                        count(*) FILTER (WHERE bar_count_lookback >= 240)  AS bars_ge_240,
                        count(*) FILTER (WHERE bar_count_lookback >= 200)  AS bars_ge_200,
                        count(*) FILTER (WHERE bar_count_lookback BETWEEN 1 AND 199) AS bars_lt_200,
                        count(*) FILTER (WHERE bar_count_lookback = 0)     AS no_bars,
                        count(*) FILTER (WHERE technical_eval IS NOT NULL) AS tech_cached_count,
                        count(*) FILTER (WHERE technical_pass)             AS tech_pass_count,
                        count(*) FILTER (WHERE technical_insufficient)     AS tech_insufficient_count
                    FROM public.stock_readiness_daily
                    WHERE as_of_date = CURRENT_DATE
                      AND included_in_universe = true
                """)
                tech_result = dict(cur.fetchone() or {})
                for k in list(tech_result):
                    tech_result[k] = int(tech_result[k] or 0)
            except Exception as e:
                logger.warning("criteria_stats technical query failed: %s", e)

            try:
                cur.execute("""
                    SELECT coalesce(notes, 'unknown') AS notes, count(*) AS cnt
                    FROM public.stock_readiness_daily
                    WHERE as_of_date = CURRENT_DATE
                      AND included_in_universe = true
                      AND price_ready = false
                    GROUP BY notes ORDER BY cnt DESC
                """)
                failure_reasons = [{"notes": r.get("notes"), "cnt": int(r.get("cnt") or 0)} for r in cur.fetchall()]
            except Exception as e:
                logger.debug("criteria_stats failure_reasons query failed: %s", e)

            # --- Technical per-condition pass/fail (jsonb_array_elements) ---
            tech_conditions: List[Dict[str, Any]] = []
            try:
                cur.execute(
                    """
                    SELECT
                        cond->>'id'                                          AS id,
                        count(*) FILTER (WHERE (cond->>'pass')::boolean)     AS pass,
                        count(*) FILTER (WHERE NOT (cond->>'pass')::boolean) AS fail
                    FROM public.stock_readiness_daily,
                         jsonb_array_elements(technical_eval->'conditions') AS cond
                    WHERE as_of_date = CURRENT_DATE
                      AND included_in_universe = true
                      AND technical_eval IS NOT NULL
                      AND coalesce((technical_eval->>'insufficient_data')::boolean, false) IS NOT TRUE
                    GROUP BY cond->>'id'
                    """
                )
                row_map = {
                    str(r.get("id") or ""): {
                        "pass": int(r.get("pass") or 0),
                        "fail": int(r.get("fail") or 0),
                    }
                    for r in (cur.fetchall() or [])
                }
                for cid in _TECH_COND_IDS:
                    bucket = row_map.get(cid, {"pass": 0, "fail": 0})
                    tech_conditions.append({
                        "id": cid,
                        "label": _TECH_COND_LABELS.get(cid, cid),
                        "pass": int(bucket["pass"]),
                        "fail": int(bucket["fail"]),
                    })
            except Exception as e:
                logger.warning("criteria_stats technical per-condition query failed: %s", e)
                tech_conditions = [
                    {"id": cid, "label": _TECH_COND_LABELS.get(cid, cid), "pass": 0, "fail": 0}
                    for cid in _TECH_COND_IDS
                ]

            # --- Tier 2: Momentum per-indicator pass/fail ---
            momentum_conditions: List[Dict[str, Any]] = []
            try:
                cur.execute(
                    """
                    SELECT
                        ind->>'id'                                          AS id,
                        count(*) FILTER (WHERE (ind->>'pass')::boolean)     AS pass,
                        count(*) FILTER (WHERE NOT (ind->>'pass')::boolean) AS fail
                    FROM public.stock_readiness_daily,
                         jsonb_array_elements(technical_eval->'tiers'->'momentum'->'indicators') AS ind
                    WHERE as_of_date = CURRENT_DATE
                      AND included_in_universe = true
                      AND technical_eval IS NOT NULL
                      AND technical_eval->'tiers'->'momentum'->'indicators' IS NOT NULL
                      AND coalesce((technical_eval->>'insufficient_data')::boolean, false) IS NOT TRUE
                    GROUP BY ind->>'id'
                    """
                )
                m_row_map = {
                    str(r.get("id") or ""): {
                        "pass": int(r.get("pass") or 0),
                        "fail": int(r.get("fail") or 0),
                    }
                    for r in (cur.fetchall() or [])
                }
                for cid in _MOMENTUM_IND_IDS:
                    bucket = m_row_map.get(cid, {"pass": 0, "fail": 0})
                    momentum_conditions.append({
                        "id": cid,
                        "label": _MOMENTUM_IND_LABELS.get(cid, cid),
                        "pass": int(bucket["pass"]),
                        "fail": int(bucket["fail"]),
                        "tier": "momentum",
                    })
            except Exception as e:
                logger.debug("criteria_stats momentum per-indicator query failed: %s", e)
                momentum_conditions = [
                    {"id": cid, "label": _MOMENTUM_IND_LABELS.get(cid, cid), "pass": 0, "fail": 0, "tier": "momentum"}
                    for cid in _MOMENTUM_IND_IDS
                ]

            # --- Tier 2: Momentum score distribution (0..10) ---
            momentum_score_dist: list = []
            try:
                cur.execute("""
                    SELECT
                        coalesce((technical_eval->'tiers'->'momentum'->>'score')::int, 0) AS score,
                        count(*)::int AS symbol_count
                    FROM public.stock_readiness_daily
                    WHERE as_of_date = CURRENT_DATE
                      AND included_in_universe = true
                      AND technical_eval IS NOT NULL
                      AND technical_eval->'tiers'->'momentum' IS NOT NULL
                      AND coalesce((technical_eval->>'insufficient_data')::boolean, false) IS NOT TRUE
                    GROUP BY 1
                    ORDER BY 1 DESC
                """)
                dist_rows = cur.fetchall() or []
                m_dist_map = {int(r.get("score") or 0): int(r.get("symbol_count") or 0) for r in dist_rows}
                momentum_score_dist = [
                    {"score": i, "symbol_count": m_dist_map.get(i, 0)}
                    for i in range(10, -1, -1)
                ]
            except Exception as e:
                logger.debug("criteria_stats momentum_score_distribution query failed: %s", e)

            # --- Tier 3: Structure per-diagnostic active/inactive ---
            structure_conditions: List[Dict[str, Any]] = []
            try:
                cur.execute(
                    """
                    SELECT
                        diag->>'id'                                            AS id,
                        count(*) FILTER (WHERE (diag->>'active')::boolean)     AS active,
                        count(*) FILTER (WHERE NOT (diag->>'active')::boolean) AS inactive
                    FROM public.stock_readiness_daily,
                         jsonb_array_elements(technical_eval->'tiers'->'structure'->'diagnostics') AS diag
                    WHERE as_of_date = CURRENT_DATE
                      AND included_in_universe = true
                      AND technical_eval IS NOT NULL
                      AND technical_eval->'tiers'->'structure'->'diagnostics' IS NOT NULL
                      AND coalesce((technical_eval->>'insufficient_data')::boolean, false) IS NOT TRUE
                    GROUP BY diag->>'id'
                    """
                )
                s_row_map = {
                    str(r.get("id") or ""): {
                        "pass": int(r.get("active") or 0),
                        "fail": int(r.get("inactive") or 0),
                    }
                    for r in (cur.fetchall() or [])
                }
                for cid in _STRUCTURE_DIAG_IDS:
                    bucket = s_row_map.get(cid, {"pass": 0, "fail": 0})
                    structure_conditions.append({
                        "id": cid,
                        "label": _STRUCTURE_DIAG_LABELS.get(cid, cid),
                        "pass": int(bucket["pass"]),
                        "fail": int(bucket["fail"]),
                        "tier": "structure",
                    })
            except Exception as e:
                logger.debug("criteria_stats structure per-diagnostic query failed: %s", e)
                structure_conditions = [
                    {"id": cid, "label": _STRUCTURE_DIAG_LABELS.get(cid, cid), "pass": 0, "fail": 0, "tier": "structure"}
                    for cid in _STRUCTURE_DIAG_IDS
                ]

            # --- Tier 4: Sentiment per-indicator pass/fail ---
            sentiment_conditions: List[Dict[str, Any]] = []
            try:
                cur.execute(
                    """
                    SELECT
                        ind->>'id'                                          AS id,
                        count(*) FILTER (WHERE (ind->>'pass')::boolean)     AS pass,
                        count(*) FILTER (WHERE NOT (ind->>'pass')::boolean) AS fail
                    FROM public.stock_readiness_daily,
                         jsonb_array_elements(technical_eval->'tiers'->'sentiment'->'indicators') AS ind
                    WHERE as_of_date = CURRENT_DATE
                      AND included_in_universe = true
                      AND technical_eval IS NOT NULL
                      AND technical_eval->'tiers'->'sentiment'->'indicators' IS NOT NULL
                      AND coalesce((technical_eval->>'insufficient_data')::boolean, false) IS NOT TRUE
                    GROUP BY ind->>'id'
                    """
                )
                st_row_map = {
                    str(r.get("id") or ""): {
                        "pass": int(r.get("pass") or 0),
                        "fail": int(r.get("fail") or 0),
                    }
                    for r in (cur.fetchall() or [])
                }
                for cid in _SENTIMENT_IND_IDS:
                    bucket = st_row_map.get(cid, {"pass": 0, "fail": 0})
                    sentiment_conditions.append({
                        "id": cid,
                        "label": _SENTIMENT_IND_LABELS.get(cid, cid),
                        "pass": int(bucket["pass"]),
                        "fail": int(bucket["fail"]),
                        "tier": "sentiment",
                    })
            except Exception as e:
                logger.debug("criteria_stats sentiment per-indicator query failed: %s", e)
                sentiment_conditions = [
                    {"id": cid, "label": _SENTIMENT_IND_LABELS.get(cid, cid), "pass": 0, "fail": 0, "tier": "sentiment"}
                    for cid in _SENTIMENT_IND_IDS
                ]

            # --- Technical pass-count distribution (0–11 conditions) ---
            tech_dist: list = []
            try:
                cur.execute("""
                    SELECT
                        coalesce(technical_pass_count, 0) AS conditions_passed,
                        count(*)::int AS symbol_count
                    FROM public.stock_readiness_daily
                    WHERE as_of_date = CURRENT_DATE
                      AND included_in_universe = true
                      AND technical_eval IS NOT NULL
                      AND coalesce((technical_eval->>'insufficient_data')::boolean, false) IS NOT TRUE
                    GROUP BY 1
                    ORDER BY 1 DESC
                """)
                dist_rows = cur.fetchall() or []
                dist_map = {int(r.get("conditions_passed") or 0): int(r.get("symbol_count") or 0) for r in dist_rows}
                tech_dist = [
                    {"conditions_passed": i, "symbol_count": dist_map.get(i, 0)}
                    for i in range(11, -1, -1)
                ]
            except Exception as e:
                logger.debug("criteria_stats tech_pass_count_distribution query failed: %s", e)
                tech_dist = []

        return {
            "ok": True,
            "universe_count": universe_count,
            "fundamental": fund_result,
            "technical": {
                **tech_result,
                "failure_reasons": failure_reasons,
                "conditions": tech_conditions,
                "pass_count_distribution": tech_dist,
                "momentum_conditions": momentum_conditions,
                "momentum_score_distribution": momentum_score_dist,
                "structure_conditions": structure_conditions,
                "sentiment_conditions": sentiment_conditions,
            },
            "computed_at": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


def compute_data_inventory_stats(status_config: dict) -> Dict[str, Any]:
    """Return fill-rate counts for financial jsonb keys scoped to the SEPA universe.

    Reads stock_financials via Plugin API. Response keys keep legacy
    table aliases (``stock_income_statements``, …) for FE compatibility.
    """
    from bifrost_api.research.market_data_client import fetch_readiness_financials_fill_rate

    universe_count = 0
    if _db_ok(status_config):
        params = _get_conn_params(status_config)
        params["connect_timeout"] = 15
        try:
            conn = psycopg2.connect(**params)
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT count(*) AS n FROM v_us_equity_universe")
                universe_count = int((cur.fetchone() or {}).get("n") or 0)
            conn.close()
        except Exception:
            pass

    try:
        resp = fetch_readiness_financials_fill_rate()
        result = resp.get("tables", {})
    except Exception as e:
        return {"ok": False, "error": str(e)}

    return {"ok": True, "universe_count": universe_count, "tables": result}


def get_sepa_grouped_backfill_dates(
    status_config: dict,
    *,
    days_back: int = 420,
) -> Dict[str, Any]:
    """Return weekday dates in the past `days_back` days where stock_day has fewer than 1000 symbols.

    Dates with < 1000 symbols indicate either no data at all or watchlist-only partial data.
    Grouped Daily API covers the full market in one call, so any date it ran should have 5000+ rows.

    Returns: {ok, missing_dates: [str], missing_count, checked_dates}
    """
    if not _db_ok(status_config):
        return {"ok": False, "error": "PostgreSQL not configured"}
    params = _get_conn_params(status_config)
    params["connect_timeout"] = 15
    try:
        conn = psycopg2.connect(**params)
    except Exception as e:
        logger.warning("get_sepa_grouped_backfill_dates connect failed: %s", e)
        return {"ok": False, "error": str(e)}
    conn.close()
    try:
        from bifrost_api.research.market_data_client import fetch_readiness_date_coverage
        resp = fetch_readiness_date_coverage(days_back=int(days_back), min_symbols=1000)
        low_coverage = resp.get("low_coverage_dates", [])
        missing_dates: List[str] = []
        for r in low_coverage:
            dt_v = r.get("date") if isinstance(r, dict) else None
            if dt_v and str(dt_v).strip():
                missing_dates.append(str(dt_v).strip())
        return {
            "ok": True,
            "missing_dates": missing_dates,
            "missing_count": len(missing_dates),
            "checked_dates": len(low_coverage),
        }
    except Exception as e:
        logger.warning("get_sepa_grouped_backfill_dates query failed: %s", e)
        return {"ok": False, "error": str(e)}




def get_sepa_price_gap_details(
    status_config: dict,
    *,
    limit: int = 2000,
) -> Dict[str, Any]:
    """Return per-symbol gaps for Step 3 stock_day checks via Plugin HTTP.

    Delegates to ``GET /readiness/vendor-gap?detail=true`` on the Market Data Plugin,
    which evaluates vendor calendar gaps and readiness fallback gaps against
    ``market.stock_snapshot`` + ``market.stock_daily`` (Golden Source).

    Returns: {ok, total_gap_count, returned, items: [...]}
    """
    try:
        from bifrost_api.research.market_data_client import _get_json as _plugin_get
        data = _plugin_get(
            f"/readiness/vendor-gap?detail=true&limit={int(limit)}",
            timeout=60,
        )
        gaps = data.get("gaps") or []
        gap_count = data.get("gap_count", len(gaps))
        return {
            "ok": True,
            "total_gap_count": gap_count,
            "returned": len(gaps),
            "items": gaps,
        }
    except Exception as e:
        logger.warning("get_sepa_price_gap_details plugin fetch failed: %s", e)
        return {"ok": False, "error": str(e), "total_gap_count": 0, "returned": 0, "items": []}


def get_sepa_price_gap_symbols(
    status_config: dict,
    *,
    batch_size: int = 50,
) -> Dict[str, Any]:
    """Return symbols matching Step 3 vendor-calendar / readiness fallback gap rule via Plugin HTTP.

    Returns: {ok, gap_count, batches: [[symbol, ...], ...]}
    """
    try:
        from bifrost_api.research.market_data_client import _get_json as _plugin_get
        data = _plugin_get("/readiness/vendor-gap?detail=true&limit=50000", timeout=60)
        gaps = data.get("gaps") or []
        symbols: List[str] = []
        for g in gaps:
            s = g.get("symbol") if isinstance(g, dict) else None
            if s:
                symbols.append(str(s).strip().upper())
        batches: List[List[str]] = [
            symbols[i : i + batch_size] for i in range(0, len(symbols), batch_size)
        ]
        return {"ok": True, "gap_count": len(symbols), "batches": batches}
    except Exception as e:
        logger.warning("get_sepa_price_gap_symbols plugin fetch failed: %s", e)
        return {"ok": False, "error": str(e), "gap_count": 0, "batches": []}
