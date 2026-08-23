"""SEPA universe + stock_day readiness snapshot (shared by Research API and scripts)."""

from __future__ import annotations

from copy import deepcopy
import logging
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

def _db_ok(status_config: Optional[dict]) -> bool:
    if not status_config:
        return False
    return status_config.get("sink") == "postgres" or bool(status_config.get("postgres"))




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
                from bifrost_api.research.analytics_reader import get_conn as _a_conn

                with _a_conn() as _ac:
                    with _ac.cursor(cursor_factory=RealDictCursor) as _acur:
                        _acur.execute(
                            "SELECT count(*)::bigint AS n FROM dw_stock.mart_sepa_fundamental_eval "
                            "WHERE insufficient_data = false"
                        )
                        out["fund_cache_valid_count"] = int((_acur.fetchone() or {}).get("n") or 0)
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

            try:
                from bifrost_api.research.analytics_reader import use_analytics as _use_a2, get_conn as _a_conn2
                if _use_a2():
                    with _a_conn2() as _ac2:
                        with _ac2.cursor(cursor_factory=RealDictCursor) as _acur2:
                            _acur2.execute(
                                "SELECT count(*)::bigint AS n FROM dw_stock.mart_sepa_fundamental_eval"
                            )
                            snap_total = int((_acur2.fetchone() or {}).get("n") or 0)
                            _acur2.execute(
                                "SELECT count(*)::bigint AS n FROM dw_stock.mart_sepa_fundamental_eval "
                                "WHERE insufficient_data = false"
                            )
                            snap_ready = int((_acur2.fetchone() or {}).get("n") or 0)
                    out["snapshot_populated"] = snap_total > 0
                    out["snapshot_today"] = {
                        "rows_total": snap_total,
                        "included_in_universe": snap_total,
                        "price_ready": snap_ready,
                    }
                    out["notes_breakdown"] = []
                else:
                    raise ImportError("fallback to legacy")
            except Exception:
                out["snapshot_populated"] = False
                out["snapshot_today"] = {
                    "rows_total": 0,
                    "included_in_universe": 0,
                    "price_ready": 0,
                }
                out["notes_breakdown"] = []

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
                    FROM raw_market.us_market_holiday
                    """
                )
                hr = cur.fetchone() or {}
                cur.execute(
                    """
                    SELECT exchange, count(*)::bigint AS cnt
                    FROM raw_market.us_market_holiday
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
