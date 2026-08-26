"""SEPA universe + stock_day readiness snapshot (shared by Research API and scripts)."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import psycopg2
from psycopg2.extras import RealDictCursor

from bifrost_core.persistence.postgres.connection import _get_conn_params

logger = logging.getLogger(__name__)

# Step 3 vendor gap detection is delegated to Market Data Plugin via
# GET /readiness/vendor-gap. Local CTE helpers have been retired.

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
    ],
}


def _db_ok(status_config: Optional[dict]) -> bool:
    if not status_config:
        return False
    return status_config.get("sink") == "postgres" or bool(status_config.get("postgres"))


def fetch_sepa_readiness_summary(status_config: dict) -> Dict[str, Any]:
    """Passthrough to Market Data Plugin composite readiness summary.

    Authority lives in Golden Source (``ops_jobs.data_source_void`` + ``raw_market.*``
    + ``dw_stock.mart_sepa_*``). Trade API no longer aggregates locally.
    Attaches static ``data_catalog`` for FE Data Catalog panel compatibility.
    ``status_config`` is retained for call-site compatibility and unused.
    """
    _ = status_config
    try:
        from bifrost_api.research.market_data_client import _get_json

        out = _get_json("/readiness/summary", timeout=90)
        if isinstance(out, dict) and out.get("ok") is not False and "data_catalog" not in out:
            out = {**out, "data_catalog": READINESS_DATA_CATALOG}
        return out if isinstance(out, dict) else {"ok": False, "error": "invalid plugin response"}
    except Exception as e:
        logger.warning("plugin readiness summary failed: %s", e)
        return {"ok": False, "error": f"Market Data Plugin summary unavailable: {e}"}


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
