from __future__ import annotations

import json
from typing import Any, Dict, Optional

from fastapi import APIRouter, Body, Request

from bifrost_api.research.sepa.readiness_snapshot import (
    compute_data_inventory_stats,
    compute_sepa_criteria_stats,
    fetch_sepa_readiness_summary,
    get_sepa_price_gap_details,
    run_fundamentals_local_backfill,
    run_sepa_universe_readiness_snapshot,
    run_technical_local_backfill,
)
from bifrost_api.research.sepa.stock_unified_snapshot_refresh import run_refresh_cache_stock_unified_snapshots

router = APIRouter(tags=["research"])


def _massive_enqueue_retired(
    message: str = "Massive Celery queues retired — use market-data plugin",
) -> Dict[str, Any]:
    return {
        "ok": False,
        "error": message,
        "reason": "massive_retired",
        "job_ids": [],
        "chunks": 0,
    }


def _db_config(request: Request) -> Optional[dict]:
    return request.app.state.control_via_db or getattr(request.app.state, "status_cfg_for_read", None)


@router.get("/research/data/readiness/summary")
def get_sepa_readiness_summary(request: Request) -> Dict[str, Any]:
    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}
    return fetch_sepa_readiness_summary(db)


@router.post("/research/data/readiness/snapshot")
def post_sepa_readiness_snapshot(request: Request) -> Dict[str, Any]:
    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}
    return run_sepa_universe_readiness_snapshot(db)


@router.post("/research/data/readiness/stock-unified-snapshot")
def post_sepa_stock_unified_snapshot(request: Request) -> Dict[str, Any]:
    """Batch GET /v3/snapshot (stocks) for v_us_equity_universe; UPSERT cache_stock_snapshot."""
    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}
    reader = getattr(request.app.state, "reader", None)
    merged_config = reader._config if reader else {}
    return run_refresh_cache_stock_unified_snapshots(db, merged_config)


@router.get("/research/data/readiness/price-gaps")
def get_sepa_price_gaps(request: Request) -> Dict[str, Any]:
    """Return detailed per-symbol gap list for symbols in the SEPA universe that are NOT price_ready."""
    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}
    return get_sepa_price_gap_details(db)


@router.post("/research/data/readiness/backfill-price-gaps")
def post_sepa_backfill_price_gaps(
    request: Request,
    body: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    """Retired: Massive daily_smart Celery enqueue — use market-data plugin ingest."""
    _ = (request, body)
    return _massive_enqueue_retired()


@router.post("/research/data/readiness/backfill-fundamentals")
def post_sepa_backfill_fundamentals(
    request: Request,
    body: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    """Evaluate SEPA fundamentals for all universe symbols and write directly to stock_readiness_daily.

    Reads from stock_income_statements (already downloaded by Steps 4/5) and evaluates the 8 SEPA
    fundamental conditions for every symbol whose today's stock_readiness_daily row lacks fundamental_eval.
    No Phase1 / CRS filtering is applied — this is a data-completeness backfill, not a screening run.
    The heavy work is done in a background thread so the endpoint returns immediately.
    """
    import threading

    import psycopg2
    from psycopg2.extras import RealDictCursor

    from bifrost_core.persistence.postgres.connection import _get_conn_params

    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}

    params = _get_conn_params(db)
    params["connect_timeout"] = 15
    try:
        conn = psycopg2.connect(**params)
    except Exception as e:
        return {"ok": False, "error": f"DB connect failed: {e}"}

    from bifrost_api.research.sepa.readiness_snapshot import _sync_plugin_materialized_tables

    _sync_plugin_materialized_tables(conn)

    only_missing = bool(body.get("only_missing", True))
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SET statement_timeout = 30000")
            if only_missing:
                cur.execute(
                    """
                    SELECT u.symbol
                    FROM public.v_us_equity_universe u
                    LEFT JOIN public.stock_readiness_daily srd
                        ON srd.symbol = u.symbol
                       AND srd.as_of_date = CURRENT_DATE
                       AND srd.universe_rule_version = 'v1'
                       AND srd.price_source = 'massive'
                       AND srd.fundamental_eval IS NOT NULL
                    WHERE srd.symbol IS NULL
                    ORDER BY u.symbol
                    """
                )
            else:
                cur.execute(
                    """
                    SELECT symbol FROM public.v_us_equity_universe ORDER BY symbol
                    """
                )
            rows = cur.fetchall() or []
    except Exception as e:
        return {"ok": False, "error": f"Query failed: {e}"}
    finally:
        conn.close()

    symbols = [str(r["symbol"]) for r in rows]
    if not symbols:
        return {
            "ok": True,
            "gap_count": 0,
            "message": "All universe symbols already have valid fundamentals cache.",
        }

    max_symbols = int(body.get("max_symbols", 50000))
    symbols = symbols[:max_symbols]
    cache_ttl_sec = int(body.get("cache_ttl_sec", 21600))

    t = threading.Thread(
        target=run_fundamentals_local_backfill,
        kwargs={
            "status_config": db,
            "symbols": symbols,
            "cache_ttl_sec": cache_ttl_sec,
        },
        daemon=True,
    )
    t.start()

    return {
        "ok": True,
        "gap_count": len(symbols),
        "message": f"Local fundamentals backfill started for {len(symbols)} symbols (no Phase1/CRS filter).",
    }


@router.post("/research/data/readiness/backfill-technical")
def post_sepa_backfill_technical(
    request: Request,
    body: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    """Evaluate 11 SEPA technical conditions (10 Phase-1 + CRS) for universe symbols and
    write directly to stock_readiness_daily.technical_eval.

    Reads only from local stock_day (massive). No vendor calls. CRS is computed
    universe-wide so the rank percentile is meaningful. The heavy work runs in a
    background thread so this endpoint returns immediately.
    """
    import threading

    import psycopg2
    from psycopg2.extras import RealDictCursor

    from bifrost_core.persistence.postgres.connection import _get_conn_params

    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}

    params = _get_conn_params(db)
    params["connect_timeout"] = 15
    try:
        conn = psycopg2.connect(**params)
    except Exception as e:
        return {"ok": False, "error": f"DB connect failed: {e}"}

    from bifrost_api.research.sepa.readiness_snapshot import _sync_plugin_materialized_tables

    _sync_plugin_materialized_tables(conn)

    only_missing = bool(body.get("only_missing", True))
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SET statement_timeout = 30000")
            if only_missing:
                cur.execute(
                    """
                    SELECT u.symbol
                    FROM public.v_us_equity_universe u
                    LEFT JOIN public.stock_readiness_daily srd
                        ON srd.symbol = u.symbol
                       AND srd.as_of_date = CURRENT_DATE
                       AND srd.universe_rule_version = 'v1'
                       AND srd.price_source = 'massive'
                       AND srd.technical_eval IS NOT NULL
                    WHERE srd.symbol IS NULL
                    ORDER BY u.symbol
                    """
                )
            else:
                cur.execute(
                    """
                    SELECT symbol FROM public.v_us_equity_universe ORDER BY symbol
                    """
                )
            rows = cur.fetchall() or []
    except Exception as e:
        return {"ok": False, "error": f"Query failed: {e}"}
    finally:
        conn.close()

    symbols = [str(r["symbol"]) for r in rows]
    if not symbols:
        return {
            "ok": True,
            "gap_count": 0,
            "message": "All universe symbols already have a technical_eval row for today.",
        }

    max_symbols = int(body.get("max_symbols", 50000))
    symbols = symbols[:max_symbols]
    min_crs = float(body.get("min_crs", 70.0))
    lookback_days = int(body.get("lookback_days", 420))

    t = threading.Thread(
        target=run_technical_local_backfill,
        kwargs={
            "status_config": db,
            "symbols": symbols,
            "min_crs": min_crs,
            "lookback_days": lookback_days,
        },
        daemon=True,
    )
    t.start()

    return {
        "ok": True,
        "gap_count": len(symbols),
        "message": (
            f"Local technical backfill started for {len(symbols)} symbols "
            f"(Phase-1 + CRS ≥ {min_crs:g})."
        ),
    }


@router.post("/research/data/readiness/sync-holidays")
def post_sepa_sync_holidays(request: Request) -> Dict[str, Any]:
    """Retired: Massive holidays sync — use market-data plugin."""
    _ = request
    return {
        "ok": False,
        "error": "Market holidays sync via Massive retired — use market-data plugin",
        "reason": "massive_retired",
    }


@router.post("/research/data/readiness/backfill-grouped-history")
def post_sepa_backfill_grouped_history(
    request: Request,
    body: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    """Retired: Massive grouped daily Celery enqueue — use market-data plugin ingest."""
    _ = (request, body)
    return _massive_enqueue_retired()


def _post_sepa_financials_backfill(
    request: Request,
    body: Dict[str, Any],
    *,
    kind: str,
) -> Dict[str, Any]:
    """Retired: Massive financials Celery enqueue — use market-data plugin."""
    _ = (request, body, kind)
    return {**_massive_enqueue_retired(), "kind": kind}


def _get_sepa_financials_gaps(
    request: Request,
    *,
    detail_fetcher: str,
    limit: int = 2000,
    ingest_status: str | None = None,
    ingest_note: str | None = None,
) -> Dict[str, Any]:
    import psycopg2
    from psycopg2.extras import RealDictCursor

    from bifrost_core.persistence.postgres.connection import _get_conn_params
    from bifrost_api.research.sepa import financials_data as fd

    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}
    params = _get_conn_params(db)
    params["connect_timeout"] = 15
    try:
        conn = psycopg2.connect(**params)
    except Exception as e:
        return {"ok": False, "error": str(e)}
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            fn = getattr(fd, detail_fetcher)
            rows, total = fn(cur, limit=limit)
    finally:
        conn.close()
    out: Dict[str, Any] = {
        "ok": True,
        "gaps": rows,
        "total_gap_count": total,
        "returned": len(rows),
    }
    if ingest_status:
        out["ingest_status"] = ingest_status
    if ingest_note:
        out["ingest_note"] = ingest_note
    return out


_PENDING_PLUGIN_NOTE = (
    "Market-data plugin currently ingests statement report_types only "
    "(income_statement / balance_sheet / cash_flow_statement). "
    "ratios / short_interest / short_volume ingest is pending."
)


@router.get("/research/data/readiness/income-statements-gaps")
def get_sepa_income_statements_gaps(request: Request) -> Dict[str, Any]:
    return _get_sepa_financials_gaps(request, detail_fetcher="get_income_statements_gap_details")


@router.post("/research/data/readiness/backfill-income-statements")
def post_sepa_backfill_income_statements(
    request: Request,
    body: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    return _post_sepa_financials_backfill(request, body, kind="feed_stocks_income_statements")


@router.get("/research/data/readiness/balance-sheets-gaps")
def get_sepa_balance_sheets_gaps(request: Request) -> Dict[str, Any]:
    return _get_sepa_financials_gaps(request, detail_fetcher="get_balance_sheet_gap_details")


@router.post("/research/data/readiness/backfill-balance-sheets")
def post_sepa_backfill_balance_sheets(
    request: Request,
    body: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    return _post_sepa_financials_backfill(request, body, kind="feed_stocks_balance_sheets")


@router.get("/research/data/readiness/cash-flows-gaps")
def get_sepa_cash_flows_gaps(request: Request) -> Dict[str, Any]:
    return _get_sepa_financials_gaps(request, detail_fetcher="get_cash_flow_gap_details")


@router.post("/research/data/readiness/backfill-cash-flows")
def post_sepa_backfill_cash_flows(
    request: Request,
    body: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    return _post_sepa_financials_backfill(request, body, kind="feed_stocks_cash_flows")


@router.get("/research/data/readiness/ratios-gaps")
def get_sepa_ratios_gaps(request: Request) -> Dict[str, Any]:
    return _get_sepa_financials_gaps(
        request,
        detail_fetcher="get_ratios_gap_details",
        ingest_status="pending_plugin",
        ingest_note=_PENDING_PLUGIN_NOTE,
    )


@router.post("/research/data/readiness/backfill-ratios")
def post_sepa_backfill_ratios(
    request: Request,
    body: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    return _post_sepa_financials_backfill(request, body, kind="feed_stocks_ratios")


@router.get("/research/data/readiness/short-interest-gaps")
def get_sepa_short_interest_gaps(request: Request) -> Dict[str, Any]:
    return _get_sepa_financials_gaps(
        request,
        detail_fetcher="get_short_interest_gap_details",
        ingest_status="pending_plugin",
        ingest_note=_PENDING_PLUGIN_NOTE,
    )


@router.post("/research/data/readiness/backfill-short-interest")
def post_sepa_backfill_short_interest(
    request: Request,
    body: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    return _post_sepa_financials_backfill(request, body, kind="feed_stocks_short_interest")


@router.get("/research/data/readiness/short-volume-gaps")
def get_sepa_short_volume_gaps(request: Request) -> Dict[str, Any]:
    return _get_sepa_financials_gaps(
        request,
        detail_fetcher="get_short_volume_gap_details",
        ingest_status="pending_plugin",
        ingest_note=_PENDING_PLUGIN_NOTE,
    )


@router.post("/research/data/readiness/backfill-short-volume")
def post_sepa_backfill_short_volume(
    request: Request,
    body: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    return _post_sepa_financials_backfill(request, body, kind="feed_stocks_short_volume")


_VALID_GAP_ACK_TYPES = frozenset(
    ("income_statements", "balance_sheets", "cash_flows", "ratios", "short_interest", "short_volume")
)


def _gap_ack_db(request: Request):
    import psycopg2
    from bifrost_core.persistence.postgres.connection import _get_conn_params

    db = _db_config(request)
    if not db:
        return None, {"ok": False, "error": "PostgreSQL not configured"}
    params = _get_conn_params(db)
    params["connect_timeout"] = 10
    try:
        conn = psycopg2.connect(**params)
        return conn, None
    except Exception as e:
        return None, {"ok": False, "error": str(e)}


@router.get("/research/data/readiness/gap-ack")
def get_sepa_gap_ack(request: Request) -> Dict[str, Any]:
    from psycopg2.extras import RealDictCursor

    conn, err = _gap_ack_db(request)
    if err:
        return err
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT data_type, is_void, acked_gap_count, void_reason, acked_at::text AS acked_at "
                "FROM public.preference_data_gap_ack ORDER BY data_type"
            )
            rows = [dict(r) for r in (cur.fetchall() or [])]
    finally:
        conn.close()
    return {"ok": True, "acks": rows}


@router.post("/research/data/readiness/gap-ack")
def post_sepa_gap_ack(
    request: Request,
    body: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    data_type = str(body.get("data_type", "")).strip()
    if data_type not in _VALID_GAP_ACK_TYPES:
        return {"ok": False, "error": f"Invalid data_type: {data_type!r}"}
    is_void = bool(body.get("is_void", False))
    # gap_count is the current total gap count at acknowledgment time — used as the baseline
    gap_count = max(0, int(body.get("gap_count") or 0))
    void_reason = body.get("void_reason") or None

    conn, err = _gap_ack_db(request)
    if err:
        return err
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.preference_data_gap_ack
                    (data_type, is_void, acked_gap_count, void_reason, acked_at)
                VALUES (%s, %s, %s, %s, now())
                ON CONFLICT (data_type) DO UPDATE
                    SET is_void = EXCLUDED.is_void,
                        acked_gap_count = EXCLUDED.acked_gap_count,
                        void_reason = EXCLUDED.void_reason,
                        acked_at = now()
                """,
                (data_type, is_void, gap_count, void_reason),
            )
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "data_type": data_type, "is_void": is_void, "acked_gap_count": gap_count}


@router.get("/research/data/readiness/criteria-stats")
def get_sepa_criteria_stats(request: Request) -> Dict[str, Any]:
    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}
    return compute_sepa_criteria_stats(db)


@router.get("/research/data/readiness/fundamental-distribution/symbols")
def get_fundamental_distribution_symbols(
    request: Request,
    conditions_passed: int = 0,
) -> Dict[str, Any]:
    """Return symbols that passed exactly N out of 8 SEPA fundamental conditions today."""
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from bifrost_core.persistence.postgres.connection import _get_conn_params

    if conditions_passed < 0 or conditions_passed > 8:
        return {"ok": False, "error": "conditions_passed must be 0–8"}
    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}
    params = _get_conn_params(db)
    params["connect_timeout"] = 10
    try:
        conn = psycopg2.connect(**params)
    except Exception as e:
        return {"ok": False, "error": str(e)}
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SET statement_timeout = 15000")
            cur.execute(
                """
                SELECT
                    symbol,
                    coalesce((fundamental_eval->>'pass_count')::int, 0) AS pass_count,
                    coalesce((fundamental_eval->>'fundamental_pass')::boolean, false) AS fund_pass,
                    fundamental_eval->'conditions' AS conditions
                FROM public.stock_readiness_daily
                WHERE as_of_date = CURRENT_DATE
                  AND included_in_universe = true
                  AND fundamental_eval IS NOT NULL
                  AND coalesce((fundamental_eval->>'insufficient_data')::boolean, false) IS NOT TRUE
                  AND coalesce((fundamental_eval->>'pass_count')::int, 0) = %(n)s
                ORDER BY symbol
                """,
                {"n": conditions_passed},
            )
            rows = cur.fetchall() or []
        symbols = []
        for r in rows:
            cond_list = r.get("conditions") or []
            passed_ids = [c.get("id") for c in cond_list if c.get("pass") is True] if isinstance(cond_list, list) else []
            symbols.append({
                "symbol": r["symbol"],
                "pass_count": int(r.get("pass_count") or 0),
                "passed_conditions": passed_ids,
            })
        return {"ok": True, "conditions_passed": conditions_passed, "count": len(symbols), "symbols": symbols}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


@router.get("/research/data/readiness/technical-distribution/symbols")
def get_technical_distribution_symbols(
    request: Request,
    conditions_passed: int = 0,
) -> Dict[str, Any]:
    """Return symbols that passed exactly N out of 11 SEPA technical conditions today."""
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from bifrost_core.persistence.postgres.connection import _get_conn_params

    if conditions_passed < 0 or conditions_passed > 11:
        return {"ok": False, "error": "conditions_passed must be 0–11"}
    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}
    params = _get_conn_params(db)
    params["connect_timeout"] = 10
    try:
        conn = psycopg2.connect(**params)
    except Exception as e:
        return {"ok": False, "error": str(e)}
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SET statement_timeout = 15000")
            cur.execute(
                """
                SELECT
                    symbol,
                    coalesce(technical_pass_count, 0) AS pass_count,
                    technical_eval->'conditions'      AS conditions
                FROM public.stock_readiness_daily
                WHERE as_of_date = CURRENT_DATE
                  AND included_in_universe = true
                  AND technical_eval IS NOT NULL
                  AND coalesce((technical_eval->>'insufficient_data')::boolean, false) IS NOT TRUE
                  AND coalesce(technical_pass_count, 0) = %(n)s
                ORDER BY symbol
                """,
                {"n": conditions_passed},
            )
            rows = cur.fetchall() or []
        symbols = []
        for r in rows:
            cond_list = r.get("conditions") or []
            passed_ids = [c.get("id") for c in cond_list if c.get("pass") is True] if isinstance(cond_list, list) else []
            symbols.append({
                "symbol": r["symbol"],
                "pass_count": int(r.get("pass_count") or 0),
                "passed_conditions": passed_ids,
            })
        return {"ok": True, "conditions_passed": conditions_passed, "count": len(symbols), "symbols": symbols}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


@router.get("/research/data/readiness/data-inventory")
def get_sepa_data_inventory(request: Request) -> Dict[str, Any]:
    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}
    return compute_data_inventory_stats(db)


@router.get("/research/data/readiness/fundamental-conditions")
def get_fundamental_conditions_by_symbol(
    request: Request,
    symbol: str = "",
) -> Dict[str, Any]:
    """Return today's SEPA fundamental conditions snapshot for a single symbol.

    Reads from ``public.stock_readiness_daily.fundamental_eval`` (jsonb) for the latest
    ``as_of_date <= CURRENT_DATE``. Falls back to the most recent stored row when today's
    snapshot is missing (e.g. before the daily Phase4 run).
    """
    import psycopg2
    from psycopg2.extras import RealDictCursor

    from bifrost_core.persistence.postgres.connection import _get_conn_params

    sym = (symbol or "").strip().upper()
    if not sym:
        return {"ok": False, "error": "symbol is required"}
    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}
    params = _get_conn_params(db)
    params["connect_timeout"] = 10
    try:
        conn = psycopg2.connect(**params)
    except Exception as e:
        return {"ok": False, "error": str(e)}
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SET statement_timeout = 8000")
            cur.execute(
                """
                SELECT
                    symbol,
                    as_of_date::text                                       AS as_of_date,
                    coalesce(fundamental_pass, false)                      AS fundamental_pass,
                    coalesce(fundamental_pass_count, 0)                    AS pass_count,
                    coalesce(fundamental_insufficient, false)              AS insufficient_data,
                    fundamental_eval
                FROM public.stock_readiness_daily
                WHERE symbol = %(sym)s
                  AND fundamental_eval IS NOT NULL
                ORDER BY as_of_date DESC, universe_rule_version DESC, price_source DESC
                LIMIT 1
                """,
                {"sym": sym},
            )
            row = cur.fetchone()
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()

    if not row:
        return {"ok": True, "symbol": sym, "found": False}

    eval_json = row.get("fundamental_eval") or {}
    conditions_raw = eval_json.get("conditions") if isinstance(eval_json, dict) else []
    conditions = []
    if isinstance(conditions_raw, list):
        for c in conditions_raw:
            if not isinstance(c, dict):
                continue
            conditions.append(
                {
                    "id": str(c.get("id") or ""),
                    "group": str(c.get("group") or "sepa_core"),
                    "pass": bool(c.get("pass") or False),
                    "actual": c.get("actual"),
                    "threshold": c.get("threshold"),
                    "reason": c.get("reason"),
                }
            )

    groups_raw = eval_json.get("groups") if isinstance(eval_json, dict) else None

    return {
        "ok": True,
        "symbol": sym,
        "found": True,
        "as_of_date": row.get("as_of_date"),
        "pass_count": int(row.get("pass_count") or 0),
        "fundamental_pass": bool(row.get("fundamental_pass") or False),
        "insufficient_data": bool(row.get("insufficient_data") or False),
        "conditions": conditions,
        "groups": groups_raw,
    }


@router.get("/research/data/readiness/symbol-technical-conditions")
def get_symbol_technical_conditions(
    request: Request,
    symbol: str = "",
) -> Dict[str, Any]:
    """Return today's SEPA technical conditions snapshot for a single symbol.

    Reads from ``public.stock_readiness_daily.technical_eval`` (jsonb) for the latest
    ``as_of_date <= CURRENT_DATE``. Falls back to the most recent stored row when today's
    snapshot is missing.
    """
    import psycopg2
    from psycopg2.extras import RealDictCursor

    from bifrost_core.persistence.postgres.connection import _get_conn_params

    sym = (symbol or "").strip().upper()
    if not sym:
        return {"ok": False, "error": "symbol is required"}
    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}
    params = _get_conn_params(db)
    params["connect_timeout"] = 10
    try:
        conn = psycopg2.connect(**params)
    except Exception as e:
        return {"ok": False, "error": str(e)}
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SET statement_timeout = 8000")
            cur.execute(
                """
                SELECT
                    symbol,
                    as_of_date::text                                       AS as_of_date,
                    coalesce(technical_pass, false)                        AS technical_pass,
                    coalesce(technical_pass_count, 0)                      AS pass_count,
                    coalesce(technical_insufficient, false)                AS insufficient_data,
                    technical_eval
                FROM public.stock_readiness_daily
                WHERE symbol = %(sym)s
                  AND technical_eval IS NOT NULL
                ORDER BY as_of_date DESC, universe_rule_version DESC, price_source DESC
                LIMIT 1
                """,
                {"sym": sym},
            )
            row = cur.fetchone()
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()

    if not row:
        return {"ok": True, "symbol": sym, "found": False}

    eval_json = row.get("technical_eval") or {}
    conditions_raw = eval_json.get("conditions") if isinstance(eval_json, dict) else []
    conditions = []
    if isinstance(conditions_raw, list):
        for c in conditions_raw:
            if not isinstance(c, dict):
                continue
            conditions.append(
                {
                    "id": str(c.get("id") or ""),
                    "pass": bool(c.get("pass") or False),
                    "actual": c.get("actual"),
                    "threshold": c.get("threshold"),
                    "reason": c.get("reason"),
                }
            )

    metrics: Dict[str, Any] = {}
    if isinstance(eval_json, dict):
        metrics = eval_json.get("metrics") or {}

    tiers = None
    if isinstance(eval_json, dict) and "tiers" in eval_json:
        tiers = eval_json["tiers"]

    return {
        "ok": True,
        "symbol": sym,
        "found": True,
        "as_of_date": row.get("as_of_date"),
        "pass_count": int(row.get("pass_count") or 0),
        "technical_pass": bool(row.get("technical_pass") or False),
        "insufficient_data": bool(row.get("insufficient_data") or False),
        "conditions": conditions,
        "metrics": metrics,
        "tiers": tiers,
    }


_SEPA_FUND_GROUPS: Dict[str, frozenset] = {
    "sepa_core": frozenset((
        "eps_q2q_ge_25pct", "rev_q2q_ge_25pct",
        "eps_acc_2q", "rev_acc_2q",
        "eps_3y_ge_15pct", "rev_3y_ge_15pct",
        "eps_acc_fy", "rev_acc_fy",
    )),
    "quality": frozenset((
        "gross_margin_ge_30pct", "operating_margin_ge_10pct", "net_margin_ge_5pct",
        "ocf_to_ni_ge_0_7", "interest_coverage_ge_5x",
    )),
    "balance": frozenset((
        "current_ratio_ge_1_5", "quick_ratio_ge_1_0",
        "debt_to_equity_le_1", "net_debt_to_ebitda_le_3",
    )),
    "cashflow": frozenset((
        "fcf_positive", "fcf_margin_ge_5pct",
        "fcf_yield_ge_3pct", "capex_intensity_le_15pct",
    )),
    "valuation": frozenset((
        "pe_le_60", "ps_le_15", "pb_le_8", "ev_to_ebitda_le_30",
    )),
    "profitability": frozenset((
        "roe_ge_15pct", "roa_ge_5pct",
    )),
    "efficiency": frozenset((
        "asset_turnover_ge_0_5", "dso_le_75_days", "dio_le_120_days",
    )),
    "sentiment": frozenset((
        "days_to_cover_le_5", "short_volume_ratio_recent_le_30pct",
        "short_interest_pct_of_float_le_15pct",
    )),
}

_SEPA_VALID_CONDITION_IDS = frozenset().union(*_SEPA_FUND_GROUPS.values())

_FUND_CONDITION_CATALOG = [
    {"id": "eps_q2q_ge_25pct", "group": "sepa_core", "label": "EPS quarterly YoY growth >= 25%", "threshold": 0.25, "source_table": "stock_income_statements"},
    {"id": "rev_q2q_ge_25pct", "group": "sepa_core", "label": "Revenue quarterly YoY growth >= 25%", "threshold": 0.25, "source_table": "stock_income_statements"},
    {"id": "eps_acc_2q", "group": "sepa_core", "label": "EPS YoY growth accelerating 2 quarters", "threshold": None, "source_table": "stock_income_statements"},
    {"id": "rev_acc_2q", "group": "sepa_core", "label": "Revenue YoY growth accelerating 2 quarters", "threshold": None, "source_table": "stock_income_statements"},
    {"id": "eps_3y_ge_15pct", "group": "sepa_core", "label": "EPS 3-year CAGR >= 15%", "threshold": 0.15, "source_table": "stock_income_statements"},
    {"id": "rev_3y_ge_15pct", "group": "sepa_core", "label": "Revenue 3-year CAGR >= 15%", "threshold": 0.15, "source_table": "stock_income_statements"},
    {"id": "eps_acc_fy", "group": "sepa_core", "label": "EPS annual growth acceleration", "threshold": None, "source_table": "stock_income_statements"},
    {"id": "rev_acc_fy", "group": "sepa_core", "label": "Revenue annual growth acceleration", "threshold": None, "source_table": "stock_income_statements"},
    {"id": "gross_margin_ge_30pct", "group": "quality", "label": "Gross margin >= 30%", "threshold": 0.30, "source_table": "stock_income_statements"},
    {"id": "operating_margin_ge_10pct", "group": "quality", "label": "Operating margin >= 10%", "threshold": 0.10, "source_table": "stock_income_statements"},
    {"id": "net_margin_ge_5pct", "group": "quality", "label": "Net margin >= 5%", "threshold": 0.05, "source_table": "stock_income_statements"},
    {"id": "ocf_to_ni_ge_0_7", "group": "quality", "label": "OCF / net income >= 0.7 (earnings quality)", "threshold": 0.70, "source_table": "stock_cash_flows,stock_income_statements"},
    {"id": "interest_coverage_ge_5x", "group": "quality", "label": "Interest coverage >= 5x", "threshold": 5.0, "source_table": "stock_income_statements"},
    {"id": "current_ratio_ge_1_5", "group": "balance", "label": "Current ratio >= 1.5", "threshold": 1.5, "source_table": "stock_balance_sheets"},
    {"id": "quick_ratio_ge_1_0", "group": "balance", "label": "Quick ratio >= 1.0", "threshold": 1.0, "source_table": "stock_balance_sheets"},
    {"id": "debt_to_equity_le_1", "group": "balance", "label": "Debt-to-equity <= 1.0", "threshold": 1.0, "source_table": "stock_ratios"},
    {"id": "net_debt_to_ebitda_le_3", "group": "balance", "label": "Net debt / EBITDA <= 3.0", "threshold": 3.0, "source_table": "stock_balance_sheets,stock_income_statements"},
    {"id": "fcf_positive", "group": "cashflow", "label": "Free cash flow positive", "threshold": 0, "source_table": "stock_cash_flows"},
    {"id": "fcf_margin_ge_5pct", "group": "cashflow", "label": "FCF margin >= 5%", "threshold": 0.05, "source_table": "stock_cash_flows,stock_income_statements"},
    {"id": "fcf_yield_ge_3pct", "group": "cashflow", "label": "FCF yield >= 3%", "threshold": 0.03, "source_table": "stock_cash_flows,stock_ratios"},
    {"id": "capex_intensity_le_15pct", "group": "cashflow", "label": "CapEx intensity <= 15%", "threshold": 0.15, "source_table": "stock_cash_flows,stock_income_statements"},
    {"id": "pe_le_60", "group": "valuation", "label": "P/E <= 60", "threshold": 60.0, "source_table": "stock_ratios"},
    {"id": "ps_le_15", "group": "valuation", "label": "P/S <= 15", "threshold": 15.0, "source_table": "stock_ratios"},
    {"id": "pb_le_8", "group": "valuation", "label": "P/B <= 8", "threshold": 8.0, "source_table": "stock_ratios"},
    {"id": "ev_to_ebitda_le_30", "group": "valuation", "label": "EV/EBITDA <= 30", "threshold": 30.0, "source_table": "stock_ratios"},
    {"id": "roe_ge_15pct", "group": "profitability", "label": "Return on equity >= 15%", "threshold": 0.15, "source_table": "stock_ratios"},
    {"id": "roa_ge_5pct", "group": "profitability", "label": "Return on assets >= 5%", "threshold": 0.05, "source_table": "stock_ratios"},
    {"id": "asset_turnover_ge_0_5", "group": "efficiency", "label": "Asset turnover >= 0.5", "threshold": 0.5, "source_table": "stock_income_statements,stock_balance_sheets"},
    {"id": "dso_le_75_days", "group": "efficiency", "label": "Days sales outstanding <= 75", "threshold": 75.0, "source_table": "stock_income_statements,stock_balance_sheets"},
    {"id": "dio_le_120_days", "group": "efficiency", "label": "Days inventory outstanding <= 120", "threshold": 120.0, "source_table": "stock_income_statements,stock_balance_sheets"},
    {"id": "days_to_cover_le_5", "group": "sentiment", "label": "Days to cover <= 5", "threshold": 5.0, "source_table": "stock_short_interest"},
    {"id": "short_volume_ratio_recent_le_30pct", "group": "sentiment", "label": "Short volume ratio avg <= 30%", "threshold": 0.30, "source_table": "stock_short_volume"},
    {"id": "short_interest_pct_of_float_le_15pct", "group": "sentiment", "label": "Short interest % of float <= 15%", "threshold": 0.15, "source_table": "stock_short_interest,stock_income_statements"},
]

_TECH_VALID_CONDITION_IDS = frozenset(
    (
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
    )
)

_TECH_MOMENTUM_INDICATOR_IDS = frozenset(
    (
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
    )
)

_TECH_STRUCTURE_INDICATOR_IDS = frozenset(
    (
        "realized_vol_contraction",
        "bb_squeeze",
        "obv_slope_30d_positive",
        "adx_14_ge_25",
        "aroon_oscillator_ge_50",
        "tight_closes_5d",
        "vcp_contraction_3m",
        "pocket_pivot_count",
        "rsl_new_high",
        "base_metrics",
    )
)

_TECH_SENTIMENT_INDICATOR_IDS = frozenset(
    (
        "days_to_cover_ge_5",
        "short_volume_ratio_le_30pct_recent",
        "short_volume_ratio_trend_4w_falling",
    )
)


@router.get("/research/data/readiness/fundamental-condition-catalog")
def get_fundamental_condition_catalog() -> Dict[str, Any]:
    """Return static catalog of all fundamental condition IDs with group/label/threshold metadata."""
    return {
        "ok": True,
        "groups": list(_SEPA_FUND_GROUPS.keys()),
        "conditions": _FUND_CONDITION_CATALOG,
        "total": len(_FUND_CONDITION_CATALOG),
    }


@router.get("/research/data/readiness/fundamental-filter")
def get_fundamental_filter(
    request: Request,
    include: str = "",
    limit: int = 500,
) -> Dict[str, Any]:
    """Return universe symbols whose today's SEPA snapshot **passes every** condition in ``include``.

    Query parameter ``include`` is a comma-separated list of canonical condition IDs (see
    ``_SEPA_VALID_CONDITION_IDS``). Insufficient-data rows and rows outside the universe are
    excluded. Results are sorted by descending ``pass_count``, then symbol.
    """
    import psycopg2
    from psycopg2.extras import RealDictCursor

    from bifrost_core.persistence.postgres.connection import _get_conn_params

    raw_ids = [s.strip() for s in (include or "").split(",") if s.strip()]
    cond_ids = [c for c in raw_ids if c in _SEPA_VALID_CONDITION_IDS]
    if not raw_ids:
        return {"ok": True, "include": [], "count": 0, "symbols": [], "limit": limit}
    if not cond_ids:
        return {"ok": False, "error": "no valid condition IDs"}

    try:
        eff_limit = max(1, min(int(limit), 5000))
    except Exception:
        eff_limit = 500

    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}
    params = _get_conn_params(db)
    params["connect_timeout"] = 10
    try:
        conn = psycopg2.connect(**params)
    except Exception as e:
        return {"ok": False, "error": str(e)}

    # Build dynamic WHERE: one jsonb containment predicate per required condition.
    where_clauses = [
        "as_of_date = CURRENT_DATE",
        "included_in_universe = true",
        "fundamental_eval IS NOT NULL",
        "coalesce((fundamental_eval->>'insufficient_data')::boolean, false) IS NOT TRUE",
    ]
    sql_params: Dict[str, Any] = {"lim": eff_limit}
    for i, cid in enumerate(cond_ids):
        key = f"c{i}"
        where_clauses.append(f"fundamental_eval->'conditions' @> %({key})s::jsonb")
        sql_params[key] = json.dumps([{"id": cid, "pass": True}])

    sql = (
        "SELECT symbol, "
        "       coalesce((fundamental_eval->>'pass_count')::int, 0) AS pass_count, "
        "       fundamental_eval->'conditions' AS conditions "
        "FROM public.stock_readiness_daily "
        f"WHERE {' AND '.join(where_clauses)} "
        "ORDER BY pass_count DESC, symbol ASC "
        "LIMIT %(lim)s"
    )

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SET statement_timeout = 10000")
            cur.execute(sql, sql_params)
            rows = cur.fetchall() or []
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()

    symbols = []
    for r in rows:
        cond_list = r.get("conditions") or []
        passed_ids = (
            [c.get("id") for c in cond_list if isinstance(c, dict) and c.get("pass") is True]
            if isinstance(cond_list, list)
            else []
        )
        symbols.append(
            {
                "symbol": r["symbol"],
                "pass_count": int(r.get("pass_count") or 0),
                "passed_conditions": passed_ids,
            }
        )
    return {
        "ok": True,
        "include": cond_ids,
        "count": len(symbols),
        "symbols": symbols,
        "limit": eff_limit,
    }


@router.get("/research/data/readiness/technical-filter")
def get_technical_filter(
    request: Request,
    include: str = "",
    limit: int = 500,
) -> Dict[str, Any]:
    """Return universe symbols whose today's technical snapshot **passes every** condition in ``include``.

    Mirrors ``fundamental-filter`` but reads from ``technical_eval`` JSONB.
    ``include`` is a comma-separated list of canonical technical condition IDs (see
    ``_TECH_VALID_CONDITION_IDS``). Insufficient-data rows are excluded.
    Results are sorted by descending ``pass_count``, then symbol.
    """
    import psycopg2
    from psycopg2.extras import RealDictCursor

    from bifrost_core.persistence.postgres.connection import _get_conn_params

    raw_ids = [s.strip() for s in (include or "").split(",") if s.strip()]
    cond_ids = [c for c in raw_ids if c in _TECH_VALID_CONDITION_IDS]
    if not raw_ids:
        return {"ok": True, "include": [], "count": 0, "symbols": [], "limit": limit}
    if not cond_ids:
        return {"ok": False, "error": "no valid technical condition IDs"}

    try:
        eff_limit = max(1, min(int(limit), 5000))
    except Exception:
        eff_limit = 500

    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}
    params = _get_conn_params(db)
    params["connect_timeout"] = 10
    try:
        conn = psycopg2.connect(**params)
    except Exception as e:
        return {"ok": False, "error": str(e)}

    where_clauses = [
        "as_of_date = CURRENT_DATE",
        "included_in_universe = true",
        "technical_eval IS NOT NULL",
        "coalesce((technical_eval->>'insufficient_data')::boolean, false) IS NOT TRUE",
    ]
    sql_params: Dict[str, Any] = {"lim": eff_limit}
    for i, cid in enumerate(cond_ids):
        key = f"c{i}"
        where_clauses.append(f"technical_eval->'conditions' @> %({key})s::jsonb")
        sql_params[key] = json.dumps([{"id": cid, "pass": True}])

    sql = (
        "SELECT symbol, "
        "       coalesce((technical_eval->>'pass_count')::int, 0) AS pass_count, "
        "       technical_eval->'conditions' AS conditions "
        "FROM public.stock_readiness_daily "
        f"WHERE {' AND '.join(where_clauses)} "
        "ORDER BY pass_count DESC, symbol ASC "
        "LIMIT %(lim)s"
    )

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SET statement_timeout = 10000")
            cur.execute(sql, sql_params)
            rows = cur.fetchall() or []
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()

    symbols = []
    for r in rows:
        cond_list = r.get("conditions") or []
        passed_ids = (
            [c.get("id") for c in cond_list if isinstance(c, dict) and c.get("pass") is True]
            if isinstance(cond_list, list)
            else []
        )
        symbols.append(
            {
                "symbol": r["symbol"],
                "pass_count": int(r.get("pass_count") or 0),
                "passed_conditions": passed_ids,
            }
        )
    return {
        "ok": True,
        "include": cond_ids,
        "count": len(symbols),
        "symbols": symbols,
        "limit": eff_limit,
    }


@router.get("/research/data/readiness/symbols-snapshot")
def get_symbols_readiness_snapshot(
    request: Request,
    symbols: str = "",
) -> Dict[str, Any]:
    """Return the latest ``stock_readiness_daily`` row for each requested symbol.

    Used by the Stock Screener main results table: instead of re-running Phase1/CRS/
    Fundamentals on demand, just read what the unified Stock Data Readiness daily
    pipeline already wrote. For each symbol we pick the row with the largest
    ``as_of_date`` (typically today), regardless of which ``universe_rule_version``
    or ``price_source`` produced it (priority: most recent computed_at).
    """
    import psycopg2
    from psycopg2.extras import RealDictCursor

    from bifrost_core.persistence.postgres.connection import _get_conn_params

    raw = (symbols or "").strip()
    if not raw:
        return {"ok": True, "as_of_date": None, "count": 0, "symbols": []}
    syms = [s.strip().upper() for s in raw.replace(";", ",").split(",") if s.strip()]
    syms = [s for s in syms if s][:500]
    if not syms:
        return {"ok": True, "as_of_date": None, "count": 0, "symbols": []}

    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}
    params = _get_conn_params(db)
    params["connect_timeout"] = 10
    try:
        conn = psycopg2.connect(**params)
    except Exception as e:
        return {"ok": False, "error": str(e)}

    rows_by_symbol: Dict[str, Dict[str, Any]] = {}
    latest_as_of: Optional[str] = None
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SET statement_timeout = 10000")
            # DISTINCT ON keeps the most recent row per symbol (largest as_of_date,
            # then largest computed_at as tiebreaker).
            cur.execute(
                """
                SELECT DISTINCT ON (symbol)
                  symbol,
                  as_of_date,
                  included_in_universe,
                  price_ready,
                  bar_count_lookback,
                  first_bar_date,
                  last_bar_date,
                  income_stmt_ready,
                  income_stmt_q_count,
                  income_stmt_a_count,
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
                FROM public.stock_readiness_daily
                WHERE symbol = ANY(%(syms)s)
                ORDER BY symbol, as_of_date DESC, computed_at DESC
                """,
                {"syms": syms},
            )
            for r in cur.fetchall() or []:
                sym = r["symbol"]
                fund_eval = r.get("fundamental_eval") or {}
                fund_cond_list = fund_eval.get("conditions") if isinstance(fund_eval, dict) else None
                passed_conditions = (
                    [c.get("id") for c in fund_cond_list if isinstance(c, dict) and c.get("pass") is True]
                    if isinstance(fund_cond_list, list)
                    else []
                )
                # Build passed_conditions_by_group from fund_eval conditions
                passed_by_group: Dict[str, list] = {}
                if isinstance(fund_cond_list, list):
                    for c in fund_cond_list:
                        if isinstance(c, dict) and c.get("pass") is True:
                            grp = c.get("group") or "sepa_core"
                            passed_by_group.setdefault(grp, []).append(c.get("id"))
                fund_groups_summary = fund_eval.get("groups") if isinstance(fund_eval, dict) else None
                tech_eval = r.get("technical_eval") or {}
                tech_cond_list = tech_eval.get("conditions") if isinstance(tech_eval, dict) else None
                passed_tech_conditions = (
                    [c.get("id") for c in tech_cond_list if isinstance(c, dict) and c.get("pass") is True]
                    if isinstance(tech_cond_list, list)
                    else []
                )
                ao = r.get("as_of_date")
                ao_str = ao.isoformat() if hasattr(ao, "isoformat") else (str(ao) if ao else None)
                first_bar = r.get("first_bar_date")
                last_bar = r.get("last_bar_date")
                rows_by_symbol[sym] = {
                    "symbol": sym,
                    "found": True,
                    "as_of_date": ao_str,
                    "included_in_universe": bool(r.get("included_in_universe") or False),
                    "price_ready": bool(r.get("price_ready") or False),
                    "bar_count_lookback": int(r.get("bar_count_lookback") or 0),
                    "first_bar_date": first_bar.isoformat() if hasattr(first_bar, "isoformat") else (str(first_bar) if first_bar else None),
                    "last_bar_date": last_bar.isoformat() if hasattr(last_bar, "isoformat") else (str(last_bar) if last_bar else None),
                    "income_stmt_ready": bool(r.get("income_stmt_ready") or False),
                    "income_stmt_q_count": int(r.get("income_stmt_q_count") or 0),
                    "income_stmt_a_count": int(r.get("income_stmt_a_count") or 0),
                    "balance_sheet_present": bool(r.get("balance_sheet_present") or False),
                    "cash_flow_present": bool(r.get("cash_flow_present") or False),
                    "ratios_present": bool(r.get("ratios_present") or False),
                    "short_interest_present": bool(r.get("short_interest_present") or False),
                    "short_volume_present": bool(r.get("short_volume_present") or False),
                    "fundamental_pass": bool(r.get("fundamental_pass") or False),
                    "fundamental_pass_count": int(r.get("fundamental_pass_count") or 0),
                    "fundamental_insufficient": bool(r.get("fundamental_insufficient") or False),
                    "passed_conditions": passed_conditions,
                    "passed_conditions_by_group": passed_by_group,
                    "fund_groups": fund_groups_summary,
                    "technical_pass": bool(r.get("technical_pass") or False),
                    "technical_pass_count": int(r.get("technical_pass_count") or 0),
                    "technical_insufficient": bool(r.get("technical_insufficient") or False),
                    "passed_tech_conditions": passed_tech_conditions,
                }
                if ao_str and (latest_as_of is None or ao_str > latest_as_of):
                    latest_as_of = ao_str
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()

    # Preserve request order; fill missing symbols with placeholder rows.
    ordered = []
    for s in syms:
        if s in rows_by_symbol:
            ordered.append(rows_by_symbol[s])
        else:
            ordered.append({"symbol": s, "found": False})

    return {
        "ok": True,
        "as_of_date": latest_as_of,
        "count": len(ordered),
        "symbols": ordered,
    }


@router.get("/research/data/readiness/symbol-fundamental-raw-data")
def get_symbol_fundamental_raw_data(
    request: Request,
    symbol: str = "",
) -> Dict[str, Any]:
    """Return raw quarterly/annual income statement rows + computed metrics for one symbol.

    Used by the Stock Inspector sidebar to display the underlying EPS/revenue data
    behind each SEPA fundamental condition and highlight which rows feed each condition.
    """
    import psycopg2
    from psycopg2.extras import RealDictCursor

    from bifrost_core.persistence.postgres.connection import _get_conn_params

    sym = (symbol or "").strip().upper()
    if not sym:
        return {"ok": False, "error": "symbol is required"}
    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}
    params = _get_conn_params(db)
    params["connect_timeout"] = 10
    try:
        conn = psycopg2.connect(**params)
    except Exception as e:
        return {"ok": False, "error": str(e)}
    try:
        from bifrost_api.research.sepa.financials_data import (
            _INCOME_FIELDS,
            unpack_financial_data,
        )
        from bifrost_api.research.market_data_client import fetch_sepa_income_rows

        income = fetch_sepa_income_rows(sym)

        quarterly = []
        for r in (income.get("quarterly") or [])[-10:]:
            flat = unpack_financial_data(r.get("data"), _INCOME_FIELDS)
            quarterly.append({
                "fiscal_year": r.get("fiscal_year"),
                "fiscal_quarter": r.get("fiscal_quarter"),
                "eps": flat.get("basic_earnings_per_share"),
                "revenues": flat.get("revenue"),
            })
        quarterly.reverse()

        annual = []
        for r in (income.get("annual") or [])[-5:]:
            flat = unpack_financial_data(r.get("data"), _INCOME_FIELDS)
            annual.append({
                "fiscal_year": r.get("fiscal_year"),
                "eps": flat.get("basic_earnings_per_share"),
                "revenues": flat.get("revenue"),
            })
        annual.reverse()

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SET statement_timeout = 8000")
            cur.execute(
                """
                SELECT fundamental_eval
                FROM public.stock_readiness_daily
                WHERE symbol = %(sym)s
                  AND fundamental_eval IS NOT NULL
                ORDER BY as_of_date DESC
                LIMIT 1
                """,
                {"sym": sym},
            )
            srd = cur.fetchone()
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()

    metrics: Dict[str, Any] = {}
    if srd and isinstance(srd.get("fundamental_eval"), dict):
        metrics = srd["fundamental_eval"].get("metrics") or {}

    return {
        "ok": True,
        "symbol": sym,
        "quarterly": quarterly,
        "annual": annual,
        "metrics": metrics,
    }


@router.get("/research/data/readiness/symbol-option-pcr")
def get_symbol_option_pcr(
    request: Request,
    symbol: str = "",
    lookback_days: int = 365,
) -> Dict[str, Any]:
    """Put/call ratio trend and option chain rollup by expiry for Stock Inspector."""
    from bifrost_api.research.sepa_engine.stock_option_pcr import fetch_symbol_option_pcr

    sym = (symbol or "").strip().upper()
    if not sym:
        return {"ok": False, "error": "symbol is required"}
    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}
    return fetch_symbol_option_pcr(db, sym, lookback_days=lookback_days)


@router.get("/research/data/readiness/symbol-statements")
def get_symbol_statements(
    request: Request,
    symbol: str = "",
) -> Dict[str, Any]:
    """Return latest balance sheet, cash flow, ratios, short interest, and short volume rows for a symbol."""
    sym = (symbol or "").strip().upper()
    if not sym:
        return {"ok": False, "error": "symbol is required"}
    try:
        from bifrost_api.research.sepa.financials_data import (
            REPORT_BALANCE,
            REPORT_CASH_FLOW,
            REPORT_RATIOS,
            REPORT_SHORT_INTEREST,
            REPORT_SHORT_VOLUME,
            _BALANCE_FIELDS,
            _CASH_FLOW_FIELDS,
            _RATIOS_FIELDS,
            _SHORT_INTEREST_FIELDS,
            _SHORT_VOLUME_FIELDS,
            unpack_financial_data,
        )
        from bifrost_api.research.market_data_client import fetch_sepa_financials

        def _fetch_rows(rtype: str, period: str | None, limit: int) -> list:
            data = fetch_sepa_financials([sym], rtype, period_type=period, limit=limit)
            rows = data.get(sym, [])
            rows.sort(key=lambda r: r.get("period_date") or "", reverse=True)
            return rows

        raw_balance = _fetch_rows(REPORT_BALANCE, "quarterly", 6)
        balance_sheets = []
        for r in raw_balance:
            flat = unpack_financial_data(r.get("data"), _BALANCE_FIELDS)
            balance_sheets.append({
                "period_end": r.get("period_date"),
                "fiscal_year": r.get("fiscal_year"),
                "fiscal_quarter": r.get("fiscal_quarter"),
                **{k: flat.get(k) for k in (
                    "cash_and_equivalents", "total_current_assets", "total_current_liabilities",
                    "total_assets", "total_liabilities", "total_equity",
                    "receivables", "inventories", "debt_current",
                    "long_term_debt_and_capital_lease_obligations",
                    "property_plant_equipment_net", "retained_earnings_deficit",
                )},
            })

        raw_cf = _fetch_rows(REPORT_CASH_FLOW, "quarterly", 6)
        cash_flows = []
        for r in raw_cf:
            flat = unpack_financial_data(r.get("data"), _CASH_FLOW_FIELDS)
            cash_flows.append({
                "period_end": r.get("period_date"),
                "fiscal_year": r.get("fiscal_year"),
                "fiscal_quarter": r.get("fiscal_quarter"),
                **{k: flat.get(k) for k in (
                    "net_income", "net_cash_from_operating_activities",
                    "net_cash_from_investing_activities", "net_cash_from_financing_activities",
                    "depreciation_depletion_and_amortization",
                    "purchase_of_property_plant_and_equipment",
                    "change_in_cash_and_equivalents",
                )},
            })

        raw_ratios = _fetch_rows(REPORT_RATIOS, None, 8)
        ratios = []
        for r in raw_ratios:
            flat = unpack_financial_data(r.get("data"), _RATIOS_FIELDS)
            ratios.append({
                "date": r.get("period_date"),
                **{k: flat.get(k) for k in (
                    "price_to_earnings", "price_to_sales", "price_to_book",
                    "price_to_free_cash_flow", "debt_to_equity",
                    "return_on_equity", "return_on_assets",
                    "market_cap", "free_cash_flow", "earnings_per_share",
                    "average_volume", "dividend_yield",
                )},
            })

        raw_si = _fetch_rows(REPORT_SHORT_INTEREST, None, 8)
        short_interest = []
        for r in raw_si:
            flat = unpack_financial_data(r.get("data"), _SHORT_INTEREST_FIELDS)
            short_interest.append({
                "settlement_date": r.get("period_date"),
                "short_interest": flat.get("short_interest"),
                "avg_daily_volume": flat.get("avg_daily_volume"),
                "days_to_cover": flat.get("days_to_cover"),
            })

        raw_sv = _fetch_rows(REPORT_SHORT_VOLUME, None, 12)
        short_volume = []
        for r in raw_sv:
            flat = unpack_financial_data(r.get("data"), _SHORT_VOLUME_FIELDS)
            short_volume.append({
                "trade_date": r.get("period_date"),
                "short_volume": flat.get("short_volume"),
                "short_volume_ratio": flat.get("short_volume_ratio"),
                "total_volume": flat.get("total_volume"),
            })

    except Exception as e:
        return {"ok": False, "error": str(e)}

    def _serialize(rows: list) -> list:
        import datetime
        out = []
        for row in rows:
            d: Dict[str, Any] = {}
            for k, v in row.items():
                if isinstance(v, (datetime.date, datetime.datetime)):
                    d[k] = str(v)
                elif v is not None and not isinstance(v, (str, int, float, bool)):
                    d[k] = str(v)
                else:
                    d[k] = v
            out.append(d)
        return out

    return {
        "ok": True,
        "symbol": sym,
        "balance_sheets": _serialize(balance_sheets),
        "cash_flows": _serialize(cash_flows),
        "ratios": _serialize(ratios),
        "short_interest": _serialize(short_interest),
        "short_volume": _serialize(short_volume),
    }


@router.get("/research/data/ticker-overview/{symbol}")
def get_ticker_overview(symbol: str, request: Request) -> Dict[str, Any]:
    """Return ticker detail (+ related peers when available) for a single symbol via Plugin API."""
    import datetime

    import psycopg2
    from psycopg2.extras import RealDictCursor

    from bifrost_api.research.market_data_client import fetch_ticker_detail
    from bifrost_core.persistence.postgres.connection import _get_conn_params

    sym = symbol.strip().upper()

    ticker = fetch_ticker_detail(sym)
    if not ticker:
        return {"ok": True, "found": False, "symbol": sym}

    db = _db_config(request)
    related: list = []
    if db:
        params = _get_conn_params(db)
        params["connect_timeout"] = 10
        try:
            conn = psycopg2.connect(**params)
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SET statement_timeout = 10000")
                cur.execute(
                    """
                    SELECT rt.to_symbol
                    FROM market.ticker_related rt
                    WHERE rt.from_symbol = %s
                    ORDER BY rt.rank ASC
                    LIMIT 12
                    """,
                    (sym,),
                )
                related = [r["to_symbol"] for r in cur.fetchall()]
            conn.close()
        except Exception:
            pass

    data: Dict[str, Any] = {
        "ticker": ticker.get("symbol", sym),
        "name": ticker.get("name"),
        "primary_exchange": ticker.get("primary_exchange"),
        "instrument_type": ticker.get("instrument_type"),
        "active": ticker.get("active"),
        "currency_name": ticker.get("currency"),
        "cik": ticker.get("cik"),
        "sector": ticker.get("sector"),
        "industry": ticker.get("industry"),
        "sic_description": None,
        "market_cap": ticker.get("market_cap"),
        "total_employees": ticker.get("total_employees"),
        "description": ticker.get("description"),
        "homepage_url": ticker.get("homepage_url"),
        "address_city": None,
        "address_state": None,
        "list_date": ticker.get("list_date"),
        "exchange": ticker.get("primary_exchange"),
        "share_class_shares_outstanding": None,
        "weighted_shares_outstanding": None,
    }
    for k, v in data.items():
        if isinstance(v, (datetime.date, datetime.datetime)):
            data[k] = str(v)
        elif v is not None and not isinstance(v, (str, int, float, bool)):
            data[k] = str(v)

    return {"ok": True, "found": True, "symbol": sym, **data, "related_tickers": related}


# ── Tier 2–4 new endpoints ────────────────────────────────────────────────────


@router.get("/research/data/readiness/momentum-distribution")
def get_momentum_distribution(request: Request) -> Dict[str, Any]:
    """Return universe-wide histogram of momentum_score (0..10)."""
    import psycopg2
    from psycopg2.extras import RealDictCursor

    from bifrost_core.persistence.postgres.connection import _get_conn_params

    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}
    params = _get_conn_params(db)
    params["connect_timeout"] = 10
    try:
        conn = psycopg2.connect(**params)
    except Exception as e:
        return {"ok": False, "error": str(e)}

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SET statement_timeout = 15000")
            cur.execute(
                """
                SELECT
                    coalesce(
                        (technical_eval->'tiers'->'momentum'->>'score')::int,
                        0
                    ) AS score,
                    count(*) AS cnt
                FROM public.stock_readiness_daily
                WHERE as_of_date = CURRENT_DATE
                  AND technical_eval IS NOT NULL
                  AND technical_eval->'tiers'->'momentum' IS NOT NULL
                GROUP BY 1
                ORDER BY 1
                """
            )
            rows = cur.fetchall() or []

        distribution = {i: 0 for i in range(11)}
        total = 0
        for r in rows:
            s = int(r.get("score", 0))
            c = int(r.get("cnt", 0))
            distribution[s] = c
            total += c

        return {"ok": True, "distribution": distribution, "total": total}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


@router.get("/research/data/readiness/momentum-filter")
def get_momentum_filter(
    request: Request,
    include: str = "",
    min_score: int = 0,
    limit: int = 500,
) -> Dict[str, Any]:
    """Filter symbols by momentum sub-conditions and/or minimum momentum score.

    ``include``: comma-separated momentum indicator IDs (validated against whitelist).
    ``min_score``: minimum momentum_score (0..10).
    """
    import psycopg2
    from psycopg2.extras import RealDictCursor

    from bifrost_core.persistence.postgres.connection import _get_conn_params

    raw_ids = [s.strip() for s in (include or "").split(",") if s.strip()]
    cond_ids = [c for c in raw_ids if c in _TECH_MOMENTUM_INDICATOR_IDS]

    try:
        eff_limit = max(1, min(int(limit), 5000))
        eff_score = max(0, min(int(min_score), 10))
    except Exception:
        eff_limit = 500
        eff_score = 0

    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}
    params = _get_conn_params(db)
    params["connect_timeout"] = 10
    try:
        conn = psycopg2.connect(**params)
    except Exception as e:
        return {"ok": False, "error": str(e)}

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SET statement_timeout = 15000")

            where_parts = [
                "as_of_date = CURRENT_DATE",
                "technical_eval IS NOT NULL",
                "technical_eval->'tiers'->'momentum' IS NOT NULL",
            ]
            if eff_score > 0:
                where_parts.append(
                    f"(technical_eval->'tiers'->'momentum'->>'score')::int >= {eff_score}"
                )
            for cid in cond_ids:
                where_parts.append(
                    f"""EXISTS (
                        SELECT 1 FROM jsonb_array_elements(
                            technical_eval->'tiers'->'momentum'->'indicators'
                        ) elem
                        WHERE elem->>'id' = '{cid}' AND (elem->>'pass')::boolean = true
                    )"""
                )

            query = f"""
                SELECT
                    symbol,
                    (technical_eval->'tiers'->'momentum'->>'score')::int AS momentum_score,
                    coalesce(technical_pass_count, 0) AS core_pass_count
                FROM public.stock_readiness_daily
                WHERE {' AND '.join(where_parts)}
                ORDER BY (technical_eval->'tiers'->'momentum'->>'score')::int DESC, symbol
                LIMIT {eff_limit}
            """
            cur.execute(query)
            rows = cur.fetchall() or []

        symbols = [
            {"symbol": r["symbol"], "momentum_score": r["momentum_score"], "core_pass_count": r["core_pass_count"]}
            for r in rows
        ]
        return {
            "ok": True,
            "include": cond_ids,
            "min_score": eff_score,
            "count": len(symbols),
            "symbols": symbols,
            "limit": eff_limit,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


_TIER_INDICATOR_IDS: Dict[str, frozenset] = {
    "structure": _TECH_STRUCTURE_INDICATOR_IDS,
    "sentiment": _TECH_SENTIMENT_INDICATOR_IDS,
}
_TIER_MAX_SCORE: Dict[str, int] = {
    "structure": 10,
    "sentiment": 3,
}


@router.get("/research/data/readiness/tier-filter")
def get_tier_filter(
    request: Request,
    tier: str = "structure",
    include: str = "",
    min_score: int = 0,
    limit: int = 500,
) -> Dict[str, Any]:
    """Filter symbols by structure or sentiment tier sub-conditions and/or minimum tier score.

    ``tier``: 'structure' or 'sentiment'.
    ``include``: comma-separated indicator IDs (validated against tier whitelist).
    ``min_score``: minimum tier score.
    """
    import psycopg2
    from psycopg2.extras import RealDictCursor

    from bifrost_core.persistence.postgres.connection import _get_conn_params

    if tier not in _TIER_INDICATOR_IDS:
        return {"ok": False, "error": f"tier must be one of: {list(_TIER_INDICATOR_IDS.keys())}"}

    valid_ids = _TIER_INDICATOR_IDS[tier]
    max_score = _TIER_MAX_SCORE.get(tier, 10)
    raw_ids = [s.strip() for s in (include or "").split(",") if s.strip()]
    cond_ids = [c for c in raw_ids if c in valid_ids]

    try:
        eff_limit = max(1, min(int(limit), 5000))
        eff_score = max(0, min(int(min_score), max_score))
    except Exception:
        eff_limit = 500
        eff_score = 0

    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}
    params = _get_conn_params(db)
    params["connect_timeout"] = 10
    try:
        conn = psycopg2.connect(**params)
    except Exception as e:
        return {"ok": False, "error": str(e)}

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SET statement_timeout = 15000")

            where_parts = [
                "as_of_date = CURRENT_DATE",
                "technical_eval IS NOT NULL",
                f"technical_eval->'tiers'->'{tier}' IS NOT NULL",
            ]
            if eff_score > 0:
                where_parts.append(
                    f"(technical_eval->'tiers'->'{tier}'->>'score')::int >= {eff_score}"
                )
            for cid in cond_ids:
                where_parts.append(
                    f"""EXISTS (
                        SELECT 1 FROM jsonb_array_elements(
                            technical_eval->'tiers'->'{tier}'->'indicators'
                        ) elem
                        WHERE elem->>'id' = '{cid}' AND (elem->>'pass')::boolean = true
                    )"""
                )

            query = f"""
                SELECT
                    symbol,
                    (technical_eval->'tiers'->'{tier}'->>'score')::int AS tier_score,
                    coalesce(technical_pass_count, 0) AS core_pass_count
                FROM public.stock_readiness_daily
                WHERE {' AND '.join(where_parts)}
                ORDER BY (technical_eval->'tiers'->'{tier}'->>'score')::int DESC, symbol
                LIMIT {eff_limit}
            """
            cur.execute(query)
            rows = cur.fetchall() or []

        symbols = [
            {"symbol": r["symbol"], "tier_score": r["tier_score"], "core_pass_count": r["core_pass_count"]}
            for r in rows
        ]
        return {
            "ok": True,
            "tier": tier,
            "include": cond_ids,
            "min_score": eff_score,
            "count": len(symbols),
            "symbols": symbols,
            "limit": eff_limit,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


@router.get("/research/data/readiness/symbol-technical-tiers")
def get_symbol_technical_tiers(
    request: Request,
    symbol: str = "",
) -> Dict[str, Any]:
    """Return the full 4-tier technical evaluation for a single symbol."""
    import psycopg2
    from psycopg2.extras import RealDictCursor

    from bifrost_core.persistence.postgres.connection import _get_conn_params

    sym = (symbol or "").strip().upper()
    if not sym:
        return {"ok": False, "error": "symbol is required"}

    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}
    params = _get_conn_params(db)
    params["connect_timeout"] = 10
    try:
        conn = psycopg2.connect(**params)
    except Exception as e:
        return {"ok": False, "error": str(e)}

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SET statement_timeout = 8000")
            cur.execute(
                """
                SELECT
                    symbol,
                    as_of_date::text AS as_of_date,
                    coalesce(technical_pass, false) AS technical_pass,
                    coalesce(technical_pass_count, 0) AS pass_count,
                    coalesce(technical_insufficient, false) AS insufficient_data,
                    technical_eval
                FROM public.stock_readiness_daily
                WHERE symbol = %(sym)s
                  AND technical_eval IS NOT NULL
                ORDER BY as_of_date DESC, universe_rule_version DESC, price_source DESC
                LIMIT 1
                """,
                {"sym": sym},
            )
            row = cur.fetchone()
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()

    if not row:
        return {"ok": True, "symbol": sym, "found": False}

    eval_json = row.get("technical_eval") or {}
    tiers = eval_json.get("tiers") if isinstance(eval_json, dict) else None

    return {
        "ok": True,
        "symbol": sym,
        "found": True,
        "as_of_date": row.get("as_of_date"),
        "technical_pass": bool(row.get("technical_pass") or False),
        "pass_count": int(row.get("pass_count") or 0),
        "insufficient_data": bool(row.get("insufficient_data") or False),
        "tiers": tiers,
        "rule_version": eval_json.get("rule_version") if isinstance(eval_json, dict) else None,
    }
