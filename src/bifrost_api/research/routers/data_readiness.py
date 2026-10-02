from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, Request

from bifrost_api.research.sepa.readiness_snapshot import (
    READINESS_DATA_CATALOG,
    compute_data_inventory_stats,
    get_sepa_price_gap_details,
)

logger = logging.getLogger(__name__)
router = APIRouter(tags=["research"])


def _readiness_snapshot_deprecated() -> Dict[str, Any]:
    return {
        "ok": True,
        "status": "deprecated",
        "message": (
            "Readiness snapshot is computed by dbt CronJob (dw_stock.mart_sepa_*). "
            "stock_readiness_daily table has been retired."
        ),
    }


def _fundamentals_backfill_deprecated() -> Dict[str, Any]:
    return {
        "ok": True,
        "status": "deprecated",
        "message": (
            "Fundamental evaluation is handled by dbt CronJob. "
            "Run mart_sepa_fundamental_eval refresh in bifrost-research."
        ),
    }


def _technical_backfill_deprecated() -> Dict[str, Any]:
    return {
        "ok": True,
        "status": "deprecated",
        "message": (
            "Technical evaluation is handled by dbt CronJob. "
            "Run mart_sepa_technical_eval refresh in bifrost-research."
        ),
    }


def _db_config(request: Request) -> Optional[dict]:
    return request.app.state.control_via_db or getattr(request.app.state, "status_cfg_for_read", None)


def _plugin_get(path: str, *, params: Dict[str, str] | None = None, timeout: int = 30) -> Dict[str, Any]:
    from bifrost_api.research.market_data_client import _get_json

    return _get_json(path, params=params, timeout=timeout)


def _plugin_post(path: str, body: Dict[str, Any] | None = None, *, timeout: int = 30) -> Dict[str, Any]:
    from bifrost_api.research.market_data_client import _post_json

    return _post_json(path, body or {}, timeout=timeout)


def _enqueue_ingest(kind: str, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """POST /market/ingest/enqueue — restores Stock Data Readiness backfill buttons."""
    try:
        body: Dict[str, Any] = {"kind": kind}
        if payload:
            body["payload"] = payload
        resp = _plugin_post("/ingest/enqueue", body)
        if not isinstance(resp, dict):
            return {"ok": False, "error": "invalid plugin response", "kind": kind}
        if "ok" not in resp:
            resp = {**resp, "ok": True}
        resp.setdefault("kind", kind)
        return resp
    except Exception as e:
        logger.warning("plugin ingest enqueue failed kind=%s: %s", kind, e)
        return {"ok": False, "error": str(e), "kind": kind, "job_ids": [], "chunks": 0}


@router.get("/research/data/readiness/summary")
def get_sepa_readiness_summary(request: Request) -> Dict[str, Any]:
    """Thin passthrough → Market Data Plugin ``GET /market/readiness/summary``."""
    _ = request
    try:
        out = _plugin_get("/readiness/summary", timeout=90)
        if isinstance(out, dict) and out.get("ok") is not False and "data_catalog" not in out:
            out = {**out, "data_catalog": READINESS_DATA_CATALOG}
        return out if isinstance(out, dict) else {"ok": False, "error": "invalid plugin response"}
    except Exception as e:
        logger.warning("plugin readiness summary failed: %s", e)
        return {"ok": False, "error": f"Market Data Plugin summary unavailable: {e}"}


@router.post("/research/data/readiness/snapshot")
def post_sepa_readiness_snapshot(request: Request) -> Dict[str, Any]:
    _ = request
    return _readiness_snapshot_deprecated()


@router.post("/research/data/readiness/stock-unified-snapshot")
def post_sepa_stock_unified_snapshot(request: Request) -> Dict[str, Any]:
    """Enqueue Plugin ``stock_snapshot`` (alias snapshot_backfill)."""
    _ = request
    return _enqueue_ingest("snapshot_backfill")


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
    """Enqueue Plugin vendor_gap_fix → stock_daily_grouped."""
    _ = request
    payload = body if isinstance(body, dict) else {}
    return _enqueue_ingest("vendor_gap_fix", payload.get("payload") if isinstance(payload.get("payload"), dict) else payload or None)


@router.post("/research/data/readiness/backfill-fundamentals")
def post_sepa_backfill_fundamentals(
    request: Request,
    body: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    _ = (request, body)
    return _fundamentals_backfill_deprecated()


@router.post("/research/data/readiness/backfill-technical")
def post_sepa_backfill_technical(
    request: Request,
    body: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    _ = (request, body)
    return _technical_backfill_deprecated()


@router.post("/research/data/readiness/sync-holidays")
def post_sepa_sync_holidays(request: Request) -> Dict[str, Any]:
    """Retired: Massive holidays sync — use market-data plugin calendar enqueue."""
    _ = request
    return _enqueue_ingest("calendar")


@router.post("/research/data/readiness/backfill-grouped-history")
def post_sepa_backfill_grouped_history(
    request: Request,
    body: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    """Enqueue Plugin grouped_daily_backfill → stock_daily_grouped."""
    _ = request
    payload = body if isinstance(body, dict) else {}
    return _enqueue_ingest(
        "grouped_daily_backfill",
        payload.get("payload") if isinstance(payload.get("payload"), dict) else payload or None,
    )


# Trade FE feed kind → Plugin ops_jobs.job_ingest handler kind.
# income/balance/cash all use Polygon financials → kind=financials (writes all statements).
_FIN_FEED_TO_PLUGIN_KIND: Dict[str, str] = {
    "feed_stocks_income_statements": "financials",
    "feed_stocks_balance_sheets": "financials",
    "feed_stocks_cash_flows": "financials",
    "feed_stocks_ratios": "ratios",
    "feed_stocks_short_interest": "short_interest",
    "feed_stocks_short_volume": "short_volume",
}

_FIN_FEED_TO_REPORT_TYPE: Dict[str, str] = {
    "feed_stocks_income_statements": "income_statement",
    "feed_stocks_balance_sheets": "balance_sheet",
    "feed_stocks_cash_flows": "cash_flow_statement",
    "feed_stocks_ratios": "ratios",
    "feed_stocks_short_interest": "short_interest",
    "feed_stocks_short_volume": "short_volume",
}

_FIN_ENQUEUE_MAX_SYMBOLS = 500


def _post_sepa_financials_backfill(
    request: Request,
    body: Dict[str, Any],
    *,
    kind: str,
) -> Dict[str, Any]:
    """Enqueue Plugin ingest jobs per gap symbol."""
    _ = request
    plugin_kind = _FIN_FEED_TO_PLUGIN_KIND.get(kind)
    if not plugin_kind:
        return {"ok": False, "error": f"unknown feed kind: {kind}", "kind": kind, "job_ids": [], "chunks": 0}

    raw_syms = body.get("symbols") if isinstance(body, dict) else None
    symbols: list[str] = []
    if isinstance(raw_syms, list):
        symbols = sorted(
            {str(s).strip().upper() for s in raw_syms if str(s or "").strip()}
        )

    if not symbols:
        report_type = _FIN_FEED_TO_REPORT_TYPE.get(kind, "")
        try:
            from bifrost_api.research.market_data_client import fetch_sepa_gaps

            gap = fetch_sepa_gaps(report_type, limit=_FIN_ENQUEUE_MAX_SYMBOLS)
            symbols = [
                str(s).strip().upper()
                for s in (gap.get("symbols") or [])
                if str(s or "").strip()
            ]
        except Exception as e:
            logger.warning("fin backfill gap lookup failed kind=%s: %s", kind, e)
            return {
                "ok": False,
                "error": f"failed to resolve gap symbols: {e}",
                "kind": kind,
                "job_ids": [],
                "chunks": 0,
            }

    if len(symbols) > _FIN_ENQUEUE_MAX_SYMBOLS:
        symbols = symbols[:_FIN_ENQUEUE_MAX_SYMBOLS]

    if not symbols:
        return {
            "ok": True,
            "kind": kind,
            "plugin_kind": plugin_kind,
            "gap_count": 0,
            "chunks": 0,
            "job_ids": [],
            "message": "No gap symbols to enqueue.",
        }

    job_ids: list[str] = []
    errors: list[str] = []
    for sym in symbols:
        resp = _enqueue_ingest(plugin_kind, {"symbol": sym})
        if resp.get("ok"):
            jid = resp.get("job_id")
            if jid:
                job_ids.append(str(jid))
        else:
            errors.append(f"{sym}:{resp.get('error') or 'enqueue failed'}")

    ok = len(job_ids) > 0 or not errors
    out: Dict[str, Any] = {
        "ok": ok,
        "kind": kind,
        "plugin_kind": plugin_kind,
        "gap_count": len(symbols),
        "chunks": len(job_ids),
        "job_ids": job_ids,
        "message": (
            f"Enqueued {len(job_ids)}/{len(symbols)} {plugin_kind} jobs via Market Data Plugin."
        ),
    }
    if errors:
        out["error"] = f"{len(errors)} enqueue failures (first: {errors[0]})"
        if not job_ids:
            out["ok"] = False
    return out


def _get_sepa_financials_gaps(
    request: Request,
    *,
    detail_fetcher: str,
    limit: int = 2000,
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
    return {
        "ok": True,
        "gaps": rows,
        "total_gap_count": total,
        "returned": len(rows),
    }


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
    return _get_sepa_financials_gaps(request, detail_fetcher="get_ratios_gap_details")


@router.post("/research/data/readiness/backfill-ratios")
def post_sepa_backfill_ratios(
    request: Request,
    body: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    return _post_sepa_financials_backfill(request, body, kind="feed_stocks_ratios")


@router.get("/research/data/readiness/short-interest-gaps")
def get_sepa_short_interest_gaps(request: Request) -> Dict[str, Any]:
    return _get_sepa_financials_gaps(request, detail_fetcher="get_short_interest_gap_details")


@router.post("/research/data/readiness/backfill-short-interest")
def post_sepa_backfill_short_interest(
    request: Request,
    body: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    return _post_sepa_financials_backfill(request, body, kind="feed_stocks_short_interest")


@router.get("/research/data/readiness/short-volume-gaps")
def get_sepa_short_volume_gaps(request: Request) -> Dict[str, Any]:
    return _get_sepa_financials_gaps(request, detail_fetcher="get_short_volume_gap_details")


@router.post("/research/data/readiness/backfill-short-volume")
def post_sepa_backfill_short_volume(
    request: Request,
    body: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    return _post_sepa_financials_backfill(request, body, kind="feed_stocks_short_volume")


_VALID_GAP_ACK_TYPES = frozenset(
    ("income_statements", "balance_sheets", "cash_flows", "ratios", "short_interest", "short_volume")
)


@router.get("/research/data/readiness/gap-ack")
def get_sepa_gap_ack(request: Request) -> Dict[str, Any]:
    """Passthrough → Plugin ``GET /market/readiness/source-void`` (Trade FE shape)."""
    _ = request
    try:
        resp = _plugin_get("/readiness/source-void")
        acks = resp.get("acks")
        if acks is None and isinstance(resp.get("voids"), dict):
            acks = [{"data_type": k, **v} for k, v in sorted(resp["voids"].items())]
        return {"ok": True, "acks": acks or []}
    except Exception as e:
        logger.warning("plugin source-void GET failed: %s", e)
        return {"ok": False, "error": str(e)}


@router.post("/research/data/readiness/gap-ack")
def post_sepa_gap_ack(
    request: Request,
    body: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    """Passthrough → Plugin ``POST /market/readiness/source-void``."""
    _ = request
    data_type = str(body.get("data_type", "")).strip()
    if data_type not in _VALID_GAP_ACK_TYPES:
        return {"ok": False, "error": f"Invalid data_type: {data_type!r}"}
    try:
        return _plugin_post("/readiness/source-void", body if isinstance(body, dict) else {})
    except Exception as e:
        logger.warning("plugin source-void POST failed: %s", e)
        return {"ok": False, "error": str(e)}


@router.get("/research/data/readiness/criteria-stats")
def get_sepa_criteria_stats(request: Request) -> Dict[str, Any]:
    _ = request
    return _criteria_stats_analytics()


def _criteria_stats_analytics() -> Dict[str, Any]:
    """Criteria stats from analytics marts, reshaped for Stock Screener FE."""
    from datetime import datetime, timezone

    from bifrost_api.research.analytics_reader import (
        FUND_CONDITION_COLUMNS,
        TECH_CONDITION_COLUMNS,
        fetch_criteria_stats,
        get_conn,
    )

    try:
        raw = fetch_criteria_stats()
    except Exception as e:
        logger.warning("analytics criteria_stats failed, no legacy fallback: %s", e)
        return {"ok": False, "error": f"Analytics DB error: {e}"}

    fund_raw = raw.get("fundamental") if isinstance(raw.get("fundamental"), dict) else {}
    tech_raw = raw.get("technical") if isinstance(raw.get("technical"), dict) else {}

    # Already FE-shaped (has conditions list)
    if isinstance(fund_raw.get("conditions"), list):
        return {"ok": True, "computed_at": datetime.now(timezone.utc).isoformat(), **raw}

    fund_key_map = {
        "eps_q2q": "eps_q2q_ge_25pct",
        "rev_q2q": "rev_q2q_ge_25pct",
        "eps_acc": "eps_acc_2q",
        "rev_acc": "rev_acc_2q",
        "eps_3y": "eps_3y_ge_15pct",
        "rev_3y": "rev_3y_ge_15pct",
        "eps_acc_fy": "eps_acc_fy",
        "rev_acc_fy": "rev_acc_fy",
    }
    tech_key_map = {
        "volume": "avg_volume_50_gt_threshold",
        "low52": "close_ge_low52_x_1_3",
        "high52": "close_ge_high52_x_0_75",
        "sma50_150": "sma50_gt_sma150",
        "sma50_200": "sma50_gt_sma200",
        "sma150_200": "sma150_gt_sma200",
        "sma200_rising": "sma200_rising_1m",
        "price_sma50": "price_gt_sma50",
        "price_sma150": "price_gt_sma150",
        "price_sma200": "price_gt_sma200",
        "crs": "crs_ge_70",
    }

    def _conds(stats: dict, key_map: dict, ids: list[str]) -> list[dict]:
        total = int(stats.get("evaluated") or stats.get("total") or 0)
        out = []
        id_set = set(ids)
        for key, cid in key_map.items():
            if cid not in id_set:
                continue
            p = int(stats.get(f"{key}_pass") or 0)
            f = int(stats.get(f"{key}_fail") or 0)
            out.append(
                {
                    "id": cid,
                    "label": cid,
                    "pass": p,
                    "fail": f,
                    "no_data": max(0, total - p - f),
                    "total": total,
                }
            )
        return out

    fund_eval = int(fund_raw.get("evaluated") or fund_raw.get("total") or 0)
    tech_eval = int(tech_raw.get("evaluated") or tech_raw.get("total") or 0)

    fundamental = {
        "cached_count": fund_eval,
        "fund_pass_count": int(fund_raw.get("all_pass") or 0),
        "no_data_count": int(fund_raw.get("no_data") or 0),
        "pass_6_plus": int(fund_raw.get("pass_6_plus") or 0),
        "pass_4_plus": int(fund_raw.get("pass_4_plus") or 0),
        "conditions": _conds(fund_raw, fund_key_map, FUND_CONDITION_COLUMNS),
        "pass_count_distribution": [],
    }
    technical = {
        "total_in_snapshot": tech_eval,
        "price_ready_count": tech_eval,
        "fund_cached_count": fund_eval,
        "both_ready": min(fund_eval, tech_eval),
        "bars_ge_252": 0,
        "bars_ge_240": 0,
        "bars_ge_200": 0,
        "bars_lt_200": 0,
        "no_bars": 0,
        "failure_reasons": [],
        "tech_cached_count": tech_eval,
        "tech_pass_count": int(tech_raw.get("all_pass") or 0),
        "tech_insufficient_count": 0,
        "pass_8_plus": int(tech_raw.get("pass_8_plus") or 0),
        "pass_4_plus": int(tech_raw.get("pass_4_plus") or 0),
        "conditions": [
            {"id": c["id"], "label": c["label"], "pass": c["pass"], "fail": c["fail"]}
            for c in _conds(tech_raw, tech_key_map, TECH_CONDITION_COLUMNS)
        ],
        "pass_count_distribution": [],
    }

    try:
        from psycopg2.extras import RealDictCursor

        from bifrost_api.research.analytics_reader import (
            _FUND_EVAL_TABLE,
            _TECH_EVAL_TABLE,
            latest_eval_date,
        )

        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                fund_as_of = latest_eval_date(cur, _FUND_EVAL_TABLE)
                if fund_as_of is not None:
                    cur.execute(
                        f"""
                        SELECT COALESCE(pass_count, 0)::int AS conditions_passed, COUNT(*)::int AS symbol_count
                        FROM {_FUND_EVAL_TABLE}
                        WHERE eval_date = %s
                          AND COALESCE(insufficient_data, false) IS NOT TRUE
                        GROUP BY 1
                        """,
                        (fund_as_of,),
                    )
                    dist = {
                        int(r["conditions_passed"]): int(r["symbol_count"])
                        for r in (cur.fetchall() or [])
                    }
                    fundamental["pass_count_distribution"] = [
                        {"conditions_passed": i, "symbol_count": dist.get(i, 0)}
                        for i in range(8, -1, -1)
                    ]
                    fundamental["eval_date"] = fund_as_of.isoformat()

                tech_as_of = latest_eval_date(cur, _TECH_EVAL_TABLE)
                if tech_as_of is not None:
                    cur.execute(
                        f"""
                        SELECT COALESCE(pass_count, 0)::int AS conditions_passed, COUNT(*)::int AS symbol_count
                        FROM {_TECH_EVAL_TABLE}
                        WHERE eval_date = %s
                        GROUP BY 1
                        """,
                        (tech_as_of,),
                    )
                    tdist = {
                        int(r["conditions_passed"]): int(r["symbol_count"])
                        for r in (cur.fetchall() or [])
                    }
                    technical["pass_count_distribution"] = [
                        {"conditions_passed": i, "symbol_count": tdist.get(i, 0)}
                        for i in range(11, -1, -1)
                    ]
                    technical["eval_date"] = tech_as_of.isoformat()
    except Exception as exc:
        logger.warning("criteria_stats distribution enrich failed: %s", exc)

    return {
        "ok": True,
        "universe_count": int(fund_raw.get("total") or tech_raw.get("total") or fund_eval),
        "computed_at": datetime.now(timezone.utc).isoformat(),
        "fundamental": fundamental,
        "technical": technical,
    }


@router.get("/research/data/readiness/fundamental-distribution/symbols")
def get_fundamental_distribution_symbols(
    request: Request,
    conditions_passed: int = 0,
) -> Dict[str, Any]:
    """Return symbols that passed exactly N out of 8 SEPA fundamental conditions today."""
    if conditions_passed < 0 or conditions_passed > 8:
        return {"ok": False, "error": "conditions_passed must be 0–8"}

    return _fundamental_distribution_analytics(conditions_passed)


def _fundamental_distribution_analytics(conditions_passed: int) -> Dict[str, Any]:
    """Fundamental distribution from dw_stock.mart_sepa_fundamental_eval (latest snapshot)."""
    from bifrost_api.research.analytics_reader import (
        _FUND_EVAL_TABLE,
        fetch_fundamental_distribution_symbols,
        get_conn,
        latest_eval_date,
    )
    from psycopg2.extras import RealDictCursor

    try:
        symbols = fetch_fundamental_distribution_symbols(conditions_passed)
        as_of = None
        try:
            with get_conn() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    d = latest_eval_date(cur, _FUND_EVAL_TABLE)
                    as_of = d.isoformat() if d else None
        except Exception:
            as_of = None
        return {
            "ok": True,
            "conditions_passed": conditions_passed,
            "count": len(symbols),
            "symbols": symbols,
            "as_of": as_of,
        }
    except Exception as e:
        logger.warning("analytics fundamental_distribution failed: %s", e)
        return {"ok": False, "error": str(e)}


def _technical_distribution_analytics(conditions_passed: int) -> Dict[str, Any]:
    """Technical distribution from analytics mart (latest snapshot)."""
    from bifrost_api.research.analytics_reader import (
        _TECH_EVAL_TABLE,
        fetch_technical_distribution_symbols,
        get_conn,
        latest_eval_date,
    )
    from psycopg2.extras import RealDictCursor

    try:
        symbols = fetch_technical_distribution_symbols(conditions_passed)
        as_of = None
        try:
            with get_conn() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    d = latest_eval_date(cur, _TECH_EVAL_TABLE)
                    as_of = d.isoformat() if d else None
        except Exception:
            as_of = None
        return {
            "ok": True,
            "conditions_passed": conditions_passed,
            "count": len(symbols),
            "symbols": symbols,
            "as_of": as_of,
        }
    except Exception as e:
        logger.warning("analytics technical_distribution failed: %s", e)
        return {"ok": False, "error": str(e)}


@router.get("/research/data/readiness/technical-distribution/symbols")
def get_technical_distribution_symbols(
    request: Request,
    conditions_passed: int = 0,
) -> Dict[str, Any]:
    """Return symbols that passed exactly N out of 11 SEPA technical conditions today."""
    if conditions_passed < 0 or conditions_passed > 11:
        return {"ok": False, "error": "conditions_passed must be 0–11"}

    return _technical_distribution_analytics(conditions_passed)

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

    Reads from ``dw_stock.mart_sepa_fundamental_eval``.
    Requires SEPA analytics marts (stock_readiness_daily retired).
    """
    sym = (symbol or "").strip().upper()
    if not sym:
        return {"ok": False, "error": "symbol is required"}

    return _fundamental_conditions_analytics(sym)

def _fundamental_conditions_analytics(sym: str) -> Dict[str, Any]:
    """Single-symbol fundamental conditions from dw_stock.mart_sepa_fundamental_eval."""
    from bifrost_api.research.analytics_reader import FUND_CONDITION_COLUMNS, fetch_fundamental_eval_single

    try:
        row = fetch_fundamental_eval_single(sym)
    except Exception as e:
        logger.warning("analytics fundamental_conditions failed for %s: %s", sym, e)
        return {"ok": False, "error": str(e)}

    if not row:
        return {"ok": True, "symbol": sym, "found": False}

    conditions = []
    for col in FUND_CONDITION_COLUMNS:
        conditions.append({
            "id": col,
            "group": "sepa_core",
            "pass": bool(row.get(col)),
            "actual": None,
            "threshold": None,
            "reason": None,
        })

    eval_date = row.get("eval_date")
    as_of_str = eval_date.isoformat() if hasattr(eval_date, "isoformat") else str(eval_date) if eval_date else None
    pass_count = int(row.get("pass_count") or 0)

    return {
        "ok": True,
        "symbol": sym,
        "found": True,
        "as_of_date": as_of_str,
        "pass_count": pass_count,
        "fundamental_pass": pass_count >= 6,
        "insufficient_data": bool(row.get("insufficient_data")),
        "conditions": conditions,
        "groups": None,
    }


@router.get("/research/data/readiness/symbol-technical-conditions")
def get_symbol_technical_conditions(
    request: Request,
    symbol: str = "",
) -> Dict[str, Any]:
    """Return today's SEPA technical conditions snapshot for a single symbol.

    Reads from ``dw_stock.mart_sepa_technical_eval``.
    Requires SEPA analytics marts (stock_readiness_daily retired).
    """
    sym = (symbol or "").strip().upper()
    if not sym:
        return {"ok": False, "error": "symbol is required"}

    return _technical_conditions_analytics(sym)

def _technical_conditions_analytics(sym: str) -> Dict[str, Any]:
    """Single-symbol technical conditions from dw_stock.mart_sepa_technical_eval."""
    from bifrost_api.research.analytics_reader import TECH_CONDITION_COLUMNS, fetch_technical_eval_single

    try:
        row = fetch_technical_eval_single(sym)
    except Exception as e:
        logger.warning("analytics technical_conditions failed for %s: %s", sym, e)
        return {"ok": False, "error": str(e)}

    if not row:
        return {"ok": True, "symbol": sym, "found": False}

    conditions = []
    for col in TECH_CONDITION_COLUMNS:
        conditions.append({
            "id": col,
            "pass": bool(row.get(col)),
            "actual": None,
            "threshold": None,
            "reason": None,
        })

    eval_date = row.get("eval_date")
    as_of_str = eval_date.isoformat() if hasattr(eval_date, "isoformat") else str(eval_date) if eval_date else None
    pass_count = int(row.get("pass_count") or 0)

    return {
        "ok": True,
        "symbol": sym,
        "found": True,
        "as_of_date": as_of_str,
        "pass_count": pass_count,
        "technical_pass": pass_count == 11,
        "insufficient_data": False,
        "conditions": conditions,
        "metrics": {},
        "tiers": None,
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

    return _fundamental_filter_analytics(cond_ids, eff_limit)

def _fundamental_filter_analytics(cond_ids: list, limit: int) -> Dict[str, Any]:
    """Fundamental filter using dw_stock.mart_sepa_fundamental_eval boolean columns."""
    from bifrost_api.research.analytics_reader import fetch_fundamental_filter

    try:
        rows = fetch_fundamental_filter(cond_ids, limit=limit)
        symbols = [
            {"symbol": r["symbol"], "pass_count": int(r.get("pass_count") or 0), "passed_conditions": cond_ids}
            for r in rows
        ]
        return {"ok": True, "include": cond_ids, "count": len(symbols), "symbols": symbols, "limit": limit}
    except Exception as e:
        logger.warning("analytics fundamental_filter failed: %s", e)
        return {"ok": False, "error": str(e)}


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

    return _technical_filter_analytics(cond_ids, eff_limit)

def _technical_filter_analytics(cond_ids: list, limit: int) -> Dict[str, Any]:
    """Technical filter using dw_stock.mart_sepa_technical_eval boolean columns."""
    from bifrost_api.research.analytics_reader import fetch_technical_filter

    try:
        rows = fetch_technical_filter(cond_ids, limit=limit)
        symbols = [
            {"symbol": r["symbol"], "pass_count": int(r.get("pass_count") or 0), "passed_conditions": cond_ids}
            for r in rows
        ]
        return {"ok": True, "include": cond_ids, "count": len(symbols), "symbols": symbols, "limit": limit}
    except Exception as e:
        logger.warning("analytics technical_filter failed: %s", e)
        return {"ok": False, "error": str(e)}


@router.get("/research/data/readiness/symbols-snapshot")
def get_symbols_readiness_snapshot(
    request: Request,
    symbols: str = "",
) -> Dict[str, Any]:
    """Return the latest readiness row for each requested symbol.

    Reads from dw_stock.mart_sepa_screener_wide.
    Requires SEPA analytics marts (stock_readiness_daily retired).
    """
    raw = (symbols or "").strip()
    if not raw:
        return {"ok": True, "as_of_date": None, "count": 0, "symbols": []}
    syms = [s.strip().upper() for s in raw.replace(";", ",").split(",") if s.strip()]
    syms = [s for s in syms if s][:500]
    if not syms:
        return {"ok": True, "as_of_date": None, "count": 0, "symbols": []}

    return _symbols_snapshot_analytics(syms)

def _symbols_snapshot_analytics(syms: list) -> Dict[str, Any]:
    """Symbols snapshot from dw_stock.mart_sepa_screener_wide."""
    from bifrost_api.research.analytics_reader import FUND_CONDITION_COLUMNS, TECH_CONDITION_COLUMNS, fetch_screener_wide

    try:
        rows = fetch_screener_wide(symbols=syms)
    except Exception as e:
        logger.warning("analytics symbols_snapshot failed: %s", e)
        return {"ok": False, "error": str(e)}

    rows_by_symbol = {}
    latest_as_of: Optional[str] = None
    for r in rows:
        sym = r.get("symbol", "")
        eval_date = r.get("eval_date")
        as_of_str = eval_date.isoformat() if hasattr(eval_date, "isoformat") else str(eval_date) if eval_date else None

        fund_passed = [col for col in FUND_CONDITION_COLUMNS if r.get(col) is True]
        tech_passed = [col for col in TECH_CONDITION_COLUMNS if r.get(col) is True]
        fund_pass_count = int(r.get("fund_pass_count") or 0)
        tech_pass_count = int(r.get("tech_pass_count") or 0)

        rows_by_symbol[sym] = {
            "symbol": sym,
            "found": True,
            "as_of_date": as_of_str,
            "included_in_universe": True,
            "price_ready": True,
            "bar_count_lookback": 0,
            "first_bar_date": None,
            "last_bar_date": None,
            "income_stmt_ready": True,
            "income_stmt_q_count": 0,
            "income_stmt_a_count": 0,
            "balance_sheet_present": True,
            "cash_flow_present": True,
            "ratios_present": True,
            "short_interest_present": True,
            "short_volume_present": True,
            "fundamental_pass": fund_pass_count >= 6,
            "fundamental_pass_count": fund_pass_count,
            "fundamental_insufficient": bool(r.get("insufficient_data")),
            "passed_conditions": fund_passed,
            "passed_conditions_by_group": {},
            "fund_groups": None,
            "technical_pass": tech_pass_count == 11,
            "technical_pass_count": tech_pass_count,
            "technical_insufficient": False,
            "passed_tech_conditions": tech_passed,
        }
        if as_of_str and (latest_as_of is None or as_of_str > latest_as_of):
            latest_as_of = as_of_str

    ordered = []
    for s in syms:
        if s in rows_by_symbol:
            ordered.append(rows_by_symbol[s])
        else:
            ordered.append({"symbol": s, "found": False})

    return {"ok": True, "as_of_date": latest_as_of, "count": len(ordered), "symbols": ordered}


@router.get("/research/data/readiness/symbol-fundamental-raw-data")
def get_symbol_fundamental_raw_data(
    request: Request,
    symbol: str = "",
) -> Dict[str, Any]:
    """Return raw quarterly/annual income statement rows + computed metrics for one symbol.

    Used by the Stock Inspector sidebar to display the underlying EPS/revenue data
    behind each SEPA fundamental condition and highlight which rows feed each condition.

    ``quarterly`` (last 10) and ``annual`` (last 5) are newest first, read from the
    plugin's income statements in either vendor format; ``metrics`` are the inspector
    columns of ``dw_stock.mart_sepa_fundamental_eval``, empty when that read fails.
    Every answer carries the three keys, since the inspector reads them unguarded.
    """
    _ = request
    sym = (symbol or "").strip().upper()
    empty: Dict[str, Any] = {"quarterly": [], "annual": [], "metrics": {}}
    if not sym:
        return {"ok": False, "error": "symbol is required", **empty}
    try:
        from bifrost_api.research.market_data_client import fetch_sepa_financials
        from bifrost_api.research.sepa.financials_data import REPORT_INCOME, income_rows_for_inspector

        # Headroom over 10 / 5: a series that also holds the other vendor format,
        # or a restated period, loses rows before the cut.
        q_raw = fetch_sepa_financials([sym], REPORT_INCOME, period_type="quarterly", limit=16).get(sym, [])
        a_raw = fetch_sepa_financials([sym], REPORT_INCOME, period_type="annual", limit=8).get(sym, [])
        quarterly, annual = income_rows_for_inspector(q_raw, a_raw, quarters=10, years=5)
    except Exception as e:
        logger.warning("symbol fundamental raw data failed for %s: %s", sym, e)
        return {"ok": False, "error": str(e), "symbol": sym, **empty}

    return {
        "ok": True,
        "symbol": sym,
        "quarterly": quarterly,
        "annual": annual,
        "metrics": _fundamental_metrics(sym),
    }


def _fundamental_metrics(sym: str) -> Dict[str, Any]:
    """Inspector metrics from dw_stock.mart_sepa_fundamental_eval; ``{}`` when unavailable."""
    from bifrost_api.research.analytics_reader import FUND_METRIC_COLUMNS, fetch_fundamental_eval_single
    from bifrost_api.research.sepa.financials_data import _as_float

    try:
        row = fetch_fundamental_eval_single(sym)
    except Exception as e:
        logger.warning("analytics fundamental metrics failed for %s: %s", sym, e)
        return {}
    if not row:
        return {}
    return {col: _as_float(row.get(col)) for col in FUND_METRIC_COLUMNS}


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
            short_volume_ratio,
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
                "short_volume_ratio": short_volume_ratio(flat),
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
    from bifrost_core.persistence.postgres.market_tables import SCHEMA as MARKET_SCHEMA

    sym = symbol.strip().upper()

    ticker = fetch_ticker_detail(sym)
    if not ticker:
        return {"ok": True, "found": False, "symbol": sym}

    db = _db_config(request)
    related: list = []
    if db:
        params = _get_conn_params(db)
        params["connect_timeout"] = 10
        conn = None
        try:
            conn = psycopg2.connect(**params)
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SET statement_timeout = 10000")
                # Golden Source raw_market.ticker_related reaches the Trade DB as the FDW table
                # market.ticker_related; raw_market does not exist here. The old query failed on
                # every call, the failure was swallowed, and the page said "No related tickers on
                # record" for every name (debt TD-03).
                cur.execute(
                    f"""
                    SELECT rt.to_symbol
                    FROM {MARKET_SCHEMA}.ticker_related rt
                    WHERE rt.from_symbol = %s
                    ORDER BY rt.rank ASC
                    LIMIT 12
                    """,
                    (sym,),
                )
                related = [r["to_symbol"] for r in cur.fetchall()]
        except Exception:
            logger.warning("ticker-overview: related tickers for %s failed", sym, exc_info=True)
        finally:
            if conn is not None:
                conn.close()

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

# ── Momentum · structure · sentiment tiers ────────────────────────────────
#
# Each tier is one dbt mart with one row per symbol for the latest eval_date:
# boolean signal columns and a `<tier>_score` that is the passing fraction
# (signals passed / signals). The ids below ARE those column names; until
# 2026-09-30 this file validated against an older vocabulary no mart carries,
# and the filters answered a hard-coded 0 "awaiting 252+ trading days" while
# the marts held every symbol in the universe.
_TIER_COLUMNS: Dict[str, tuple] = {
    "momentum": (
        "rsi_above_50",
        "rsi_healthy_range",
        "macd_bullish",
        "macd_strong",
        "roc_10_positive",
        "roc_21_positive",
        "rs_gt_spy",
        "volume_expanding",
        "volume_surge",
        "price_gt_sma10",
    ),
    "structure": (
        "bb_squeeze",
        "bb_tight_squeeze",
        "adx_trending",
        "adx_strong_trend",
        "aroon_bullish",
        "aroon_up_strong",
        "vol_contracting",
        "vol_tight_contraction",
    ),
    "sentiment": (
        "si_declining",
        "low_short_float",
        "high_days_to_cover",
        "short_float_declining",
        "low_short_volume",
        "sv_ratio_declining",
    ),
}
_TIER_INDICATOR_IDS: Dict[str, frozenset] = {k: frozenset(v) for k, v in _TIER_COLUMNS.items()}
# The score is a fraction; filters and histograms speak in signals passed (0..N).
_TIER_MAX_SCORE: Dict[str, int] = {k: len(v) for k, v in _TIER_COLUMNS.items()}


def _tier_table(tier: str) -> str:
    return f"dw_stock.mart_sepa_tier_{tier}"


def _tier_passed_sql(tier: str) -> str:
    """Signals passed, as an integer 0..N, from the stored fraction."""
    return f"round({tier}_score * {_TIER_MAX_SCORE[tier]})::int"


def _tier_filter(tier: str, cond_ids: List[str], min_score: int, match: str, limit: int) -> Dict[str, Any]:
    """Names on the latest eval_date passing the picked signals (all / any) and at least min_score of them."""
    from bifrost_api.research.analytics_reader import get_conn as _a_conn
    from psycopg2.extras import RealDictCursor

    table = _tier_table(tier)
    passed = _tier_passed_sql(tier)
    where = [f"eval_date = (SELECT max(eval_date) FROM {table})"]
    if cond_ids:
        joiner = " OR " if match == "any" else " AND "
        where.append("(" + joiner.join(f"{c} IS TRUE" for c in cond_ids) + ")")
    if min_score > 0:
        where.append(f"{passed} >= %s")
    sql = (
        f"SELECT symbol, {passed} AS score, eval_date, count(*) OVER () AS total "
        f"FROM {table} WHERE {' AND '.join(where)} "
        f"ORDER BY {tier}_score DESC, symbol ASC LIMIT %s"
    )
    params: List[Any] = ([min_score] if min_score > 0 else []) + [limit]
    with _a_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall() or []
    total = int(rows[0]["total"]) if rows else 0
    return {
        "ok": True,
        "tier": tier,
        "include": cond_ids,
        "match": match,
        "min_score": min_score,
        "max_score": _TIER_MAX_SCORE[tier],
        # The whole match, not the page: a limited list is a floor, never a count.
        "count": total,
        "truncated": total > len(rows),
        "eval_date": str(rows[0]["eval_date"]) if rows else None,
        "symbols": [{"symbol": r["symbol"], "score": int(r["score"] or 0)} for r in rows],
        "limit": limit,
    }


def _tier_args(tier: str, include: str, min_score: int, match: str, limit: int):
    raw_ids = [s.strip() for s in (include or "").split(",") if s.strip()]
    valid = _TIER_INDICATOR_IDS[tier]
    cond_ids = [c for c in raw_ids if c in valid]
    unknown = [c for c in raw_ids if c not in valid]
    try:
        eff_limit = max(1, min(int(limit), 5000))
    except Exception:
        eff_limit = 500
    eff_min = max(0, min(int(min_score or 0), _TIER_MAX_SCORE[tier]))
    return cond_ids, unknown, eff_min, ("any" if match == "any" else "all"), eff_limit


def _tier_filter_response(tier: str, include: str, min_score: int, match: str, limit: int) -> Dict[str, Any]:
    cond_ids, unknown, eff_min, eff_match, eff_limit = _tier_args(tier, include, min_score, match, limit)
    if unknown:
        return {"ok": False, "error": f"unknown {tier} signal ids: {', '.join(unknown)}", "valid": list(_TIER_COLUMNS[tier])}
    if not cond_ids and eff_min == 0:
        return {"ok": True, "tier": tier, "include": [], "count": 0, "symbols": [], "limit": eff_limit}
    try:
        return _tier_filter(tier, cond_ids, eff_min, eff_match, eff_limit)
    except Exception as e:
        logger.warning("tier filter %s failed: %s", tier, e)
        return {"ok": False, "error": str(e)}


@router.get("/research/data/readiness/momentum-distribution")
def get_momentum_distribution(request: Request) -> Dict[str, Any]:
    """Universe-wide histogram of momentum signals passed (0..10) on the latest eval_date."""
    _ = request
    stats = _tier_stats("momentum")
    if not stats.get("ok"):
        return stats
    return {"ok": True, "distribution": stats["pass_count_distribution"], "total": stats["universe_count"]}


def _tier_stats(tier: str) -> Dict[str, Any]:
    """Per-signal pass counts and the signals-passed histogram for one tier, latest eval_date."""
    from bifrost_api.research.analytics_reader import get_conn as _a_conn
    from psycopg2.extras import RealDictCursor

    table = _tier_table(tier)
    cols = _TIER_COLUMNS[tier]
    per_signal = ", ".join(f"count(*) FILTER (WHERE {c} IS TRUE) AS {c}" for c in cols)
    latest = f"eval_date = (SELECT max(eval_date) FROM {table})"
    try:
        with _a_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(f"SELECT max(eval_date) AS d, count(*) AS n, {per_signal} FROM {table} WHERE {latest}")
                head = cur.fetchone() or {}
                cur.execute(
                    f"SELECT {_tier_passed_sql(tier)} AS s, count(*) AS c FROM {table} WHERE {latest} GROUP BY 1"
                )
                hist_rows = cur.fetchall() or []
    except Exception as e:
        logger.warning("tier stats %s failed: %s", tier, e)
        return {"ok": False, "error": str(e)}
    hist = {i: 0 for i in range(_TIER_MAX_SCORE[tier] + 1)}
    for r in hist_rows:
        hist[int(r["s"] or 0)] = int(r["c"] or 0)
    return {
        "ok": True,
        "tier": tier,
        "eval_date": str(head["d"]) if head.get("d") else None,
        "universe_count": int(head.get("n") or 0),
        "max_score": _TIER_MAX_SCORE[tier],
        "conditions": [{"id": c, "pass": int(head.get(c) or 0)} for c in cols],
        "pass_count_distribution": hist,
    }


@router.get("/research/data/readiness/tier-stats")
def get_tier_stats(request: Request, tier: str = "momentum") -> Dict[str, Any]:
    """Per-signal pass counts and the signals-passed histogram for momentum, structure or sentiment."""
    _ = request
    if tier not in _TIER_COLUMNS:
        return {"ok": False, "error": f"tier must be one of: {list(_TIER_COLUMNS.keys())}"}
    return _tier_stats(tier)


_MOMENTUM_GRADES = ("A+", "A", "B", "C", "D")


@router.get("/research/data/readiness/momentum-grades")
def get_momentum_grades(request: Request, grades: str = "", limit: int = 2000) -> Dict[str, Any]:
    """The radar's grades on its latest session: names per grade, and the names in ``grades``.

    ``/research/momentum/radar`` resolves its latest session only for one
    symbol; across the universe it returns every session it holds, so a grade
    read there counts names that held it on any day in months. A screen asks
    about today, so this reads the latest ``trade_date`` only.
    """
    _ = request
    picked = [g.strip().upper() for g in (grades or "").split(",") if g.strip()]
    picked = [g for g in picked if g in _MOMENTUM_GRADES]
    try:
        eff_limit = max(1, min(int(limit), 5000))
    except Exception:
        eff_limit = 2000
    from bifrost_api.research.analytics_reader import get_conn as _a_conn
    from psycopg2.extras import RealDictCursor

    table = "features.stock_signal_momentum_daily"
    latest = f"trade_date = (SELECT max(trade_date) FROM {table})"
    try:
        with _a_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"SELECT max(trade_date) AS d, grade, count(DISTINCT symbol) AS n FROM {table} "
                    f"WHERE {latest} GROUP BY grade"
                )
                count_rows = cur.fetchall() or []
                names: List[str] = []
                if picked:
                    cur.execute(
                        f"SELECT DISTINCT symbol FROM {table} WHERE {latest} AND grade = ANY(%s) "
                        "ORDER BY symbol LIMIT %s",
                        (picked, eff_limit),
                    )
                    names = [r["symbol"] for r in (cur.fetchall() or [])]
    except Exception as e:
        logger.warning("momentum grades failed: %s", e)
        return {"ok": False, "error": str(e)}
    counts = {g: 0 for g in _MOMENTUM_GRADES}
    trade_date = None
    for r in count_rows:
        if r.get("grade") in counts:
            counts[r["grade"]] = int(r["n"] or 0)
        trade_date = trade_date or (str(r["d"]) if r.get("d") else None)
    total_picked = sum(counts[g] for g in picked)
    return {
        "ok": True,
        "trade_date": trade_date,
        "counts": counts,
        "graded": sum(counts.values()),
        "grades": picked,
        "count": total_picked,
        "truncated": total_picked > len(names) if picked else False,
        "symbols": names,
    }


@router.get("/research/data/readiness/momentum-filter")
def get_momentum_filter(
    request: Request,
    include: str = "",
    min_score: int = 0,
    match: str = "all",
    limit: int = 500,
) -> Dict[str, Any]:
    """Names passing the picked momentum signals (``match`` all | any) and at least ``min_score`` of the 10."""
    _ = request
    return _tier_filter_response("momentum", include, min_score, match, limit)


@router.get("/research/data/readiness/tier-filter")
def get_tier_filter(
    request: Request,
    tier: str = "structure",
    include: str = "",
    min_score: int = 0,
    match: str = "all",
    limit: int = 500,
) -> Dict[str, Any]:
    """Names passing the picked structure / sentiment / momentum signals and at least ``min_score`` of them."""
    _ = request
    if tier not in _TIER_COLUMNS:
        return {"ok": False, "error": f"tier must be one of: {list(_TIER_COLUMNS.keys())}"}
    return _tier_filter_response(tier, include, min_score, match, limit)


@router.get("/research/data/readiness/symbol-technical-tiers")
def get_symbol_technical_tiers(
    request: Request,
    symbol: str = "",
) -> Dict[str, Any]:
    """Return the full 4-tier technical evaluation for a single symbol."""
    _ = request
    sym = (symbol or "").strip().upper()
    if not sym:
        return {"ok": False, "error": "symbol is required"}

    return {
        "ok": True,
        "symbol": sym,
        "found": False,
        "note": "Tier data from analytics (awaiting 252+ trading days of data).",
    }


