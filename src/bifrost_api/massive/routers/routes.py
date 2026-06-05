"""Massive / Polygon option research REST endpoints.

Extracted from research.py — all routes with prefix ``/research/massive/``.
No IB client dependencies.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse

from bifrost_worker.data.massive.massive_job_goal import describe_massive_job_goal
from bifrost_worker.data.massive.vendor.client import _as_error_str

logger = logging.getLogger(__name__)

router = APIRouter(tags=["massive"])

MASSIVE_STOP_EXIT_DELAY_SEC = 2.5


# ── shared helpers (thin, no IB) ──────────────────────────────────────────────

def _db_config(request: Request) -> Optional[dict]:
    return request.app.state.control_via_db or getattr(request.app.state, "status_cfg_for_read", None)


def _norm_expiry_key(expiration: str) -> str:
    e = (expiration or "").strip()
    if len(e) >= 10 and e[4] == "-":
        return e[:4] + e[5:7] + e[8:10]
    return e


def _massive_job_to_api(j: Dict[str, Any]) -> Dict[str, Any]:
    created_ts = j.get("created_at")
    if hasattr(created_ts, "timestamp"):
        created_ts = created_ts.timestamp()
    updated_ts = j.get("updated_at")
    if hasattr(updated_ts, "timestamp"):
        updated_ts = updated_ts.timestamp()
    res = j.get("result")
    if isinstance(res, str):
        try:
            res = json.loads(res)
        except json.JSONDecodeError:
            pass
    raw_payload = j.get("payload")
    goal = describe_massive_job_goal(str(j.get("kind") or ""), raw_payload)
    out: Dict[str, Any] = {
        "job_id": str(j.get("job_massive_backfill_id", "")),
        "type": "massive_backfill",
        "kind": j.get("kind"),
        "status": j.get("status"),
        "result": res,
        "goal": goal,
        "celery_task_id": j.get("celery_task_id"),
        "created_ts": created_ts,
        "updated_ts": updated_ts,
    }
    ph = j.get("payload_hash")
    if ph:
        out["payload_hash"] = ph[:16]
    return out


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/research/massive/status")
def get_massive_status(request: Request) -> Dict[str, Any]:
    """Massive/Polygon configuration summary (no API key returned)."""
    from bifrost_worker.data.massive.vendor.config import get_massive_settings, massive_delay_notice_english

    reader = getattr(request.app.state, "reader", None)
    cfg = reader._config if reader else {}
    ms = get_massive_settings(cfg)
    return {
        "configured": bool(ms["api_key"]),
        "tier": ms["tier"],
        "delay_notice": massive_delay_notice_english(),
        "trades_enabled": ms["trades_enabled"],
        "daily_full_backfill_years": ms["daily_full_backfill_years"],
    }


@router.post("/research/massive/shutdown")
def post_massive_shutdown() -> Dict[str, Any]:
    """Terminate the Massive API process (same pattern as POST /control/monitor_stop on the status server)."""

    def _exit_after_send() -> None:
        time.sleep(MASSIVE_STOP_EXIT_DELAY_SEC)
        logger.info("Massive shutdown: exiting process.")
        os._exit(0)

    threading.Thread(target=_exit_after_send, daemon=True).start()
    return {"ok": True}


@router.get("/research/massive/daily-checklist")
def get_massive_daily_checklist(
    request: Request,
    symbols: str = Query(..., description="Comma-separated underlying symbols (Watchlist STK)"),
    trade_date: Optional[str] = Query(
        None,
        description="Session calendar date YYYY-MM-DD (US). Default: today in America/New_York",
    ),
) -> Dict[str, Any]:
    """Per-symbol daily data readiness (snapshot, OI, Max Pain, corporate, WS ingest)."""
    from datetime import datetime
    from zoneinfo import ZoneInfo

    from bifrost_worker.data.massive.vendor.reader import get_massive_daily_checklist_data

    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}
    sym_list = [s.strip().upper() for s in (symbols or "").split(",") if s.strip()][:80]
    if not sym_list:
        return {"ok": False, "error": "symbols is required"}
    td = (trade_date or "").strip()
    if not td:
        td = datetime.now(ZoneInfo("America/New_York")).date().isoformat()
    data = get_massive_daily_checklist_data(db, sym_list, td)
    err = data.get("error") if isinstance(data, dict) else None
    syms_out = data.get("symbols") if isinstance(data, dict) else None
    fatal_err = (
        isinstance(err, str)
        and err.strip()
        and (not isinstance(syms_out, dict) or len(syms_out) == 0)
    )
    if fatal_err:
        return {"ok": False, "error": err.strip(), "trade_date": data.get("trade_date", td)}
    return {"ok": True, **data}


@router.post("/research/massive/api-coverage/sync")
def post_massive_api_coverage_sync() -> Dict[str, Any]:
    """Sync docs/plans/massive_api_coverage.html to frontend/public/plans for UI embed."""
    # routes.py is backend/massive/routers/ — repo root is parents[3], not parents[2] (backend/).
    root = Path(__file__).resolve().parents[3]
    src = root / "docs" / "plans" / "massive_api_coverage.html"
    dst = root / "frontend" / "public" / "plans" / "massive_api_coverage.html"
    if not src.is_file():
        return {"ok": False, "error": f"Source file not found: {src}"}
    try:
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        return {
            "ok": True,
            "source": str(src),
            "target": str(dst),
            "size_bytes": dst.stat().st_size,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


@router.post("/research/massive/stocks-api-coverage/sync")
def post_massive_stocks_api_coverage_sync() -> Dict[str, Any]:
    """Sync docs/plans/massive_stocks_api_coverage.html to frontend/public/plans for UI embed."""
    root = Path(__file__).resolve().parents[3]
    src = root / "docs" / "plans" / "massive_stocks_api_coverage.html"
    dst = root / "frontend" / "public" / "plans" / "massive_stocks_api_coverage.html"
    if not src.is_file():
        return {"ok": False, "error": f"Source file not found: {src}"}
    try:
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        return {
            "ok": True,
            "source": str(src),
            "target": str(dst),
            "size_bytes": dst.stat().st_size,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


@router.get("/research/massive/greeks-coverage")
def get_massive_greeks_coverage(
    request: Request,
    symbol: str = Query(..., description="Underlying symbol"),
    expiration: str = Query("", description="Expiration YYYYMMDD or YYYY-MM-DD (optional; omit for all)"),
    source: str = Query("massive", description="Snapshot source: massive | ib"),
) -> Dict[str, Any]:
    """Greeks/IV coverage and freshness stats from option_snapshots."""
    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}
    sym = (symbol or "").strip().upper()
    if not sym:
        return {"ok": False, "error": "symbol is required"}
    src = (source or "massive").strip().lower()
    if src not in ("massive", "ib"):
        src = "massive"
    exp = (expiration or "").strip()
    exp_norm = _norm_expiry_key(exp) if exp else None

    import psycopg2

    from bifrost_core.persistence.postgres.connection import _get_conn_params

    try:
        params = _get_conn_params(db)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                where = "source = %s AND contract_key LIKE %s"
                args: list = [src, f"{sym}%"]
                if exp_norm:
                    where += " AND contract_key LIKE %s"
                    args.append(f"%{exp_norm}%")
                cur.execute(
                    f"""
                    SELECT
                        count(*) AS total,
                        count(iv) AS with_iv,
                        count(delta) AS with_delta,
                        count(gamma) AS with_gamma,
                        count(theta) AS with_theta,
                        count(vega) AS with_vega,
                        count(CASE WHEN delta IS NOT NULL AND gamma IS NOT NULL
                                    AND theta IS NOT NULL AND vega IS NOT NULL THEN 1 END) AS with_full_greeks,
                        count(open_interest) AS with_oi,
                        min(snapshot_ts) AS oldest_ts,
                        max(snapshot_ts) AS newest_ts,
                        count(CASE WHEN snapshot_ts < now() - interval '24 hours' THEN 1 END) AS stale_rows
                    FROM (
                        SELECT DISTINCT ON (contract_key)
                            iv, delta, gamma, theta, vega, open_interest, snapshot_ts
                        FROM option_snapshots
                        WHERE {where}
                        ORDER BY contract_key, snapshot_ts DESC
                    ) latest
                    """,
                    args,
                )
                row = cur.fetchone()
                if not row or row[0] == 0:
                    return {
                        "ok": True,
                        "symbol": sym,
                        "expiration": exp_norm or "",
                        "source": src,
                        "total": 0,
                        "coverage": {},
                    }
                (total, w_iv, w_delta, w_gamma, w_theta, w_vega,
                 w_full, w_oi, oldest, newest, stale) = row
                pct = lambda n: round(n / total * 100, 1) if total else 0  # noqa: E731
                return {
                    "ok": True,
                    "symbol": sym,
                    "expiration": exp_norm or "",
                    "source": src,
                    "total": total,
                    "coverage": {
                        "with_iv": w_iv,
                        "iv_pct": pct(w_iv),
                        "with_delta": w_delta,
                        "with_gamma": w_gamma,
                        "with_theta": w_theta,
                        "with_vega": w_vega,
                        "with_full_greeks": w_full,
                        "full_greeks_pct": pct(w_full),
                        "with_oi": w_oi,
                    },
                    "freshness": {
                        "oldest_ts": oldest.isoformat() if oldest else None,
                        "newest_ts": newest.isoformat() if newest else None,
                        "stale_rows": stale,
                    },
                }
        finally:
            conn.close()
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


@router.get("/research/massive/contracts-coverage")
def get_massive_contracts_coverage(
    request: Request,
    symbol: str = Query(..., description="Underlying symbol"),
    expiration: str = Query("", description="Expiration YYYYMMDD or YYYY-MM-DD (optional)"),
) -> Dict[str, Any]:
    """Contract reference coverage and mapping consistency from option_contracts."""
    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}
    sym = (symbol or "").strip().upper()
    if not sym:
        return {"ok": False, "error": "symbol is required"}
    exp = (expiration or "").strip()
    exp_norm = _norm_expiry_key(exp) if exp else None

    import psycopg2

    from bifrost_core.persistence.postgres.connection import _get_conn_params

    try:
        params = _get_conn_params(db)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                where = "symbol = %s"
                args: list = [sym]
                if exp_norm:
                    where += " AND expiry = %s"
                    args.append(exp_norm)
                cur.execute(
                    f"""
                    SELECT
                        count(*) AS total,
                        count(massive_option_ticker) AS with_ticker,
                        count(CASE WHEN symbol != '' AND expiry != ''
                                    AND option_right != '' THEN 1 END) AS with_complete_identity,
                        count(CASE WHEN massive_option_ticker IS NOT NULL
                                    AND massive_option_ticker != ''
                                    AND contract_key NOT LIKE '%%' || symbol || '%%' THEN 1 END) AS mapping_mismatch,
                        count(CASE WHEN exercise_style IS NOT NULL AND trim(exercise_style) <> ''
                                    THEN 1 END) AS with_exercise_style,
                        count(CASE WHEN shares_per_contract IS NOT NULL THEN 1 END) AS with_shares_per_contract,
                        min(created_at) AS oldest_ts,
                        max(created_at) AS newest_ts,
                        count(CASE WHEN created_at < now() - interval '7 days' THEN 1 END) AS stale_rows,
                        count(DISTINCT expiry) AS distinct_expirations,
                        count(DISTINCT strike) AS distinct_strikes
                    FROM option_contracts
                    WHERE {where}
                    """,
                    args,
                )
                row = cur.fetchone()
                if not row or row[0] == 0:
                    return {
                        "ok": True, "symbol": sym, "expiration": exp_norm or "",
                        "total": 0, "coverage": {}, "freshness": {},
                    }
                (total, w_ticker, w_identity, mismatch,
                 w_ex_style, w_spc,
                 oldest, newest, stale, dist_exp, dist_strikes) = row
                pct = lambda n: round(n / total * 100, 1) if total else 0  # noqa: E731
                data_avg = (
                    round((int(w_ex_style or 0) + int(w_spc or 0)) / (2.0 * int(total)) * 100, 1)
                    if total
                    else 0.0
                )
                return {
                    "ok": True, "symbol": sym, "expiration": exp_norm or "",
                    "total": total,
                    "coverage": {
                        "with_massive_ticker": w_ticker,
                        "ticker_pct": pct(w_ticker),
                        "with_complete_identity": w_identity,
                        "identity_pct": pct(w_identity),
                        "mapping_mismatch": mismatch,
                        "with_exercise_style": w_ex_style,
                        "exercise_style_pct": pct(w_ex_style),
                        "with_shares_per_contract": w_spc,
                        "shares_per_contract_pct": pct(w_spc),
                        "optional_data_fill_avg_pct": data_avg,
                        "distinct_expirations": dist_exp,
                        "distinct_strikes": dist_strikes,
                    },
                    "freshness": {
                        "oldest_ts": oldest.isoformat() if oldest else None,
                        "newest_ts": newest.isoformat() if newest else None,
                        "stale_rows": stale,
                    },
                }
        finally:
            conn.close()
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


@router.get("/research/massive/db-coverage-summary")
def get_db_coverage_summary(request: Request) -> Dict[str, Any]:
    """Aggregated PostgreSQL coverage: distinct underlyings and freshness per research table."""
    from datetime import date, datetime, timezone

    import psycopg2

    from bifrost_core.persistence.postgres.connection import _get_conn_params

    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}

    def _iso(dt: Any) -> Optional[str]:
        if dt is None:
            return None
        if isinstance(dt, datetime):
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc).isoformat()
            return dt.isoformat()
        if isinstance(dt, date):
            return dt.isoformat()
        return str(dt)

    def _append_table(
        rows_out: List[Dict[str, Any]],
        *,
        id_: str,
        table_name: str,
        dataset_label: str,
        domain: str,
        drill_down_hash: str,
        cur: Any,
        sql: str,
        args: Optional[tuple] = None,
    ) -> None:
        try:
            cur.execute(sql, args or ())
            one = cur.fetchone()
            if not one:
                rows_out.append(
                    {
                        "id": id_,
                        "table_name": table_name,
                        "dataset_label": dataset_label,
                        "domain": domain,
                        "drill_down_hash": drill_down_hash,
                        "distinct_symbols": 0,
                        "newest_activity": None,
                        "newest_trade_date": None,
                        "error": None,
                    }
                )
                return
            distinct = int(one[0] or 0)
            newest = one[1] if len(one) > 1 else None
            newest_trade = one[2] if len(one) > 2 else None
            rows_out.append(
                {
                    "id": id_,
                    "table_name": table_name,
                    "dataset_label": dataset_label,
                    "domain": domain,
                    "drill_down_hash": drill_down_hash,
                    "distinct_symbols": distinct,
                    "newest_activity": _iso(newest),
                    "newest_trade_date": _iso(newest_trade) if newest_trade is not None else None,
                    "error": None,
                }
            )
        except Exception as exc:  # noqa: BLE001 — return per-table error for admin UI
            rows_out.append(
                {
                    "id": id_,
                    "table_name": table_name,
                    "dataset_label": dataset_label,
                    "domain": domain,
                    "drill_down_hash": drill_down_hash,
                    "distinct_symbols": None,
                    "newest_activity": None,
                    "newest_trade_date": None,
                    "error": str(exc),
                }
            )

    tables: List[Dict[str, Any]] = []
    try:
        params = _get_conn_params(db)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                _append_table(
                    tables,
                    id_="option_contracts",
                    table_name="option_contracts",
                    dataset_label="Option contract reference (Massive ticker)",
                    domain="Option",
                    drill_down_hash="coverage-option",
                    cur=cur,
                    sql="""
                        SELECT COUNT(DISTINCT UPPER(TRIM(symbol)))::bigint,
                               MAX(created_at),
                               NULL::date
                        FROM option_contracts
                        WHERE massive_option_ticker IS NOT NULL
                          AND TRIM(COALESCE(massive_option_ticker, '')) <> ''
                        """,
                )
                _append_table(
                    tables,
                    id_="option_snapshots",
                    table_name="option_snapshots",
                    dataset_label="Option snapshots (greeks / IV) (Massive)",
                    domain="Option",
                    drill_down_hash="coverage-option",
                    cur=cur,
                    sql="""
                        SELECT COUNT(DISTINCT UPPER(TRIM(split_part(contract_key, '|', 1))))::bigint,
                               MAX(snapshot_ts),
                               NULL::date
                        FROM option_snapshots
                        WHERE source = 'massive'
                          AND position('|' IN contract_key) > 0
                        """,
                )
                _append_table(
                    tables,
                    id_="report_option_atm_iv_daily",
                    table_name="report_option_atm_iv_daily",
                    dataset_label="ATM IV daily rollup (Massive)",
                    domain="Option",
                    drill_down_hash="coverage-option",
                    cur=cur,
                    sql="""
                        SELECT COUNT(DISTINCT UPPER(TRIM(symbol)))::bigint,
                               MAX(created_at),
                               MAX(trade_date)
                        FROM report_option_atm_iv_daily
                        WHERE source = 'massive'
                        """,
                )
                _append_table(
                    tables,
                    id_="stock_day",
                    table_name="stock_day",
                    dataset_label="Stock daily bars (underlying spot) (Massive)",
                    domain="Shared",
                    drill_down_hash="coverage-massive-stock",
                    cur=cur,
                    sql="""
                        SELECT COUNT(DISTINCT UPPER(TRIM(symbol)))::bigint,
                               MAX(created_at),
                               MAX(bar_time)
                        FROM stock_day
                        WHERE source = 'massive'
                        """,
                )
        finally:
            conn.close()
    except Exception as exc:
        return {"ok": False, "error": str(exc)}

    return {
        "ok": True,
        "source_scope": "massive",
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "tables": tables,
    }


@router.get("/research/massive/celery-beat-schedule")
def get_massive_celery_beat_schedule() -> Dict[str, Any]:
    """Celery Beat entries for Massive tasks (UTC). Does not require Celery broker to be up."""
    from bifrost_worker.data.massive.beat_schedule_public import public_celery_beat_schedule_response

    return public_celery_beat_schedule_response()


@router.get("/research/massive/watchlist-db-coverage")
def get_watchlist_db_coverage(request: Request) -> Dict[str, Any]:
    """Per-symbol PostgreSQL coverage for watchlist STK rows with optionable=true (max 80 symbols).

    option_contracts.* uses row ``created_at`` as last-write proxy (not a dedicated sync-job timestamp).
    """
    from datetime import date, datetime, timezone

    import psycopg2

    from bifrost_core.persistence.postgres.connection import _get_conn_params
    from bifrost_worker.data.massive.vendor.reader import get_watchlist_optionable_stk_symbols

    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}

    def _iso(dt: Any) -> Optional[str]:
        if dt is None:
            return None
        if isinstance(dt, datetime):
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc).isoformat()
            return dt.isoformat()
        if isinstance(dt, date):
            return dt.isoformat()
        return str(dt)

    def _age_seconds(newest: Any) -> Optional[int]:
        if newest is None:
            return None
        if not isinstance(newest, datetime):
            return None
        now = datetime.now(timezone.utc)
        ts = newest if newest.tzinfo else newest.replace(tzinfo=timezone.utc)
        return max(0, int((now - ts).total_seconds()))

    syms = get_watchlist_optionable_stk_symbols(db)[:80]
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    if not syms:
        return {
            "ok": True,
            "source_scope": "massive",
            "generated_at": generated_at,
            "universe": "watchlist_optionable_stk",
            "symbols_count": 0,
            "symbols": [],
            "message": "No optionable STK symbols on watchlist.",
        }

    contracts_map: Dict[str, Dict[str, Any]] = {}
    contracts_check_map: Dict[str, Any] = {}  # sym -> last check updated_at from job_ticker_reference_state
    snapshots_map: Dict[str, Dict[str, Any]] = {}
    atm_map: Dict[str, tuple] = {}  # sym -> (max trade_date, max created_at)
    stock_day_map: Dict[str, tuple] = {}  # sym -> aggregate tuple (like option_day)
    stock_min_map: Dict[str, tuple] = {}  # sym -> stock_min aggregate
    tickers_ref_map: Dict[str, tuple] = {}  # sym -> (tickers_id, updated_at, last_updated_utc, overview_updated_at)
    ticker_types_global: Optional[tuple] = None  # (row_count, max(created_at))
    option_day_map: Dict[str, tuple] = {}  # sym -> (row_count, max bar_time, max created_at)
    option_min_map: Dict[str, tuple] = {}
    suv_day_map: Dict[str, tuple] = {}  # option_snapshots_with_underlying_day: (count, max snapshot_ts, max created_at)
    oec_map: Dict[str, tuple] = {}  # option_expiration_cache: (count, max updated_at)
    oi_daily_map: Dict[str, tuple] = {}  # option_open_interest_daily: (count, max trade_date, max created_at)
    max_pain_map: Dict[str, tuple] = {}  # report_option_max_pain_daily: (count, max trade_date, max created_at)

    try:
        params = _get_conn_params(db)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        UPPER(TRIM(symbol)) AS u,
                        COUNT(*)::bigint AS total,
                        MAX(created_at) AS newest_ts,
                        COUNT(massive_option_ticker) AS with_ticker,
                        COUNT(CASE WHEN symbol != '' AND expiry != ''
                                      AND option_right != '' THEN 1 END) AS with_complete_identity,
                        COUNT(CASE WHEN massive_option_ticker IS NOT NULL
                                    AND massive_option_ticker != ''
                                    AND contract_key NOT LIKE '%%' || symbol || '%%' THEN 1 END) AS mapping_mismatch,
                        COUNT(CASE WHEN exercise_style IS NOT NULL AND trim(exercise_style) <> ''
                                    THEN 1 END) AS with_exercise_style,
                        COUNT(CASE WHEN shares_per_contract IS NOT NULL THEN 1 END) AS with_shares_per_contract,
                        COUNT(DISTINCT expiry)::bigint AS distinct_expirations,
                        COUNT(DISTINCT strike)::bigint AS distinct_strikes,
                        COALESCE(SUM(CASE WHEN exercise_style IS NULL THEN 1 ELSE 0 END), 0)::bigint AS exercise_style_null_rows,
                        COALESCE(SUM(CASE WHEN shares_per_contract IS NULL THEN 1 ELSE 0 END), 0)::bigint AS shares_per_contract_null_rows
                    FROM option_contracts
                    WHERE UPPER(TRIM(symbol)) = ANY(%s)
                    GROUP BY UPPER(TRIM(symbol))
                    """,
                    (syms,),
                )
                for row in cur.fetchall() or []:
                    if not row or not row[0]:
                        continue
                    u = str(row[0]).strip().upper()
                    total = int(row[1] or 0)
                    newest_ts = row[2]
                    w_ticker = int(row[3] or 0)
                    w_identity = int(row[4] or 0)
                    mismatch = int(row[5] or 0)
                    w_ex_style = int(row[6] or 0)
                    w_spc = int(row[7] or 0)
                    dist_exp = int(row[8] or 0)
                    dist_strikes = int(row[9] or 0)
                    es_null_rows = int(row[10] or 0)
                    spc_null_rows = int(row[11] or 0)
                    column_gap_count = es_null_rows + spc_null_rows
                    pct = lambda n: round(n / total * 100, 1) if total else 0.0  # noqa: E731
                    # Average fill across nullable *data* columns (exercise_style, shares_per_contract), not ticker/identity.
                    data_avg = (
                        round((w_ex_style + w_spc) / (2.0 * total) * 100, 1) if total else 0.0
                    )
                    contracts_map[u] = {
                        "row_count": total,
                        "newest_created_at": _iso(newest_ts),
                        "age_seconds": _age_seconds(newest_ts),
                        "ticker_pct": pct(w_ticker),
                        "identity_pct": pct(w_identity),
                        "mapping_mismatch_count": mismatch,
                        "exercise_style_pct": pct(w_ex_style),
                        "shares_per_contract_pct": pct(w_spc),
                        "optional_data_fill_avg_pct": data_avg,
                        "distinct_expirations": dist_exp,
                        "distinct_strikes": dist_strikes,
                        "exercise_style_null_row_count": es_null_rows,
                        "shares_per_contract_null_row_count": spc_null_rows,
                        "column_gap_count": column_gap_count,
                    }

                cur.execute(
                    """
                    WITH latest AS (
                        SELECT DISTINCT ON (os.contract_key)
                            UPPER(TRIM(SPLIT_PART(os.contract_key, '|', 1))) AS u,
                            os.contract_key,
                            os.snapshot_ts,
                            os.iv,
                            os.delta,
                            os.gamma,
                            os.theta,
                            os.vega,
                            os.open_interest
                        FROM option_snapshots os
                        WHERE os.source = 'massive'
                          AND POSITION('|' IN os.contract_key) > 0
                          AND UPPER(TRIM(SPLIT_PART(os.contract_key, '|', 1))) = ANY(%s)
                        ORDER BY os.contract_key, os.snapshot_ts DESC
                    )
                    SELECT
                        u,
                        COUNT(*)::bigint AS n,
                        MAX(snapshot_ts) AS newest_ts,
                        COUNT(iv) AS with_iv,
                        COUNT(CASE WHEN delta IS NOT NULL AND gamma IS NOT NULL
                                    AND theta IS NOT NULL AND vega IS NOT NULL THEN 1 END) AS with_full_greeks,
                        COUNT(open_interest) AS with_oi,
                        COUNT(CASE WHEN snapshot_ts < now() - interval '24 hours' THEN 1 END) AS stale_rows
                    FROM latest
                    GROUP BY u
                    """,
                    (syms,),
                )
                for row in cur.fetchall() or []:
                    if not row or not row[0]:
                        continue
                    u = str(row[0]).strip().upper()
                    total = int(row[1] or 0)
                    newest_ts = row[2]
                    w_iv = int(row[3] or 0)
                    w_fg = int(row[4] or 0)
                    w_oi = int(row[5] or 0)
                    stale = int(row[6] or 0)
                    pct = lambda n: round(n / total * 100, 1) if total else 0.0  # noqa: E731
                    data_avg = round((pct(w_fg) + pct(w_oi)) / 2.0, 1) if total else 0.0
                    snapshots_map[u] = {
                        "row_count": total,
                        "newest_ts": newest_ts,
                        "age_seconds": _age_seconds(newest_ts),
                        "iv_pct": pct(w_iv),
                        "full_greeks_pct": pct(w_fg),
                        "open_interest_pct": pct(w_oi),
                        "optional_data_fill_avg_pct": data_avg,
                        "stale_snapshot_rows": stale,
                    }

                cur.execute(
                    """
                    SELECT UPPER(TRIM(symbol)) AS u, MAX(trade_date), MAX(created_at)
                    FROM report_option_atm_iv_daily
                    WHERE source = 'massive'
                      AND UPPER(TRIM(symbol)) = ANY(%s)
                    GROUP BY UPPER(TRIM(symbol))
                    """,
                    (syms,),
                )
                for row in cur.fetchall() or []:
                    if row and row[0]:
                        atm_map[str(row[0]).strip().upper()] = (row[1], row[2])

                cur.execute(
                    """
                    SELECT
                        UPPER(TRIM(symbol)) AS u,
                        COUNT(*)::bigint AS row_count,
                        MAX(bar_time) AS last_bar_time,
                        MAX(created_at) AS last_created_at,
                        ROUND(COUNT(CASE WHEN open IS NOT NULL AND high IS NOT NULL
                                          AND low IS NOT NULL AND close IS NOT NULL
                                     THEN 1 END)::numeric / NULLIF(COUNT(*),0) * 100, 1) AS ohlc_complete_pct,
                        ROUND(COUNT(volume)::numeric / NULLIF(COUNT(*),0) * 100, 1) AS volume_pct,
                        ROUND(COUNT(vwap)::numeric / NULLIF(COUNT(*),0) * 100, 1) AS vwap_pct,
                        COUNT(DISTINCT bar_time)::bigint AS distinct_bar_dates
                    FROM stock_day
                    WHERE source = 'massive'
                      AND UPPER(TRIM(symbol)) = ANY(%s)
                    GROUP BY UPPER(TRIM(symbol))
                    """,
                    (syms,),
                )
                for row in cur.fetchall() or []:
                    if row and row[0]:
                        stock_day_map[str(row[0]).strip().upper()] = row[1:]

                cur.execute(
                    """
                    SELECT
                        UPPER(TRIM(symbol)) AS u,
                        COUNT(*)::bigint AS row_count,
                        MAX(bar_time) AS last_bar_time,
                        MAX(created_at) AS last_created_at,
                        ROUND(COUNT(CASE WHEN open IS NOT NULL AND high IS NOT NULL
                                          AND low IS NOT NULL AND close IS NOT NULL
                                     THEN 1 END)::numeric / NULLIF(COUNT(*),0) * 100, 1) AS ohlc_complete_pct,
                        ROUND(COUNT(volume)::numeric / NULLIF(COUNT(*),0) * 100, 1) AS volume_pct,
                        ROUND(COUNT(vwap)::numeric / NULLIF(COUNT(*),0) * 100, 1) AS vwap_pct,
                        COUNT(DISTINCT period)::bigint AS distinct_periods
                    FROM stock_min
                    WHERE source = 'massive'
                      AND UPPER(TRIM(symbol)) = ANY(%s)
                    GROUP BY UPPER(TRIM(symbol))
                    """,
                    (syms,),
                )
                for row in cur.fetchall() or []:
                    if row and row[0]:
                        stock_min_map[str(row[0]).strip().upper()] = row[1:]

                cur.execute(
                    """
                    SELECT
                        UPPER(TRIM(symbol)) AS u,
                        COUNT(*)::bigint AS row_count,
                        MAX(bar_time) AS last_bar_time,
                        MAX(created_at) AS last_created_at,
                        ROUND(COUNT(CASE WHEN open IS NOT NULL AND high IS NOT NULL
                                          AND low IS NOT NULL AND close IS NOT NULL
                                     THEN 1 END)::numeric / NULLIF(COUNT(*),0) * 100, 1) AS ohlc_complete_pct,
                        ROUND(COUNT(volume)::numeric / NULLIF(COUNT(*),0) * 100, 1) AS volume_pct,
                        ROUND(COUNT(vwap)::numeric / NULLIF(COUNT(*),0) * 100, 1) AS vwap_pct,
                        COUNT(DISTINCT expiry)::bigint AS distinct_expirations,
                        COUNT(DISTINCT CONCAT(expiry,'|',strike::text,'|',option_right))::bigint AS distinct_contracts
                    FROM option_day
                    WHERE source = 'massive'
                      AND UPPER(TRIM(symbol)) = ANY(%s)
                    GROUP BY UPPER(TRIM(symbol))
                    """,
                    (syms,),
                )
                for row in cur.fetchall() or []:
                    if row and row[0]:
                        # row: (u, row_count, last_bar_time, last_created_at, ohlc_pct, vol_pct, vwap_pct, dist_exp, dist_contracts)
                        option_day_map[str(row[0]).strip().upper()] = row[1:]

                cur.execute(
                    """
                    SELECT
                        UPPER(TRIM(symbol)) AS u,
                        COUNT(*)::bigint AS row_count,
                        MAX(bar_time) AS last_bar_time,
                        MAX(created_at) AS last_created_at,
                        ROUND(COUNT(CASE WHEN open IS NOT NULL AND high IS NOT NULL
                                          AND low IS NOT NULL AND close IS NOT NULL
                                     THEN 1 END)::numeric / NULLIF(COUNT(*),0) * 100, 1) AS ohlc_complete_pct,
                        ROUND(COUNT(volume)::numeric / NULLIF(COUNT(*),0) * 100, 1) AS volume_pct,
                        ROUND(COUNT(vwap)::numeric / NULLIF(COUNT(*),0) * 100, 1) AS vwap_pct,
                        COUNT(DISTINCT expiry)::bigint AS distinct_expirations,
                        COUNT(DISTINCT CONCAT(expiry,'|',strike::text,'|',option_right))::bigint AS distinct_contracts
                    FROM option_min
                    WHERE source = 'massive'
                      AND UPPER(TRIM(symbol)) = ANY(%s)
                    GROUP BY UPPER(TRIM(symbol))
                    """,
                    (syms,),
                )
                for row in cur.fetchall() or []:
                    if row and row[0]:
                        # row: (u, row_count, last_bar_time, last_created_at, ohlc_pct, vol_pct, vwap_pct, dist_exp, dist_contracts)
                        option_min_map[str(row[0]).strip().upper()] = row[1:]

                cur.execute(
                    """
                    SELECT UPPER(TRIM(underlying_ticker)) AS u, COUNT(*)::bigint, MAX(snapshot_ts), MAX(created_at)
                    FROM option_snapshots_with_underlying_day
                    WHERE source = 'massive'
                      AND UPPER(TRIM(underlying_ticker)) = ANY(%s)
                    GROUP BY UPPER(TRIM(underlying_ticker))
                    """,
                    (syms,),
                )
                for row in cur.fetchall() or []:
                    if row and row[0]:
                        suv_day_map[str(row[0]).strip().upper()] = (int(row[1] or 0), row[2], row[3])

                check_kinds = [f"option_contracts:{s}" for s in syms]
                cur.execute(
                    """
                    SELECT SPLIT_PART(sync_kind, ':', 2) AS sym, updated_at
                    FROM job_ticker_reference_state
                    WHERE sync_kind = ANY(%s)
                    """,
                    (check_kinds,),
                )
                for row in cur.fetchall() or []:
                    if row and row[0]:
                        contracts_check_map[str(row[0]).strip().upper()] = row[1]

                cur.execute(
                    """
                    SELECT UPPER(TRIM(symbol)) AS u, COUNT(*)::bigint, MAX(updated_at)
                    FROM option_expiration_cache
                    WHERE source = 'massive'
                      AND UPPER(TRIM(symbol)) = ANY(%s)
                    GROUP BY UPPER(TRIM(symbol))
                    """,
                    (syms,),
                )
                for row in cur.fetchall() or []:
                    if row and row[0]:
                        oec_map[str(row[0]).strip().upper()] = (int(row[1] or 0), row[2])

                cur.execute(
                    """
                    SELECT UPPER(TRIM(symbol)) AS u, COUNT(*)::bigint, MAX(trade_date), MAX(created_at)
                    FROM option_open_interest_daily
                    WHERE source = 'massive'
                      AND UPPER(TRIM(symbol)) = ANY(%s)
                    GROUP BY UPPER(TRIM(symbol))
                    """,
                    (syms,),
                )
                for row in cur.fetchall() or []:
                    if row and row[0]:
                        oi_daily_map[str(row[0]).strip().upper()] = (int(row[1] or 0), row[2], row[3])

                cur.execute(
                    """
                    SELECT UPPER(TRIM(symbol)) AS u, COUNT(*)::bigint, MAX(trade_date), MAX(created_at)
                    FROM report_option_max_pain_daily
                    WHERE source = 'massive'
                      AND UPPER(TRIM(symbol)) = ANY(%s)
                    GROUP BY UPPER(TRIM(symbol))
                    """,
                    (syms,),
                )
                for row in cur.fetchall() or []:
                    if row and row[0]:
                        max_pain_map[str(row[0]).strip().upper()] = (int(row[1] or 0), row[2], row[3])

                cur.execute(
                    """
                    SELECT UPPER(TRIM(t.ticker)) AS u,
                           t.tickers_id,
                           t.updated_at,
                           t.last_updated_utc,
                           o.overview_updated_at
                    FROM tickers t
                    LEFT JOIN ticker_overview o ON o.tickers_id = t.tickers_id
                    WHERE UPPER(TRIM(t.ticker)) = ANY(%s)
                    """,
                    (syms,),
                )
                for row in cur.fetchall() or []:
                    if row and row[0]:
                        tickers_ref_map[str(row[0]).strip().upper()] = (
                            int(row[1]) if row[1] is not None else None,
                            row[2],
                            row[3],
                            row[4],
                        )

                cur.execute("SELECT COUNT(*)::bigint, MAX(created_at) FROM ticker_types")
                tt_one = cur.fetchone()
                if tt_one and tt_one[0] is not None:
                    ticker_types_global = (int(tt_one[0] or 0), tt_one[1])
        finally:
            conn.close()
    except Exception as exc:
        return {"ok": False, "error": str(exc)}

    out_symbols: List[Dict[str, Any]] = []
    for sym in syms:
        ca = contracts_map.get(sym)
        cc = contracts_check_map.get(sym)
        sn = snapshots_map.get(sym)
        atm = atm_map.get(sym)
        sd = stock_day_map.get(sym)
        sm_st = stock_min_map.get(sym)
        tr_ref = tickers_ref_map.get(sym)
        od = option_day_map.get(sym)
        om = option_min_map.get(sym)
        suv = suv_day_map.get(sym)
        oec = oec_map.get(sym)
        oid = oi_daily_map.get(sym)
        rmp = max_pain_map.get(sym)
        if ca:
            oc_payload = {
                "has_data": True,
                "row_count": ca["row_count"],
                "newest_created_at": ca["newest_created_at"],
                "age_seconds": ca["age_seconds"],
                "last_check_at": _iso(cc),
                "last_check_age_seconds": _age_seconds(cc),
                "ticker_pct": ca["ticker_pct"],
                "identity_pct": ca["identity_pct"],
                "mapping_mismatch_count": ca["mapping_mismatch_count"],
                "exercise_style_pct": ca["exercise_style_pct"],
                "shares_per_contract_pct": ca["shares_per_contract_pct"],
                "optional_data_fill_avg_pct": ca["optional_data_fill_avg_pct"],
                "distinct_expirations": ca["distinct_expirations"],
                "distinct_strikes": ca["distinct_strikes"],
                "contracts_last_at": ca["newest_created_at"],
                "exercise_style_null_row_count": ca["exercise_style_null_row_count"],
                "shares_per_contract_null_row_count": ca["shares_per_contract_null_row_count"],
                "column_gap_count": ca["column_gap_count"],
            }
        else:
            oc_payload = {
                "has_data": False,
                "row_count": None,
                "newest_created_at": None,
                "age_seconds": None,
                "last_check_at": _iso(cc),
                "last_check_age_seconds": _age_seconds(cc),
                "ticker_pct": None,
                "identity_pct": None,
                "mapping_mismatch_count": None,
                "exercise_style_pct": None,
                "shares_per_contract_pct": None,
                "optional_data_fill_avg_pct": None,
                "distinct_expirations": None,
                "distinct_strikes": None,
                "contracts_last_at": None,
                "exercise_style_null_row_count": 0,
                "shares_per_contract_null_row_count": 0,
                "column_gap_count": 0,
            }
        out_symbols.append(
            {
                "symbol": sym,
                "option_contracts": oc_payload,
                "option_snapshots": (
                    {
                        "has_data": True,
                        "row_count": sn["row_count"],
                        "snapshots_last_ts": _iso(sn["newest_ts"]),
                        "age_seconds": sn["age_seconds"],
                        "iv_pct": sn["iv_pct"],
                        "full_greeks_pct": sn["full_greeks_pct"],
                        "open_interest_pct": sn["open_interest_pct"],
                        "optional_data_fill_avg_pct": sn["optional_data_fill_avg_pct"],
                        "stale_snapshot_rows": sn["stale_snapshot_rows"],
                    }
                    if sn
                    else {
                        "has_data": False,
                        "row_count": None,
                        "snapshots_last_ts": None,
                        "age_seconds": None,
                        "iv_pct": None,
                        "full_greeks_pct": None,
                        "open_interest_pct": None,
                        "optional_data_fill_avg_pct": None,
                        "stale_snapshot_rows": None,
                    }
                ),
                "report_option_atm_iv_daily": {
                    "has_data": atm is not None,
                    "atm_iv_last_trade_date": _iso(atm[0]) if atm else None,
                    "atm_iv_last_created_at": _iso(atm[1]) if atm else None,
                },
                "stock_day": {
                    "has_data": sd is not None,
                    "stock_day_last_bar": _iso(sd[1]) if sd else None,
                    "stock_day_last_created_at": _iso(sd[2]) if sd else None,
                    "row_count": int(sd[0]) if sd else None,
                    "ohlc_complete_pct": float(sd[3]) if sd and sd[3] is not None else None,
                    "volume_pct": float(sd[4]) if sd and sd[4] is not None else None,
                    "vwap_pct": float(sd[5]) if sd and sd[5] is not None else None,
                    "optional_avg_pct": (
                        round((float(sd[4]) + float(sd[5])) / 2, 1)
                        if sd and sd[4] is not None and sd[5] is not None
                        else None
                    ),
                    "distinct_bar_dates": int(sd[6]) if sd and sd[6] is not None else None,
                },
                "stock_min": {
                    "has_data": sm_st is not None,
                    "row_count": int(sm_st[0]) if sm_st else None,
                    "last_bar_time": _iso(sm_st[1]) if sm_st else None,
                    "last_created_at": _iso(sm_st[2]) if sm_st else None,
                    "ohlc_complete_pct": float(sm_st[3]) if sm_st and sm_st[3] is not None else None,
                    "volume_pct": float(sm_st[4]) if sm_st and sm_st[4] is not None else None,
                    "vwap_pct": float(sm_st[5]) if sm_st and sm_st[5] is not None else None,
                    "optional_avg_pct": (
                        round((float(sm_st[4]) + float(sm_st[5])) / 2, 1)
                        if sm_st and sm_st[4] is not None and sm_st[5] is not None
                        else None
                    ),
                    "distinct_periods": int(sm_st[6]) if sm_st and sm_st[6] is not None else None,
                },
                "tickers": {
                    "has_data": tr_ref is not None,
                    "tickers_id": int(tr_ref[0]) if tr_ref and tr_ref[0] is not None else None,
                    "tickers_updated_at": _iso(tr_ref[1]) if tr_ref else None,
                    "last_updated_utc": _iso(tr_ref[2]) if tr_ref else None,
                },
                "ticker_overview": {
                    "has_data": tr_ref is not None and tr_ref[3] is not None,
                    "overview_updated_at": _iso(tr_ref[3]) if tr_ref else None,
                },
                "ticker_types": {
                    "has_data": ticker_types_global is not None and int(ticker_types_global[0] or 0) > 0,
                    "dictionary_row_count": int(ticker_types_global[0]) if ticker_types_global else None,
                    "dictionary_last_created_at": _iso(ticker_types_global[1]) if ticker_types_global else None,
                },
                "option_day": {
                    "has_data": od is not None,
                    "row_count": int(od[0]) if od else None,
                    "last_bar_time": _iso(od[1]) if od else None,
                    "last_created_at": _iso(od[2]) if od else None,
                    "ohlc_complete_pct": float(od[3]) if od and od[3] is not None else None,
                    "volume_pct": float(od[4]) if od and od[4] is not None else None,
                    "vwap_pct": float(od[5]) if od and od[5] is not None else None,
                    "optional_avg_pct": (
                        round((float(od[4]) + float(od[5])) / 2, 1)
                        if od and od[4] is not None and od[5] is not None
                        else None
                    ),
                    "distinct_expirations": int(od[6]) if od and od[6] is not None else None,
                    "distinct_contracts": int(od[7]) if od and od[7] is not None else None,
                },
                "option_min": {
                    "has_data": om is not None,
                    "row_count": int(om[0]) if om else None,
                    "last_bar_time": _iso(om[1]) if om else None,
                    "last_created_at": _iso(om[2]) if om else None,
                    "ohlc_complete_pct": float(om[3]) if om and om[3] is not None else None,
                    "volume_pct": float(om[4]) if om and om[4] is not None else None,
                    "vwap_pct": float(om[5]) if om and om[5] is not None else None,
                    "optional_avg_pct": (
                        round((float(om[4]) + float(om[5])) / 2, 1)
                        if om and om[4] is not None and om[5] is not None
                        else None
                    ),
                    "distinct_expirations": int(om[6]) if om and om[6] is not None else None,
                    "distinct_contracts": int(om[7]) if om and om[7] is not None else None,
                },
                "option_snapshots_with_underlying_day": {
                    "has_data": suv is not None,
                    "row_count": int(suv[0]) if suv else None,
                    "last_snapshot_ts": _iso(suv[1]) if suv else None,
                    "last_created_at": _iso(suv[2]) if suv else None,
                },
                "option_expiration_cache": {
                    "has_data": oec is not None,
                    "row_count": int(oec[0]) if oec else None,
                    "last_updated_at": _iso(oec[1]) if oec else None,
                },
                "option_open_interest_daily": {
                    "has_data": oid is not None,
                    "row_count": int(oid[0]) if oid else None,
                    "last_trade_date": _iso(oid[1]) if oid else None,
                    "last_created_at": _iso(oid[2]) if oid else None,
                },
                "report_option_max_pain_daily": {
                    "has_data": rmp is not None,
                    "row_count": int(rmp[0]) if rmp else None,
                    "last_trade_date": _iso(rmp[1]) if rmp else None,
                    "last_created_at": _iso(rmp[2]) if rmp else None,
                },
            }
        )

    return {
        "ok": True,
        "source_scope": "massive",
        "generated_at": generated_at,
        "universe": "watchlist_optionable_stk",
        "symbols_count": len(out_symbols),
        "symbols": out_symbols,
    }


@router.get("/research/massive/option-contracts-reference-gap")
def get_option_contracts_reference_gap(
    request: Request,
    symbol: str = Query(..., description="Underlying symbol"),
    max_expiries: Optional[int] = Query(
        None,
        ge=1,
        le=120,
        description="Max distinct expiries to scan (default 60; cap 120).",
    ),
    max_pages_per_expiry: Optional[int] = Query(
        None,
        ge=1,
        le=30,
        description="Max Massive API pages per expiry (default 20; cap 30).",
    ),
) -> Dict[str, Any]:
    """Compare option_contracts row counts per expiry to Massive GET /v3/reference/options/contracts (paginated)."""
    import psycopg2

    from bifrost_core.persistence.postgres.connection import _get_conn_params
    from bifrost_worker.data.massive.vendor.client import MassiveClient
    from bifrost_worker.data.massive.vendor.config import get_massive_settings
    from bifrost_worker.data.massive.vendor.contracts_reference_gap import compute_option_contracts_reference_gap

    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}

    reader = getattr(request.app.state, "reader", None)
    cfg = reader._config if reader else {}
    ms = get_massive_settings(cfg)
    if not ms["api_key"]:
        return {"ok": False, "error": "Massive API key not configured"}

    sym = (symbol or "").strip().upper()
    if not sym:
        return {"ok": False, "error": "symbol is required"}

    client = MassiveClient(ms["api_key"], ms["rest_base"])
    try:
        params = _get_conn_params(db)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                me = max_expiries if max_expiries is not None else 60
                mp = max_pages_per_expiry if max_pages_per_expiry is not None else 20
                result = compute_option_contracts_reference_gap(
                    cur, client, sym, max_expiries=me, max_pages_per_expiry=mp
                )
                cur.execute(
                    """
                    INSERT INTO job_ticker_reference_state (sync_kind, last_cursor, status, updated_at)
                    VALUES (%s, NULL, 'done', now())
                    ON CONFLICT (sync_kind) DO UPDATE SET
                      status = 'done',
                      updated_at = now()
                    """,
                    (f"option_contracts:{sym}",),
                )
            conn.commit()
            return result
        finally:
            conn.close()
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


@router.post("/research/massive/option-contracts-reference-gap/batch")
def post_option_contracts_reference_gap_batch(
    request: Request,
    payload: Dict[str, Any] = Body(...),
) -> Dict[str, Any]:
    """Batch compare (max 10 symbols). Reuses one DB connection; small delay between symbols for API pacing."""
    import time

    import psycopg2

    from bifrost_core.persistence.postgres.connection import _get_conn_params
    from bifrost_worker.data.massive.vendor.client import MassiveClient
    from bifrost_worker.data.massive.vendor.config import get_massive_settings
    from bifrost_worker.data.massive.vendor.contracts_reference_gap import compute_option_contracts_reference_gap

    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}

    reader = getattr(request.app.state, "reader", None)
    cfg = reader._config if reader else {}
    ms = get_massive_settings(cfg)
    if not ms["api_key"]:
        return {"ok": False, "error": "Massive API key not configured"}

    raw = payload.get("symbols") if isinstance(payload, dict) else None
    if not isinstance(raw, list):
        return {"ok": False, "error": "payload.symbols must be a non-empty array"}
    syms: List[str] = []
    seen: set = set()
    for x in raw:
        u = (str(x) or "").strip().upper()
        if u and u not in seen:
            seen.add(u)
            syms.append(u)
    if not syms:
        return {"ok": False, "error": "payload.symbols must be a non-empty array"}
    if len(syms) > 10:
        return {"ok": False, "error": "At most 10 symbols per batch"}

    me = payload.get("max_expiries") if isinstance(payload, dict) else None
    mp = payload.get("max_pages_per_expiry") if isinstance(payload, dict) else None
    try:
        max_e = int(me) if me is not None else 60
    except (TypeError, ValueError):
        max_e = 60
    try:
        max_p = int(mp) if mp is not None else 20
    except (TypeError, ValueError):
        max_p = 20

    client = MassiveClient(ms["api_key"], ms["rest_base"])
    results: Dict[str, Any] = {}
    try:
        params = _get_conn_params(db)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                for i, s in enumerate(syms):
                    if i > 0:
                        time.sleep(0.05)
                    try:
                        results[s] = compute_option_contracts_reference_gap(
                            cur, client, s, max_expiries=max_e, max_pages_per_expiry=max_p
                        )
                        cur.execute(
                            """
                            INSERT INTO job_ticker_reference_state (sync_kind, last_cursor, status, updated_at)
                            VALUES (%s, NULL, 'done', now())
                            ON CONFLICT (sync_kind) DO UPDATE SET
                              status = 'done',
                              updated_at = now()
                            """,
                            (f"option_contracts:{s}",),
                        )
                    except Exception as exc:  # noqa: BLE001
                        results[s] = {"ok": False, "symbol": s, "error": str(exc)}
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:
        return {"ok": False, "error": str(exc)}

    return {"ok": True, "results": results}


@router.get("/research/massive/option-snapshots-contracts-gap")
def get_option_snapshots_contracts_gap(
    request: Request,
    symbol: str = Query(..., description="Underlying symbol"),
) -> Dict[str, Any]:
    """Compare option_snapshots coverage to Massive GET /v3/snapshot/options/{underlying} (per expiry, vs option_contracts)."""
    import psycopg2

    from bifrost_core.persistence.postgres.connection import _get_conn_params
    from bifrost_worker.data.massive.vendor.client import MassiveClient
    from bifrost_worker.data.massive.vendor.config import get_massive_settings
    from bifrost_worker.data.massive.vendor.snapshots_contracts_gap import compute_option_snapshots_contracts_gap

    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}

    reader = getattr(request.app.state, "reader", None)
    cfg = reader._config if reader else {}
    ms = get_massive_settings(cfg)
    if not ms["api_key"]:
        return {"ok": False, "error": "Massive API key not configured"}

    sym = (symbol or "").strip().upper()
    if not sym:
        return {"ok": False, "error": "symbol is required"}

    client = MassiveClient(ms["api_key"], ms["rest_base"])
    try:
        params = _get_conn_params(db)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                return compute_option_snapshots_contracts_gap(cur, client, sym)
        finally:
            conn.close()
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


@router.post("/research/massive/option-snapshots-contracts-gap/batch")
def post_option_snapshots_contracts_gap_batch(
    request: Request,
    payload: Dict[str, Any] = Body(...),
) -> Dict[str, Any]:
    """Batch snapshot vs contracts gap (max 10 symbols)."""
    import time

    import psycopg2

    from bifrost_core.persistence.postgres.connection import _get_conn_params
    from bifrost_worker.data.massive.vendor.client import MassiveClient
    from bifrost_worker.data.massive.vendor.config import get_massive_settings
    from bifrost_worker.data.massive.vendor.snapshots_contracts_gap import compute_option_snapshots_contracts_gap

    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}

    reader = getattr(request.app.state, "reader", None)
    cfg = reader._config if reader else {}
    ms = get_massive_settings(cfg)
    if not ms["api_key"]:
        return {"ok": False, "error": "Massive API key not configured"}

    raw = payload.get("symbols") if isinstance(payload, dict) else None
    if not isinstance(raw, list):
        return {"ok": False, "error": "payload.symbols must be a non-empty array"}
    syms: List[str] = []
    seen: set = set()
    for x in raw:
        u = (str(x) or "").strip().upper()
        if u and u not in seen:
            seen.add(u)
            syms.append(u)
    if not syms:
        return {"ok": False, "error": "payload.symbols must be a non-empty array"}
    if len(syms) > 10:
        return {"ok": False, "error": "At most 10 symbols per batch"}

    client = MassiveClient(ms["api_key"], ms["rest_base"])
    results: Dict[str, Any] = {}
    try:
        params = _get_conn_params(db)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                for i, s in enumerate(syms):
                    if i > 0:
                        time.sleep(0.05)
                    try:
                        results[s] = compute_option_snapshots_contracts_gap(cur, client, s)
                    except Exception as exc:  # noqa: BLE001
                        results[s] = {"ok": False, "symbol": s, "error": str(exc)}
        finally:
            conn.close()
    except Exception as exc:
        return {"ok": False, "error": str(exc)}

    return {"ok": True, "results": results}


@router.get("/research/massive/option-bars-contracts-gap")
def get_option_bars_contracts_gap(
    request: Request,
    symbol: str = Query(..., description="Underlying symbol"),
    table: str = Query("option_day", description="option_day or option_min"),
    period: Optional[str] = Query(None, description="option_min period filter (e.g. '1 min')"),
) -> Dict[str, Any]:
    """Compare option_day / option_min bar coverage to option_contracts (purely local, no external API)."""
    import psycopg2

    from bifrost_core.persistence.postgres.connection import _get_conn_params
    from bifrost_worker.data.massive.vendor.bars_contracts_gap import compute_option_bars_contracts_gap

    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}

    sym = (symbol or "").strip().upper()
    if not sym:
        return {"ok": False, "error": "symbol is required"}
    if table not in ("option_day", "option_min"):
        return {"ok": False, "error": "table must be 'option_day' or 'option_min'"}

    try:
        params = _get_conn_params(db)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                return compute_option_bars_contracts_gap(cur, sym, table=table, period=period)
        finally:
            conn.close()
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


@router.post("/research/massive/option-bars-contracts-gap/batch")
def post_option_bars_contracts_gap_batch(
    request: Request,
    payload: Dict[str, Any] = Body(...),
) -> Dict[str, Any]:
    """Batch option bars vs contracts gap (max 10 symbols, no external API)."""
    import psycopg2

    from bifrost_core.persistence.postgres.connection import _get_conn_params
    from bifrost_worker.data.massive.vendor.bars_contracts_gap import compute_option_bars_contracts_gap

    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}

    raw = payload.get("symbols") if isinstance(payload, dict) else None
    if not isinstance(raw, list):
        return {"ok": False, "error": "payload.symbols must be a non-empty array"}
    table = str(payload.get("table") or "option_day")
    if table not in ("option_day", "option_min"):
        return {"ok": False, "error": "payload.table must be 'option_day' or 'option_min'"}
    period = payload.get("period") if isinstance(payload, dict) else None
    period = str(period) if period else None

    syms: List[str] = []
    seen: set = set()
    for x in raw:
        u = (str(x) or "").strip().upper()
        if u and u not in seen:
            seen.add(u)
            syms.append(u)
    if not syms:
        return {"ok": False, "error": "payload.symbols must be a non-empty array"}
    if len(syms) > 10:
        return {"ok": False, "error": "At most 10 symbols per batch"}

    results: Dict[str, Any] = {}
    try:
        params = _get_conn_params(db)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                for s in syms:
                    try:
                        results[s] = compute_option_bars_contracts_gap(cur, s, table=table, period=period)
                    except Exception as exc:  # noqa: BLE001
                        results[s] = {"ok": False, "symbol": s, "error": str(exc)}
        finally:
            conn.close()
    except Exception as exc:
        return {"ok": False, "error": str(exc)}

    return {"ok": True, "results": results}


@router.get("/research/massive/bar-quality-detail")
def get_bar_quality_detail(
    request: Request,
    symbol: str = Query(..., description="Underlying symbol"),
    table: str = Query("option_day", description="option_day or option_min"),
    period: Optional[str] = Query(None, description="option_min period filter"),
    days: int = Query(30, description="Days of daily history to return"),
) -> Dict[str, Any]:
    """Per-day / per-expiry / per-period bar quality breakdown for option_day or option_min."""
    import psycopg2
    from datetime import date, datetime, timezone

    from bifrost_core.persistence.postgres.connection import _get_conn_params

    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}

    sym = (symbol or "").strip().upper()
    if not sym:
        return {"ok": False, "error": "symbol is required"}
    if table not in ("option_day", "option_min"):
        return {"ok": False, "error": "table must be 'option_day' or 'option_min'"}
    if days < 1 or days > 365:
        days = 30

    def _iso(dt: Any) -> Optional[str]:
        if dt is None:
            return None
        if isinstance(dt, datetime):
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc).isoformat()
            return dt.isoformat()
        if isinstance(dt, date):
            return dt.isoformat()
        return str(dt)

    try:
        params = _get_conn_params(db)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                # Query A — Daily breakdown (last N days)
                period_filter = "AND period = %(period)s" if (table == "option_min" and period) else ""
                cur.execute(
                    f"""
                    WITH daily_latest AS (
                        SELECT DISTINCT ON (
                            DATE(timezone('America/New_York', bar_time)),
                            expiry, strike, option_right
                        )
                            DATE(timezone('America/New_York', bar_time)) AS bar_day,
                            open, high, low, close, volume, vwap
                        FROM {table}
                        WHERE source = 'massive'
                          AND UPPER(TRIM(symbol)) = %(symbol)s
                          AND bar_time >= NOW() - (%(days)s || ' days')::interval
                          {period_filter}
                        ORDER BY
                            DATE(timezone('America/New_York', bar_time)),
                            expiry, strike, option_right,
                            bar_time DESC
                    )
                    SELECT
                        bar_day,
                        COUNT(*)::int AS contract_count,
                        ROUND(COUNT(CASE WHEN open IS NOT NULL AND high IS NOT NULL
                                          AND low IS NOT NULL AND close IS NOT NULL
                                     THEN 1 END)::numeric / NULLIF(COUNT(*),0) * 100, 1) AS ohlc_pct,
                        ROUND(COUNT(volume)::numeric / NULLIF(COUNT(*),0) * 100, 1) AS volume_pct,
                        ROUND(COUNT(vwap)::numeric / NULLIF(COUNT(*),0) * 100, 1) AS vwap_pct
                    FROM daily_latest
                    GROUP BY bar_day
                    ORDER BY bar_day DESC
                    """,
                    {"symbol": sym, "days": days, "period": period},
                )
                daily_rows = []
                for row in cur.fetchall() or []:
                    daily_rows.append({
                        "bar_day": str(row[0]) if row[0] else None,
                        "contract_count": int(row[1] or 0),
                        "ohlc_pct": float(row[2]) if row[2] is not None else None,
                        "volume_pct": float(row[3]) if row[3] is not None else None,
                        "vwap_pct": float(row[4]) if row[4] is not None else None,
                    })

                # Latest date for expiry query
                latest_date = daily_rows[0]["bar_day"] if daily_rows else None

                # Query B — By expiry (using most recent bar_day worth of data)
                if latest_date:
                    cur.execute(
                        f"""
                        WITH expiry_latest AS (
                            SELECT DISTINCT ON (expiry, strike, option_right)
                                expiry,
                                open, high, low, close, volume, vwap
                            FROM {table}
                            WHERE source = 'massive'
                              AND UPPER(TRIM(symbol)) = %(symbol)s
                              AND DATE(timezone('America/New_York', bar_time)) = %(latest_date)s
                              {period_filter}
                            ORDER BY expiry, strike, option_right, bar_time DESC
                        )
                        SELECT
                            expiry,
                            COUNT(*)::int AS contract_count,
                            ROUND(COUNT(CASE WHEN open IS NOT NULL AND high IS NOT NULL
                                              AND low IS NOT NULL AND close IS NOT NULL
                                         THEN 1 END)::numeric / NULLIF(COUNT(*),0) * 100, 1) AS ohlc_pct,
                            ROUND(COUNT(volume)::numeric / NULLIF(COUNT(*),0) * 100, 1) AS volume_pct,
                            ROUND(COUNT(vwap)::numeric / NULLIF(COUNT(*),0) * 100, 1) AS vwap_pct
                        FROM expiry_latest
                        GROUP BY expiry
                        ORDER BY expiry ASC
                        """,
                        {"symbol": sym, "latest_date": latest_date, "period": period},
                    )
                    today_str = datetime.now(timezone.utc).strftime("%Y%m%d")
                    expiry_rows = []
                    for row in cur.fetchall() or []:
                        exp = str(row[0]).strip() if row[0] else ""
                        try:
                            # DTE: expiry is YYYYMMDD, today is YYYYMMDD
                            exp_date = date(int(exp[:4]), int(exp[4:6]), int(exp[6:8]))
                            today_date = date(int(today_str[:4]), int(today_str[4:6]), int(today_str[6:8]))
                            dte = (exp_date - today_date).days
                        except Exception:
                            dte = None
                        expiry_rows.append({
                            "expiry": exp,
                            "dte": dte,
                            "contract_count": int(row[1] or 0),
                            "ohlc_pct": float(row[2]) if row[2] is not None else None,
                            "volume_pct": float(row[3]) if row[3] is not None else None,
                            "vwap_pct": float(row[4]) if row[4] is not None else None,
                        })
                else:
                    expiry_rows = []

                # Query C — By period (option_min only)
                period_rows = []
                if table == "option_min":
                    cur.execute(
                        """
                        SELECT
                            period,
                            COUNT(*)::int AS row_count,
                            MAX(bar_time) AS last_bar_time,
                            ROUND(COUNT(CASE WHEN open IS NOT NULL AND high IS NOT NULL
                                              AND low IS NOT NULL AND close IS NOT NULL
                                         THEN 1 END)::numeric / NULLIF(COUNT(*),0) * 100, 1) AS ohlc_pct,
                            ROUND(COUNT(volume)::numeric / NULLIF(COUNT(*),0) * 100, 1) AS volume_pct,
                            ROUND(COUNT(vwap)::numeric / NULLIF(COUNT(*),0) * 100, 1) AS vwap_pct
                        FROM option_min
                        WHERE source = 'massive'
                          AND UPPER(TRIM(symbol)) = %s
                        GROUP BY period
                        ORDER BY period ASC
                        """,
                        (sym,),
                    )
                    for row in cur.fetchall() or []:
                        period_rows.append({
                            "period": str(row[0]) if row[0] else "",
                            "row_count": int(row[1] or 0),
                            "last_bar_time": _iso(row[2]) if row[2] else None,
                            "ohlc_pct": float(row[3]) if row[3] is not None else None,
                            "volume_pct": float(row[4]) if row[4] is not None else None,
                            "vwap_pct": float(row[5]) if row[5] is not None else None,
                        })

        finally:
            conn.close()
    except Exception as exc:
        return {"ok": False, "symbol": sym, "table": table, "error": str(exc),
                "latest_date": None, "daily": [], "expiries": [], "periods": []}

    return {
        "ok": True,
        "symbol": sym,
        "table": table,
        "latest_date": latest_date,
        "daily": daily_rows,
        "expiries": expiry_rows,
        "periods": period_rows,
    }


@router.post("/research/massive/option-min-fill-eligibility")
def post_option_min_fill_eligibility(
    request: Request, body: Dict[str, Any] = Body(...)
) -> Dict[str, Any]:
    """Whether option_min row/column fill is needed per symbol (local PG only)."""
    import psycopg2

    from bifrost_worker.data.massive.option_bars_period import period_label_to_db_period
    from bifrost_worker.data.massive.option_min_pool_fill import option_min_has_incomplete_rows
    from bifrost_core.persistence.postgres.connection import _get_conn_params
    from bifrost_worker.data.massive.vendor.bars_contracts_gap import compute_option_bars_contracts_gap

    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}

    raw_syms = body.get("symbols")
    if not isinstance(raw_syms, list) or not raw_syms:
        return {"ok": False, "error": "body.symbols must be a non-empty array"}
    period_label = (body.get("period") or "").strip()
    if not period_label or period_label == "1 D":
        return {"ok": False, "error": "body.period is required (intraday, e.g. 5 mins)"}
    try:
        period_db = period_label_to_db_period(period_label)
    except ValueError as e:
        return {"ok": False, "error": str(e)}

    lookback_days = int(body.get("lookback_days") or 7)
    lookback_days = max(1, min(lookback_days, 366))

    syms = sorted(
        {str(s).strip().upper() for s in raw_syms if s and str(s).strip()}
    )
    if not syms:
        return {"ok": False, "error": "no valid symbols"}
    if len(syms) > 20:
        return {"ok": False, "error": "at most 20 symbols per request"}

    try:
        params = _get_conn_params(db)
        conn = psycopg2.connect(**params)
        try:
            results: Dict[str, Any] = {}
            with conn.cursor() as cur:
                for s in syms:
                    gap = compute_option_bars_contracts_gap(
                        cur, s, table="option_min", period=period_db
                    )
                    gap_n = gap.get("gap")
                    needs_row = (
                        gap.get("ok")
                        and gap.get("has_rows")
                        and isinstance(gap_n, (int, float))
                        and gap_n > 0
                    )
                    needs_col = option_min_has_incomplete_rows(
                        cur, s, period_db, lookback_days
                    )
                    results[s] = {
                        "needs_row_fill": bool(needs_row),
                        "needs_column_fill": bool(needs_col),
                        "gap": gap_n,
                        "coverage_pct": gap.get("coverage_pct"),
                    }
        finally:
            conn.close()
    except Exception as exc:
        return {"ok": False, "error": str(exc)}

    return {
        "ok": True,
        "period": period_label,
        "lookback_days": lookback_days,
        "results": results,
    }


@router.post("/research/massive/option-day-fill-eligibility")
def post_option_day_fill_eligibility(
    request: Request, body: Dict[str, Any] = Body(...)
) -> Dict[str, Any]:
    """Whether option_day row/column fill is needed per symbol (local PG only)."""
    import psycopg2

    from bifrost_worker.data.massive.option_day_pool_fill import option_day_has_incomplete_rows
    from bifrost_core.persistence.postgres.connection import _get_conn_params
    from bifrost_worker.data.massive.vendor.bars_contracts_gap import compute_option_bars_contracts_gap

    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}

    raw_syms = body.get("symbols")
    if not isinstance(raw_syms, list) or not raw_syms:
        return {"ok": False, "error": "body.symbols must be a non-empty array"}

    column_lookback_days = int(body.get("column_lookback_days") or 30)
    column_lookback_days = max(1, min(column_lookback_days, 366))

    syms = sorted(
        {str(s).strip().upper() for s in raw_syms if s and str(s).strip()}
    )
    if not syms:
        return {"ok": False, "error": "no valid symbols"}
    if len(syms) > 20:
        return {"ok": False, "error": "at most 20 symbols per request"}

    try:
        params = _get_conn_params(db)
        conn = psycopg2.connect(**params)
        try:
            results: Dict[str, Any] = {}
            with conn.cursor() as cur:
                for s in syms:
                    gap = compute_option_bars_contracts_gap(
                        cur, s, table="option_day", period=None
                    )
                    gap_n = gap.get("gap")
                    needs_row = (
                        gap.get("ok")
                        and gap.get("has_rows")
                        and isinstance(gap_n, (int, float))
                        and gap_n > 0
                    )
                    needs_col = option_day_has_incomplete_rows(
                        cur, s, column_lookback_days
                    )
                    results[s] = {
                        "needs_row_fill": bool(needs_row),
                        "needs_column_fill": bool(needs_col),
                        "gap": gap_n,
                        "coverage_pct": gap.get("coverage_pct"),
                    }
        finally:
            conn.close()
    except Exception as exc:
        return {"ok": False, "error": str(exc)}

    return {
        "ok": True,
        "column_lookback_days": column_lookback_days,
        "results": results,
    }


@router.get("/research/massive/option-contracts-reference-column-parity")
def get_option_contracts_reference_column_parity(
    request: Request,
    symbol: str = Query(..., description="Underlying symbol"),
) -> Dict[str, Any]:
    """L2: compare reference API contract fields to PostgreSQL option_contracts rows (ref-owned columns only)."""
    import psycopg2

    from bifrost_core.persistence.postgres.connection import _get_conn_params
    from bifrost_worker.data.massive.vendor.client import MassiveClient
    from bifrost_worker.data.massive.vendor.config import get_massive_settings
    from bifrost_worker.data.massive.vendor.contracts_reference_column_parity import (
        compute_option_contracts_reference_column_parity,
    )

    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}

    reader = getattr(request.app.state, "reader", None)
    cfg = reader._config if reader else {}
    ms = get_massive_settings(cfg)
    if not ms["api_key"]:
        return {"ok": False, "error": "Massive API key not configured"}

    sym = (symbol or "").strip().upper()
    if not sym:
        return {"ok": False, "error": "symbol is required"}

    client = MassiveClient(ms["api_key"], ms["rest_base"])
    try:
        params = _get_conn_params(db)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                return compute_option_contracts_reference_column_parity(cur, client, sym)
        finally:
            conn.close()
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


@router.post("/research/massive/option-contracts-reference-column-parity/batch")
def post_option_contracts_reference_column_parity_batch(
    request: Request,
    payload: Dict[str, Any] = Body(...),
) -> Dict[str, Any]:
    """Batch L2 column parity (max 10 symbols)."""
    import time

    import psycopg2

    from bifrost_core.persistence.postgres.connection import _get_conn_params
    from bifrost_worker.data.massive.vendor.client import MassiveClient
    from bifrost_worker.data.massive.vendor.config import get_massive_settings
    from bifrost_worker.data.massive.vendor.contracts_reference_column_parity import (
        compute_option_contracts_reference_column_parity,
    )

    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}

    reader = getattr(request.app.state, "reader", None)
    cfg = reader._config if reader else {}
    ms = get_massive_settings(cfg)
    if not ms["api_key"]:
        return {"ok": False, "error": "Massive API key not configured"}

    raw = payload.get("symbols") if isinstance(payload, dict) else None
    if not isinstance(raw, list):
        return {"ok": False, "error": "payload.symbols must be a non-empty array"}
    syms: List[str] = []
    seen: set = set()
    for x in raw:
        u = (str(x) or "").strip().upper()
        if u and u not in seen:
            seen.add(u)
            syms.append(u)
    if not syms:
        return {"ok": False, "error": "payload.symbols must be a non-empty array"}
    if len(syms) > 10:
        return {"ok": False, "error": "At most 10 symbols per batch"}

    client = MassiveClient(ms["api_key"], ms["rest_base"])
    results: Dict[str, Any] = {}
    try:
        params = _get_conn_params(db)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                for i, s in enumerate(syms):
                    if i > 0:
                        time.sleep(0.05)
                    try:
                        results[s] = compute_option_contracts_reference_column_parity(cur, client, s)
                    except Exception as exc:  # noqa: BLE001
                        results[s] = {"ok": False, "symbol": s, "error": str(exc)}
        finally:
            conn.close()
    except Exception as exc:
        return {"ok": False, "error": str(exc)}

    return {"ok": True, "results": results}


# ── Market Ops (REST-only, read-only) ────────────────────────────────────────

@router.get("/research/massive/market-ops/conditions")
def get_massive_market_conditions(
    request: Request,
    asset_class: Optional[str] = Query(None, description="options | stocks | crypto | fx"),
    data_type: Optional[str] = Query(None, description="trade | bbo | nbbo"),
    limit: int = Query(1000, ge=1, le=1000),
) -> Dict[str, Any]:
    """Condition codes from Massive REST (read-only, no DB write)."""
    from bifrost_worker.data.massive.vendor.config import get_massive_settings
    from bifrost_worker.data.massive.vendor.client import MassiveClient

    reader = getattr(request.app.state, "reader", None)
    cfg = reader._config if reader else {}
    ms = get_massive_settings(cfg)
    if not ms["api_key"]:
        return {"ok": False, "error": "Massive API key not configured", "results": []}
    client = MassiveClient(ms["api_key"], ms["rest_base"])
    data = client.fetch_market_conditions(asset_class=asset_class, data_type=data_type, limit=limit)
    if data.get("error"):
        return {"ok": False, "error": data["error"], "results": []}
    return {"ok": True, "results": data.get("results") or [], "count": len(data.get("results") or [])}


@router.get("/research/massive/market-ops/exchanges")
def get_massive_market_exchanges(
    request: Request,
    asset_class: Optional[str] = Query(None, description="stocks | options | crypto | fx"),
    locale: Optional[str] = Query(None, description="us | global"),
) -> Dict[str, Any]:
    """Exchange list from Massive REST (read-only, no DB write)."""
    from bifrost_worker.data.massive.vendor.config import get_massive_settings
    from bifrost_worker.data.massive.vendor.client import MassiveClient

    reader = getattr(request.app.state, "reader", None)
    cfg = reader._config if reader else {}
    ms = get_massive_settings(cfg)
    if not ms["api_key"]:
        return {"ok": False, "error": "Massive API key not configured", "results": []}
    client = MassiveClient(ms["api_key"], ms["rest_base"])
    data = client.fetch_market_exchanges(asset_class=asset_class, locale=locale)
    if data.get("error"):
        return {"ok": False, "error": data["error"], "results": []}
    return {"ok": True, "results": data.get("results") or [], "count": len(data.get("results") or [])}


@router.get("/research/massive/market-ops/holidays")
def get_massive_market_holidays(request: Request) -> Dict[str, Any]:
    """Upcoming market holidays from Massive REST + local reference_us_holidays comparison."""
    from bifrost_worker.data.massive.vendor.config import get_massive_settings
    from bifrost_worker.data.massive.vendor.client import MassiveClient

    reader = getattr(request.app.state, "reader", None)
    cfg = reader._config if reader else {}
    ms = get_massive_settings(cfg)
    if not ms["api_key"]:
        return {"ok": False, "error": "Massive API key not configured", "massive_holidays": [], "local_holidays": []}
    client = MassiveClient(ms["api_key"], ms["rest_base"])
    data = client.fetch_market_holidays()
    massive_holidays = data.get("results") or []
    if not isinstance(massive_holidays, list):
        massive_holidays = []
    if data.get("error"):
        return {"ok": False, "error": data["error"], "massive_holidays": [], "local_holidays": []}

    local_holidays: List[Dict[str, Any]] = []
    db = _db_config(request)
    if db:
        try:
            from bifrost_core.monitor.reader.market import get_market_holidays
            local_holidays = get_market_holidays(db, exchange="NYSE")
        except Exception:
            pass

    local_dates = {h.get("holiday_date") for h in local_holidays if h.get("holiday_date")}
    massive_dates = set()
    for h in massive_holidays:
        d = h.get("date")
        if d:
            massive_dates.add(d)

    return {
        "ok": True,
        "massive_holidays": massive_holidays,
        "massive_count": len(massive_holidays),
        "local_holidays": local_holidays,
        "local_count": len(local_holidays),
        "comparison": {
            "in_massive_only": sorted(massive_dates - local_dates),
            "in_local_only": sorted(local_dates - massive_dates),
            "in_both": sorted(massive_dates & local_dates),
        },
    }


@router.get("/research/massive/market-ops/status")
def get_massive_market_status(request: Request) -> Dict[str, Any]:
    """Current market trading status from Massive REST (read-only)."""
    from bifrost_worker.data.massive.vendor.config import get_massive_settings
    from bifrost_worker.data.massive.vendor.client import MassiveClient

    reader = getattr(request.app.state, "reader", None)
    cfg = reader._config if reader else {}
    ms = get_massive_settings(cfg)
    if not ms["api_key"]:
        return {"ok": False, "error": "Massive API key not configured"}
    client = MassiveClient(ms["api_key"], ms["rest_base"])
    data = client.fetch_market_status()
    if data.get("error"):
        return {"ok": False, "error": data["error"]}
    return {"ok": True, "status": data}


# ── Tickers reference (Stocks REST, read-only) ───────────────────────────────

@router.get("/research/massive/tickers")
def get_massive_reference_tickers(
    request: Request,
    ticker: Optional[str] = Query(None),
    instrument_type: Optional[str] = Query(None, alias="type"),
    market: Optional[str] = Query(None),
    exchange: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    active: Optional[bool] = Query(None),
    date: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    sort: str = Query("ticker"),
    order: str = Query("asc"),
    cursor: Optional[str] = Query(None),
) -> Dict[str, Any]:
    """GET /v3/reference/tickers — paginated ticker universe (proxy)."""
    from bifrost_worker.data.massive.vendor.config import get_massive_settings
    from bifrost_worker.data.massive.vendor.client import MassiveClient

    reader = getattr(request.app.state, "reader", None)
    cfg = reader._config if reader else {}
    ms = get_massive_settings(cfg)
    if not ms["api_key"]:
        return {"ok": False, "error": "Massive API key not configured"}
    client = MassiveClient(ms["api_key"], ms["rest_base"])
    data = client.fetch_reference_tickers(
        ticker=ticker,
        instrument_type=instrument_type,
        market=market,
        exchange=exchange,
        search=search,
        active=active,
        date=date,
        limit=limit,
        sort=sort,
        order=order,
        cursor=cursor,
    )
    if data.get("error"):
        return {"ok": False, "error": _as_error_str(data["error"])}
    return {"ok": True, "data": data}


@router.get("/research/massive/tickers/types")
def get_massive_ticker_types(
    request: Request,
    asset_class: Optional[str] = Query(None),
    locale: Optional[str] = Query(None),
) -> Dict[str, Any]:
    """GET /v3/reference/tickers/types — registered before /tickers/{ticker} so *types* is not captured as a symbol."""
    from bifrost_worker.data.massive.vendor.config import get_massive_settings
    from bifrost_worker.data.massive.vendor.client import MassiveClient

    reader = getattr(request.app.state, "reader", None)
    cfg = reader._config if reader else {}
    ms = get_massive_settings(cfg)
    if not ms["api_key"]:
        return {"ok": False, "error": "Massive API key not configured"}
    client = MassiveClient(ms["api_key"], ms["rest_base"])
    data = client.fetch_ticker_types(asset_class=asset_class, locale=locale)
    if data.get("error"):
        return {"ok": False, "error": _as_error_str(data["error"])}
    return {"ok": True, "data": data}


# ── Ticker reference (PostgreSQL + Redis cache) ─────────────────────────────
# Paths under ``/reference/tickers/`` avoid collision with ``GET .../tickers/{ticker:path}`` (upstream proxy).


def _pg_configured(request: Request) -> Optional[dict]:
    db = _db_config(request)
    if not db or (db.get("sink") != "postgres" and not db.get("postgres")):
        return None
    return db


def _ticker_ref_search_impl(
    request: Request,
    q: str,
    limit: int,
) -> Dict[str, Any]:
    import psycopg2

    from bifrost_core.persistence.postgres.connection import _get_conn_params
    from bifrost_core.persistence.postgres.ticker_reference import search_tickers
    from bifrost_worker.data.massive.vendor.reference_cache_keys import (
        CACHE_TTL_SEARCH_SEC,
        key_search,
        normalize_search_key,
        redis_client_from_status_config,
    )

    cfg = _pg_configured(request)
    if not cfg:
        return {"ok": False, "error": "PostgreSQL not configured"}
    nq = normalize_search_key(q)
    rds = redis_client_from_status_config(cfg)
    cache_key = key_search(nq) if nq else None
    if rds and cache_key and nq:
        try:
            raw = rds.get(cache_key)
            if raw:
                return {"ok": True, "cached": True, "results": json.loads(raw)}
        except (json.JSONDecodeError, TypeError):
            pass
    params = _get_conn_params(cfg)
    conn = psycopg2.connect(**params)
    try:
        with conn.cursor() as cur:
            rows = search_tickers(cur, q, limit)
    finally:
        conn.close()
    if rds and cache_key and nq:
        try:
            rds.setex(cache_key, CACHE_TTL_SEARCH_SEC, json.dumps(rows, default=str))
        except Exception:
            pass
    return {"ok": True, "cached": False, "results": rows}


@router.get("/research/massive/reference/tickers/search")
def get_ticker_reference_search(request: Request, q: str = Query("", max_length=128), limit: int = Query(20, ge=1, le=100)) -> Dict[str, Any]:
    """Autocomplete over ``tickers`` (ticker prefix + name ILIKE)."""
    return _ticker_ref_search_impl(request, q, limit)


@router.get("/research/massive/stocks/search")
def get_stock_reference_search_legacy(
    request: Request,
    q: str = Query("", max_length=128),
    limit: int = Query(20, ge=1, le=100),
) -> Dict[str, Any]:
    """Deprecated: use ``GET /research/massive/reference/tickers/search``."""
    return _ticker_ref_search_impl(request, q, limit)


def _ticker_ref_overview_coverage_impl(request: Request) -> Dict[str, Any]:
    import psycopg2

    from bifrost_core.persistence.postgres.connection import _get_conn_params
    from bifrost_core.persistence.postgres.ticker_reference import count_ticker_overview_coverage

    cfg = _pg_configured(request)
    if not cfg:
        return {"ok": False, "error": "PostgreSQL not configured"}
    params = _get_conn_params(cfg)
    conn = psycopg2.connect(**params)
    try:
        with conn.cursor() as cur:
            counts = count_ticker_overview_coverage(cur)
    finally:
        conn.close()
    return {"ok": True, **counts}


@router.get("/research/massive/reference/tickers/overview-coverage")
def get_ticker_reference_overview_coverage(request: Request) -> Dict[str, Any]:
    """Counts for ``tickers`` vs ``ticker_overview`` (missing gap + filled). Registered before ``.../tickers/{ticker}``."""
    return _ticker_ref_overview_coverage_impl(request)


def _ticker_ref_missing_overview_impl(request: Request, limit: int, offset: int) -> Dict[str, Any]:
    import psycopg2

    from bifrost_core.persistence.postgres.connection import _get_conn_params
    from bifrost_core.persistence.postgres.ticker_reference import (
        count_ticker_overview_coverage,
        list_tickers_missing_overview_page,
    )

    cfg = _pg_configured(request)
    if not cfg:
        return {"ok": False, "error": "PostgreSQL not configured"}
    params = _get_conn_params(cfg)
    conn = psycopg2.connect(**params)
    try:
        with conn.cursor() as cur:
            counts = count_ticker_overview_coverage(cur)
            total_missing = int(counts.get("missing") or 0)
            tickers = list_tickers_missing_overview_page(cur, limit, offset)
    finally:
        conn.close()
    loaded = offset + len(tickers)
    has_more = total_missing > 0 and loaded < total_missing
    return {
        "ok": True,
        "tickers": tickers,
        "limit": limit,
        "offset": offset,
        "total_missing": total_missing,
        "has_more": has_more,
    }


@router.get("/research/massive/reference/tickers/missing-overview")
def get_ticker_reference_missing_overview(
    request: Request,
    limit: int = Query(500, ge=1, le=2000),
    offset: int = Query(0, ge=0),
) -> Dict[str, Any]:
    """Paged tickers with no ``ticker_overview`` row. Registered before ``.../tickers/{ticker}``."""
    return _ticker_ref_missing_overview_impl(request, limit, offset)


def _ticker_ref_related_coverage_impl(request: Request) -> Dict[str, Any]:
    import psycopg2

    from bifrost_core.persistence.postgres.connection import _get_conn_params
    from bifrost_core.persistence.postgres.ticker_reference import count_ticker_related_coverage

    cfg = _pg_configured(request)
    if not cfg:
        return {"ok": False, "error": "PostgreSQL not configured"}
    params = _get_conn_params(cfg)
    conn = psycopg2.connect(**params)
    try:
        with conn.cursor() as cur:
            counts = count_ticker_related_coverage(cur)
    finally:
        conn.close()
    return {"ok": True, **counts}


@router.get("/research/massive/reference/tickers/related-coverage")
def get_ticker_reference_related_coverage(request: Request) -> Dict[str, Any]:
    """Counts for ``tickers`` vs ``ticker_related_tickers`` (from_tickers_id)."""
    return _ticker_ref_related_coverage_impl(request)


def _ticker_ref_missing_related_impl(request: Request, limit: int, offset: int) -> Dict[str, Any]:
    import psycopg2

    from bifrost_core.persistence.postgres.connection import _get_conn_params
    from bifrost_core.persistence.postgres.ticker_reference import (
        count_ticker_related_coverage,
        list_tickers_missing_related_page,
    )

    cfg = _pg_configured(request)
    if not cfg:
        return {"ok": False, "error": "PostgreSQL not configured"}
    params = _get_conn_params(cfg)
    conn = psycopg2.connect(**params)
    try:
        with conn.cursor() as cur:
            counts = count_ticker_related_coverage(cur)
            total_missing = int(counts.get("missing") or 0)
            tickers = list_tickers_missing_related_page(cur, limit, offset)
    finally:
        conn.close()
    loaded = offset + len(tickers)
    has_more = total_missing > 0 and loaded < total_missing
    return {
        "ok": True,
        "tickers": tickers,
        "limit": limit,
        "offset": offset,
        "total_missing": total_missing,
        "has_more": has_more,
    }


@router.get("/research/massive/reference/tickers/missing-related")
def get_ticker_reference_missing_related(
    request: Request,
    limit: int = Query(500, ge=1, le=2000),
    offset: int = Query(0, ge=0),
) -> Dict[str, Any]:
    """Paged tickers with no related rows (``ticker_related_tickers``)."""
    return _ticker_ref_missing_related_impl(request, limit, offset)


def _ticker_ref_filled_related_impl(request: Request, limit: int, offset: int) -> Dict[str, Any]:
    import psycopg2

    from bifrost_core.persistence.postgres.connection import _get_conn_params
    from bifrost_core.persistence.postgres.ticker_reference import (
        count_ticker_related_coverage,
        list_tickers_filled_related_page,
    )

    cfg = _pg_configured(request)
    if not cfg:
        return {"ok": False, "error": "PostgreSQL not configured"}
    params = _get_conn_params(cfg)
    conn = psycopg2.connect(**params)
    try:
        with conn.cursor() as cur:
            counts = count_ticker_related_coverage(cur)
            total_filled = int(counts.get("filled") or 0)
            tickers = list_tickers_filled_related_page(cur, limit, offset)
    finally:
        conn.close()
    loaded = offset + len(tickers)
    has_more = total_filled > 0 and loaded < total_filled
    return {
        "ok": True,
        "tickers": tickers,
        "limit": limit,
        "offset": offset,
        "total_filled": total_filled,
        "has_more": has_more,
    }


@router.get("/research/massive/reference/tickers/filled-related")
def get_ticker_reference_filled_related(
    request: Request,
    limit: int = Query(500, ge=1, le=2000),
    offset: int = Query(0, ge=0),
) -> Dict[str, Any]:
    """Paged tickers that have at least one related row."""
    return _ticker_ref_filled_related_impl(request, limit, offset)


def _ticker_types_db_impl(
    request: Request,
    asset_class: str = "*",
    locale: str = "*",
) -> Dict[str, Any]:
    import psycopg2

    from bifrost_core.persistence.postgres.connection import _get_conn_params
    from bifrost_core.persistence.postgres.ticker_reference import list_ticker_types
    from bifrost_worker.data.massive.vendor.reference_cache_keys import (
        CACHE_TTL_TICKER_TYPES_SEC,
        key_ticker_types,
        redis_client_from_status_config,
    )

    cfg = _pg_configured(request)
    if not cfg:
        return {"ok": False, "error": "PostgreSQL not configured"}
    loc = (locale or "*").strip() or "*"
    ac = (asset_class or "*").strip() or "*"
    rds = redis_client_from_status_config(cfg)
    k = key_ticker_types(loc, ac)
    if rds:
        try:
            raw = rds.get(k)
            if raw:
                return {"ok": True, "cached": True, "results": json.loads(raw)}
        except (json.JSONDecodeError, TypeError):
            pass
    params = _get_conn_params(cfg)
    conn = psycopg2.connect(**params)
    try:
        with conn.cursor() as cur:
            rows = list_ticker_types(cur)
    finally:
        conn.close()
    if rds:
        try:
            rds.setex(k, CACHE_TTL_TICKER_TYPES_SEC, json.dumps(rows, default=str))
        except Exception:
            pass
    return {"ok": True, "cached": False, "results": rows}


def _ticker_ref_universe_count_impl(request: Request) -> Dict[str, Any]:
    import psycopg2

    from bifrost_core.persistence.postgres.connection import _get_conn_params
    from bifrost_core.persistence.postgres.ticker_reference import count_tickers_rows

    cfg = _pg_configured(request)
    if not cfg:
        return {"ok": False, "error": "PostgreSQL not configured"}
    params = _get_conn_params(cfg)
    conn = psycopg2.connect(**params)
    try:
        with conn.cursor() as cur:
            n = count_tickers_rows(cur)
    finally:
        conn.close()
    return {"ok": True, "total_tickers": n}


@router.get("/research/massive/reference/tickers/universe-count")
def get_ticker_reference_universe_count(request: Request) -> Dict[str, Any]:
    """Row count for ``public.tickers``. Registered before ``.../tickers/{ticker}``."""
    return _ticker_ref_universe_count_impl(request)


def _ticker_ref_ticker_types_count_impl(request: Request) -> Dict[str, Any]:
    import psycopg2

    from bifrost_core.persistence.postgres.connection import _get_conn_params
    from bifrost_core.persistence.postgres.ticker_reference import count_ticker_types_rows

    cfg = _pg_configured(request)
    if not cfg:
        return {"ok": False, "error": "PostgreSQL not configured"}
    params = _get_conn_params(cfg)
    conn = psycopg2.connect(**params)
    try:
        with conn.cursor() as cur:
            n = count_ticker_types_rows(cur)
    finally:
        conn.close()
    return {"ok": True, "total_ticker_types": n}


@router.get("/research/massive/reference/ticker-types/count")
def get_ticker_types_row_count(request: Request) -> Dict[str, Any]:
    """Row count for ``public.ticker_types``. Registered before ``.../ticker-types`` (list)."""
    return _ticker_ref_ticker_types_count_impl(request)


@router.get("/research/massive/reference/ticker-types")
def get_ticker_types_db(
    request: Request,
    asset_class: str = Query("*"),
    locale: str = Query("*"),
) -> Dict[str, Any]:
    """Ticker type dictionary from ``ticker_types`` (synced via Celery)."""
    return _ticker_types_db_impl(request, asset_class, locale)


def _ticker_ref_related_impl(request: Request, symbol: str) -> Dict[str, Any]:
    import psycopg2

    from bifrost_core.persistence.postgres.connection import _get_conn_params
    from bifrost_core.persistence.postgres.ticker_reference import fetch_related_with_names
    from bifrost_worker.data.massive.vendor.reference_cache_keys import (
        CACHE_TTL_PEERS_SEC,
        key_peers,
        normalize_symbol,
        redis_client_from_status_config,
    )

    cfg = _pg_configured(request)
    if not cfg:
        return {"ok": False, "error": "PostgreSQL not configured"}
    sym = normalize_symbol(symbol)
    if not sym:
        return {"ok": False, "error": "Invalid symbol"}
    rds = redis_client_from_status_config(cfg)
    if rds:
        try:
            raw = rds.get(key_peers(sym))
            if raw:
                return {"ok": True, "cached": True, "data": json.loads(raw)}
        except (json.JSONDecodeError, TypeError):
            pass
    params = _get_conn_params(cfg)
    conn = psycopg2.connect(**params)
    try:
        with conn.cursor() as cur:
            tid, peers = fetch_related_with_names(cur, sym)
    finally:
        conn.close()
    payload = {"from_tickers_id": tid, "symbol": sym, "ticker": sym, "related": peers}
    if rds:
        try:
            rds.setex(key_peers(sym), CACHE_TTL_PEERS_SEC, json.dumps(payload, default=str))
        except Exception:
            pass
    return {"ok": True, "cached": False, "data": payload}


@router.get("/research/massive/reference/tickers/{ticker}/related")
def get_ticker_reference_related(request: Request, ticker: str) -> Dict[str, Any]:
    """Related tickers from ``ticker_related_tickers`` (+ peer names from ``tickers``)."""
    return _ticker_ref_related_impl(request, ticker)


@router.get("/research/massive/stocks/{symbol}/related")
def get_stock_reference_related_legacy(request: Request, symbol: str) -> Dict[str, Any]:
    """Deprecated: use ``GET /research/massive/reference/tickers/{ticker}/related``."""
    return _ticker_ref_related_impl(request, symbol)


def _ticker_ref_detail_impl(request: Request, symbol: str) -> Dict[str, Any]:
    import psycopg2

    from bifrost_core.persistence.postgres.connection import _get_conn_params
    from bifrost_core.persistence.postgres.ticker_reference import fetch_ticker_detail_merged
    from bifrost_worker.data.massive.vendor.reference_cache_keys import (
        CACHE_TTL_TICKER_SEC,
        key_ticker,
        normalize_symbol,
        redis_client_from_status_config,
    )

    cfg = _pg_configured(request)
    if not cfg:
        return {"ok": False, "error": "PostgreSQL not configured"}
    sym = normalize_symbol(symbol)
    if not sym:
        return {"ok": False, "error": "Invalid symbol"}
    rds = redis_client_from_status_config(cfg)
    if rds:
        try:
            raw = rds.get(key_ticker(sym))
            if raw:
                return {"ok": True, "cached": True, "ticker": json.loads(raw)}
        except (json.JSONDecodeError, TypeError):
            pass
    params = _get_conn_params(cfg)
    conn = psycopg2.connect(**params)
    try:
        with conn.cursor() as cur:
            row = fetch_ticker_detail_merged(cur, sym)
    finally:
        conn.close()
    if not row:
        return {"ok": False, "error": "Not found", "symbol": sym}
    if rds:
        try:
            rds.setex(key_ticker(sym), CACHE_TTL_TICKER_SEC, json.dumps(row, default=str))
        except Exception:
            pass
    return {"ok": True, "cached": False, "ticker": row}


@router.get("/research/massive/reference/tickers/{ticker}")
def get_ticker_reference_detail(request: Request, ticker: str) -> Dict[str, Any]:
    """Single merged row from ``tickers`` + ``ticker_overview``."""
    return _ticker_ref_detail_impl(request, ticker)


# ── Stock aggregate bars (read-only Polygon proxy; registered before ``/stocks/{symbol}``) ──


@router.get("/research/massive/stocks/bars/range")
def get_massive_stock_bars_range(
    request: Request,
    ticker: str = Query(..., description="Stock symbol, e.g. AAPL"),
    multiplier: int = Query(1, ge=1),
    timespan: str = Query("minute"),
    start_ms: int = Query(..., description="Range start (Unix ms)"),
    end_ms: int = Query(..., description="Range end (Unix ms)"),
) -> Dict[str, Any]:
    """GET /v2/aggs/ticker/.../range/... — custom OHLCV for a stock (proxy)."""
    from bifrost_worker.data.massive.vendor.config import get_massive_settings
    from bifrost_worker.data.massive.vendor.client import MassiveClient

    reader = getattr(request.app.state, "reader", None)
    cfg = reader._config if reader else {}
    ms = get_massive_settings(cfg)
    if not ms["api_key"]:
        return {"ok": False, "error": "Massive API key not configured"}
    client = MassiveClient(ms["api_key"], ms["rest_base"])
    data = client.fetch_stock_aggs(ticker, multiplier, timespan, start_ms, end_ms)
    if data.get("error"):
        return {"ok": False, "error": _as_error_str(data["error"])}
    return {"ok": True, "data": data}


@router.get("/research/massive/stocks/bars/grouped-daily/{date}")
def get_massive_stock_bars_grouped_daily(
    request: Request,
    date: str,
    adjusted: bool = Query(True),
) -> Dict[str, Any]:
    """GET /v2/aggs/grouped/locale/us/market/stocks/{date} — all US stocks for one date (proxy)."""
    from bifrost_worker.data.massive.vendor.config import get_massive_settings
    from bifrost_worker.data.massive.vendor.client import MassiveClient

    reader = getattr(request.app.state, "reader", None)
    cfg = reader._config if reader else {}
    ms = get_massive_settings(cfg)
    if not ms["api_key"]:
        return {"ok": False, "error": "Massive API key not configured"}
    client = MassiveClient(ms["api_key"], ms["rest_base"])
    data = client.fetch_stock_grouped_daily(date, adjusted=adjusted)
    if data.get("error"):
        return {"ok": False, "error": _as_error_str(data["error"])}
    return {"ok": True, "data": data}


@router.get("/research/massive/stocks/bars/open-close/{ticker:path}/{date}")
def get_massive_stock_bars_open_close(
    request: Request,
    ticker: str,
    date: str,
    adjusted: bool = Query(True),
) -> Dict[str, Any]:
    """GET /v1/open-close/{ticker}/{date} — daily OHLC for a stock (proxy)."""
    from bifrost_worker.data.massive.vendor.config import get_massive_settings
    from bifrost_worker.data.massive.vendor.client import MassiveClient

    reader = getattr(request.app.state, "reader", None)
    cfg = reader._config if reader else {}
    ms = get_massive_settings(cfg)
    if not ms["api_key"]:
        return {"ok": False, "error": "Massive API key not configured"}
    client = MassiveClient(ms["api_key"], ms["rest_base"])
    data = client.fetch_stock_open_close(ticker, date, adjusted=adjusted)
    if data.get("error"):
        return {"ok": False, "error": _as_error_str(data["error"])}
    return {"ok": True, "data": data}


@router.get("/research/massive/stocks/bars/prev/{ticker:path}")
def get_massive_stock_bars_prev(
    request: Request,
    ticker: str,
    adjusted: bool = Query(True),
) -> Dict[str, Any]:
    """GET /v2/aggs/ticker/.../prev — previous trading day OHLC for a stock (proxy)."""
    from bifrost_worker.data.massive.vendor.config import get_massive_settings
    from bifrost_worker.data.massive.vendor.client import MassiveClient

    reader = getattr(request.app.state, "reader", None)
    cfg = reader._config if reader else {}
    ms = get_massive_settings(cfg)
    if not ms["api_key"]:
        return {"ok": False, "error": "Massive API key not configured"}
    client = MassiveClient(ms["api_key"], ms["rest_base"])
    data = client.fetch_stock_previous_day(ticker, adjusted=adjusted)
    if data.get("error"):
        return {"ok": False, "error": _as_error_str(data["error"])}
    return {"ok": True, "data": data}


# ── Stock Fundamentals (Financials v1 + Short Interest / Short Volume / Float) ──


def _massive_fundamentals_client(request: Request):
    from bifrost_worker.data.massive.vendor.config import get_massive_settings
    from bifrost_worker.data.massive.vendor.client import MassiveClient
    reader = getattr(request.app.state, "reader", None)
    cfg = reader._config if reader else {}
    ms = get_massive_settings(cfg)
    if not ms["api_key"]:
        return None, {"ok": False, "error": "Massive API key not configured"}
    return MassiveClient(ms["api_key"], ms["rest_base"]), None


@router.get("/research/massive/stocks/fundamentals/income-statements")
def get_stock_income_statements(
    request: Request,
    ticker: str = Query(..., description="Stock symbol, e.g. AAPL"),
    timeframe: Optional[str] = Query(None, description="quarterly | annual | trailing_twelve_months"),
    fiscal_year: Optional[int] = Query(None),
    fiscal_quarter: Optional[int] = Query(None, ge=1, le=4),
    period_end: Optional[str] = Query(None, description="YYYY-MM-DD"),
    filing_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    limit: int = Query(10, ge=1, le=1000),
    sort: Optional[str] = Query(None),
) -> Dict[str, Any]:
    """GET /stocks/financials/v1/income-statements — P&L proxy."""
    client, err = _massive_fundamentals_client(request)
    if err:
        return err
    data = client.fetch_stock_income_statements(
        ticker, timeframe=timeframe, fiscal_year=fiscal_year, fiscal_quarter=fiscal_quarter,
        period_end=period_end, filing_date=filing_date, limit=limit, sort=sort,
    )
    if data.get("error"):
        return {"ok": False, "error": _as_error_str(data["error"])}
    return {"ok": True, "data": data}


@router.get("/research/massive/stocks/fundamentals/balance-sheets")
def get_stock_balance_sheets(
    request: Request,
    ticker: str = Query(..., description="Stock symbol, e.g. AAPL"),
    timeframe: Optional[str] = Query(None),
    fiscal_year: Optional[int] = Query(None),
    fiscal_quarter: Optional[int] = Query(None, ge=1, le=4),
    period_end: Optional[str] = Query(None),
    filing_date: Optional[str] = Query(None),
    limit: int = Query(10, ge=1, le=1000),
    sort: Optional[str] = Query(None),
) -> Dict[str, Any]:
    """GET /stocks/financials/v1/balance-sheets — assets, liabilities, equity proxy."""
    client, err = _massive_fundamentals_client(request)
    if err:
        return err
    data = client.fetch_stock_balance_sheets(
        ticker, timeframe=timeframe, fiscal_year=fiscal_year, fiscal_quarter=fiscal_quarter,
        period_end=period_end, filing_date=filing_date, limit=limit, sort=sort,
    )
    if data.get("error"):
        return {"ok": False, "error": _as_error_str(data["error"])}
    return {"ok": True, "data": data}


@router.get("/research/massive/stocks/fundamentals/cash-flow-statements")
def get_stock_cash_flow_statements(
    request: Request,
    ticker: str = Query(..., description="Stock symbol, e.g. AAPL"),
    timeframe: Optional[str] = Query(None),
    fiscal_year: Optional[int] = Query(None),
    fiscal_quarter: Optional[int] = Query(None, ge=1, le=4),
    period_end: Optional[str] = Query(None),
    filing_date: Optional[str] = Query(None),
    limit: int = Query(10, ge=1, le=1000),
    sort: Optional[str] = Query(None),
) -> Dict[str, Any]:
    """GET /stocks/financials/v1/cash-flow-statements — OCF, CapEx, financing proxy."""
    client, err = _massive_fundamentals_client(request)
    if err:
        return err
    data = client.fetch_stock_cash_flow_statements(
        ticker, timeframe=timeframe, fiscal_year=fiscal_year, fiscal_quarter=fiscal_quarter,
        period_end=period_end, filing_date=filing_date, limit=limit, sort=sort,
    )
    if data.get("error"):
        return {"ok": False, "error": _as_error_str(data["error"])}
    return {"ok": True, "data": data}


@router.get("/research/massive/stocks/fundamentals/ratios")
def get_stock_ratios(
    request: Request,
    ticker: str = Query(..., description="Stock symbol, e.g. AAPL"),
    limit: int = Query(10, ge=1, le=1000),
    sort: Optional[str] = Query(None),
) -> Dict[str, Any]:
    """GET /stocks/financials/v1/ratios — P/E, P/B, ROE, D/E, dividend yield proxy."""
    client, err = _massive_fundamentals_client(request)
    if err:
        return err
    data = client.fetch_stock_ratios(ticker, limit=limit, sort=sort)
    if data.get("error"):
        return {"ok": False, "error": _as_error_str(data["error"])}
    return {"ok": True, "data": data}


@router.get("/research/massive/stocks/fundamentals/short-interest")
def get_stock_short_interest(
    request: Request,
    ticker: str = Query(..., description="Stock symbol, e.g. AAPL"),
    settlement_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    limit: int = Query(10, ge=1, le=1000),
    sort: Optional[str] = Query(None),
) -> Dict[str, Any]:
    """GET /stocks/v1/short-interest — short interest and days-to-cover proxy."""
    client, err = _massive_fundamentals_client(request)
    if err:
        return err
    data = client.fetch_stock_short_interest(ticker, settlement_date=settlement_date, limit=limit, sort=sort)
    if data.get("error"):
        return {"ok": False, "error": _as_error_str(data["error"])}
    return {"ok": True, "data": data}


@router.get("/research/massive/stocks/fundamentals/short-volume")
def get_stock_short_volume(
    request: Request,
    ticker: str = Query(..., description="Stock symbol, e.g. AAPL"),
    date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    limit: int = Query(10, ge=1, le=1000),
    sort: Optional[str] = Query(None),
) -> Dict[str, Any]:
    """GET /stocks/v1/short-volume — daily short volume per venue + ratio proxy."""
    client, err = _massive_fundamentals_client(request)
    if err:
        return err
    data = client.fetch_stock_short_volume(ticker, date=date, limit=limit, sort=sort)
    if data.get("error"):
        return {"ok": False, "error": _as_error_str(data["error"])}
    return {"ok": True, "data": data}


@router.get("/research/massive/stocks/fundamentals/float")
def get_stock_float(
    request: Request,
    ticker: str = Query(..., description="Stock symbol, e.g. AAPL"),
    limit: int = Query(10, ge=1, le=5000),
    sort: Optional[str] = Query(None),
) -> Dict[str, Any]:
    """GET /stocks/vX/float — free float shares and float percent proxy."""
    client, err = _massive_fundamentals_client(request)
    if err:
        return err
    data = client.fetch_stock_float(ticker, limit=limit, sort=sort)
    if data.get("error"):
        return {"ok": False, "error": _as_error_str(data["error"])}
    return {"ok": True, "data": data}


@router.get("/research/massive/stocks/news")
def get_stock_news(
    request: Request,
    ticker: Optional[str] = Query(None, description="Stock symbol, e.g. AAPL"),
    published_utc_gte: Optional[str] = Query(None, description="RFC3339 datetime lower bound"),
    published_utc_lte: Optional[str] = Query(None, description="RFC3339 datetime upper bound"),
    limit: int = Query(10, ge=1, le=1000),
    sort: Optional[str] = Query(None),
    order: Optional[str] = Query(None),
) -> Dict[str, Any]:
    """GET /v2/reference/news — stock market news articles."""
    client, err = _massive_fundamentals_client(request)
    if err:
        return err
    data = client.fetch_stock_news(
        ticker=ticker,
        published_utc_gte=published_utc_gte,
        published_utc_lte=published_utc_lte,
        limit=limit,
        sort=sort,
        order=order,
    )
    if data.get("error"):
        return {"ok": False, "error": _as_error_str(data["error"])}
    return {"ok": True, "data": data}


# ── SEC Filings & Disclosures ─────────────────────────────────────────────────


@router.get("/research/massive/stocks/filings/edgar-index")
def get_edgar_index(
    request: Request,
    ticker: Optional[str] = Query(None),
    cik: Optional[str] = Query(None),
    form_type: Optional[str] = Query(None),
    filing_date: Optional[str] = Query(None),
    filing_date_gt: Optional[str] = Query(None),
    filing_date_gte: Optional[str] = Query(None),
    filing_date_lt: Optional[str] = Query(None),
    filing_date_lte: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=50000),
    sort: Optional[str] = Query(None),
) -> Dict[str, Any]:
    """GET /stocks/filings/vX/index — EDGAR filing index search."""
    client, err = _massive_fundamentals_client(request)
    if err:
        return err
    data = client.fetch_edgar_index(
        ticker=ticker, cik=cik, form_type=form_type,
        filing_date=filing_date, filing_date_gt=filing_date_gt,
        filing_date_gte=filing_date_gte, filing_date_lt=filing_date_lt,
        filing_date_lte=filing_date_lte, limit=limit, sort=sort,
    )
    if data.get("error"):
        return {"ok": False, "error": _as_error_str(data["error"])}
    return {"ok": True, "data": data}


@router.get("/research/massive/stocks/filings/10k-sections")
def get_10k_sections(
    request: Request,
    ticker: Optional[str] = Query(None),
    cik: Optional[str] = Query(None),
    section: Optional[str] = Query(None),
    filing_date: Optional[str] = Query(None),
    filing_date_gt: Optional[str] = Query(None),
    filing_date_gte: Optional[str] = Query(None),
    filing_date_lt: Optional[str] = Query(None),
    filing_date_lte: Optional[str] = Query(None),
    period_end: Optional[str] = Query(None),
    period_end_gte: Optional[str] = Query(None),
    period_end_lte: Optional[str] = Query(None),
    limit: int = Query(10, ge=1, le=99),
    sort: Optional[str] = Query(None),
) -> Dict[str, Any]:
    """GET /stocks/filings/10-K/vX/sections — plain-text sections from 10-K filings."""
    client, err = _massive_fundamentals_client(request)
    if err:
        return err
    data = client.fetch_10k_sections(
        ticker=ticker, cik=cik, section=section,
        filing_date=filing_date, filing_date_gt=filing_date_gt,
        filing_date_gte=filing_date_gte, filing_date_lt=filing_date_lt,
        filing_date_lte=filing_date_lte, period_end=period_end,
        period_end_gte=period_end_gte, period_end_lte=period_end_lte,
        limit=limit, sort=sort,
    )
    if data.get("error"):
        return {"ok": False, "error": _as_error_str(data["error"])}
    return {"ok": True, "data": data}


@router.get("/research/massive/stocks/filings/8k-text")
def get_8k_text(
    request: Request,
    ticker: Optional[str] = Query(None),
    cik: Optional[str] = Query(None),
    form_type: Optional[str] = Query(None),
    filing_date: Optional[str] = Query(None),
    filing_date_gt: Optional[str] = Query(None),
    filing_date_gte: Optional[str] = Query(None),
    filing_date_lt: Optional[str] = Query(None),
    filing_date_lte: Optional[str] = Query(None),
    limit: int = Query(10, ge=1, le=99),
    sort: Optional[str] = Query(None),
) -> Dict[str, Any]:
    """GET /stocks/filings/8-K/vX/text — parsed plain-text from 8-K current report Items."""
    client, err = _massive_fundamentals_client(request)
    if err:
        return err
    data = client.fetch_8k_text(
        ticker=ticker, cik=cik, form_type=form_type,
        filing_date=filing_date, filing_date_gt=filing_date_gt,
        filing_date_gte=filing_date_gte, filing_date_lt=filing_date_lt,
        filing_date_lte=filing_date_lte, limit=limit, sort=sort,
    )
    if data.get("error"):
        return {"ok": False, "error": _as_error_str(data["error"])}
    return {"ok": True, "data": data}


@router.get("/research/massive/stocks/filings/13f")
def get_13f_filings(
    request: Request,
    filer_cik: Optional[str] = Query(None),
    filing_date: Optional[str] = Query(None),
    filing_date_gt: Optional[str] = Query(None),
    filing_date_gte: Optional[str] = Query(None),
    filing_date_lt: Optional[str] = Query(None),
    filing_date_lte: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    sort: Optional[str] = Query(None),
) -> Dict[str, Any]:
    """GET /stocks/filings/vX/13-F — institutional holdings from Form 13-F."""
    client, err = _massive_fundamentals_client(request)
    if err:
        return err
    data = client.fetch_13f_filings(
        filer_cik=filer_cik,
        filing_date=filing_date, filing_date_gt=filing_date_gt,
        filing_date_gte=filing_date_gte, filing_date_lt=filing_date_lt,
        filing_date_lte=filing_date_lte, limit=limit, sort=sort,
    )
    if data.get("error"):
        return {"ok": False, "error": _as_error_str(data["error"])}
    return {"ok": True, "data": data}


@router.get("/research/massive/stocks/filings/risk-factors")
def get_risk_factors(
    request: Request,
    ticker: Optional[str] = Query(None),
    cik: Optional[str] = Query(None),
    filing_date: Optional[str] = Query(None),
    filing_date_gt: Optional[str] = Query(None),
    filing_date_gte: Optional[str] = Query(None),
    filing_date_lt: Optional[str] = Query(None),
    filing_date_lte: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=49999),
    sort: Optional[str] = Query(None),
) -> Dict[str, Any]:
    """GET /stocks/filings/vX/risk-factors — standardized risk factor disclosures."""
    client, err = _massive_fundamentals_client(request)
    if err:
        return err
    data = client.fetch_risk_factors(
        ticker=ticker, cik=cik,
        filing_date=filing_date, filing_date_gt=filing_date_gt,
        filing_date_gte=filing_date_gte, filing_date_lt=filing_date_lt,
        filing_date_lte=filing_date_lte, limit=limit, sort=sort,
    )
    if data.get("error"):
        return {"ok": False, "error": _as_error_str(data["error"])}
    return {"ok": True, "data": data}


@router.get("/research/massive/stocks/filings/risk-categories")
def get_risk_categories(
    request: Request,
    taxonomy: Optional[int] = Query(None),
    primary_category: Optional[str] = Query(None),
    secondary_category: Optional[str] = Query(None),
    tertiary_category: Optional[str] = Query(None),
    limit: int = Query(200, ge=1, le=999),
    sort: Optional[str] = Query(None),
) -> Dict[str, Any]:
    """GET /stocks/taxonomies/vX/risk-factors — hierarchical risk factor taxonomy."""
    client, err = _massive_fundamentals_client(request)
    if err:
        return err
    data = client.fetch_risk_categories(
        taxonomy=taxonomy, primary_category=primary_category,
        secondary_category=secondary_category, tertiary_category=tertiary_category,
        limit=limit, sort=sort,
    )
    if data.get("error"):
        return {"ok": False, "error": _as_error_str(data["error"])}
    return {"ok": True, "data": data}


@router.get("/research/massive/stocks/filings/form-3")
def get_form_3(
    request: Request,
    issuer_cik: Optional[str] = Query(None),
    owner_cik: Optional[str] = Query(None),
    tickers: Optional[str] = Query(None),
    form_type: Optional[str] = Query(None),
    filing_date: Optional[str] = Query(None),
    filing_date_gt: Optional[str] = Query(None),
    filing_date_gte: Optional[str] = Query(None),
    filing_date_lt: Optional[str] = Query(None),
    filing_date_lte: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=10000),
    sort: Optional[str] = Query(None),
) -> Dict[str, Any]:
    """GET /stocks/filings/vX/form-3 — initial insider ownership statements."""
    client, err = _massive_fundamentals_client(request)
    if err:
        return err
    data = client.fetch_form_3(
        issuer_cik=issuer_cik, owner_cik=owner_cik, tickers=tickers, form_type=form_type,
        filing_date=filing_date, filing_date_gt=filing_date_gt,
        filing_date_gte=filing_date_gte, filing_date_lt=filing_date_lt,
        filing_date_lte=filing_date_lte, limit=limit, sort=sort,
    )
    if data.get("error"):
        return {"ok": False, "error": _as_error_str(data["error"])}
    return {"ok": True, "data": data}


@router.get("/research/massive/stocks/filings/form-4")
def get_form_4(
    request: Request,
    issuer_cik: Optional[str] = Query(None),
    owner_cik: Optional[str] = Query(None),
    tickers: Optional[str] = Query(None),
    form_type: Optional[str] = Query(None),
    transaction_code: Optional[str] = Query(None),
    filing_date: Optional[str] = Query(None),
    filing_date_gt: Optional[str] = Query(None),
    filing_date_gte: Optional[str] = Query(None),
    filing_date_lt: Optional[str] = Query(None),
    filing_date_lte: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=10000),
    sort: Optional[str] = Query(None),
) -> Dict[str, Any]:
    """GET /stocks/filings/vX/form-4 — insider ownership changes."""
    client, err = _massive_fundamentals_client(request)
    if err:
        return err
    data = client.fetch_form_4(
        issuer_cik=issuer_cik, owner_cik=owner_cik, tickers=tickers, form_type=form_type,
        transaction_code=transaction_code,
        filing_date=filing_date, filing_date_gt=filing_date_gt,
        filing_date_gte=filing_date_gte, filing_date_lt=filing_date_lt,
        filing_date_lte=filing_date_lte, limit=limit, sort=sort,
    )
    if data.get("error"):
        return {"ok": False, "error": _as_error_str(data["error"])}
    return {"ok": True, "data": data}


@router.get("/research/massive/stocks/{symbol}")
def get_stock_reference_detail_legacy(request: Request, symbol: str) -> Dict[str, Any]:
    """Deprecated: use ``GET /research/massive/reference/tickers/{ticker}``."""
    out = _ticker_ref_detail_impl(request, symbol)
    if out.get("ok") and isinstance(out.get("ticker"), dict):
        out["stock"] = out["ticker"]
    return out


@router.get("/research/massive/instrument-types")
def get_instrument_types_db(
    request: Request,
    asset_class: str = Query("*"),
    locale: str = Query("*"),
) -> Dict[str, Any]:
    """Deprecated: use ``GET /research/massive/reference/ticker-types``."""
    return _ticker_types_db_impl(request, asset_class, locale)


@router.post("/research/massive/jobs/ticker-reference")
@router.post("/research/massive/jobs/stock-reference")
def post_jobs_ticker_reference(request: Request, body: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    """Enqueue ticker reference Celery job via ``apply_async``."""
    from bifrost_worker.data.massive.celery_queues import celery_queue_for_massive_job
    from bifrost_worker.data.massive.tasks import run_massive_job
    from bifrost_worker.data.massive.vendor.config import get_massive_settings
    from bifrost_worker.data.massive.vendor.reader import insert_job_massive_backfill, update_job_massive_backfill_celery_task_id
    from bifrost_core.persistence.postgres.ticker_reference import normalize_ticker_ref_kind

    reader = getattr(request.app.state, "reader", None)
    cfg = reader._config if reader else {}
    ms = get_massive_settings(cfg)

    kind_raw = (body.get("kind") or "").strip().lower()
    kind = normalize_ticker_ref_kind(kind_raw)
    payload = body.get("payload") if isinstance(body.get("payload"), dict) else {}
    allowed = frozenset(
        {
            "feed_stocks_tickers_reference_universe",
            "feed_stocks_tickers_overview",
            "feed_stocks_tickers_related",
            "feed_stocks_tickers_types",
        }
    )
    if kind not in allowed:
        return {"ok": False, "error": f"Invalid kind; allowed: {sorted(allowed)} (legacy stock_reference_* also accepted)"}

    if not ms["api_key"]:
        return {"ok": False, "error": "Massive API key not configured"}

    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}

    jid, deduplicated = insert_job_massive_backfill(db, kind, payload)
    if jid is None:
        return {"ok": False, "error": "Failed to enqueue job"}

    if deduplicated:
        return {"ok": True, "job_id": str(jid), "deduplicated": True}

    try:
        priority_high = str(body.get("priority") or "").strip().lower() == "high"
        queue_name = celery_queue_for_massive_job(kind, priority_high=priority_high)
        async_result = run_massive_job.apply_async(args=[jid], task_id=str(jid), queue=queue_name)
        update_job_massive_backfill_celery_task_id(db, jid, async_result.id)
    except Exception as e:
        return {"ok": False, "error": str(e)}
    return {"ok": True, "job_id": str(jid)}


@router.get("/research/massive/tickers/{ticker:path}")
def get_massive_ticker_detail(
    request: Request,
    ticker: str,
    date: Optional[str] = Query(None),
) -> Dict[str, Any]:
    """GET /v3/reference/tickers/{ticker} — single ticker (proxy). Path allows dots (e.g. BRK.A)."""
    from bifrost_worker.data.massive.vendor.config import get_massive_settings
    from bifrost_worker.data.massive.vendor.client import MassiveClient

    reader = getattr(request.app.state, "reader", None)
    cfg = reader._config if reader else {}
    ms = get_massive_settings(cfg)
    if not ms["api_key"]:
        return {"ok": False, "error": "Massive API key not configured"}
    client = MassiveClient(ms["api_key"], ms["rest_base"])
    data = client.fetch_ticker_detail(ticker, date=date)
    if data.get("error"):
        return {"ok": False, "error": _as_error_str(data["error"])}
    return {"ok": True, "data": data}


@router.get("/research/massive/related-companies/{ticker}")
def get_massive_related_companies(request: Request, ticker: str) -> Dict[str, Any]:
    """GET /v1/related-companies/{ticker} (proxy)."""
    from bifrost_worker.data.massive.vendor.config import get_massive_settings
    from bifrost_worker.data.massive.vendor.client import MassiveClient

    reader = getattr(request.app.state, "reader", None)
    cfg = reader._config if reader else {}
    ms = get_massive_settings(cfg)
    if not ms["api_key"]:
        return {"ok": False, "error": "Massive API key not configured"}
    client = MassiveClient(ms["api_key"], ms["rest_base"])
    data = client.fetch_related_companies(ticker)
    if data.get("error"):
        return {"ok": False, "error": _as_error_str(data["error"])}
    return {"ok": True, "data": data}


# ── Technical Indicators (cross-asset, read-only) ────────────────────────────

@router.get("/research/massive/technical-indicators/{indicator}/{ticker}")
def get_massive_technical_indicator(
    request: Request,
    indicator: str,
    ticker: str,
    timespan: str = Query("day"),
    window: int = Query(14, ge=1, le=500),
    series_type: str = Query("close"),
    adjusted: bool = Query(True),
    order: str = Query("desc"),
    limit: int = Query(50, ge=1, le=5000),
    short_window: Optional[int] = Query(None, ge=1, description="MACD only"),
    long_window: Optional[int] = Query(None, ge=1, description="MACD only"),
    signal_window: Optional[int] = Query(None, ge=1, description="MACD only"),
) -> Dict[str, Any]:
    """SMA / EMA / RSI / MACD from Massive REST (read-only, no DB write)."""
    from bifrost_worker.data.massive.vendor.config import get_massive_settings
    from bifrost_worker.data.massive.vendor.client import MassiveClient

    allowed = {"sma", "ema", "rsi", "macd"}
    if indicator not in allowed:
        return {"ok": False, "error": f"Unknown indicator '{indicator}'. Allowed: {', '.join(sorted(allowed))}"}

    reader = getattr(request.app.state, "reader", None)
    cfg = reader._config if reader else {}
    ms = get_massive_settings(cfg)
    if not ms["api_key"]:
        return {"ok": False, "error": "Massive API key not configured", "results": {}}
    client = MassiveClient(ms["api_key"], ms["rest_base"])

    kwargs: Dict[str, Any] = dict(
        timespan=timespan, window=window, series_type=series_type,
        adjusted=adjusted, order=order, limit=limit,
    )
    if indicator == "macd":
        if short_window is not None:
            kwargs["short_window"] = short_window
        if long_window is not None:
            kwargs["long_window"] = long_window
        if signal_window is not None:
            kwargs["signal_window"] = signal_window
        data = client.fetch_indicator_macd(ticker, **kwargs)
    else:
        fetcher = getattr(client, f"fetch_indicator_{indicator}")
        data = fetcher(ticker, **kwargs)

    if data.get("error"):
        return {"ok": False, "error": data["error"], "results": {}}

    results = data.get("results") or {}
    values = results.get("values") or [] if isinstance(results, dict) else []
    return {
        "ok": True,
        "indicator": indicator,
        "ticker": ticker.strip().upper(),
        "count": len(values),
        "results": results,
    }


# ── Trades & Quotes (Options REST, read-only) ────────────────────────────────

@router.get("/research/massive/trades-quotes/last-trade/{options_ticker}")
def get_massive_last_trade(request: Request, options_ticker: str) -> Dict[str, Any]:
    """GET /v2/last/trade/{optionsTicker} — most recent trade for a contract (read-only, Starter)."""
    from bifrost_worker.data.massive.vendor.config import get_massive_settings
    from bifrost_worker.data.massive.vendor.client import MassiveClient

    reader = getattr(request.app.state, "reader", None)
    cfg = reader._config if reader else {}
    ms = get_massive_settings(cfg)
    if not ms["api_key"]:
        return {"ok": False, "error": "Massive API key not configured"}
    client = MassiveClient(ms["api_key"], ms["rest_base"])
    data = client.fetch_last_trade(options_ticker)
    if data.get("error"):
        return {"ok": False, "error": data["error"]}
    return {"ok": True, **data}


@router.get("/research/massive/trades-quotes/quotes/{options_ticker}")
def get_massive_hist_quotes(
    request: Request,
    options_ticker: str,
    timestamp_gte: Optional[str] = Query(None, description="Nanosecond timestamp lower bound"),
    timestamp_lte: Optional[str] = Query(None, description="Nanosecond timestamp upper bound"),
    limit: int = Query(100, ge=1, le=50000),
    sort: str = Query("timestamp"),
    order: str = Query("asc"),
) -> Dict[str, Any]:
    """GET /v3/quotes/{optionsTicker} — historical BBO quotes (read-only, Starter)."""
    from bifrost_worker.data.massive.vendor.config import get_massive_settings
    from bifrost_worker.data.massive.vendor.client import MassiveClient

    reader = getattr(request.app.state, "reader", None)
    cfg = reader._config if reader else {}
    ms = get_massive_settings(cfg)
    if not ms["api_key"]:
        return {"ok": False, "error": "Massive API key not configured"}
    client = MassiveClient(ms["api_key"], ms["rest_base"])
    data = client.fetch_option_quotes(
        options_ticker,
        timestamp_gte=timestamp_gte,
        timestamp_lte=timestamp_lte,
        limit=limit,
        sort=sort,
        order=order,
    )
    if data.get("error"):
        return {"ok": False, "error": data["error"]}
    results = data.get("results") or []
    return {"ok": True, "count": len(results) if isinstance(results, list) else 0, **data}


@router.get("/research/massive/trades-quotes/trades/{options_ticker}")
def get_massive_hist_trades(
    request: Request,
    options_ticker: str,
    timestamp_gte: Optional[str] = Query(None, description="Nanosecond timestamp lower bound"),
    timestamp_lte: Optional[str] = Query(None, description="Nanosecond timestamp upper bound"),
    limit: int = Query(100, ge=1, le=50000),
    sort: str = Query("timestamp"),
    order: str = Query("asc"),
) -> Any:
    """GET /v3/trades/{optionsTicker} — tick-level trades (read-only, Developer tier gate)."""
    from bifrost_worker.data.massive.vendor.config import get_massive_settings
    from bifrost_worker.data.massive.vendor.client import MassiveClient

    reader = getattr(request.app.state, "reader", None)
    cfg = reader._config if reader else {}
    ms = get_massive_settings(cfg)
    if not ms["trades_enabled"]:
        return JSONResponse(
            status_code=403,
            content={
                "ok": False,
                "error": "Historical trades API requires Developer tier and trades_enabled.",
            },
        )
    if not ms["api_key"]:
        return {"ok": False, "error": "Massive API key not configured"}
    client = MassiveClient(ms["api_key"], ms["rest_base"])
    data = client.fetch_option_trades(
        options_ticker,
        timestamp_gte=timestamp_gte,
        timestamp_lte=timestamp_lte,
        limit=limit,
        sort=sort,
        order=order,
    )
    if data.get("error"):
        return {"ok": False, "error": data["error"]}
    results = data.get("results") or []
    return {"ok": True, "count": len(results) if isinstance(results, list) else 0, **data}


# ── Sync / Jobs ───────────────────────────────────────────────────────────────

@router.post("/research/massive/sync")
def post_massive_sync(request: Request, body: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    """Enqueue Celery job via ``apply_async`` (queue depends on kind)."""
    from bifrost_worker.data.massive.celery_queues import celery_queue_for_massive_job
    from bifrost_worker.data.massive.tasks import run_massive_job
    from bifrost_worker.data.massive.vendor.config import get_massive_settings
    from bifrost_worker.data.massive.vendor.reader import insert_job_massive_backfill, update_job_massive_backfill_celery_task_id
    from bifrost_core.persistence.postgres.ticker_reference import normalize_ticker_ref_kind

    reader = getattr(request.app.state, "reader", None)
    cfg = reader._config if reader else {}
    ms = get_massive_settings(cfg)

    kind_raw = (body.get("kind") or "").strip().lower()
    kind = normalize_ticker_ref_kind(kind_raw)
    payload = body.get("payload") if isinstance(body.get("payload"), dict) else {}
    allowed = frozenset(
        {
            "feed_options_aggregate",
            "aggregates",
            "feed_option_snapshots",
            "snapshot",
            "oi",
            "reference",
            "corporate_action",
            "feed_stocks_corporate_action",
            "trades",
            "feed_options_trades_quotes",
            "trades_quotes",
            "feed_option_contracts",
            "contracts",
            "eod_pipeline",
            "report_option_max_pain",
            "max_pain",
            "reconcile",
            "trim_jobs",
            "feed_stocks_tickers_reference_universe",
            "ticker_reference_universe",
            "feed_stocks_tickers_overview",
            "ticker_reference_overview",
            "feed_stocks_tickers_related",
            "ticker_reference_related",
            "feed_stocks_tickers_types",
            "ticker_reference_ticker_types",
            "ticker_reference_instrument_types",
            "stock_reference_universe",
            "stock_reference_overview",
            "stock_reference_related",
            "stock_reference_instrument_types",
            "feed_stocks_aggregate",
            "stock_ohlc_sync",
        }
    )
    if kind_raw not in allowed:
        return {"ok": False, "error": f"Invalid kind; allowed: {sorted(allowed)}"}

    if kind == "trades" and not ms["trades_enabled"]:
        return JSONResponse(
            status_code=403,
            content={
                "ok": False,
                "message": "Option trades sync is disabled. Enable massive.features.trades_enabled or use Developer tier.",
            },
        )
    if kind == "feed_options_trades_quotes":
        mode = (payload.get("mode") or "").strip().lower()
        if mode == "trades" and not ms["trades_enabled"]:
            return JSONResponse(
                status_code=403,
                content={
                    "ok": False,
                    "message": "Historical trades require Developer tier and trades_enabled.",
                },
            )

    if kind == "feed_stocks_aggregate":
        mode_ohlc = (payload.get("mode") or "custom_bars").strip().lower()
        if mode_ohlc == "custom_bars":
            syms = payload.get("symbols")
            raw_ticker = payload.get("ticker")
            if syms is not None:
                if not isinstance(syms, list) or len(syms) == 0:
                    return JSONResponse(
                        status_code=400,
                        content={
                            "ok": False,
                            "error": "payload.symbols must be a non-empty array",
                        },
                    )
                if len(syms) > 50:
                    return JSONResponse(
                        status_code=400,
                        content={
                            "ok": False,
                            "error": "payload.symbols must have at most 50 entries",
                        },
                    )
                if (raw_ticker or "").strip():
                    return JSONResponse(
                        status_code=400,
                        content={
                            "ok": False,
                            "error": "Use payload.ticker or payload.symbols, not both",
                        },
                    )

    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}

    # option_day_pool_row_gap: optional fan-out — one DB query for targets, then N smaller Celery jobs.
    mode_payload = (payload.get("mode") or "").strip()
    if kind == "feed_options_aggregate" and mode_payload == "option_day_pool_row_gap":
        raw_chunk = payload.get("chunk_size")
        if raw_chunk is not None:
            try:
                chunk_sz = int(raw_chunk)
            except (TypeError, ValueError):
                chunk_sz = 0
            if chunk_sz >= 2:
                chunk_sz = max(5, min(chunk_sz, 200))
                import psycopg2

                from bifrost_worker.data.massive.option_day_pool_fill import (
                    chunk_option_day_row_gap_targets,
                    list_option_day_row_gap_targets,
                    row_gap_targets_to_payload_dicts,
                )
                from bifrost_core.persistence.postgres.connection import _get_conn_params

                sym_u = (payload.get("underlying") or payload.get("symbol") or "").strip().upper()
                if not sym_u:
                    return {"ok": False, "error": "payload.underlying (or symbol) required for row-gap fan-out"}

                max_c = int(payload.get("max_contracts") or 300)
                max_c = max(1, min(max_c, 2000))
                max_e = int(payload.get("max_expiries") or 60)
                max_e = max(1, min(max_e, 120))
                exp_f_raw = (payload.get("expiration_date") or "").strip()[:32]
                exp_f = exp_f_raw or None
                fanout_max_chunks = 25

                try:
                    pg_params = _get_conn_params(db)
                    pg_conn = psycopg2.connect(**pg_params)
                    try:
                        with pg_conn.cursor() as cur:
                            targets_all = list_option_day_row_gap_targets(
                                cur, sym_u, max_e, max_c, expiration_date=exp_f
                            )
                    finally:
                        pg_conn.close()
                except Exception as exc:
                    return {"ok": False, "error": f"row-gap fan-out target query failed: {exc}"}

                chunks = chunk_option_day_row_gap_targets(targets_all, chunk_sz)
                if len(chunks) > fanout_max_chunks:
                    return JSONResponse(
                        status_code=400,
                        content={
                            "ok": False,
                            "error": (
                                f"Fan-out would create {len(chunks)} jobs; maximum is {fanout_max_chunks}. "
                                "Increase chunk_size or reduce max_contracts."
                            ),
                        },
                    )
                if not chunks:
                    return {
                        "ok": True,
                        "fan_out": True,
                        "job_ids": [],
                        "chunks": 0,
                        "targets_total": 0,
                        "message": "No row-gap targets for this symbol.",
                    }

                job_ids: List[str] = []
                n_chunks = len(chunks)
                for idx, chunk in enumerate(chunks):
                    pl = {k: v for k, v in payload.items() if k != "chunk_size"}
                    pl["row_gap_targets"] = row_gap_targets_to_payload_dicts(chunk)
                    pl["fan_out_chunk_index"] = idx + 1
                    pl["fan_out_chunks_total"] = n_chunks
                    jid, deduplicated = insert_job_massive_backfill(db, kind, pl)
                    if jid is None:
                        return {
                            "ok": False,
                            "error": "Failed to enqueue job (fan-out chunk)",
                            "job_ids": job_ids,
                        }
                    if deduplicated:
                        job_ids.append(str(jid))
                        continue
                    try:
                        priority_high = str(body.get("priority") or "").strip().lower() == "high"
                        queue_name = celery_queue_for_massive_job(kind, priority_high=priority_high)
                        async_result = run_massive_job.apply_async(args=[jid], task_id=str(jid), queue=queue_name)
                        update_job_massive_backfill_celery_task_id(db, jid, async_result.id)
                    except Exception as e:
                        return {"ok": False, "error": str(e), "job_ids": job_ids}
                    job_ids.append(str(jid))
                return {
                    "ok": True,
                    "fan_out": True,
                    "job_ids": job_ids,
                    "chunks": n_chunks,
                    "targets_total": len(targets_all),
                }

    jid, deduplicated = insert_job_massive_backfill(db, kind, payload)
    if jid is None:
        return {"ok": False, "error": "Failed to enqueue job"}

    if deduplicated:
        return {"ok": True, "job_id": str(jid), "deduplicated": True}

    try:
        priority_high = str(body.get("priority") or "").strip().lower() == "high"
        queue_name = celery_queue_for_massive_job(kind, priority_high=priority_high)
        async_result = run_massive_job.apply_async(args=[jid], task_id=str(jid), queue=queue_name)
        update_job_massive_backfill_celery_task_id(db, jid, async_result.id)
    except Exception as e:
        return {"ok": False, "error": str(e)}
    return {"ok": True, "job_id": str(jid)}


@router.get("/research/massive/jobs/{job_id}/events")
async def stream_massive_job_events(
    request: Request,
    job_id: str,
    timeout_sec: int = Query(180, ge=10, le=86400),
) -> StreamingResponse:
    """SSE: poll job row until terminal status or timeout (1s interval). Long ticker-reference jobs may run for hours."""
    import time

    from bifrost_worker.data.massive.vendor.reader import get_job_massive_backfill

    db = _db_config(request)

    async def event_gen():
        if not db:
            yield f"data: {json.dumps({'ok': False, 'error': 'No DB'})}\n\n"
            return
        start = time.monotonic()
        while time.monotonic() - start < timeout_sec:
            job = await asyncio.to_thread(get_job_massive_backfill, db, job_id)
            if job is None:
                yield f"data: {json.dumps({'ok': False, 'error': 'Job not found'})}\n\n"
                return
            payload = _massive_job_to_api(dict(job))
            yield f"data: {json.dumps({'ok': True, 'job': payload})}\n\n"
            st = (job.get("status") or "").strip().lower()
            if st in ("done", "failed"):
                return
            await asyncio.sleep(1.0)
        yield f"data: {json.dumps({'ok': False, 'error': 'timeout'})}\n\n"

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/research/massive/snapshot-quality-detail")
def get_snapshot_quality_detail(
    request: Request,
    symbol: str = Query(..., description="Underlying symbol"),
    source: str = Query("massive", description="Snapshot source: massive | ib"),
    days: int = Query(30, ge=1, le=365, description="Lookback window in days"),
) -> Dict[str, Any]:
    """Per-symbol snapshot quality: daily history + latest-date expiry breakdown."""
    import psycopg2

    from bifrost_core.persistence.postgres.connection import _get_conn_params

    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}
    sym = (symbol or "").strip().upper()
    if not sym:
        return {"ok": False, "error": "symbol is required"}
    src = (source or "massive").strip().lower()
    if src not in ("massive", "ib"):
        src = "massive"

    try:
        params = _get_conn_params(db)
        conn = psycopg2.connect(**params)
        try:
            # Query A — daily aggregates (last 30 days, one row per contract per day)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH daily_latest AS (
                        SELECT DISTINCT ON (
                            DATE(timezone('America/New_York', snapshot_ts)),
                            contract_key
                        )
                            DATE(timezone('America/New_York', snapshot_ts)) AS snap_day,
                            iv, delta, gamma, theta, vega, open_interest, day_close
                        FROM option_snapshots
                        WHERE source = %(source)s
                          AND UPPER(TRIM(SPLIT_PART(contract_key, '|', 1))) = %(symbol)s
                          AND snapshot_ts >= NOW() - (%(days)s || ' days')::interval
                        ORDER BY DATE(timezone('America/New_York', snapshot_ts)),
                                 contract_key,
                                 snapshot_ts DESC
                    )
                    SELECT
                        snap_day,
                        COUNT(*)::int AS contract_count,
                        ROUND(COUNT(iv)::numeric / COUNT(*)::numeric * 100, 1) AS iv_pct,
                        ROUND(COUNT(CASE WHEN delta IS NOT NULL AND gamma IS NOT NULL
                                         AND theta IS NOT NULL AND vega IS NOT NULL
                                    THEN 1 END)::numeric / COUNT(*)::numeric * 100, 1) AS full_greeks_pct,
                        ROUND(COUNT(open_interest)::numeric / COUNT(*)::numeric * 100, 1) AS oi_pct,
                        ROUND(COUNT(day_close)::numeric / COUNT(*)::numeric * 100, 1) AS day_price_pct
                    FROM daily_latest
                    GROUP BY snap_day
                    ORDER BY snap_day DESC
                    """,
                    {"source": src, "symbol": sym, "days": days},
                )
                daily_raw = cur.fetchall()

            daily_rows = [
                {
                    "snap_day": row[0].isoformat() if hasattr(row[0], "isoformat") else str(row[0]),
                    "contract_count": row[1],
                    "iv_pct": float(row[2]) if row[2] is not None else None,
                    "full_greeks_pct": float(row[3]) if row[3] is not None else None,
                    "oi_pct": float(row[4]) if row[4] is not None else None,
                    "day_price_pct": float(row[5]) if row[5] is not None else None,
                }
                for row in daily_raw
            ]

            latest_date = daily_rows[0]["snap_day"] if daily_rows else None

            # Query B — expiry breakdown for the latest date
            expiry_rows: list = []
            if latest_date:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        WITH latest_day_rows AS (
                            SELECT DISTINCT ON (contract_key)
                                contract_key,
                                iv, delta, gamma, theta, vega, open_interest, day_close
                            FROM option_snapshots
                            WHERE source = %(source)s
                              AND UPPER(TRIM(SPLIT_PART(contract_key, '|', 1))) = %(symbol)s
                              AND DATE(timezone('America/New_York', snapshot_ts)) = %(latest_date)s
                            ORDER BY contract_key, snapshot_ts DESC
                        )
                        SELECT
                            SPLIT_PART(contract_key, '|', 3) AS expiry,
                            (TO_DATE(SPLIT_PART(contract_key,'|',3),'YYYYMMDD') - CURRENT_DATE)::int AS dte,
                            COUNT(*)::int AS contract_count,
                            ROUND(COUNT(iv)::numeric / COUNT(*)::numeric * 100, 1) AS iv_pct,
                            ROUND(COUNT(CASE WHEN delta IS NOT NULL AND gamma IS NOT NULL
                                             AND theta IS NOT NULL AND vega IS NOT NULL
                                        THEN 1 END)::numeric / COUNT(*)::numeric * 100, 1) AS full_greeks_pct,
                            ROUND(COUNT(open_interest)::numeric / COUNT(*)::numeric * 100, 1) AS oi_pct,
                            ROUND(COUNT(day_close)::numeric / COUNT(*)::numeric * 100, 1) AS day_price_pct
                        FROM latest_day_rows
                        GROUP BY expiry
                        ORDER BY expiry
                        """,
                        {"source": src, "symbol": sym, "latest_date": latest_date},
                    )
                    expiry_raw = cur.fetchall()
                expiry_rows = [
                    {
                        "expiry": row[0],
                        "dte": int(row[1]) if row[1] is not None else None,
                        "contract_count": row[2],
                        "iv_pct": float(row[3]) if row[3] is not None else None,
                        "full_greeks_pct": float(row[4]) if row[4] is not None else None,
                        "oi_pct": float(row[5]) if row[5] is not None else None,
                        "day_price_pct": float(row[6]) if row[6] is not None else None,
                    }
                    for row in expiry_raw
                ]

            return {
                "ok": True,
                "symbol": sym,
                "source": src,
                "latest_date": latest_date,
                "daily": daily_rows,
                "expiries": expiry_rows,
            }
        finally:
            conn.close()
    except Exception as exc:
        return {"ok": False, "symbol": sym, "source": src, "latest_date": None,
                "daily": [], "expiries": [], "error": str(exc)}


@router.get("/research/massive/stock-day-gap")
def get_stock_day_gap(
    request: Request,
    symbol: str = Query(..., description="Stock ticker (e.g. NVDA)"),
    years: int = Query(10, ge=1, le=20, description="Lookback window in years"),
) -> Dict[str, Any]:
    """Compare stock_day bar coverage against the global trading-day calendar (purely local)."""
    import psycopg2
    from datetime import timedelta

    from bifrost_worker.data.massive.stock_ohlc_daily_smart import is_ny_session_safely_closed, ny_calendar_today
    from bifrost_core.persistence.postgres.connection import _get_conn_params
    from bifrost_worker.data.massive.vendor.stock_day_gap import compute_stock_day_gap

    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}

    sym = (symbol or "").strip().upper()
    if not sym:
        return {"ok": False, "error": "symbol is required"}

    # Cap the ref/covered sets at the last safely-closed trading date.
    # When the NYSE session is still open, using CURRENT_DATE would count today
    # as a phantom gap (other symbols may already have today's bar, this one doesn't).
    # After 16:20 ET the session is considered closed and today is safe to include.
    today_ny = ny_calendar_today()
    session_closed = is_ny_session_safely_closed()
    cap_d = today_ny if session_closed else today_ny - timedelta(days=1)

    try:
        params = _get_conn_params(db)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                result = compute_stock_day_gap(cur, sym, lookback_years=years, cap_date=cap_d)
                result["today_pending"] = not session_closed
                return result
        finally:
            conn.close()
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


@router.post("/research/massive/stock-day-gap/batch")
def post_stock_day_gap_batch(
    request: Request,
    payload: Dict[str, Any] = Body(...),
) -> Dict[str, Any]:
    """Batch stock_day gap check — max 20 symbols, no external API."""
    import psycopg2
    from datetime import timedelta

    from bifrost_worker.data.massive.stock_ohlc_daily_smart import is_ny_session_safely_closed, ny_calendar_today
    from bifrost_core.persistence.postgres.connection import _get_conn_params
    from bifrost_worker.data.massive.vendor.stock_day_gap import compute_stock_day_gap

    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}

    raw = payload.get("symbols") if isinstance(payload, dict) else None
    if not isinstance(raw, list):
        return {"ok": False, "error": "payload.symbols must be a non-empty array"}

    years = int(payload.get("years") or 10)
    years = max(1, min(20, years))

    syms: List[str] = []
    seen: set = set()
    for x in raw:
        u = (str(x) or "").strip().upper()
        if u and u not in seen:
            seen.add(u)
            syms.append(u)
    if not syms:
        return {"ok": False, "error": "payload.symbols must be a non-empty array"}
    if len(syms) > 20:
        return {"ok": False, "error": "At most 20 symbols per batch"}

    # Compute the safe date cap once for the whole batch (same session state for all symbols).
    today_ny = ny_calendar_today()
    session_closed = is_ny_session_safely_closed()
    cap_d = today_ny if session_closed else today_ny - timedelta(days=1)

    results: Dict[str, Any] = {}
    try:
        params = _get_conn_params(db)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                for s in syms:
                    try:
                        r = compute_stock_day_gap(cur, s, lookback_years=years, cap_date=cap_d)
                        r["today_pending"] = not session_closed
                        results[s] = r
                    except Exception as exc:  # noqa: BLE001
                        results[s] = {"ok": False, "symbol": s, "error": str(exc)}
        finally:
            conn.close()
    except Exception as exc:
        return {"ok": False, "error": str(exc)}

    return {"ok": True, "results": results}


@router.get("/research/massive/stock-day-quality-detail")
def get_stock_day_quality_detail(
    request: Request,
    symbol: str = Query(..., description="Stock ticker (e.g. NVDA)"),
    days: int = Query(90, ge=1, le=365, description="Days of daily history to return"),
) -> Dict[str, Any]:
    """Per-day OHLC / volume / VWAP completeness for stock_day."""
    import psycopg2

    from bifrost_core.persistence.postgres.connection import _get_conn_params
    from bifrost_worker.data.massive.vendor.stock_day_gap import compute_stock_day_quality_detail

    db = _db_config(request)
    if not db:
        return {"ok": False, "symbol": symbol, "latest_date": None, "daily": [], "error": "PostgreSQL not configured"}

    sym = (symbol or "").strip().upper()
    if not sym:
        return {"ok": False, "symbol": "", "latest_date": None, "daily": [], "error": "symbol is required"}

    try:
        params = _get_conn_params(db)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                return compute_stock_day_quality_detail(cur, sym, days=days)
        finally:
            conn.close()
    except Exception as exc:
        return {"ok": False, "symbol": sym, "latest_date": None, "daily": [], "error": str(exc)}


@router.get("/research/massive/corporate-actions")
def get_massive_corporate_actions(
    request: Request,
    symbol: str = Query(..., description="Stock ticker (e.g. AAPL)"),
    action_type: Optional[str] = Query(None, description="dividend | split"),
    limit: int = Query(50, ge=1, le=500),
) -> Dict[str, Any]:
    """Corporate actions persisted by Massive sync (dividends, splits)."""
    from bifrost_worker.data.massive.vendor.reader import get_corporate_actions

    db = _db_config(request)
    if not db:
        return {"ok": False, "rows": [], "error": "PostgreSQL not configured"}
    rows = get_corporate_actions(db, symbol, action_type=action_type, limit=limit)
    serialised = []
    for r in rows:
        row = dict(r)
        for k in ("ex_date", "record_date", "payment_date", "created_at"):
            v = row.get(k)
            if hasattr(v, "isoformat"):
                row[k] = v.isoformat()
        serialised.append(row)
    return {"ok": True, "rows": serialised}
