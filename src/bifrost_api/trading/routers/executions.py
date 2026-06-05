"""Executions and transactions: CRUD, Flex fetch, IB fetch, performance."""

import asyncio
import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, HTTPException, Query, Request

from bifrost_core.portfolio.reader.option_stock_link import delete_option_stock_link, insert_option_stock_link
from bifrost_core.portfolio.services.executions_fetch_flex import (
    fetch_flex_trades_and_upsert_executions,
    upsert_executions_from_uploaded_flex_xml,
)
from bifrost_core.portfolio.services.transactions_fetch import fetch_cash_transactions_from_flex
from bifrost_core.monitor.reader import (
    write_account_executions_to_db,
    insert_one_execution,
    update_one_execution,
    delete_one_execution,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["executions"])


def _fmt_db_id_list(ids: Any, *, max_show: int = 48) -> str:
    """Format ``executions_raw_tws_id`` lists for API message text (truncate long lists)."""
    if not ids:
        return "none"
    try:
        nums = [int(x) for x in list(ids)]
    except (TypeError, ValueError):
        return "none"
    if len(nums) <= max_show:
        return "[" + ", ".join(str(i) for i in nums) + "]"
    head = nums[:max_show]
    return "[" + ", ".join(str(i) for i in head) + f", … +{len(nums) - max_show} more]"


def _publish_tws_fetch_system_message(
    config: dict,
    *,
    ok: bool,
    title: str,
    message: str,
    reason: Optional[str] = None,
    level: Optional[str] = None,
) -> None:
    """Best-effort Redis message center (Monitor materializes for SSE)."""
    try:
        import redis as redis_mod

        from bifrost_core.core.message_center import (
            build_portfolio_tws_executions_fetch_event,
            publish_system_message_event,
        )
        from bifrost_core.core.redis_url import effective_redis_dict, format_redis_url

        url = format_redis_url(effective_redis_dict(config, default_db=0))
        if not url:
            return
        r = redis_mod.from_url(url, decode_responses=True)
        try:
            ev = build_portfolio_tws_executions_fetch_event(
                ok=ok, title=title, message=message, reason=reason, level=level
            )
            publish_system_message_event(r, ev)
        finally:
            r.close()
    except Exception as e:
        logger.debug("tws fetch message center publish failed: %s", e)


@router.get("/executions")
def get_executions(
    request: Request,
    since_ts: Optional[float] = Query(None, description="Filter executions with time >= this (Unix s)"),
    until_ts: Optional[float] = Query(None, description="Filter executions with time <= this"),
    account_id: Optional[str] = Query(None, description="Filter by account ID"),
    limit: int = Query(200, ge=0, le=10000, description="Max rows to return; 0 = no limit"),
    include_opt_pairs: bool = Query(False, description="Include C<>P pairing"),
    strategy_opportunity_id: Optional[int] = Query(None, description="Filter by strategy opportunity ID"),
    strategy_instance_id: Optional[int] = Query(None, description="Filter by strategy instance ID"),
    source_scope: Optional[str] = Query(
        None,
        description="Optional: all (default, full account_executions) | performance_book (account_executions_final) | on_the_fly (account_executions_fly: TWS not covered by final, no BAG) | tws_raw (executions_raw_tws only, synthetic negative account_executions_id)",
    ),
) -> Dict[str, Any]:
    """Account-level executions/trades (R-A2). If include_opt_pairs=true: returns paired_execution_ids and opt_pairs."""
    reader = request.app.state.reader
    effective_limit: Optional[int] = limit if limit > 0 else None
    if include_opt_pairs:
        return reader.get_executions_with_opt_pairs(
            since_ts=since_ts,
            until_ts=until_ts,
            account_id=account_id,
            limit=effective_limit or 5000,
            strategy_opportunity_id=strategy_opportunity_id,
            strategy_instance_id=strategy_instance_id,
            source_scope=source_scope,
        )
    items = reader.get_executions(
        since_ts=since_ts,
        until_ts=until_ts,
        account_id=account_id,
        limit=effective_limit,
        strategy_opportunity_id=strategy_opportunity_id,
        strategy_instance_id=strategy_instance_id,
        source_scope=source_scope,
    )
    return {"executions": items}


@router.get("/executions/position-attribution")
def get_position_attribution(
    request: Request,
    account_id: Optional[str] = Query(None, description="Filter by account ID"),
    sec_type: Optional[str] = Query(None, description="Filter by sec_type (e.g. OPT, STK)"),
) -> Dict[str, Any]:
    """Position x Instance attribution (net-estimated). Returns one row per (position, instance)."""
    reader = request.app.state.reader
    items = reader.get_position_instance_attribution(
        account_id=account_id,
        sec_type_filter=sec_type,
    )
    return {"attributions": items}


@router.get("/executions/link-candidates")
def get_executions_link_candidates(
    request: Request,
    account_id: str = Query(..., description="IB account id"),
    contract_key: Optional[str] = Query(None, description="Exact contract_key match (preferred)"),
    symbol: Optional[str] = Query(None),
    expiry: Optional[str] = Query(None, description="Option expiry (any format; used if contract_key yields no rows)"),
    strike: Optional[float] = Query(None),
    option_right: Optional[str] = Query(None, description="C or P"),
    limit: int = Query(200, ge=1, le=500),
) -> Dict[str, Any]:
    """Existing account_executions rows to link strategy attribution (no insert)."""
    reader = request.app.state.reader
    if not (contract_key and contract_key.strip()) and (
        not (symbol and symbol.strip()) or strike is None or expiry is None or str(expiry).strip() == ""
    ):
        return {
            "executions": [],
            "error": "Provide contract_key, or symbol+expiry+strike for fallback matching.",
        }
    items = reader.get_executions_for_strategy_link(
        account_id=account_id.strip(),
        contract_key=(contract_key or "").strip() or None,
        symbol=(symbol or "").strip() or None,
        expiry=expiry,
        strike=strike,
        option_right=(option_right or "").strip() or None,
        limit=limit,
    )
    return {"executions": items}


@router.post("/executions/option-stock-links/query")
def post_option_stock_links_query(request: Request, body: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    """Bulk load link rows for many option_account_executions_id values (grouped by account_id).

    Body: { "batches": [ { "account_id": "...", "option_account_executions_ids": [1, 2, ...] }, ... ] }
    Returns: { "by_option_id": { "<id>": { "links": [...], "slippage_total": number | null } } }
    """
    reader = request.app.state.reader
    raw_batches = body.get("batches")
    if not isinstance(raw_batches, list):
        return {"by_option_id": {}, "error": "batches must be a list"}
    batches: List[Any] = []
    for item in raw_batches:
        if not isinstance(item, dict):
            continue
        acc = (item.get("account_id") or "").strip()
        ids_raw = item.get("option_account_executions_ids")
        if not acc or not isinstance(ids_raw, list):
            continue
        batches.append((acc, ids_raw))
    return reader.get_option_stock_links_bulk(batches)


@router.get("/executions/option-stock-links")
def get_option_stock_links_route(
    request: Request,
    account_id: str = Query(..., description="IB account id"),
    option_account_executions_id: int = Query(..., description="Unified account_executions_id of the OPT leg"),
) -> Dict[str, Any]:
    """List stock legs linked to an option execution; includes slippage_vs_close per row and slippage_total."""
    reader = request.app.state.reader
    return reader.get_option_stock_links(account_id.strip(), option_account_executions_id)


@router.get("/executions/stock-link-candidates")
def get_stock_link_candidates_route(
    request: Request,
    account_id: str = Query(..., description="IB account id"),
    option_account_executions_id: int = Query(
        ...,
        description="OPT row id (performance book); underlying symbol and date window derived from this row",
    ),
    trade_date_from: Optional[str] = Query(None, description="YYYY-MM-DD override (default: option trade_date − 7d)"),
    trade_date_to: Optional[str] = Query(None, description="YYYY-MM-DD override (default: option trade_date + 7d)"),
    limit: int = Query(200, ge=1, le=500),
) -> Dict[str, Any]:
    """STK executions in performance book matching option underlying; excludes already-linked rows for this option."""
    reader = request.app.state.reader
    return reader.get_stock_link_candidates(
        account_id.strip(),
        option_account_executions_id,
        trade_date_from=trade_date_from,
        trade_date_to=trade_date_to,
        limit=limit,
    )


@router.post("/executions/option-stock-links")
def post_option_stock_links(request: Request, body: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    """Link one OPT execution to one STK execution (both must exist on account_executions_final)."""
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return {"ok": False, "error": "PostgreSQL is required.", "link_id": None}
    ok, link_id, err, warning = insert_option_stock_link(control_via_db, body)
    return {"ok": ok, "link_id": link_id, "error": err, "warning": warning}


@router.delete("/executions/option-stock-links/{link_id}")
def delete_option_stock_links_route(
    request: Request,
    link_id: str,
    account_id: str = Query(..., description="Must match link row account_id"),
) -> Dict[str, Any]:
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return {"ok": False, "error": "PostgreSQL is required."}
    try:
        lid = int(str(link_id).strip())
    except (TypeError, ValueError):
        raise HTTPException(status_code=422, detail="Invalid link id") from None
    ok, err = delete_option_stock_link(control_via_db, lid, account_id.strip())
    if ok:
        return {"ok": True, "message": "Link removed."}
    return {"ok": False, "error": err or "Delete failed."}


@router.get("/executions/freshness")
def get_executions_freshness(request: Request) -> Dict[str, Any]:
    """Execution data freshness per (account_id, source). Latest exec_time and days_since_latest."""
    reader = request.app.state.reader
    items = reader.get_executions_freshness()
    return {"items": items}


@router.get("/performance")
def get_performance(
    request: Request,
    since_ts: Optional[float] = Query(None),
    until_ts: Optional[float] = Query(None),
    account_id: Optional[str] = Query(None),
    granularity: str = Query("day", description="day | week | month"),
    strategy_opportunity_id: Optional[int] = Query(None, description="Filter by strategy opportunity ID"),
    strategy_instance_id: Optional[int] = Query(None, description="Filter by strategy instance ID"),
    source_scope: str = Query(
        "performance_book",
        description="performance_book (default, account_executions_final) | on_the_fly (account_executions_fly)",
    ),
    summary_only: bool = Query(
        False,
        description="With strategy_instance_id only: return summary via one SQL aggregate (fast)",
    ),
) -> Dict[str, Any]:
    """Performance stats and calendar PnL. Default source_scope=performance_book reads account_executions_final (flex+journal only)."""
    reader = request.app.state.reader
    if summary_only:
        if strategy_instance_id is None:
            raise HTTPException(status_code=400, detail="summary_only requires strategy_instance_id")
        if (account_id is not None and str(account_id).strip()) or strategy_opportunity_id is not None:
            raise HTTPException(
                status_code=400,
                detail="summary_only allows only strategy_instance_id (no account_id / opportunity filter)",
            )
        out = reader.get_performance_instance_summary(
            strategy_instance_id=strategy_instance_id,
            since_ts=since_ts,
            until_ts=until_ts,
        )
        return out
    out = reader.get_performance_stats(
        since_ts=since_ts,
        until_ts=until_ts,
        account_id=account_id,
        granularity=granularity,
        strategy_opportunity_id=strategy_opportunity_id,
        strategy_instance_id=strategy_instance_id,
        source_scope=source_scope,
    )
    return out


@router.get("/transactions")
def get_transactions(
    request: Request,
    since_ts: Optional[float] = Query(None),
    until_ts: Optional[float] = Query(None),
    account_id: Optional[str] = Query(None),
    limit: int = Query(500),
) -> Dict[str, Any]:
    """List account_transactions (Flex cash transactions) for Transfer & Pay page."""
    reader = request.app.state.reader
    items = reader.get_transactions(since_ts=since_ts, until_ts=until_ts, account_id=account_id, limit=limit)
    return {"transactions": items}


@router.post("/transactions/fetch")
def post_transactions_fetch(request: Request, body: Dict[str, Any] = Body(default=None)) -> Dict[str, Any]:
    """Fetch cash transactions from IB Flex and upsert into account_transactions."""
    reader = request.app.state.reader
    control_via_db = request.app.state.control_via_db
    return fetch_cash_transactions_from_flex(reader, control_via_db, body)


@router.post("/executions/fetch-flex")
def post_executions_fetch_flex(request: Request, body: Dict[str, Any] = Body(default=None)) -> Dict[str, Any]:
    """Fetch executions/trades from IB Flex (Trades report) and upsert into account_executions."""
    reader = request.app.state.reader
    control_via_db = request.app.state.control_via_db
    return fetch_flex_trades_and_upsert_executions(reader, control_via_db, body)


@router.post("/executions/fetch-flex-upload")
def post_executions_fetch_flex_upload(request: Request, body: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    """Upload Flex Trades XML and upsert into account_executions. Body: { "xml": "<FlexStatement ...>...</FlexStatement>" }"""
    control_via_db = request.app.state.control_via_db
    reader = request.app.state.reader
    raw_xml = (body.get("xml") or "").strip()
    cfg = getattr(reader, "_config", None)
    return upsert_executions_from_uploaded_flex_xml(control_via_db, raw_xml, config=cfg)


@router.post("/executions")
def post_execution(request: Request, body: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    """Add one execution record manually (history). body: account_id, time, symbol, sec_type, side, quantity, price; optional fields."""
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return {"ok": False, "error": "PostgreSQL is required to write account_executions.", "account_executions_id": None}
    new_account_executions_id = insert_one_execution(control_via_db, body)
    if new_account_executions_id is None:
        return {"ok": False, "error": "Failed to add execution (required fields: symbol, quantity, price).", "account_executions_id": None}
    return {"ok": True, "account_executions_id": new_account_executions_id, "message": "Execution record added."}


def _parse_account_executions_path_id(execution_id: str) -> int:
    try:
        return int(str(execution_id).strip())
    except (TypeError, ValueError) as e:
        raise HTTPException(status_code=422, detail="Invalid execution id") from e


@router.put("/executions/{execution_id}")
def put_execution(request: Request, execution_id: str, body: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    """Update one execution by account_executions_id (manual correction). Negative ids = TWS raw rows."""
    eid = _parse_account_executions_path_id(execution_id)
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return {"ok": False, "error": "PostgreSQL is required to write account_executions."}
    if update_one_execution(control_via_db, eid, body):
        return {"ok": True, "message": "Execution record updated."}
    return {"ok": False, "error": "Update failed (account_executions_id missing or database error)."}


@router.delete("/executions/{execution_id}")
def delete_execution(request: Request, execution_id: str) -> Dict[str, Any]:
    """Delete one execution by account_executions_id. Negative ids = TWS raw rows."""
    eid = _parse_account_executions_path_id(execution_id)
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return {"ok": False, "error": "PostgreSQL is required to write account_executions."}
    if delete_one_execution(control_via_db, eid):
        return {"ok": True, "message": "Execution record deleted."}
    return {"ok": False, "error": "Delete failed (account_executions_id missing or database error)."}


@router.post("/executions/fetch")
async def post_executions_fetch(
    request: Request,
    days: int = Query(1, ge=1, le=7, description="1=today, 3=last 3 days, 7=last 7 days (TWS Trade Log)"),
) -> Dict[str, Any]:
    """Fetch executions from IB via IB Gateway and write to account_executions."""
    app = request.app
    reader = app.state.reader
    cfg = reader._config
    control_via_db = app.state.control_via_db
    if not control_via_db:
        return {"ok": False, "error": "PostgreSQL is required to write account_executions.", "count": 0}
    if not getattr(app.state, "monitor_enabled", True):
        return {"ok": False, "error": "Monitor stopped; cannot fetch executions.", "count": 0}
    gw = getattr(app.state, "ib_operator_client", None)
    if gw is None:
        return {"ok": False, "error": "IB Gateway client is not configured.", "count": 0}
    env = await gw.request_async(
        "fetch_executions",
        {"days": days, "account_slot": "primary"},
        caller="trading_executions_fetch",
    )
    if not env.get("ok"):
        err = str(env.get("error") or "IB gateway error")
        await asyncio.to_thread(
            _publish_tws_fetch_system_message,
            cfg,
            ok=False,
            title="TWS executions fetch failed",
            message=f"Primary slot: {err}",
            reason=err,
            level="error",
        )
        return {"ok": False, "error": err, "count": 0, "days": days, "fetched_primary": 0, "fetched_secondary": 0, "fetched_total": 0}
    data = env.get("data") or {}
    primary_execs = list(data.get("executions") or [])
    fetched_primary = len(primary_execs)
    all_execs = primary_execs
    fetched_secondary = 0
    secondary_error: Optional[str] = None
    from bifrost_core.config.startup import get_effective_ib_config

    try:
        ibc = get_effective_ib_config(reader._config)
        if (ibc.get("ib2_host") or "").strip():
            env2 = await gw.request_async(
                "fetch_executions",
                {"days": days, "account_slot": "secondary"},
                caller="trading_executions_fetch",
            )
            if env2.get("ok"):
                d2 = env2.get("data") or {}
                ex2 = list(d2.get("executions") or [])
                fetched_secondary = len(ex2)
                if ex2:
                    all_execs = (all_execs or []) + ex2
            else:
                secondary_error = str(env2.get("error") or "secondary fetch failed")
                logger.warning("executions/fetch secondary: %s", secondary_error)
    except Exception as e2:
        logger.warning("executions/fetch secondary check: %s", e2)
        secondary_error = str(e2)
    fetched_total = len(all_execs)
    if not all_execs:
        msg = (
            f"IB returned no executions (range: last {days} day(s); for a multi-day range ensure TWS Trade Log includes those days)."
        )
        if secondary_error:
            msg = f"{msg} Secondary slot error: {secondary_error}"
        detail_parts = [
            f"days={days}",
            f"fetched_primary={fetched_primary}",
            f"fetched_secondary={fetched_secondary}",
            f"fetched_total=0",
        ]
        if secondary_error:
            detail_parts.append(f"secondary_error={secondary_error}")
        await asyncio.to_thread(
            _publish_tws_fetch_system_message,
            cfg,
            ok=True,
            title="TWS executions fetch: no rows",
            message=msg + " " + " ".join(detail_parts),
            reason=None,
            level="warning",
        )
        out: Dict[str, Any] = {
            "ok": True,
            "message": msg,
            "count": 0,
            "days": days,
            "fetched_primary": fetched_primary,
            "fetched_secondary": fetched_secondary,
            "fetched_total": 0,
        }
        if secondary_error:
            out["secondary_error"] = secondary_error
        return out

    stats_out: Dict[str, Any] = {}
    if not write_account_executions_to_db(control_via_db, all_execs, stats_out=stats_out):
        await asyncio.to_thread(
            _publish_tws_fetch_system_message,
            cfg,
            ok=False,
            title="TWS executions write failed",
            message="PostgreSQL write failed after IB returned executions.",
            reason="write_account_executions_to_db",
            level="error",
        )
        return {
            "ok": False,
            "error": "Failed to write account_executions.",
            "count": 0,
            "days": days,
            "fetched_primary": fetched_primary,
            "fetched_secondary": fetched_secondary,
            "fetched_total": fetched_total,
        }

    ins = int(stats_out.get("tws_raw_inserted") or 0)
    skip = int(stats_out.get("tws_raw_skipped_duplicate") or 0)
    missing_raw = bool(stats_out.get("tws_raw_missing_table"))
    ins_ids = list(stats_out.get("tws_raw_inserted_ids") or [])
    upd_ids = list(stats_out.get("tws_raw_updated_ids") or [])
    sk_ids = list(stats_out.get("tws_raw_skipped_ids") or [])
    msg = (
        f"Fetched {fetched_total} execution(s) from IB (primary {fetched_primary}, secondary {fetched_secondary}). "
        f"executions_raw_tws: inserted {ins} row(s), ids {_fmt_db_id_list(ins_ids)}; "
        f"updated {len(upd_ids)} row(s), ids {_fmt_db_id_list(upd_ids)} (TWS path is insert-or-skip, usually 0); "
        f"skipped duplicate exec_id: {skip}, existing row ids {_fmt_db_id_list(sk_ids)}."
    )
    if missing_raw:
        msg += " (executions_raw_tws missing or unavailable; stats may be incomplete.)"
    if secondary_error:
        msg += f" Secondary slot error (merged primary only): {secondary_error}"

    await asyncio.to_thread(
        _publish_tws_fetch_system_message,
        cfg,
        ok=True,
        title="TWS executions imported",
        message=msg,
        reason=None,
        level="success",
    )

    result: Dict[str, Any] = {
        "ok": True,
        "count": fetched_total,
        "days": days,
        "fetched_primary": fetched_primary,
        "fetched_secondary": fetched_secondary,
        "fetched_total": fetched_total,
        "tws_raw_inserted": ins,
        "tws_raw_skipped_duplicate": skip,
        "tws_raw_missing_table": missing_raw,
        "tws_raw_inserted_ids": ins_ids,
        "tws_raw_updated_ids": upd_ids,
        "tws_raw_skipped_ids": sk_ids,
        "message": msg,
    }
    if secondary_error:
        result["secondary_error"] = secondary_error
    return result
