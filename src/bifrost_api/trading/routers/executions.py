"""Executions and transactions: CRUD, IB TWS fetch, performance.

Flex ingest (trades / cash / XML / config write) is served by Flex Query Plugin
(``POST /flex/ingest/trigger``, ``POST /flex/ingest/upload-xml``, ``POST /flex/config/write``).
``GET /transactions`` remains here to read already-ingested cash rows.

Failures answer a real status with ``{"detail"}`` and lists answer
``{"items", "count", ...}`` (``bifrost_api.common.envelopes``, TD-16/17; the old
list keys went in 0.4.0).

TD-15 (batch 3b-2): ``PATCH /executions/{id}/attribution`` changes only the
strategy attribution (the fill's own columns stay with PUT), and the two DELETEs
are strict; both raise core's Write* outcomes, mapped in
``bifrost_api.common.write_errors``. PUT keeps its old behaviour for one release
and is marked replaced by the PATCH.

TD-24 (batch 3c-1): the POST / PUT bodies are ``trading.schemas.requests`` models --
a wrong type is 422 and writes nothing (a malformed strategy id no longer clears the
attribution); unknown fields are ignored and logged this release.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import StrictInt

from bifrost_api.common.envelopes import error_response, list_body
from bifrost_api.common.write_errors import PatchBody, deleted_body, write_target
from bifrost_api.trading.schemas.requests import (
    ExecutionCreateBody,
    ExecutionUpdateBody,
    OptionStockLinkBody,
    OptionStockLinksQueryBody,
)
from bifrost_core.portfolio.gateway_fills import execution_rows_from_gateway_fills
from bifrost_core.portfolio.reader import accounts as accounts_module
from bifrost_core.portfolio.reader.option_stock_link import (
    delete_option_stock_link_strict,
    insert_option_stock_link,
)
from bifrost_core.monitor.reader import (
    write_account_executions_to_db,
    insert_one_execution,
    update_one_execution,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["executions"])

PG_REQUIRED_FOR_EXECUTIONS = "PostgreSQL is required to write account_executions."


class ExecutionAttributionPatch(PatchBody):
    """Strategy attribution of one execution: a trade, or quantity splits -- not both. The
    opportunity is the trade's (core 0.37.0): sent alone it is 400, sent with a trade it must match."""

    strategy_opportunity_id: Optional[StrictInt] = None
    strategy_instance_id: Optional[StrictInt] = None
    # [{strategy_instance_id, allocated_quantity}], replaced whole; [] removes the splits.
    instance_allocations: Optional[List[Dict[str, Any]]] = None

# What core's option-stock link readers and writers answer, by status. Core returns a
# message rather than a reason code, so the route sorts by the message
# (core portfolio/reader/option_stock_link.py, monitor/reader/common.py).
_LINK_UNAVAILABLE = ("PostgreSQL is required.", "database_unavailable")
_LINK_NOT_FOUND = ("not found", "Could not resolve underlying symbol.")
_LINK_CONFLICT = ("Link already exists",)
_LINK_BAD_INPUT = (
    "account_id is required.",
    "account_id required",
    "are required.",
    "role must be",
    "account_id mismatch.",
    "must refer to an OPT row.",
    "must refer to an STK row.",
    "does not match stock symbol",
    "Not an OPT row.",
    "Invalid link id.",
    "invalid option_account_executions_id",
)


def _link_error_status(err: str) -> int:
    """Status for a link reader/writer message: 503, 404, 409, 400, else 500 (DB error text)."""
    if any(err.startswith(m) for m in _LINK_UNAVAILABLE):
        return 503
    if any(m in err for m in _LINK_NOT_FOUND):
        return 404
    if any(err.startswith(m) for m in _LINK_CONFLICT):
        return 409
    if any(m in err for m in _LINK_BAD_INPUT):
        return 400
    return 500


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
        description=(
            "Which execution view to read. all (default): brokerage.executions, every fill | "
            "performance_book: brokerage.executions_final, the Flex-confirmed book | "
            "on_the_fly: brokerage.executions_fly, TWS fills Flex has not confirmed yet (no BAG) | "
            "tws_raw: brokerage.executions_raw_tws only, with synthetic negative ids. "
            "quantity is signed from side alone: SELL / SLD / S negative, anything else positive, "
            "whatever the source or the stored sign (core 0.35.0, TD-30) -- except under tws_raw, "
            "which returns the stored TWS quantity unchanged."
        ),
    ),
) -> Dict[str, Any]:
    """Account-level executions/trades (R-A2). If include_opt_pairs=true: returns paired_execution_ids and opt_pairs."""
    reader = request.app.state.reader
    effective_limit: Optional[int] = limit if limit > 0 else None
    if include_opt_pairs:
        paired = dict(
            reader.get_executions_with_opt_pairs(
                since_ts=since_ts,
                until_ts=until_ts,
                account_id=account_id,
                limit=effective_limit or 5000,
                strategy_opportunity_id=strategy_opportunity_id,
                strategy_instance_id=strategy_instance_id,
                source_scope=source_scope,
            )
            or {}
        )
        rows = paired.pop("executions", None) or []
        return list_body(rows, **paired)
    items = reader.get_executions(
        since_ts=since_ts,
        until_ts=until_ts,
        account_id=account_id,
        limit=effective_limit,
        strategy_opportunity_id=strategy_opportunity_id,
        strategy_instance_id=strategy_instance_id,
        source_scope=source_scope,
    )
    return list_body(items)


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
    return list_body(items)


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
) -> Any:
    """Existing account_executions rows to link strategy attribution (no insert)."""
    reader = request.app.state.reader
    if not (contract_key and contract_key.strip()) and (
        not (symbol and symbol.strip()) or strike is None or expiry is None or str(expiry).strip() == ""
    ):
        return error_response(
            400,
            "Provide contract_key, or symbol+expiry+strike for fallback matching.",
        )
    items = reader.get_executions_for_strategy_link(
        account_id=account_id.strip(),
        contract_key=(contract_key or "").strip() or None,
        symbol=(symbol or "").strip() or None,
        expiry=expiry,
        strike=strike,
        option_right=(option_right or "").strip() or None,
        limit=limit,
    )
    return list_body(items)


@router.post("/executions/option-stock-links/query")
def post_option_stock_links_query(request: Request, body: OptionStockLinksQueryBody) -> Any:
    """Bulk load link rows for many option_account_executions_id values (grouped by account_id).

    Body: { "batches": [ { "account_id": "...", "option_account_executions_ids": [1, 2, ...] }, ... ] }
    Returns: { "by_option_id": { "<id>": { "links": [...], "slippage_total": number | null } } }
    """
    reader = request.app.state.reader
    if body.batches is None:
        return error_response(400, "batches must be a list")
    batches: List[Any] = []
    for item in body.batches:
        acc = (item.account_id or "").strip()
        if not acc or item.option_account_executions_ids is None:
            continue
        batches.append((acc, list(item.option_account_executions_ids)))
    out = reader.get_option_stock_links_bulk(batches)
    err = out.get("error")
    if err:
        return error_response(_link_error_status(str(err)), str(err))
    return out


@router.get("/executions/option-stock-links")
def get_option_stock_links_route(
    request: Request,
    account_id: str = Query(..., description="IB account id"),
    option_account_executions_id: int = Query(..., description="Unified account_executions_id of the OPT leg"),
) -> Any:
    """List stock legs linked to an option execution; includes slippage_vs_close per row and slippage_total."""
    reader = request.app.state.reader
    out = reader.get_option_stock_links(account_id.strip(), option_account_executions_id)
    err = out.get("error")
    if err:
        return error_response(
            _link_error_status(str(err)), str(err)
        )
    return list_body(out.get("links"), slippage_total=out.get("slippage_total"))


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
) -> Any:
    """STK executions in performance book matching option underlying; excludes already-linked rows for this option."""
    reader = request.app.state.reader
    out = dict(
        reader.get_stock_link_candidates(
            account_id.strip(),
            option_account_executions_id,
            trade_date_from=trade_date_from,
            trade_date_to=trade_date_to,
            limit=limit,
        )
        or {}
    )
    err = out.get("error")
    if err:
        return error_response(_link_error_status(str(err)), str(err))
    rows = out.pop("executions", None) or []
    return list_body(rows, **out)


@router.post("/executions/option-stock-links")
def post_option_stock_links(request: Request, body: OptionStockLinkBody) -> Any:
    """Link one OPT execution to one STK execution (both must exist on account_executions_final)."""
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return error_response(503, "PostgreSQL is required.")
    ok, link_id, err, warning = insert_option_stock_link(control_via_db, body.declared(exclude_unset=True))
    if not ok:
        msg = str(err or "Failed to link the stock execution.")
        return error_response(_link_error_status(msg), msg)
    return {"ok": True, "link_id": link_id, "error": None, "warning": warning}


@router.delete("/executions/option-stock-links/{link_id}")
def delete_option_stock_links_route(
    request: Request,
    link_id: str,
    account_id: str = Query(..., description="Must match link row account_id"),
) -> Any:
    """Hard delete. 404 when there is no such link on that account."""
    try:
        lid = int(str(link_id).strip())
    except (TypeError, ValueError):
        raise HTTPException(status_code=422, detail="Invalid link id") from None
    config = write_target(request, f"option/stock link {lid}")
    return deleted_body(delete_option_stock_link_strict(config, lid, account_id))


@router.get("/executions/freshness")
def get_executions_freshness(request: Request) -> Dict[str, Any]:
    """Execution data freshness per (account_id, source). Latest exec_time and days_since_latest."""
    reader = request.app.state.reader
    items = reader.get_executions_freshness()
    return list_body(items)


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
    return list_body(items)


@router.post("/executions")
def post_execution(request: Request, body: ExecutionCreateBody) -> Any:
    """Add one execution record manually (history). body: account_id, time, symbol, sec_type, side, quantity, price; optional fields."""
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return error_response(503, PG_REQUIRED_FOR_EXECUTIONS)
    # The inputs core's writer refuses before it writes (portfolio/reader/accounts.py
    # insert_one_execution) answer 400; so does a refusal when splits were sent, which
    # core also rejects with one None (wrong account, duplicate instance, sum != quantity).
    if body.quantity is None or body.price is None:
        return error_response(
            400, "Failed to add execution (required fields: symbol, quantity, price)."
        )
    new_account_executions_id = insert_one_execution(control_via_db, body.declared(exclude_unset=True))
    if new_account_executions_id is None:
        if body.instance_allocations:
            return error_response(
                400, "Failed to add execution (instance_allocations rejected or database error)."
            )
        return error_response(500, "Failed to add execution (database error).")
    return {"ok": True, "account_executions_id": new_account_executions_id, "message": "Execution record added."}


def _parse_account_executions_path_id(execution_id: str) -> int:
    try:
        return int(str(execution_id).strip())
    except (TypeError, ValueError) as e:
        raise HTTPException(status_code=422, detail="Invalid execution id") from e


@router.put("/executions/{execution_id}")
def put_execution(request: Request, execution_id: str, body: ExecutionUpdateBody) -> Any:
    """Update one execution by account_executions_id (manual correction): the fields sent
    change. Negative ids = TWS raw rows."""
    eid = _parse_account_executions_path_id(execution_id)
    control_via_db = request.app.state.control_via_db
    if not control_via_db:
        return error_response(503, PG_REQUIRED_FOR_EXECUTIONS)
    if update_one_execution(control_via_db, eid, body.declared(exclude_unset=True)):
        return {"ok": True, "message": "Execution record updated."}
    if body.instance_allocations:
        return error_response(
            400, "Update failed (instance_allocations rejected, account_executions_id missing, or database error)."
        )
    # Core answers False both for a missing row and for a database error (it logs the
    # latter); a missing row is the one a caller reaches, so 404.
    return error_response(404, "Update failed (account_executions_id missing or database error).")


@router.patch("/executions/{execution_id}/attribution")
def patch_execution_attribution(request: Request, execution_id: str, body: ExecutionAttributionPatch) -> Any:
    """Change one execution's strategy attribution; answer its attribution fields
    (account_executions_id, account_id, the two ids, instance_allocations).

    `strategy_instance_id: null` clears the whole-fill attribution. A trade on an execution
    that has splits is 409 unless the same patch sends `instance_allocations: []`. The
    instance must be on the execution's account (400). An opportunity without a trade is
    400; with one it must be the trade's (400). Written to this environment's
    strategy_instance_execution by the fill (account_id, exec_id), so a TWS row and its
    Flex twin change together (core 0.37.0, TD-09). Negative ids are TWS raw rows."""
    eid = _parse_account_executions_path_id(execution_id)
    config = write_target(request, f"execution {eid}")
    return accounts_module.patch_execution(config, eid, body.patch_fields())


@router.delete("/executions/{execution_id}")
def delete_execution(request: Request, execution_id: str) -> Any:
    """Hard-delete one execution: its Golden Source raw row, its splits, and its commission
    when no other raw row carries the exec_id. 409 while an option/stock link names it.
    Negative ids are TWS raw rows."""
    eid = _parse_account_executions_path_id(execution_id)
    config = write_target(request, f"execution {eid}")
    return deleted_body(accounts_module.delete_execution_strict(config, eid))


@router.post("/executions/fetch")
async def post_executions_fetch(
    request: Request,
    days: int = Query(1, ge=1, le=7, description="1=today, 3=last 3 days, 7=last 7 days (TWS Trade Log)"),
) -> Any:
    """Fetch executions from IB via IB Gateway and write to account_executions.

    Reads fills only. 503 when Postgres, the monitor flag or the gateway client is
    missing, or the gateway answered an error; 500 when the write failed.

    The plugin's fills (``account`` / ``shares`` / ``ts``) are mapped to the writer's row
    by core ``portfolio.gateway_fills`` (api 0.3.3). A fill that cannot be written whole --
    no account or quantity, or an option fill without expiry / strike / right, which the
    plugin does not send yet -- is not written; ``skipped_incomplete`` counts them and
    ``skipped_exec_ids`` names them.
    """
    app = request.app
    reader = app.state.reader
    cfg = reader.config
    control_via_db = app.state.control_via_db
    if not control_via_db:
        return error_response(503, PG_REQUIRED_FOR_EXECUTIONS)
    if not getattr(app.state, "monitor_enabled", True):
        return error_response(503, "Monitor stopped; cannot fetch executions.")
    gw = getattr(app.state, "ib_operator_client", None)
    if gw is None:
        return error_response(503, "IB Gateway client is not configured.")
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
        return error_response(
            503,
            err,
        )
    data = env.get("data") or {}
    primary_execs = list(data.get("executions") or [])
    fetched_primary = len(primary_execs)
    all_execs = primary_execs
    fetched_secondary = 0
    secondary_error: Optional[str] = None
    from bifrost_core.config.startup import get_effective_ib_config

    try:
        ibc = get_effective_ib_config(reader.config)
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
            "fetched_total=0",
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

    rows, refused = execution_rows_from_gateway_fills(all_execs)
    skipped_ids = [r.get("exec_id") for r in refused]
    if refused:
        logger.warning(
            "executions/fetch: %s fill(s) not written (incomplete): %s",
            len(refused),
            "; ".join(f"{r.get('exec_id')!r} missing {','.join(r['missing'])}" for r in refused[:20]),
        )
    skipped_note = (
        f" Not written (incomplete fill: no account / quantity, or an option without expiry / strike / right): "
        f"{len(refused)}, exec_ids [{', '.join(str(x) for x in skipped_ids[:20])}"
        f"{', ...' if len(skipped_ids) > 20 else ''}]."
        if refused
        else ""
    )
    if not rows:
        msg = f"IB returned {fetched_total} execution(s); none could be written.{skipped_note}"
        await asyncio.to_thread(
            _publish_tws_fetch_system_message,
            cfg,
            ok=True,
            title="TWS executions fetch: nothing written",
            message=msg,
            reason=None,
            level="warning",
        )
        out_none: Dict[str, Any] = {
            "ok": True,
            "message": msg,
            "count": fetched_total,
            "days": days,
            "fetched_primary": fetched_primary,
            "fetched_secondary": fetched_secondary,
            "fetched_total": fetched_total,
            "skipped_incomplete": len(refused),
            "skipped_exec_ids": skipped_ids,
        }
        if secondary_error:
            out_none["secondary_error"] = secondary_error
        return out_none

    stats_out: Dict[str, Any] = {}
    if not write_account_executions_to_db(control_via_db, rows, stats_out=stats_out):
        await asyncio.to_thread(
            _publish_tws_fetch_system_message,
            cfg,
            ok=False,
            title="TWS executions write failed",
            message="PostgreSQL write failed after IB returned executions.",
            reason="write_account_executions_to_db",
            level="error",
        )
        return error_response(
            500,
            "Failed to write account_executions.",
        )

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
    msg += skipped_note
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
        "skipped_incomplete": len(refused),
        "skipped_exec_ids": skipped_ids,
        "message": msg,
    }
    if secondary_error:
        result["secondary_error"] = secondary_error
    return result
