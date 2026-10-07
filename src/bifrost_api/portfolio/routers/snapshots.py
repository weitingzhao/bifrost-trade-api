"""GET /portfolio/nav-history, /portfolio/position-snapshots, /portfolio/pnl-attribution (api 0.12.0).

The daily book snapshots (``account_nav_daily`` / ``position_snapshot_daily``, written nightly by
core's ``portfolio.snapshot`` job) read through core's ``portfolio.reader.snapshots``
(TD-138 / TD-139). Read-only. Behind the gateway: ``/api/account/portfolio/...``.

* ``nav-history`` -- closing NAV per session and account. A row read before its session's close
  (written before core 0.52.0) is not a closing balance: it is left out of ``items`` and listed in
  ``dropped`` with the reason.
* ``position-snapshots`` -- the positions of each session in the range (the latest session when
  no bound is sent), each option row with ``greeks_quality`` (``vendor`` | ``degraded`` |
  ``missing``, derived on read) and its reason, plus ``trades``: one rollup per session and
  ``trade_id`` (null = Unattributed).
* ``pnl-attribution`` -- each session against the session before it: held P&L of the book as
  it stood at the prior close, split into delta / gamma / vega / theta from that close's vendor
  Greeks, ``unexplained`` the rest; per row, per trade, per underlying, per session and in total.
  A session whose prior session has no snapshot reads ``no_prior_snapshot``.

Ranges: ``from_date`` / ``to_date`` (``YYYY-MM-DD``, inclusive). A reversed range, a bad date or
a range over core's cap is 400; a database that cannot be read is 503 (``ReadFailed``).
"""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query, Request

from bifrost_api.common.envelopes import list_body
from bifrost_api.common.query_vocab import from_date_query, to_date_query, trade_id_query

router = APIRouter(tags=["portfolio-snapshots"])

_ACCOUNT = Query(None, description="One IB account id; omit for every account")


def _day(value: Optional[str], name: str) -> Optional[date]:
    if value is None or not str(value).strip():
        return None
    try:
        return date.fromisoformat(str(value).strip()[:10])
    except ValueError:
        raise HTTPException(status_code=400, detail=f"{name} must be YYYY-MM-DD") from None


def _range(from_date: Optional[str], to_date: Optional[str]) -> Dict[str, Optional[date]]:
    return {"from_date": _day(from_date, "from_date"), "to_date": _day(to_date, "to_date")}


def _read(fn: Any, **kwargs: Any) -> Dict[str, Any]:
    try:
        return fn(**kwargs)
    except ValueError as e:  # core: reversed or over-long range
        raise HTTPException(status_code=400, detail=str(e)) from None


@router.get("/portfolio/nav-history")
def get_nav_history(
    request: Request,
    account_id: Optional[str] = _ACCOUNT,
    from_date: Optional[str] = from_date_query("snapshot_date"),
    to_date: Optional[str] = to_date_query("snapshot_date"),
) -> Dict[str, Any]:
    """Closing NAV per session and account (``account_nav_daily``); intraday rows in ``dropped``."""
    reader = request.app.state.reader
    out = _read(reader.get_nav_history, account_id=account_id, **_range(from_date, to_date))
    return list_body(out["items"], dropped=out["dropped"], sessions=out["sessions"])


@router.get("/portfolio/position-snapshots")
def get_position_snapshots(
    request: Request,
    account_id: Optional[str] = _ACCOUNT,
    from_date: Optional[str] = from_date_query("snapshot_date", note="Omit both bounds for the latest session."),
    to_date: Optional[str] = to_date_query("snapshot_date"),
    trade_id: Optional[int] = trade_id_query(),
) -> Dict[str, Any]:
    """Positions per session with ``greeks_quality`` per option row and a rollup per trade."""
    reader = request.app.state.reader
    out = _read(
        reader.get_position_snapshots, account_id=account_id, trade_id=trade_id, **_range(from_date, to_date)
    )
    return list_body(
        out["items"], trades=out["trades"], sessions=out["sessions"], greeks_quality=out["greeks_quality"]
    )


@router.get("/portfolio/pnl-attribution")
def get_pnl_attribution(
    request: Request,
    account_id: Optional[str] = _ACCOUNT,
    from_date: Optional[str] = from_date_query("snapshot_date", note="Omit both bounds for the latest session."),
    to_date: Optional[str] = to_date_query("snapshot_date"),
    trade_id: Optional[int] = trade_id_query(),
) -> Dict[str, Any]:
    """Each session against its prior session: delta / gamma / vega / theta and the unexplained rest."""
    reader = request.app.state.reader
    out = _read(
        reader.get_pnl_attribution, account_id=account_id, trade_id=trade_id, **_range(from_date, to_date)
    )
    return list_body(
        out["items"],
        sessions=out["sessions"],
        by_trade=out["by_trade"],
        by_symbol=out["by_symbol"],
        totals=out["totals"],
    )
