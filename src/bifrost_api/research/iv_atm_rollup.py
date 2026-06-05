"""Upsert report_option_atm_iv_daily from option_snapshots (Massive worker / maintenance)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from bifrost_api.research.iv_atm import (
    build_cone_key_maps,
    eod_atm_report_rows_for_expiration,
    group_hist_rows_by_snap_day,
)
from bifrost_worker.data.massive.vendor.reader import get_option_snapshots_eod_per_day


def upsert_report_atm_iv_daily_rows(
    conn: Any,
    symbol: str,
    expiry: str,
    source: str,
    rows: List[Dict[str, Any]],
) -> int:
    """Insert or update daily ATM IV rows. Returns rows written."""
    if not rows:
        return 0
    sym = (symbol or "").strip().upper()
    exp = (expiry or "").strip()
    src = (source or "massive").strip().lower()
    if src not in ("massive", "ib"):
        src = "massive"
    n = 0
    with conn.cursor() as cur:
        for r in rows:
            td = r.get("trade_date")
            if td is None:
                continue
            atm = r.get("atm_iv")
            if atm is None:
                continue
            cur.execute(
                """
                INSERT INTO report_option_atm_iv_daily (
                  symbol, expiry, trade_date, source,
                  atm_iv, iv_call, iv_put, strike, underlying_price, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (symbol, expiry, trade_date, source)
                DO UPDATE SET
                  atm_iv = EXCLUDED.atm_iv,
                  iv_call = EXCLUDED.iv_call,
                  iv_put = EXCLUDED.iv_put,
                  strike = EXCLUDED.strike,
                  underlying_price = EXCLUDED.underlying_price,
                  created_at = now()
                """,
                (
                    sym,
                    exp,
                    td,
                    src,
                    float(atm),
                    r.get("iv_call"),
                    r.get("iv_put"),
                    r.get("strike"),
                    r.get("underlying_price"),
                ),
            )
            n += 1
    return n


def rebuild_report_atm_iv_daily_for_symbol_expiry(
    status_cfg: dict,
    conn: Any,
    symbol: str,
    expiry_yyyymmdd: str,
    source: str,
    lookback_days: int,
    last_price: float,
) -> int:
    """
    Recompute daily ATM IV series from option_snapshots EOD rows and upsert into report_option_atm_iv_daily.
    Strike grid uses last_price (same as GET /research/iv-volatility-cone).
    """
    sym = (symbol or "").strip().upper()
    exp = (expiry_yyyymmdd or "").strip()
    if not sym or len(exp) != 8 or not exp.isdigit():
        return 0
    if not (last_price and last_price > 0):
        return 0
    src = (source or "massive").strip().lower()
    if src not in ("massive", "ib"):
        src = "massive"
    lb = max(1, min(90, int(lookback_days)))
    _, wide_keys, _, key_exp_wide = build_cone_key_maps(sym, [exp], float(last_price))
    if not wide_keys:
        return 0
    since_ts = datetime.now(timezone.utc) - timedelta(days=lb)
    hist_rows = get_option_snapshots_eod_per_day(
        status_cfg, wide_keys, source=src, since_ts=since_ts
    )
    by_day = group_hist_rows_by_snap_day(hist_rows)
    report_rows = eod_atm_report_rows_for_expiration(exp, key_exp_wide, by_day)
    return upsert_report_atm_iv_daily_rows(conn, sym, exp, src, report_rows)


def norm_expiry_yyyymmdd(raw: str) -> Optional[str]:
    s = (raw or "").strip()
    if len(s) >= 10 and s[4] == "-":
        return s[:4] + s[5:7] + s[8:10]
    if len(s) >= 8 and s[:8].isdigit():
        return s[:8]
    return None
