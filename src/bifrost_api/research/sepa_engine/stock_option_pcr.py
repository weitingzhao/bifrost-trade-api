"""Stock Inspector: put/call ratio trend + option chain rollup by expiry."""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


def _is_put(right: str) -> bool:
    r = (right or "").strip().upper()
    return r in ("P", "PUT")


def _is_call(right: str) -> bool:
    r = (right or "").strip().upper()
    return r in ("C", "CALL")


def _parse_expiry_date(expiry: Any) -> Optional[date]:
    if expiry is None:
        return None
    if isinstance(expiry, date) and not isinstance(expiry, datetime):
        return expiry
    s = str(expiry).strip()
    if not s:
        return None
    if len(s) >= 10 and s[4] == "-":
        try:
            return date.fromisoformat(s[:10])
        except ValueError:
            pass
    digits = s.replace("-", "")[:8]
    if len(digits) == 8 and digits.isdigit():
        try:
            return date(int(digits[:4]), int(digits[4:6]), int(digits[6:8]))
        except ValueError:
            return None
    return None


def _format_expiry_label(expiry: Any) -> str:
    d = _parse_expiry_date(expiry)
    if d is None:
        return str(expiry or "—")
    return f"{d.month:02d}/{d.day:02d}/{str(d.year)[2:]}"


def _third_friday_of_month(d: date) -> bool:
    """US equity monthly option expiry is typically the third Friday."""
    if d.weekday() != 4:
        return False
    first = date(d.year, d.month, 1)
    days_until_friday = (4 - first.weekday()) % 7
    first_friday = first + timedelta(days=days_until_friday)
    third = first_friday + timedelta(days=14)
    return d == third


def _expiry_suffix(expiry: Any, _today: date) -> str:
    d = _parse_expiry_date(expiry)
    if d is None:
        return ""
    if _third_friday_of_month(d):
        return " (m)"
    if d.weekday() == 4:
        return " (w)"
    return " (m)"


def _safe_ratio(num: float, den: float) -> Optional[float]:
    if den <= 0 or num < 0:
        return None
    return round(num / den, 3)


def _snapshot_oi_trend_by_day(cur, sym: str, lb: int) -> Dict[str, Tuple[int, int]]:
    """Daily put/call OI from chain snapshots when option_open_interest_daily is sparse."""
    try:
        cur.execute(
            """
            WITH snap AS (
              SELECT DISTINCT ON (oc.contract_key, os.day_last_updated_day)
                oc.option_right,
                COALESCE(os.open_interest, 0)::bigint AS open_interest,
                os.day_last_updated_day::date AS trade_date
              FROM option_contracts oc
              INNER JOIN option_snapshots os
                ON os.contract_key = oc.contract_key AND os.source = 'massive'
              WHERE UPPER(TRIM(oc.symbol)) = %s
                AND os.day_last_updated_day IS NOT NULL
                AND os.day_last_updated_day >= CURRENT_DATE - %s::integer
              ORDER BY oc.contract_key, os.day_last_updated_day, os.snapshot_ts DESC
            )
            SELECT trade_date,
                   SUM(CASE WHEN UPPER(TRIM(option_right)) IN ('P', 'PUT')
                       THEN open_interest ELSE 0 END)::bigint AS put_oi,
                   SUM(CASE WHEN UPPER(TRIM(option_right)) IN ('C', 'CALL')
                       THEN open_interest ELSE 0 END)::bigint AS call_oi
            FROM snap
            GROUP BY trade_date
            ORDER BY trade_date ASC
            """,
            (sym, lb),
        )
    except Exception as ex:
        logger.debug("symbol option pcr snapshot OI trend skipped: %s", ex)
        try:
            cur.connection.rollback()
        except Exception:
            pass
        return {}

    out: Dict[str, Tuple[int, int]] = {}
    for row in cur.fetchall() or []:
        td_key = _serialize_date(row.get("trade_date"))
        if not td_key:
            continue
        out[td_key] = (int(row.get("put_oi") or 0), int(row.get("call_oi") or 0))
    return out


def _merge_oi_trend_points(
    daily_by_day: Dict[str, Tuple[int, int]],
    snapshot_by_day: Dict[str, Tuple[int, int]],
) -> List[Dict[str, Any]]:
    """Prefer EOD option_open_interest_daily; fill gaps from snapshot rollups."""
    all_dates = sorted(set(daily_by_day) | set(snapshot_by_day))
    trend: List[Dict[str, Any]] = []
    for td_key in all_dates:
        if td_key in daily_by_day:
            put_oi, call_oi = daily_by_day[td_key]
        else:
            put_oi, call_oi = snapshot_by_day[td_key]
        trend.append(
            {
                "trade_date": td_key,
                "put_oi": put_oi,
                "call_oi": call_oi,
                "oi_ratio": _safe_ratio(float(put_oi), float(call_oi)),
                "put_vol": None,
                "call_vol": None,
                "vol_ratio": None,
            }
        )
    return trend


def _serialize_date(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, datetime):
        return v.date().isoformat()
    s = str(v).strip()
    return s[:10] if len(s) >= 10 else s or None


def fetch_symbol_option_pcr(
    status_config: dict,
    symbol: str,
    *,
    lookback_days: int = 365,
) -> Dict[str, Any]:
    """Aggregate OI/volume ratios and per-expiry chain stats for Stock Inspector."""
    import psycopg2
    from psycopg2.extras import RealDictCursor

    from bifrost_core.persistence.postgres.connection import _get_conn_params

    if not status_config or (
        status_config.get("sink") != "postgres" and not status_config.get("postgres")
    ):
        return {"ok": False, "error": "PostgreSQL not configured"}

    sym = (symbol or "").strip().upper()
    if not sym:
        return {"ok": False, "error": "symbol is required"}

    lb = max(30, min(int(lookback_days), 400))
    today = date.today()

    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SET statement_timeout = 30000")

                trend: List[Dict[str, Any]] = []
                as_of_date: Optional[date] = None
                oi_basis = "option_open_interest_daily"
                daily_oi_by_day: Dict[str, Tuple[int, int]] = {}

                cur.execute(
                    """
                    SELECT trade_date::date AS trade_date,
                           SUM(CASE WHEN UPPER(TRIM(option_right)) IN ('P', 'PUT')
                               THEN COALESCE(open_interest, 0) ELSE 0 END)::bigint AS put_oi,
                           SUM(CASE WHEN UPPER(TRIM(option_right)) IN ('C', 'CALL')
                               THEN COALESCE(open_interest, 0) ELSE 0 END)::bigint AS call_oi
                    FROM option_open_interest_daily
                    WHERE symbol = %s AND source = 'massive'
                      AND trade_date >= (
                        SELECT COALESCE(MAX(trade_date), CURRENT_DATE) - %s::integer
                        FROM option_open_interest_daily
                        WHERE symbol = %s AND source = 'massive'
                      )
                    GROUP BY trade_date
                    ORDER BY trade_date ASC
                    """,
                    (sym, lb, sym),
                )
                for row in cur.fetchall() or []:
                    td = row.get("trade_date")
                    if td is None:
                        continue
                    td_key = _serialize_date(td)
                    if not td_key:
                        continue
                    put_oi = int(row.get("put_oi") or 0)
                    call_oi = int(row.get("call_oi") or 0)
                    daily_oi_by_day[td_key] = (put_oi, call_oi)
                    if as_of_date is None or (isinstance(td, date) and td > as_of_date):
                        as_of_date = td if isinstance(td, date) else as_of_date

                snapshot_oi_by_day = _snapshot_oi_trend_by_day(cur, sym, lb)
                if snapshot_oi_by_day:
                    if daily_oi_by_day and len(snapshot_oi_by_day) > len(daily_oi_by_day):
                        oi_basis = "option_open_interest_daily+option_snapshots"
                    elif not daily_oi_by_day:
                        oi_basis = "option_snapshots"
                    for td_key, (_, _) in snapshot_oi_by_day.items():
                        try:
                            td_d = date.fromisoformat(td_key[:10])
                            if as_of_date is None or td_d > as_of_date:
                                as_of_date = td_d
                        except ValueError:
                            pass

                trend = _merge_oi_trend_points(daily_oi_by_day, snapshot_oi_by_day)

                vol_by_day: Dict[str, Tuple[int, int]] = {}
                try:
                    cur.execute(
                        """
                        WITH snap AS (
                          SELECT DISTINCT ON (oc.contract_key, os.day_last_updated_day)
                            oc.option_right,
                            COALESCE(os.day_volume, 0)::bigint AS day_volume,
                            os.day_last_updated_day::date AS trade_date
                          FROM option_contracts oc
                          INNER JOIN option_snapshots os
                            ON os.contract_key = oc.contract_key AND os.source = 'massive'
                          WHERE UPPER(TRIM(oc.symbol)) = %s
                            AND os.day_last_updated_day IS NOT NULL
                            AND os.day_last_updated_day >= CURRENT_DATE - %s::integer
                          ORDER BY oc.contract_key, os.day_last_updated_day, os.snapshot_ts DESC
                        )
                        SELECT trade_date,
                               SUM(CASE WHEN UPPER(TRIM(option_right)) IN ('P', 'PUT')
                                   THEN day_volume ELSE 0 END)::bigint AS put_vol,
                               SUM(CASE WHEN UPPER(TRIM(option_right)) IN ('C', 'CALL')
                                   THEN day_volume ELSE 0 END)::bigint AS call_vol
                        FROM snap
                        GROUP BY trade_date
                        ORDER BY trade_date ASC
                        """,
                        (sym, lb),
                    )
                except Exception as ex:
                    logger.debug("symbol option pcr vol trend skipped: %s", ex)
                    try:
                        cur.connection.rollback()
                    except Exception:
                        pass
                    cur.execute("SELECT 1 WHERE FALSE")

                for row in cur.fetchall() or []:
                    td_key = _serialize_date(row.get("trade_date"))
                    if not td_key:
                        continue
                    put_v = int(row.get("put_vol") or 0)
                    call_v = int(row.get("call_vol") or 0)
                    vol_by_day[td_key] = (put_v, call_v)

                if vol_by_day:
                    trend_by_date = {t["trade_date"]: t for t in trend if t.get("trade_date")}
                    for td_key, (put_v, call_v) in vol_by_day.items():
                        if td_key in trend_by_date:
                            pt = trend_by_date[td_key]
                            pt["put_vol"] = put_v
                            pt["call_vol"] = call_v
                            pt["vol_ratio"] = _safe_ratio(float(put_v), float(call_v))
                        else:
                            trend.append(
                                {
                                    "trade_date": td_key,
                                    "put_oi": 0,
                                    "call_oi": 0,
                                    "oi_ratio": None,
                                    "put_vol": put_v,
                                    "call_vol": call_v,
                                    "vol_ratio": _safe_ratio(float(put_v), float(call_v)),
                                }
                            )
                    trend.sort(key=lambda x: x.get("trade_date") or "")

                chain_rows: List[Dict[str, Any]] = []
                snapshot_as_of: Optional[date] = None
                chain_basis: Optional[str] = "option_snapshots_latest"

                try:
                    cur.execute(
                        """
                        SELECT 1 FROM pg_matviews
                        WHERE schemaname = 'public' AND matviewname = 'option_snapshots_latest'
                        LIMIT 1
                        """
                    )
                    use_mv = bool(cur.fetchone())
                except Exception:
                    use_mv = False

                chain_sql_mv = """
                    SELECT oc.expiry,
                           MAX(os.day_last_updated_day::date) AS snap_day,
                           SUM(CASE WHEN UPPER(TRIM(oc.option_right)) IN ('P', 'PUT')
                               THEN COALESCE(os.open_interest, 0) ELSE 0 END)::bigint AS put_oi,
                           SUM(CASE WHEN UPPER(TRIM(oc.option_right)) IN ('C', 'CALL')
                               THEN COALESCE(os.open_interest, 0) ELSE 0 END)::bigint AS call_oi,
                           SUM(CASE WHEN UPPER(TRIM(oc.option_right)) IN ('P', 'PUT')
                               THEN COALESCE(os.day_volume, 0) ELSE 0 END)::bigint AS put_vol,
                           SUM(CASE WHEN UPPER(TRIM(oc.option_right)) IN ('C', 'CALL')
                               THEN COALESCE(os.day_volume, 0) ELSE 0 END)::bigint AS call_vol
                    FROM option_contracts oc
                    LEFT JOIN option_snapshots_latest os
                      ON os.contract_key = oc.contract_key AND os.source = 'massive'
                    WHERE UPPER(TRIM(oc.symbol)) = %s
                    GROUP BY oc.expiry
                    ORDER BY oc.expiry ASC
                """
                chain_sql_snap = """
                    SELECT oc.expiry,
                           MAX(os.day_last_updated_day::date) AS snap_day,
                           SUM(CASE WHEN UPPER(TRIM(oc.option_right)) IN ('P', 'PUT')
                               THEN COALESCE(os.open_interest, 0) ELSE 0 END)::bigint AS put_oi,
                           SUM(CASE WHEN UPPER(TRIM(oc.option_right)) IN ('C', 'CALL')
                               THEN COALESCE(os.open_interest, 0) ELSE 0 END)::bigint AS call_oi,
                           SUM(CASE WHEN UPPER(TRIM(oc.option_right)) IN ('P', 'PUT')
                               THEN COALESCE(os.day_volume, 0) ELSE 0 END)::bigint AS put_vol,
                           SUM(CASE WHEN UPPER(TRIM(oc.option_right)) IN ('C', 'CALL')
                               THEN COALESCE(os.day_volume, 0) ELSE 0 END)::bigint AS call_vol
                    FROM option_contracts oc
                    LEFT JOIN LATERAL (
                      SELECT open_interest, day_volume, day_last_updated_day
                      FROM option_snapshots s
                      WHERE s.contract_key = oc.contract_key AND s.source = 'massive'
                      ORDER BY s.snapshot_ts DESC
                      LIMIT 1
                    ) os ON TRUE
                    WHERE UPPER(TRIM(oc.symbol)) = %s
                    GROUP BY oc.expiry
                    ORDER BY oc.expiry ASC
                """
                if use_mv:
                    cur.execute(chain_sql_mv, (sym,))
                else:
                    chain_basis = "option_snapshots"
                    cur.execute(chain_sql_snap, (sym,))

                raw_chain = cur.fetchall() or []
                if not raw_chain and as_of_date is not None:
                    chain_basis = "option_open_interest_daily"
                    cur.execute(
                        """
                        SELECT expiry,
                               SUM(CASE WHEN UPPER(TRIM(option_right)) IN ('P', 'PUT')
                                   THEN COALESCE(open_interest, 0) ELSE 0 END)::bigint AS put_oi,
                               SUM(CASE WHEN UPPER(TRIM(option_right)) IN ('C', 'CALL')
                                   THEN COALESCE(open_interest, 0) ELSE 0 END)::bigint AS call_oi
                        FROM option_open_interest_daily
                        WHERE symbol = %s AND source = 'massive' AND trade_date = %s
                        GROUP BY expiry
                        ORDER BY expiry ASC
                        """,
                        (sym, as_of_date),
                    )
                    raw_chain = [
                        {**dict(r), "put_vol": 0, "call_vol": 0, "snap_day": as_of_date}
                        for r in (cur.fetchall() or [])
                    ]

                max_put_oi = max_put_vol = max_call_oi = max_call_vol = max_total_oi = max_total_vol = 1
                for r in raw_chain:
                    max_put_oi = max(max_put_oi, int(r.get("put_oi") or 0))
                    max_call_oi = max(max_call_oi, int(r.get("call_oi") or 0))
                    max_put_vol = max(max_put_vol, int(r.get("put_vol") or 0))
                    max_call_vol = max(max_call_vol, int(r.get("call_vol") or 0))
                    tot_oi = int(r.get("put_oi") or 0) + int(r.get("call_oi") or 0)
                    tot_vol = int(r.get("put_vol") or 0) + int(r.get("call_vol") or 0)
                    max_total_oi = max(max_total_oi, tot_oi)
                    max_total_vol = max(max_total_vol, tot_vol)

                parsed_chain: List[Dict[str, Any]] = []
                for r in raw_chain:
                    exp = r.get("expiry")
                    exp_d = _parse_expiry_date(exp)
                    if exp_d is None:
                        continue
                    dte = (exp_d - today).days
                    put_oi = int(r.get("put_oi") or 0)
                    call_oi = int(r.get("call_oi") or 0)
                    put_vol = int(r.get("put_vol") or 0)
                    call_vol = int(r.get("call_vol") or 0)
                    total_oi = put_oi + call_oi
                    total_vol = put_vol + call_vol
                    snap_day = r.get("snap_day")
                    if snap_day and (snapshot_as_of is None or snap_day > snapshot_as_of):
                        snapshot_as_of = snap_day if isinstance(snap_day, date) else snapshot_as_of
                    parsed_chain.append(
                        {
                            "expiry": _serialize_date(exp) or str(exp or ""),
                            "expiry_date": exp_d,
                            "expiration_label": _format_expiry_label(exp) + _expiry_suffix(exp, today),
                            "dte": dte,
                            "put_vol": put_vol,
                            "call_vol": call_vol,
                            "total_vol": total_vol,
                            "pc_vol": _safe_ratio(float(put_vol), float(call_vol)),
                            "put_oi": put_oi,
                            "call_oi": call_oi,
                            "total_oi": total_oi,
                            "pc_oi": _safe_ratio(float(put_oi), float(call_oi)),
                        }
                    )

                # Same scope as mockup: all listed expiries with activity, incl. recently expired (DTE < 0).
                parsed_chain = [
                    row
                    for row in parsed_chain
                    if row["total_oi"] > 0 or row["total_vol"] > 0 or row["dte"] >= -14
                ]
                parsed_chain.sort(key=lambda row: row["expiry_date"])

                for r in parsed_chain:
                    put_vol = r["put_vol"]
                    call_vol = r["call_vol"]
                    put_oi = r["put_oi"]
                    call_oi = r["call_oi"]
                    total_oi = r["total_oi"]
                    total_vol = r["total_vol"]
                    chain_rows.append(
                        {
                            "expiry": r["expiry"],
                            "expiration_label": r["expiration_label"],
                            "dte": r["dte"],
                            "put_vol": put_vol,
                            "call_vol": call_vol,
                            "total_vol": total_vol,
                            "pc_vol": r["pc_vol"],
                            "put_oi": put_oi,
                            "call_oi": call_oi,
                            "total_oi": total_oi,
                            "pc_oi": r["pc_oi"],
                            "bar_put_vol_pct": round(put_vol / max_put_vol * 100, 1) if max_put_vol else 0,
                            "bar_call_vol_pct": round(call_vol / max_call_vol * 100, 1) if max_call_vol else 0,
                            "bar_total_vol_pct": round(total_vol / max_total_vol * 100, 1) if max_total_vol else 0,
                            "bar_put_oi_pct": round(put_oi / max_put_oi * 100, 1) if max_put_oi else 0,
                            "bar_call_oi_pct": round(call_oi / max_call_oi * 100, 1) if max_call_oi else 0,
                            "bar_total_oi_pct": round(total_oi / max_total_oi * 100, 1) if max_total_oi else 0,
                        }
                    )

                if snapshot_as_of and (as_of_date is None or snapshot_as_of > as_of_date):
                    as_of_date = snapshot_as_of
                    oi_basis = chain_basis

        finally:
            conn.close()
    except Exception as e:
        logger.warning("fetch_symbol_option_pcr failed: %s", e)
        return {"ok": False, "error": str(e)}

    if as_of_date is None and trend:
        last_td = trend[-1].get("trade_date")
        if last_td:
            try:
                as_of_date = date.fromisoformat(last_td[:10])
            except ValueError:
                pass

    stale_days: Optional[int] = None
    if as_of_date:
        stale_days = max(0, (today - as_of_date).days)

    oi_ratio = vol_ratio = avg_oi_5d = None
    if trend:
        last = trend[-1]
        oi_ratio = last.get("oi_ratio")
        vol_ratio = last.get("vol_ratio")
        recent_ratios = [
            t["oi_ratio"]
            for t in trend[-5:]
            if t.get("oi_ratio") is not None
        ]
        if recent_ratios:
            avg_oi_5d = round(sum(recent_ratios) / len(recent_ratios), 3)

    return {
        "ok": True,
        "symbol": sym,
        "as_of_date": _serialize_date(as_of_date),
        "stale_days": stale_days,
        "oi_basis": oi_basis,
        "chain_basis": chain_basis if chain_rows else None,
        "oi_ratio": oi_ratio,
        "vol_ratio": vol_ratio,
        "avg_oi_5d": avg_oi_5d,
        "lookback_days": lb,
        "trend_days": len(trend),
        "trend": trend,
        "chain_by_expiry": chain_rows,
    }
