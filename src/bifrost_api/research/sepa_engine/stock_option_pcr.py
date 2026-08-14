"""Stock Inspector: put/call ratio trend + option chain rollup by expiry.

Delegates PCR aggregate data to Plugin Market Data API, then computes
per-expiry chain stats locally from option_snapshot (chain layout still
requires live snapshot joins not available via Plugin).
"""

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


def _serialize_date(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, datetime):
        return v.date().isoformat()
    s = str(v).strip()
    return s[:10] if len(s) >= 10 else s or None


def _fetch_pcr_trend_via_plugin(sym: str, lb: int) -> Tuple[List[Dict[str, Any]], Optional[date], str]:
    """Fetch OI+volume trend from Plugin /options/analytics/pcr.

    Returns (trend_list, as_of_date, oi_basis).
    """
    from bifrost_api.research.market_data_client import fetch_pcr_aggregate

    trend: List[Dict[str, Any]] = []
    as_of_date: Optional[date] = None
    oi_basis = "plugin_api"

    # OI PCR
    try:
        oi_resp = fetch_pcr_aggregate(sym, pcr_type="oi", lookback_days=lb)
    except Exception as exc:
        logger.debug("Plugin PCR oi call failed: %s", exc)
        oi_resp = {"ok": False}

    oi_by_day: Dict[str, Tuple[int, int]] = {}
    if oi_resp.get("ok"):
        for pt in oi_resp.get("trend") or []:
            td_key = pt.get("trade_date")
            if not td_key:
                continue
            put_val = int(pt.get("put_value") or 0)
            call_val = int(pt.get("call_value") or 0)
            oi_by_day[td_key] = (put_val, call_val)
            try:
                d = date.fromisoformat(td_key[:10])
                if as_of_date is None or d > as_of_date:
                    as_of_date = d
            except ValueError:
                pass

    # Volume PCR
    try:
        vol_resp = fetch_pcr_aggregate(sym, pcr_type="volume", lookback_days=lb)
    except Exception as exc:
        logger.debug("Plugin PCR volume call failed: %s", exc)
        vol_resp = {"ok": False}

    vol_by_day: Dict[str, Tuple[int, int]] = {}
    if vol_resp.get("ok"):
        for pt in vol_resp.get("trend") or []:
            td_key = pt.get("trade_date")
            if not td_key:
                continue
            put_val = int(pt.get("put_value") or 0)
            call_val = int(pt.get("call_value") or 0)
            vol_by_day[td_key] = (put_val, call_val)
            try:
                d = date.fromisoformat(td_key[:10])
                if as_of_date is None or d > as_of_date:
                    as_of_date = d
            except ValueError:
                pass

    # Merge OI + volume into trend points
    all_dates = sorted(set(oi_by_day) | set(vol_by_day))
    for td_key in all_dates:
        put_oi, call_oi = oi_by_day.get(td_key, (0, 0))
        put_vol, call_vol = vol_by_day.get(td_key, (0, 0))
        trend.append({
            "trade_date": td_key,
            "put_oi": put_oi,
            "call_oi": call_oi,
            "oi_ratio": _safe_ratio(float(put_oi), float(call_oi)),
            "put_vol": put_vol if vol_by_day else None,
            "call_vol": call_vol if vol_by_day else None,
            "vol_ratio": _safe_ratio(float(put_vol), float(call_vol)) if vol_by_day else None,
        })

    return trend, as_of_date, oi_basis


def fetch_symbol_option_pcr(
    status_config: dict,
    symbol: str,
    *,
    lookback_days: int = 365,
) -> Dict[str, Any]:
    """Aggregate OI/volume ratios and per-expiry chain stats for Stock Inspector.

    All data fetched from Plugin API — no direct market.* SQL.
    """
    sym = (symbol or "").strip().upper()
    if not sym:
        return {"ok": False, "error": "symbol is required"}

    lb = max(30, min(int(lookback_days), 400))
    today = date.today()

    # Fetch PCR trend from Plugin API
    trend, as_of_date, oi_basis = _fetch_pcr_trend_via_plugin(sym, lb)

    chain_rows: List[Dict[str, Any]] = []
    chain_basis: Optional[str] = None

    try:
        from bifrost_api.research.market_data_client import fetch_chain_by_expiry

        fallback_str = as_of_date.isoformat() if as_of_date else None
        resp = fetch_chain_by_expiry(sym, fallback_date=fallback_str)
        raw_chain = resp.get("chain") or []
        chain_basis = resp.get("basis")

        snapshot_as_of: Optional[date] = None
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
            if snap_day:
                try:
                    sd = date.fromisoformat(str(snap_day)[:10])
                    if snapshot_as_of is None or sd > snapshot_as_of:
                        snapshot_as_of = sd
                except (ValueError, TypeError):
                    pass
            parsed_chain.append({
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
            })

        parsed_chain = [
            row for row in parsed_chain
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
            chain_rows.append({
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
            })

        if snapshot_as_of and (as_of_date is None or snapshot_as_of > as_of_date):
            as_of_date = snapshot_as_of
            oi_basis = chain_basis or oi_basis

    except Exception as e:
        logger.warning("fetch_symbol_option_pcr chain query failed: %s", e)

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
