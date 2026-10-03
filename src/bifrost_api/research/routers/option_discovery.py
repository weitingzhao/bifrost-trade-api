"""Research: Option Discovery and related endpoints (R-OD1)."""

import hashlib
import json
import math
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from bifrost_api.research.deps import db_config
from bifrost_api.research.iv_atm import (
    parse_contract_key,
    strikes_around_spot,
)
from bifrost_core.monitor.redis_url import redis_url_from_config

router = APIRouter(tags=["research"])

# FASTAPI_PLAN FA-2: snapshot Redis cache TTL (seconds)
SNAPSHOT_CACHE_TTL_SEC = 120


MAX_OPTION_SNAPSHOT_CONTRACTS_EXTENDED = 60  # when frontend sends many strikes (e.g. 30)


def _mark_from_snapshot_row(row: Dict[str, Any]) -> Optional[float]:
    """Option mark from Polygon chain day aggregate (day_close). NBBO/last are not persisted at current tier."""
    day_close = row.get("day_close")
    if day_close is not None:
        try:
            d = float(day_close)
            if math.isfinite(d) and d >= 0:
                return d
        except (TypeError, ValueError):
            pass
    return None


def _snapshot_ts_iso(row: Dict[str, Any]) -> Optional[str]:
    """Serialize snapshot_ts for JSON (ISO 8601)."""
    ts = row.get("snapshot_ts")
    if ts is None:
        return None
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            return ts.replace(tzinfo=timezone.utc).isoformat()
        return ts.isoformat()
    return str(ts)


def _norm_expiry_key(expiration: str) -> str:
    e = (expiration or "").strip()
    if len(e) >= 10 and e[4] == "-":
        return e[:4] + e[5:7] + e[8:10]
    return e


def _parse_strikes_csv(strikes: Optional[str]) -> List[float]:
    if not strikes or not str(strikes).strip():
        return []
    out: List[float] = []
    for part in str(strikes).split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(float(part))
        except ValueError:
            pass
    return out


def _filter_option_strikes(strikes_raw: List[float], last_price: Optional[float]) -> List[float]:
    if last_price is not None and last_price > 0:
        min_s = max(0.5, last_price * 0.01)
        max_s = last_price * 2.5
        strikes = [s for s in strikes_raw if min_s <= s <= max_s]
    else:
        strikes = [s for s in strikes_raw if s >= 5.0]
    return sorted(set(strikes))


@router.get("/research/option-snapshots", response_model=None)
def get_option_snapshots_pg(
    request: Request,
    symbol: str = Query(..., description="Underlying symbol"),
    expiration: str = Query(..., description="Expiration YYYYMMDD or YYYY-MM-DD"),
    strikes: Optional[str] = Query(
        None,
        description="Comma-separated strikes; if omitted, uses ATM ladder from daily last when available",
    ),
    source: str = Query("massive", description="Snapshot source column: massive | ib"),
) -> Any:
    """Latest option_snapshots rows from PostgreSQL (Polygon/Plugin sync or IB sink)."""
    from bifrost_api.research.polygon_http import contract_key_from_parts
    from bifrost_api.research.market_pg import get_option_snapshots_latest

    db = db_config(request)
    if not db:
        raise HTTPException(status_code=503, detail="PostgreSQL not configured")

    sym = (symbol or "").strip().upper()
    exp = (expiration or "").strip()
    if not sym or not exp:
        raise HTTPException(status_code=400, detail="symbol and expiration are required")

    src = (source or "massive").strip().lower()
    if src not in ("massive", "ib"):
        src = "massive"

    reader = getattr(request.app.state, "reader", None)
    strikes_list = _parse_strikes_csv(strikes)
    last_price: Optional[float] = None
    if reader and hasattr(reader, "get_stock_day_fallback_price"):
        fallback = reader.get_stock_day_fallback_price(sym)
        if fallback and fallback[0] is not None and fallback[0] > 0:
            last_price = float(fallback[0])
    if not strikes_list and last_price:
        strikes_list = strikes_around_spot(last_price)
    if not strikes_list:
        raise HTTPException(
            status_code=404,
            detail="No strikes to query; provide strikes= or ensure daily last price exists.",
        )

    max_half = max(1, MAX_OPTION_SNAPSHOT_CONTRACTS_EXTENDED // 2)
    if len(strikes_list) > max_half:
        strikes_list = strikes_list[:max_half]

    exp_norm = _norm_expiry_key(exp)
    keys: List[str] = []
    for st in strikes_list:
        for r in ("C", "P"):
            keys.append(contract_key_from_parts(sym, exp_norm, float(st), r))

    cache_fingerprint = hashlib.sha256(
        json.dumps(keys, sort_keys=True).encode()
    ).hexdigest()[:24]
    cache_key = f"massive:snapshot_cache:{sym}:{exp_norm}:{src}:{cache_fingerprint}"
    try:
        import redis

        rurl = redis_url_from_config(reader._config if reader else {})
        if rurl:
            rc = redis.from_url(rurl, decode_responses=True)
            cached = rc.get(cache_key)
            if cached:
                return JSONResponse(
                    content=json.loads(cached),
                    headers={
                        "X-Cache": "HIT",
                        "Cache-Control": f"private, max-age={SNAPSHOT_CACHE_TTL_SEC}",
                    },
                )
    except Exception:
        pass

    rows = get_option_snapshots_latest(db, keys, source=src)
    out_rows: List[Dict[str, Any]] = []
    underlying_price: Optional[float] = None
    for row in rows:
        ck = row.get("contract_key") or ""
        strike, right = parse_contract_key(ck)
        if strike is None or not right:
            continue
        up = row.get("underlying_price")
        if up is not None and underlying_price is None:
            try:
                underlying_price = float(up)
            except (TypeError, ValueError):
                pass
        out_rows.append(
            {
                "strike": strike,
                "right": right,
                "snapshot_ts": _snapshot_ts_iso(row),
                "mark": _mark_from_snapshot_row(row),
                "iv": row.get("iv"),
                "delta": row.get("delta"),
                "gamma": row.get("gamma"),
                "theta": row.get("theta"),
                "vega": row.get("vega"),
                "open_interest": row.get("open_interest"),
                "underlying_ticker": row.get("underlying_ticker"),
                "day_open": row.get("day_open"),
                "day_high": row.get("day_high"),
                "day_low": row.get("day_low"),
                "day_close": row.get("day_close"),
                "day_previous_close": row.get("day_previous_close"),
                "day_change": row.get("day_change"),
                "day_change_percent": row.get("day_change_percent"),
                "day_volume": row.get("day_volume"),
                "day_vwap": row.get("day_vwap"),
                "day_last_updated": row.get("day_last_updated"),
                "day_last_updated_day": (
                    row["day_last_updated_day"].isoformat()
                    if row.get("day_last_updated_day") is not None
                    and hasattr(row["day_last_updated_day"], "isoformat")
                    else row.get("day_last_updated_day")
                ),
            }
        )
    out_rows.sort(key=lambda x: (x["strike"], 0 if x["right"] == "C" else 1))

    out: Dict[str, Any] = {
        "symbol": sym,
        "expiration": exp,
        "rows": out_rows,
        "source": src,
    }
    if underlying_price is not None:
        out["underlying_price"] = underlying_price
    elif last_price is not None:
        out["underlying_price"] = last_price
    if not out_rows and keys:
        out["warning"] = (
            "No rows in option_snapshots for the requested contract keys. "
            "Run Load quotes again after a successful Polygon chain snapshot, or verify expiry/strikes match the chain."
        )
    try:
        import redis

        rurl = redis_url_from_config(reader._config if reader else {})
        if rurl:
            rc = redis.from_url(rurl, decode_responses=True)
            rc.setex(cache_key, SNAPSHOT_CACHE_TTL_SEC, json.dumps(out, default=str))
    except Exception:
        pass
    return JSONResponse(
        content=out,
        headers={
            "X-Cache": "MISS",
            "Cache-Control": f"private, max-age={SNAPSHOT_CACHE_TTL_SEC}",
        },
    )


@router.get("/research/option-contract/liquidity-summary")
def get_option_contract_liquidity_summary(
    request: Request,
    symbol: str = Query(..., description="Underlying symbol"),
    expiration: str = Query(..., description="Expiration YYYYMMDD or YYYY-MM-DD"),
    strike: float = Query(..., description="Strike price"),
    right: str = Query(..., description="C or P"),
    source: str = Query("massive", description="massive | ib"),
) -> Dict[str, Any]:
    """P1: Aggregate liquidity stats for a single contract — spread percentile, OI rank, snapshot freshness."""
    from bifrost_api.research.polygon_http import contract_key_from_parts
    from bifrost_api.research.market_pg import get_option_snapshots_latest

    db = db_config(request)
    if not db:
        raise HTTPException(status_code=503, detail="PostgreSQL not configured")
    sym = (symbol or "").strip().upper()
    exp_norm = _norm_expiry_key((expiration or "").strip())
    r = (right or "").strip().upper()
    if not sym or not exp_norm or r not in ("C", "P"):
        raise HTTPException(status_code=400, detail="symbol, expiration, strike, and right (C/P) are required")
    src = (source or "massive").strip().lower()
    if src not in ("massive", "ib"):
        src = "massive"

    reader = getattr(request.app.state, "reader", None)
    last_price: Optional[float] = None
    if reader and hasattr(reader, "get_stock_day_fallback_price"):
        fallback = reader.get_stock_day_fallback_price(sym)
        if fallback and fallback[0] is not None and fallback[0] > 0:
            last_price = float(fallback[0])
    strikes_list = strikes_around_spot(last_price) if last_price else [strike]

    all_keys: List[str] = []
    for st in strikes_list:
        for rt in ("C", "P"):
            all_keys.append(contract_key_from_parts(sym, exp_norm, float(st), rt))
    target_key = contract_key_from_parts(sym, exp_norm, float(strike), r)
    if target_key not in all_keys:
        all_keys.append(target_key)

    rows = get_option_snapshots_latest(db, all_keys, source=src)
    spreads_same_right: List[float] = []
    oi_same_right: List[int] = []
    target_row: Optional[dict] = None
    for row in rows:
        ck = row.get("contract_key") or ""
        bid = row.get("bid")
        ask = row.get("ask")
        mid_val = row.get("mid")
        oi_val = row.get("open_interest")
        r_part = ck.rsplit("|", 1)[-1] if "|" in ck else ""
        if r_part == r:
            if bid is not None and ask is not None and mid_val is not None:
                try:
                    mid_f = float(mid_val)
                    if mid_f > 0:
                        spreads_same_right.append((float(ask) - float(bid)) / mid_f * 100)
                except (TypeError, ValueError):
                    pass
            if oi_val is not None:
                try:
                    oi_same_right.append(int(oi_val))
                except (TypeError, ValueError):
                    pass
        if ck == target_key:
            target_row = row

    spread_pct: Optional[float] = None
    spread_percentile: Optional[float] = None
    if target_row:
        bid = target_row.get("bid")
        ask = target_row.get("ask")
        mid_v = target_row.get("mid")
        if bid is not None and ask is not None and mid_v is not None:
            try:
                mid_f = float(mid_v)
                if mid_f > 0:
                    spread_pct = (float(ask) - float(bid)) / mid_f * 100
            except (TypeError, ValueError):
                pass
    if spread_pct is not None and len(spreads_same_right) > 1:
        rank = sum(1 for s in spreads_same_right if s <= spread_pct)
        spread_percentile = (rank / len(spreads_same_right)) * 100

    oi_percentile: Optional[float] = None
    target_oi = target_row.get("open_interest") if target_row else None
    if target_oi is not None and len(oi_same_right) > 1:
        try:
            toi = int(target_oi)
            rank = sum(1 for o in oi_same_right if o <= toi)
            oi_percentile = (rank / len(oi_same_right)) * 100
        except (TypeError, ValueError):
            pass

    snapshot_ts: Optional[str] = None
    if target_row and target_row.get("snapshot_ts"):
        ts = target_row["snapshot_ts"]
        snapshot_ts = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)

    return {
        "ok": True,
        "symbol": sym,
        "expiration": exp_norm,
        "strike": strike,
        "right": r,
        "source": src,
        "spread_pct": round(spread_pct, 2) if spread_pct is not None else None,
        "spread_percentile": round(spread_percentile, 1) if spread_percentile is not None else None,
        "oi": int(target_oi) if target_oi is not None else None,
        "oi_percentile": round(oi_percentile, 1) if oi_percentile is not None else None,
        "contracts_compared": len(spreads_same_right),
        "snapshot_ts": snapshot_ts,
    }


@router.get("/research/option-contract/relative-value")
def get_option_contract_relative_value(
    request: Request,
    symbol: str = Query(..., description="Underlying symbol"),
    expiration: str = Query(..., description="Expiration YYYYMMDD or YYYY-MM-DD"),
    strike: float = Query(..., description="Strike price"),
    right: str = Query(..., description="C or P"),
    source: str = Query("massive", description="massive | ib"),
) -> Dict[str, Any]:
    """P2: IV relative value — z-score vs same-right contracts in same expiry."""
    from bifrost_api.research.polygon_http import contract_key_from_parts
    from bifrost_api.research.market_pg import get_option_snapshots_latest
    import math

    db = db_config(request)
    if not db:
        raise HTTPException(status_code=503, detail="PostgreSQL not configured")
    sym = (symbol or "").strip().upper()
    exp_norm = _norm_expiry_key((expiration or "").strip())
    r = (right or "").strip().upper()
    if not sym or not exp_norm or r not in ("C", "P"):
        raise HTTPException(status_code=400, detail="symbol, expiration, strike, and right (C/P) are required")
    src = (source or "massive").strip().lower()
    if src not in ("massive", "ib"):
        src = "massive"

    reader = getattr(request.app.state, "reader", None)
    last_price: Optional[float] = None
    if reader and hasattr(reader, "get_stock_day_fallback_price"):
        fallback = reader.get_stock_day_fallback_price(sym)
        if fallback and fallback[0] is not None and fallback[0] > 0:
            last_price = float(fallback[0])
    wide_strikes = strikes_around_spot(last_price, count=30) if last_price else [strike]

    keys: List[str] = []
    for st in wide_strikes:
        keys.append(contract_key_from_parts(sym, exp_norm, float(st), r))
    target_key = contract_key_from_parts(sym, exp_norm, float(strike), r)
    if target_key not in keys:
        keys.append(target_key)

    rows = get_option_snapshots_latest(db, keys, source=src)
    ivs: List[float] = []
    target_iv: Optional[float] = None
    iv_curve: List[Dict[str, Any]] = []
    for row in rows:
        iv = row.get("iv")
        if iv is None:
            continue
        try:
            iv_f = float(iv)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(iv_f):
            continue
        ck = row.get("contract_key") or ""
        parts = ck.split("|")
        row_strike = float(parts[3]) if len(parts) > 3 else 0
        ivs.append(iv_f)
        iv_curve.append({"strike": row_strike, "iv": round(iv_f, 6)})
        if ck == target_key:
            target_iv = iv_f

    if target_iv is None or len(ivs) < 3:
        return {
            "ok": True,
            "label": None,
            "iv_zscore": None,
            "this_iv": target_iv,
            "avg_iv": None,
            "contracts_compared": len(ivs),
            "iv_curve": sorted(iv_curve, key=lambda x: x["strike"]),
        }

    mean = sum(ivs) / len(ivs)
    std = math.sqrt(sum((v - mean) ** 2 for v in ivs) / len(ivs))
    if std < 1e-8:
        z = 0.0
    else:
        z = (target_iv - mean) / std
    label = "Rich" if z > 1 else ("Cheap" if z < -1 else "Neutral")

    return {
        "ok": True,
        "label": label,
        "iv_zscore": round(z, 3),
        "this_iv": round(target_iv, 6),
        "avg_iv": round(mean, 6),
        "std_iv": round(std, 6),
        "contracts_compared": len(ivs),
        "iv_curve": sorted(iv_curve, key=lambda x: x["strike"]),
    }


