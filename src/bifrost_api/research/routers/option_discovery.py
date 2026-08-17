"""Research: Option Discovery and related endpoints (R-OD1)."""

import hashlib
import json
import math
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, Query, Request
from fastapi.responses import JSONResponse

from bifrost_api.research.iv_atm import (
    assemble_volatility_cone_points,
    atm_iv_from_expiry_items,
    build_cone_key_maps,
    build_exp_iv_map,
    daily_atm_ivs_for_expiration,
    group_hist_rows_by_snap_day,
    parse_contract_key,
    rollup_daily_ivs_by_expiration,
    strikes_around_spot,
)
from bifrost_core.monitor.redis_url import redis_url_from_config

router = APIRouter(tags=["research"])

# FASTAPI_PLAN FA-2: snapshot Redis cache TTL (seconds)
SNAPSHOT_CACHE_TTL_SEC = 120


def _option_expirations_cache_response(
    body: Dict[str, Any], extra_headers: Optional[Dict[str, str]] = None
) -> JSONResponse:
    """Slow-changing expirations list: allow browser / SWR to cache 5 minutes."""
    headers: Dict[str, str] = {"Cache-Control": "max-age=300"}
    if extra_headers:
        headers.update(extra_headers)
    return JSONResponse(content=body, headers=headers)

MAX_OPTION_SNAPSHOT_CONTRACTS = 20
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


def _db_config(request: Request) -> Optional[dict]:
    return request.app.state.control_via_db or getattr(request.app.state, "status_cfg_for_read", None)




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


def _expiration_cache_is_fresh(max_updated_at: Optional[Any], ttl_sec: int) -> bool:
    if max_updated_at is None or ttl_sec <= 0:
        return False
    now = datetime.now(timezone.utc)
    mu = max_updated_at
    if hasattr(mu, "tzinfo") and getattr(mu, "tzinfo", None) is None:
        mu = mu.replace(tzinfo=timezone.utc)
    else:
        mu = mu.astimezone(timezone.utc)
    return (now - mu).total_seconds() <= float(ttl_sec)


def _ttl_sec_expiration_cache(config: dict) -> int:
    from bifrost_api.research.polygon_http import get_expiration_cache_settings
    from bifrost_api.research.market_pg import is_us_equity_regular_session_et

    s = get_expiration_cache_settings(config)
    return s["ttl_trading_sec"] if is_us_equity_regular_session_et() else s["ttl_off_hours_sec"]


async def _option_expirations_ib(request: Request, symbol: str) -> Dict[str, Any]:
    """IB reqSecDefOptParams; same shape as legacy /research/option-expirations."""
    gw = getattr(request.app.state, "ib_operator_client", None)
    if gw is None:
        return {
            "symbol": symbol,
            "expirations": [],
            "strikes": [],
            "error": "Market data client not available",
        }
    if not getattr(request.app.state, "monitor_enabled", True):
        return {
            "symbol": symbol,
            "expirations": [],
            "strikes": [],
            "error": "Monitor IB client disabled",
        }

    env = await gw.request_async(
        "fetch_option_expirations",
        {"symbol": symbol},
        caller="research_option_expirations",
    )
    if not env.get("ok"):
        return {
            "symbol": symbol,
            "expirations": [],
            "strikes": [],
            "error": str(env.get("error") or "IB gateway error"),
        }

    last_price: Optional[float] = None
    reader = getattr(request.app.state, "reader", None)
    if reader and hasattr(reader, "get_stock_day_fallback_price"):
        fallback = reader.get_stock_day_fallback_price(symbol)
        if fallback and fallback[0] is not None and fallback[0] > 0:
            last_price = float(fallback[0])

    result = env.get("data") or {}
    expirations: List[str] = result.get("expirations") or []
    strikes_raw: List[float] = result.get("strikes") or []
    strikes = _filter_option_strikes(strikes_raw, last_price)
    error = result.get("error")
    out: Dict[str, Any] = {"symbol": symbol, "expirations": expirations, "strikes": strikes}
    if last_price is not None:
        out["last_price"] = last_price
    if error:
        out["error"] = error
    return out


@router.get("/research/option-expirations")
async def get_option_expirations(
    request: Request,
    symbol: str = Query(..., description="Underlying symbol (e.g. NVDA)"),
    provider: str = Query(
        "auto",
        description="massive: Polygon/Plugin REST only (legacy provider id); ib: IB only; "
        "auto: Polygon if key + data, else IB",
    ),
    debug: bool = Query(
        False,
        description="If true and provider uses Polygon REST, include redacted request URLs, "
        "per-page JSON responses, and a sample of contract objects (no DB writes).",
    ),
    expiration: Optional[str] = Query(
        None,
        description="When set with provider=massive (Polygon), restrict contracts to this single "
        "expiration (YYYY-MM-DD or YYYYMMDD). Reduces pagination and returns strikes for that "
        "expiry only. Ignored when provider=ib.",
    ),
) -> Any:
    """R-OD1: Expirations and strikes from IB and/or Polygon REST (PostgreSQL cache first)."""
    from bifrost_api.research.polygon_http import get_expiration_cache_settings, get_polygon_settings
    from bifrost_api.research.market_pg import (
        get_option_expiration_cache_snapshot,
        get_option_expirations_from_contracts_db,
        get_strikes_for_expiry_from_contracts_db,
    )

    symbol = (symbol or "").strip()
    if not symbol:
        return {"symbol": "", "expirations": [], "strikes": [], "error": "symbol is required"}

    prov = (provider or "auto").strip().lower()
    if prov not in ("auto", "ib", "massive"):
        prov = "auto"

    reader = getattr(request.app.state, "reader", None)
    config = reader._config if reader else {}
    last_price: Optional[float] = None
    if reader and hasattr(reader, "get_stock_day_fallback_price"):
        fallback = reader.get_stock_day_fallback_price(symbol)
        if fallback and fallback[0] is not None and fallback[0] > 0:
            last_price = float(fallback[0])

    if prov == "ib":
        out = await _option_expirations_ib(request, symbol)
        out["provider"] = "ib"
        return _option_expirations_cache_response(out)

    db = _db_config(request)
    ms = get_polygon_settings(config)
    ecfg = get_expiration_cache_settings(config)
    ttl_sec = _ttl_sec_expiration_cache(config)

    async def _massive_db_first_flow() -> JSONResponse:
        """PostgreSQL cache / contracts first; IB fallback when DB empty.

        P8: market.option_contract (plugin) can satisfy Discovery without a Trade-side
        Polygon API key. REST refresh retired — DB-only or IB.
        """
        exp_q = (expiration or "").strip()

        if exp_q:
            if db:
                strikes_db = get_strikes_for_expiry_from_contracts_db(db, symbol, exp_q)
                if strikes_db:
                    strikes_f = _filter_option_strikes(strikes_db, last_price)
                    exp_norm = _norm_expiry_key(exp_q)
                    body: Dict[str, Any] = {
                        "symbol": symbol,
                        "expirations": [exp_norm] if exp_norm else [],
                        "strikes": strikes_f,
                        "provider": "massive",
                        "expiration_backend": "contracts",
                    }
                    if last_price is not None:
                        body["last_price"] = last_price
                    return _option_expirations_cache_response(body, {"X-Expiration-Cache": "hit"})
            return JSONResponse(
                content={
                    "symbol": symbol,
                    "expirations": [],
                    "strikes": [],
                    "error": "plugin_ingest_pending",
                    "provider": "massive",
                }
            )

        # Full expiration list
        if db and ecfg["enabled"]:
            snap = get_option_expiration_cache_snapshot(db, symbol)
            if snap:
                exps, max_u = snap
                if exps:
                    fresh = _expiration_cache_is_fresh(max_u, ttl_sec)
                    body_f = {
                        "symbol": symbol,
                        "expirations": exps,
                        "strikes": [],
                        "provider": "massive",
                        "expiration_backend": "cache" if fresh else "cache_stale",
                    }
                    if last_price is not None:
                        body_f["last_price"] = last_price
                    header = "fresh" if fresh else "stale"
                    return _option_expirations_cache_response(body_f, {"X-Expiration-Cache": header})

        if db:
            ex_contracts = get_option_expirations_from_contracts_db(db, symbol)
            if ex_contracts:
                body_c = {
                    "symbol": symbol,
                    "expirations": ex_contracts,
                    "strikes": [],
                    "provider": "massive",
                    "expiration_backend": "contracts",
                }
                if last_price is not None:
                    body_c["last_price"] = last_price
                return _option_expirations_cache_response(body_c, {"X-Expiration-Cache": "hit"})

        return JSONResponse(
            content={
                "symbol": symbol,
                "expirations": [],
                "strikes": [],
                "error": "plugin_ingest_pending",
                "provider": "massive",
            }
        )

    if prov == "massive":
        return await _massive_db_first_flow()

    # auto: Polygon-first (DB + REST) when configured and data present, else IB
    if ms["api_key"]:
        resp = await _massive_db_first_flow()
        try:
            payload = json.loads(resp.body.decode("utf-8"))
        except Exception:
            payload = {}
        err = payload.get("error") if isinstance(payload, dict) else None
        exps = (payload.get("expirations") or []) if isinstance(payload, dict) else []
        strikes_l = (payload.get("strikes") or []) if isinstance(payload, dict) else []
        if not err and (exps or strikes_l):
            return resp

    ib_out = await _option_expirations_ib(request, symbol)
    ib_out["provider"] = "ib"
    return _option_expirations_cache_response(ib_out)


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

    db = _db_config(request)
    if not db:
        return {"symbol": symbol, "expiration": expiration, "rows": [], "error": "PostgreSQL not configured"}

    sym = (symbol or "").strip().upper()
    exp = (expiration or "").strip()
    if not sym or not exp:
        return {"symbol": sym, "expiration": exp, "rows": [], "error": "symbol and expiration are required"}

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
        return {
            "symbol": sym,
            "expiration": exp,
            "rows": [],
            "error": "No strikes to query; provide strikes= or ensure daily last price exists.",
            "source": src,
        }

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

    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}
    sym = (symbol or "").strip().upper()
    exp_norm = _norm_expiry_key((expiration or "").strip())
    r = (right or "").strip().upper()
    if not sym or not exp_norm or r not in ("C", "P"):
        return {"ok": False, "error": "symbol, expiration, strike, and right (C/P) are required"}
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

    db = _db_config(request)
    if not db:
        return {"ok": False, "error": "PostgreSQL not configured"}
    sym = (symbol or "").strip().upper()
    exp_norm = _norm_expiry_key((expiration or "").strip())
    r = (right or "").strip().upper()
    if not sym or not exp_norm or r not in ("C", "P"):
        return {"ok": False, "error": "symbol, expiration, strike, and right (C/P) are required"}
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


@router.get("/research/option-oi")
def get_research_option_oi(
    request: Request,
    symbol: str = Query(...),
    expiry: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None, description="YYYY-MM-DD"),
    date_to: Optional[str] = Query(None, description="YYYY-MM-DD"),
    limit: int = Query(100, ge=1, le=500),
) -> Dict[str, Any]:
    from bifrost_api.research.market_pg import get_option_open_interest_daily

    db = _db_config(request)
    if not db:
        return {"rows": [], "error": "PostgreSQL not configured"}
    rows = get_option_open_interest_daily(
        db,
        symbol,
        expiry=expiry,
        limit=limit,
        date_from=date_from,
        date_to=date_to,
    )
    return {"rows": rows}


@router.get("/research/option-trades")
def get_research_option_trades(
    request: Request,
    symbol: str = Query(...),
    limit: int = Query(100, ge=1, le=500),
) -> Any:
    from bifrost_api.research.polygon_http import get_polygon_settings
    from bifrost_api.research.market_pg import get_option_trades

    reader = getattr(request.app.state, "reader", None)
    cfg = reader._config if reader else {}
    ms = get_polygon_settings(cfg)
    if not ms["trades_enabled"]:
        return JSONResponse(
            status_code=403,
            content={
                "ok": False,
                "message": "Option trades API is disabled for this configuration.",
                "trades": [],
            },
        )

    db = _db_config(request)
    if not db:
        return {"trades": [], "error": "PostgreSQL not configured"}
    rows = get_option_trades(db, symbol, limit=limit)
    return {"ok": True, "trades": rows}


@router.post("/research/option-snapshot")
async def post_option_snapshot(
    request: Request,
    body: Dict[str, Any] = Body(..., description="symbol, expiration, optional strikes"),
) -> Dict[str, Any]:
    """OD.3: Fetch option quotes (bid/ask/last/mid) for symbol+expiration with pacing; returns rows + optional underlying_price."""
    symbol = (body.get("symbol") or "").strip()
    expiration = (body.get("expiration") or "").strip()
    if not symbol or not expiration:
        return {
            "symbol": symbol,
            "expiration": expiration,
            "rows": [],
            "error": "symbol and expiration are required",
        }

    gw = getattr(request.app.state, "ib_operator_client", None)
    if gw is None:
        return {
            "symbol": symbol,
            "expiration": expiration,
            "rows": [],
            "error": "Market data client not available",
        }
    if not getattr(request.app.state, "monitor_enabled", True):
        return {
            "symbol": symbol,
            "expiration": expiration,
            "rows": [],
            "error": "Monitor IB client disabled",
        }

    strikes_raw = body.get("strikes")
    if isinstance(strikes_raw, list):
        strikes = [float(s) for s in strikes_raw if isinstance(s, (int, float)) and (isinstance(s, bool) is False)]
    else:
        strikes = []
    spot_from_stock_day: Optional[float] = None  # used for strike list and response underlying_price when from stock_day
    if not strikes:
        reader = getattr(request.app.state, "reader", None)
        if reader and hasattr(reader, "get_stock_day_fallback_price"):
            fallback = reader.get_stock_day_fallback_price(symbol)
            if fallback and fallback[0] is not None and fallback[0] > 0:
                spot_from_stock_day = float(fallback[0])
                strikes = strikes_around_spot(spot_from_stock_day)
        if not strikes:
            env_e = await gw.request_async(
                "fetch_option_expirations",
                {"symbol": symbol},
                caller="research_option_snapshot",
            )
            if env_e.get("ok"):
                result = env_e.get("data") or {}
                strikes = result.get("strikes") or []
    # When frontend sends strikes, allow up to 30 strikes (60 contracts); else cap at 10
    if strikes:
        max_contracts = min(MAX_OPTION_SNAPSHOT_CONTRACTS_EXTENDED, max(MAX_OPTION_SNAPSHOT_CONTRACTS, len(strikes) * 2))
        strikes = strikes[: max_contracts // 2]
    else:
        max_contracts = MAX_OPTION_SNAPSHOT_CONTRACTS

    out: Dict[str, Any] = {"symbol": symbol, "expiration": expiration, "rows": []}
    if spot_from_stock_day is not None:
        out["underlying_price"] = spot_from_stock_day
    env_s = await gw.request_async(
        "fetch_option_snapshot",
        {
            "symbol": symbol,
            "expiration": expiration,
            "strikes": strikes,
            "max_contracts": max_contracts,
            "pacing_sec": 0.35,
        },
        caller="research_option_snapshot",
    )
    if not env_s.get("ok"):
        out["error"] = str(env_s.get("error") or "IB gateway error")
        return out
    snap = env_s.get("data") or {}
    out["rows"] = list(snap.get("rows") or [])
    up = snap.get("underlying_price")
    if up is not None:
        out["underlying_price"] = up
    return out


@router.get("/research/iv-term-structure", response_model=None)
def get_iv_term_structure(
    request: Request,
    symbol: str = Query(..., description="Underlying symbol"),
    expirations: str = Query(
        ...,
        description="Comma-separated expiration dates (YYYYMMDD or YYYY-MM-DD), max 12",
    ),
    source: str = Query("massive", description="Snapshot source: massive | ib"),
) -> Dict[str, Any]:
    """ATM IV for multiple expirations — powers the IV term structure chart."""
    from datetime import date, datetime

    from bifrost_api.research.polygon_http import contract_key_from_parts
    from bifrost_api.research.market_pg import get_option_snapshots_latest

    db = _db_config(request)
    if not db:
        return {"ok": False, "symbol": symbol, "points": [], "error": "PostgreSQL not configured"}

    sym = (symbol or "").strip().upper()
    if not sym:
        return {"ok": False, "symbol": sym, "points": [], "error": "symbol is required"}

    src = (source or "massive").strip().lower()
    if src not in ("massive", "ib"):
        src = "massive"

    exp_list: List[str] = []
    for raw in (expirations or "").split(","):
        e = _norm_expiry_key(raw)
        if len(e) == 8 and e.isdigit():
            exp_list.append(e)
    exp_list = exp_list[:12]
    if len(exp_list) < 2:
        return {"ok": False, "symbol": sym, "points": [], "error": "Need at least 2 valid expirations"}

    reader = getattr(request.app.state, "reader", None)
    last_price: Optional[float] = None
    if reader and hasattr(reader, "get_stock_day_fallback_price"):
        fallback = reader.get_stock_day_fallback_price(sym)
        if fallback and fallback[0] is not None and fallback[0] > 0:
            last_price = float(fallback[0])
    if not last_price:
        return {"ok": False, "symbol": sym, "points": [], "error": "No underlying price available for ATM strike selection"}

    atm_strikes = strikes_around_spot(last_price, count=2)
    if not atm_strikes:
        return {"ok": False, "symbol": sym, "points": [], "error": "Cannot compute ATM strikes"}

    all_keys: List[str] = []
    key_exp_map: Dict[str, str] = {}
    for exp in exp_list:
        for st in atm_strikes:
            for r in ("C", "P"):
                k = contract_key_from_parts(sym, exp, float(st), r)
                all_keys.append(k)
                key_exp_map[k] = exp

    rows = get_option_snapshots_latest(db, all_keys, source=src)
    exp_iv = build_exp_iv_map(rows, key_exp_map, last_price)

    today = date.today()
    points: List[Dict[str, Any]] = []
    for exp in exp_list:
        items = exp_iv.get(exp, [])
        atm_iv, best_call, best_put, best_strike = atm_iv_from_expiry_items(items)
        if atm_iv is None:
            continue

        try:
            exp_date = datetime.strptime(exp, "%Y%m%d").date()
        except ValueError:
            continue
        dte = (exp_date - today).days
        if dte < 0:
            continue

        points.append({
            "expiration": exp,
            "dte_days": dte,
            "strike": best_strike,
            "iv_call": best_call,
            "iv_put": best_put,
            "atm_iv": atm_iv,
        })

    points.sort(key=lambda p: p["dte_days"])
    return {"ok": True, "symbol": sym, "underlying_price": last_price, "points": points}


@router.get("/research/iv-volatility-cone", response_model=None)
def get_iv_volatility_cone(
    request: Request,
    symbol: str = Query(..., description="Underlying symbol"),
    expirations: str = Query(
        ...,
        description="Comma-separated expiration dates (YYYYMMDD or YYYY-MM-DD), max 12",
    ),
    source: str = Query("massive", description="Snapshot source: massive | ib"),
    lookback_days: int = Query(90, ge=1, le=90, description="Calendar days of history (max 90)"),
) -> Dict[str, Any]:
    """ATM IV percentile bands per expiration from historical option_snapshots (IV volatility cone)."""
    from datetime import datetime

    from bifrost_api.research.market_pg import (
        get_option_snapshots_eod_per_day,
        get_option_snapshots_latest,
        get_report_option_atm_iv_daily,
    )

    db = _db_config(request)
    if not db:
        return {"ok": False, "symbol": symbol, "points": [], "error": "PostgreSQL not configured"}

    sym = (symbol or "").strip().upper()
    if not sym:
        return {"ok": False, "symbol": sym, "points": [], "error": "symbol is required"}

    src = (source or "massive").strip().lower()
    if src not in ("massive", "ib"):
        src = "massive"

    exp_list: List[str] = []
    for raw in (expirations or "").split(","):
        e = _norm_expiry_key(raw)
        if len(e) == 8 and e.isdigit():
            exp_list.append(e)
    exp_list = exp_list[:12]
    if len(exp_list) < 2:
        return {"ok": False, "symbol": sym, "points": [], "error": "Need at least 2 valid expirations"}

    reader = getattr(request.app.state, "reader", None)
    last_price: Optional[float] = None
    if reader and hasattr(reader, "get_stock_day_fallback_price"):
        fallback = reader.get_stock_day_fallback_price(sym)
        if fallback and fallback[0] is not None and fallback[0] > 0:
            last_price = float(fallback[0])
    if not last_price:
        return {"ok": False, "symbol": sym, "points": [], "error": "No underlying price available for ATM strike selection"}

    narrow_keys, wide_keys, key_exp_narrow, key_exp_wide = build_cone_key_maps(sym, exp_list, last_price)
    if not narrow_keys or not wide_keys:
        return {"ok": False, "symbol": sym, "points": [], "error": "Cannot compute ATM strikes"}

    since_ts = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    since_date = since_ts.date()

    latest_rows = get_option_snapshots_latest(db, narrow_keys, source=src)
    exp_iv_cur_all = build_exp_iv_map(latest_rows, key_exp_narrow, last_price)

    report_rows = get_report_option_atm_iv_daily(db, sym, exp_list, src, since_date)
    rollup_ivs = rollup_daily_ivs_by_expiration(report_rows, exp_list, min_trade_date=since_date)

    min_samples_for_bands = 5
    needs_eod = any(len(rollup_ivs.get(exp, [])) < min_samples_for_bands for exp in exp_list)
    if needs_eod:
        hist_rows = get_option_snapshots_eod_per_day(db, wide_keys, source=src, since_ts=since_ts)
        by_day = group_hist_rows_by_snap_day(hist_rows)
    else:
        by_day = {}

    per_exp_daily_ivs: Dict[str, List[float]] = {}
    rollup_used = True
    for exp in exp_list:
        r_series = rollup_ivs.get(exp, [])
        if len(r_series) >= min_samples_for_bands:
            per_exp_daily_ivs[exp] = r_series
        else:
            per_exp_daily_ivs[exp] = daily_atm_ivs_for_expiration(exp, key_exp_wide, by_day)
            rollup_used = False

    points, band_warns = assemble_volatility_cone_points(
        exp_list,
        last_price,
        exp_iv_cur_all,
        per_exp_daily_ivs,
        min_samples_for_bands=min_samples_for_bands,
    )
    out: Dict[str, Any] = {
        "ok": True,
        "symbol": sym,
        "underlying_price": last_price,
        "lookback_days": lookback_days,
        "points": points,
        "rollup_used": rollup_used,
    }
    if band_warns:
        nexp = len(band_warns)
        examples = "; ".join(f"{e}: samples={cnt}" for e, cnt in band_warns[:3])
        more = f" (+{nexp - 3} more expirations)" if nexp > 3 else ""
        out["warning"] = (
            f"p10–p90 bands need ≥{min_samples_for_bands} daily ATM IV points per expiration in the lookback; "
            f"{nexp} expiration(s) below that ({examples}{more}). "
            "See Hist. samples column. Current ATM line still uses latest snapshots."
        )
    return out
