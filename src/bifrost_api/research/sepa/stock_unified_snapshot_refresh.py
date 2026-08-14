"""Refresh cache_stock_snapshot from Massive GET /v3/snapshot (stocks, ticker.any_of batches)."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import psycopg2

from bifrost_core.persistence.postgres.connection import _get_conn_params
from bifrost_api.research.polygon_http import MassiveClient
from bifrost_api.research.polygon_http import get_massive_settings

logger = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 250
DEFAULT_INTER_CHUNK_SLEEP_SEC = 0.2

_INSERT_COLS: Tuple[str, ...] = (
    "symbol",
    "fetched_at",
    "updated_at",
    "last_minute_updated",
    "source",
    "snapshot_asset_type",
    "market_status",
    "snapshot_display_name",
    "session_open",
    "session_high",
    "session_low",
    "session_close",
    "session_previous_close",
    "session_volume",
    "session_decimal_volume",
    "session_change",
    "session_change_percent",
    "session_regular_trading_change",
    "session_regular_trading_change_percent",
    "session_early_trading_change",
    "session_early_trading_change_percent",
    "session_late_trading_change",
    "session_late_trading_change_percent",
    "last_minute_open",
    "last_minute_high",
    "last_minute_low",
    "last_minute_close",
    "last_minute_vwap",
    "last_minute_volume",
    "last_minute_decimal_volume",
    "last_minute_transactions",
    "last_trade_price",
    "last_trade_size",
    "last_trade_exchange",
    "last_trade_last_updated_ns",
    "last_trade_conditions",
    "last_quote_bid",
    "last_quote_ask",
    "last_quote_bid_size",
    "last_quote_ask_size",
    "last_quote_last_updated_ns",
)

_cols_sql = ", ".join(_INSERT_COLS)
_placeholders = ", ".join(["%s"] * len(_INSERT_COLS))
_update_sql = ", ".join(f"{c} = EXCLUDED.{c}" for c in _INSERT_COLS if c != "symbol")
_UPSERT_SQL = f"""
INSERT INTO public.cache_stock_snapshot ({_cols_sql})
VALUES ({_placeholders})
ON CONFLICT (symbol) DO UPDATE SET
{_update_sql}
"""


def _db_ok(status_config: Optional[dict]) -> bool:
    if not status_config:
        return False
    return status_config.get("sink") == "postgres" or bool(status_config.get("postgres"))


def _pick(d: Any, *keys: str) -> Any:
    if not isinstance(d, dict):
        return None
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None


def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _to_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _to_bigint(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        x = float(v)
        if x > 9e18 or x < -9e18:
            return None
        return int(x)
    except (TypeError, ValueError):
        return None


def _conditions_text(val: Any) -> Optional[str]:
    if val is None:
        return None
    if isinstance(val, list):
        parts: List[str] = []
        for x in val:
            try:
                parts.append(str(int(x)))
            except (TypeError, ValueError):
                parts.append(str(x))
        return ",".join(parts) if parts else None
    return str(val)


def _parse_ns_timestamp(val: Any) -> Optional[int]:
    """Nanosecond unix int suitable for bigint (trade/quote last_updated, sip_timestamp)."""
    if val is None:
        return None
    bi = _to_bigint(val)
    if bi is None:
        return None
    if bi > 0 and bi < 1_000_000_000_000:
        return bi * 1_000_000_000
    return bi


def _parse_last_minute_updated(last_minute: Any) -> Optional[datetime]:
    if not isinstance(last_minute, dict):
        return None
    for key in ("last_updated", "lastUpdated"):
        v = last_minute.get(key)
        if isinstance(v, str) and v.strip():
            s = v.strip().replace("Z", "+00:00")
            try:
                dt = datetime.fromisoformat(s)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                continue
        if isinstance(v, (int, float)):
            ns = _parse_ns_timestamp(v)
            if ns is not None:
                return datetime.fromtimestamp(ns / 1e9, tz=timezone.utc)
    t = last_minute.get("t")
    if t is None:
        return None
    try:
        tv = float(t)
    except (TypeError, ValueError):
        return None
    if tv > 1e17:
        return datetime.fromtimestamp(tv / 1e9, tz=timezone.utc)
    if tv > 1e12:
        return datetime.fromtimestamp(tv / 1000.0, tz=timezone.utc)
    if tv > 1e9:
        return datetime.fromtimestamp(tv, tz=timezone.utc)
    return None


def _session_floats(sess: Any) -> Tuple[Any, ...]:
    if not isinstance(sess, dict):
        return (None,) * 15
    gv = lambda *k: _to_float(_pick(sess, *k))
    gtxt = lambda *k: _pick(sess, *k)
    txt = gtxt("decimal_volume", "decimalVolume")
    if txt is not None and not isinstance(txt, str):
        txt = str(txt)
    return (
        gv("open", "o"),
        gv("high", "h"),
        gv("low", "l"),
        gv("close", "c"),
        gv("previous_close", "previousClose"),
        gv("volume", "v"),
        txt,
        gv("change"),
        gv("change_percent", "changePercent"),
        gv("regular_trading_change", "regularTradingChange"),
        gv("regular_trading_change_percent", "regularTradingChangePercent"),
        gv("early_trading_change", "earlyTradingChange"),
        gv("early_trading_change_percent", "earlyTradingChangePercent"),
        gv("late_trading_change", "lateTradingChange"),
        gv("late_trading_change_percent", "lateTradingChangePercent"),
    )


def _last_minute_vals(lm: Any) -> Tuple[Any, ...]:
    if not isinstance(lm, dict):
        return (None,) * 8
    gv = lambda *k: _to_float(_pick(lm, *k))
    gtxt = lambda *k: _pick(lm, *k)
    txt = gtxt("decimal_volume", "decimalVolume")
    if txt is not None and not isinstance(txt, str):
        txt = str(txt)
    return (
        gv("open", "o"),
        gv("high", "h"),
        gv("low", "l"),
        gv("close", "c"),
        gv("vwap", "vw"),
        gv("volume", "v"),
        txt,
        _to_bigint(_pick(lm, "transactions", "n")),
    )


def _last_trade_vals(lt: Any) -> Tuple[Any, ...]:
    if not isinstance(lt, dict):
        return (None, None, None, None, None)
    ex = _pick(lt, "exchange")
    ex_i = _to_int(ex) if ex is not None else None
    ts = _pick(lt, "sip_timestamp", "sipTimestamp", "last_updated", "lastUpdated", "t")
    ns = _parse_ns_timestamp(ts)
    return (
        _to_float(_pick(lt, "price", "p")),
        _to_bigint(_pick(lt, "size", "s")),
        ex_i,
        ns,
        _conditions_text(lt.get("conditions")),
    )


def _last_quote_vals(lq: Any) -> Tuple[Any, ...]:
    if not isinstance(lq, dict):
        return (None, None, None, None, None)
    ts = _pick(lq, "last_updated", "lastUpdated", "t")
    return (
        _to_float(_pick(lq, "bid", "bp")),
        _to_float(_pick(lq, "ask", "ap")),
        _to_bigint(_pick(lq, "bid_size", "bidSize", "bs")),
        _to_bigint(_pick(lq, "ask_size", "askSize", "as")),
        _parse_ns_timestamp(ts),
    )


def row_tuple_for_unified_result(r: Dict[str, Any], batch_ts: datetime) -> Optional[Tuple[Any, ...]]:
    """Build INSERT tuple aligned with _INSERT_COLS; returns None if row should be skipped."""
    if r.get("error") or r.get("message"):
        return None
    typ = (r.get("type") or r.get("asset_type") or "").strip().lower()
    if typ and typ not in ("stocks", "stock"):
        return None
    sym = str(r.get("ticker") or "").strip().upper()
    if not sym:
        return None
    session = r.get("session")
    last_minute = r.get("last_minute")
    last_trade = r.get("last_trade")
    last_quote = r.get("last_quote")
    lmu = _parse_last_minute_updated(last_minute)
    sess = _session_floats(session)
    lm = _last_minute_vals(last_minute)
    lt = _last_trade_vals(last_trade)
    lq = _last_quote_vals(last_quote)
    snap_type = (str(r.get("type")).strip() if r.get("type") is not None else None) or None
    mstat = (str(r.get("market_status") or "").strip() or None) if r.get("market_status") else None
    dname = r.get("name")
    dname_s = str(dname).strip() if dname is not None and str(dname).strip() else None
    return (
        sym,
        batch_ts,
        batch_ts,
        lmu,
        "massive",
        snap_type,
        mstat,
        dname_s,
        *sess,
        *lm,
        *lt,
        *lq,
    )


def _load_universe_symbols(status_config: dict) -> List[str]:
    params = _get_conn_params(status_config)
    params["connect_timeout"] = 15
    conn = psycopg2.connect(**params)
    try:
        try:
            from bifrost_core.persistence.postgres.universe_sync import (
                sync_universe_from_plugin,
            )

            sync_universe_from_plugin(conn)
            conn.commit()
        except Exception as e:
            logger.warning("universe sync before snapshot refresh failed: %s", e)
            try:
                conn.rollback()
            except Exception:
                pass
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT symbol FROM public.v_us_equity_universe
                ORDER BY symbol
                """
            )
            rows = cur.fetchall() or []
        return [str(r[0]).strip().upper() for r in rows if r and r[0]]
    finally:
        conn.close()


def run_refresh_cache_stock_unified_snapshots(
    status_config: dict,
    merged_config: dict,
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    inter_chunk_sleep_sec: float = DEFAULT_INTER_CHUNK_SLEEP_SEC,
    statement_timeout_ms: int = 600_000,
) -> Dict[str, Any]:
    """Fetch unified stock snapshots for all v_us_equity_universe symbols; UPSERT into cache_stock_snapshot."""
    if not _db_ok(status_config):
        return {"ok": False, "error": "PostgreSQL not configured"}
    ms = get_massive_settings(merged_config or {})
    if not ms.get("api_key"):
        return {"ok": False, "error": "Massive API key not configured"}

    symbols = _load_universe_symbols(status_config)
    if not symbols:
        return {
            "ok": True,
            "symbols_total": 0,
            "chunks": 0,
            "rows_upserted": 0,
            "errors": [],
            "elapsed_ms": 0,
            "message": "Universe is empty — nothing to refresh.",
        }

    client = MassiveClient(api_key=ms["api_key"], rest_base=str(ms.get("rest_base") or "https://api.polygon.io"))
    chunk_size = max(1, min(int(chunk_size), 250))
    t0 = time.monotonic()
    errors: List[str] = []
    rows_upserted = 0
    chunks = 0

    params = _get_conn_params(status_config)
    params["connect_timeout"] = 15
    conn = psycopg2.connect(**params)
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET statement_timeout = {int(max(5_000, statement_timeout_ms))}")
        conn.commit()

        for i in range(0, len(symbols), chunk_size):
            chunks += 1
            chunk = symbols[i : i + chunk_size]
            tickers_str = ",".join(chunk)
            try:
                # Do not pass asset_type with tickers: API forbids type + ticker.any_of.
                snap = client.fetch_unified_snapshot(
                    tickers=tickers_str,
                    limit=250,
                )
            except Exception as exc:
                errors.append(f"chunk {chunks}: {exc}")
                logger.warning("unified snapshot chunk failed: %s", exc)
                time.sleep(max(0.0, float(inter_chunk_sleep_sec)))
                continue

            if snap.get("error"):
                errors.append(f"chunk {chunks}: {snap.get('error')}")
                time.sleep(max(0.0, float(inter_chunk_sleep_sec)))
                continue

            results_list = snap.get("results") or []
            if not isinstance(results_list, list):
                results_list = []

            batch_ts = datetime.now(timezone.utc)
            with conn.cursor() as cur:
                for r in results_list:
                    if not isinstance(r, dict):
                        continue
                    tup = row_tuple_for_unified_result(r, batch_ts)
                    if tup is None:
                        continue
                    cur.execute(_UPSERT_SQL, tup)
                    rows_upserted += 1
            conn.commit()
            time.sleep(max(0.0, float(inter_chunk_sleep_sec)))

        elapsed_ms = int((time.monotonic() - t0) * 1000)
        return {
            "ok": True,
            "symbols_total": len(symbols),
            "chunks": chunks,
            "rows_upserted": rows_upserted,
            "errors": errors[:20],
            "elapsed_ms": elapsed_ms,
        }
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.warning("run_refresh_cache_stock_unified_snapshots failed: %s", e)
        return {"ok": False, "error": str(e), "errors": errors[:20]}
    finally:
        conn.close()
