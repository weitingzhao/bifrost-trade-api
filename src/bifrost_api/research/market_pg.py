"""Market PostgreSQL readers for research/ops (Wave 7-C).

Recovered from bifrost-worker data/massive/vendor/reader.py after deletion.
Reads ``market.*`` tables; SEPA job tables remain in public schema.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date as date_type
from datetime import datetime
from datetime import time as time_of_day
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2 import ProgrammingError
from psycopg2.extras import RealDictCursor

from bifrost_core.persistence.postgres.connection import _get_conn_params

from bifrost_api.research import market_data_client

logger = logging.getLogger(__name__)


def _use_plugin() -> bool:
    """Feature flag: MARKET_DATA_SOURCE=plugin (default) uses HTTP client."""
    return os.environ.get("MARKET_DATA_SOURCE", "plugin").lower() != "sql"

def _norm_expiry_date(expiry: str) -> Optional[date_type]:
    """Normalize expiry to date. Accepts YYYY-MM-DD or YYYYMMDD."""
    e = (expiry or "").strip()
    if not e:
        return None
    if len(e) >= 10 and e[4] == "-":
        try:
            return date_type.fromisoformat(e[:10])
        except ValueError:
            return None
    digits = "".join(c for c in e if c.isdigit())
    if len(digits) >= 8:
        try:
            return date_type(int(digits[0:4]), int(digits[4:6]), int(digits[6:8]))
        except ValueError:
            return None
    return None
def insert_job_sepa_phase4(
    status_config: dict,
    job_id: str,
    request_payload: Optional[Dict[str, Any]] = None,
    *,
    version: str = "sepa_phase4_v1",
) -> Optional[int]:
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return None
    jid = (job_id or "").strip()
    if not jid:
        return None
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO job_sepa_phase4
                        (job_id, status, progress, request, summary, errors, created_at, updated_at, version)
                    VALUES (%s, 'queued', %s::jsonb, %s::jsonb, '{}'::jsonb, '[]'::jsonb, now(), now(), %s)
                    RETURNING job_sepa_phase4_id
                    """,
                    (
                        jid,
                        json.dumps({"current": 0, "total": len((request_payload or {}).get("symbols") or []), "stage": "queued", "pct": 0.0}),
                        json.dumps(request_payload or {}),
                        version,
                    ),
                )
                row = cur.fetchone()
            conn.commit()
            return int(row[0]) if row else None
        finally:
            conn.close()
    except Exception as e:
        logger.warning("insert_job_sepa_phase4 failed: %s", e)
        return None


def get_job_sepa_phase4(
    status_config: dict,
    job_id: str,
) -> Optional[Dict[str, Any]]:
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return None
    jid = (job_id or "").strip()
    if not jid:
        return None
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT job_id, status, progress, request, summary, errors,
                           created_at, updated_at, started_at, finished_at, version
                    FROM job_sepa_phase4
                    WHERE job_id = %s
                    LIMIT 1
                    """,
                    (jid,),
                )
                row = cur.fetchone()
            return dict(row) if row else None
        finally:
            conn.close()
    except Exception as e:
        logger.warning("get_job_sepa_phase4 failed: %s", e)
        return None


def get_job_sepa_phase4_result(
    status_config: dict,
    job_id: str,
    *,
    offset: int = 0,
    limit: int = 200,
) -> Optional[Dict[str, Any]]:
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return None
    jid = (job_id or "").strip()
    if not jid:
        return None
    st = max(0, int(offset))
    lim = max(1, min(int(limit), 1000))
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT job_id, status, summary, result, version
                    FROM job_sepa_phase4
                    WHERE job_id = %s
                    LIMIT 1
                    """,
                    (jid,),
                )
                row = cur.fetchone()
            if not row:
                return None
            result = row.get("result") or {}
            rows = result.get("rows") or []
            if not isinstance(rows, list):
                rows = []
            ed = st + lim
            return {
                "job_id": row.get("job_id"),
                "status": row.get("status"),
                "summary": row.get("summary") or {},
                "rows": rows[st:ed],
                "total_rows": len(rows),
                "offset": st,
                "limit": lim,
                "version": row.get("version") or "sepa_phase4_v1",
            }
        finally:
            conn.close()
    except Exception as e:
        logger.warning("get_job_sepa_phase4_result failed: %s", e)
        return None


def update_job_sepa_phase4(
    status_config: dict,
    job_id: str,
    **fields: Any,
) -> bool:
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return False
    jid = (job_id or "").strip()
    if not jid:
        return False
    allowed = {
        "status",
        "progress",
        "request",
        "summary",
        "result",
        "errors",
        "started_at",
        "finished_at",
        "version",
    }
    set_parts: List[str] = []
    params_list: List[Any] = []
    for k, v in fields.items():
        if k not in allowed:
            continue
        if k in {"progress", "request", "summary", "result", "errors"}:
            set_parts.append(f"{k} = %s::jsonb")
            params_list.append(json.dumps(v) if v is not None else ("[]" if k == "errors" else "{}"))
        elif k in {"started_at", "finished_at"} and isinstance(v, str):
            set_parts.append(f"{k} = %s::timestamptz")
            params_list.append(v)
        else:
            set_parts.append(f"{k} = %s")
            params_list.append(v)
    if not set_parts:
        return True
    set_parts.append("updated_at = now()")
    sql = f"UPDATE job_sepa_phase4 SET {', '.join(set_parts)} WHERE job_id = %s"
    params_list.append(jid)
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params_list))
            conn.commit()
            return True
        finally:
            conn.close()
    except Exception as e:
        logger.warning("update_job_sepa_phase4 failed: %s", e)
        return False


def list_job_sepa_phase4(
    status_config: dict,
    *,
    limit: int = 50,
    offset: int = 0,
    status_filter: Optional[str] = None,
    created_from: Optional[str] = None,
    created_to: Optional[str] = None,
) -> List[Dict[str, Any]]:
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return []
    lim = max(1, min(int(limit), 200))
    off = max(0, int(offset))
    base_sql = """
        SELECT job_id, status, progress, request, summary, errors,
               created_at, updated_at, started_at, finished_at, version
        FROM job_sepa_phase4
    """
    conditions: List[str] = []
    params_list: List[Any] = []
    sf = (status_filter or "").strip()
    if sf:
        conditions.append("status = %s")
        params_list.append(sf)
    cf = (created_from or "").strip()
    if cf:
        conditions.append("created_at >= %s::timestamptz")
        params_list.append(cf)
    ct = (created_to or "").strip()
    if ct:
        conditions.append("created_at <= %s::timestamptz")
        params_list.append(ct)
    where_sql = (" WHERE " + " AND ".join(conditions)) if conditions else ""
    sql = f"{base_sql}{where_sql}"
    sql += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
    params_list.extend([lim, off])
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, tuple(params_list))
                rows = cur.fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()
    except Exception as e:
        logger.warning("list_job_sepa_phase4 failed: %s", e)
        return []


def delete_job_sepa_phase4(status_config: dict, job_id: str) -> bool:
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return False
    jid = (job_id or "").strip()
    if not jid:
        return False
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM job_sepa_phase4 WHERE job_id = %s", (jid,))
            conn.commit()
            return True
        finally:
            conn.close()
    except Exception as e:
        logger.warning("delete_job_sepa_phase4 failed: %s", e)
        return False

def _expiry_to_date_param(expiry: str) -> Optional[str]:
    """Normalize expiry string to ISO date (YYYY-MM-DD) for market.* date columns."""
    e = _norm_expiry_db(str(expiry or "").strip())
    if len(e) == 8 and e.isdigit():
        return f"{e[:4]}-{e[4:6]}-{e[6:8]}"
    raw = (expiry or "").strip()
    if len(raw) >= 10 and raw[4] == "-":
        return raw[:10]
    return None


def get_option_open_interest_daily(
    status_config: dict,
    symbol: str,
    expiry: Optional[str] = None,
    limit: int = 100,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Latest OI rows for symbol from market.option_open_interest (optional expiry / trade_date range)."""
    sym = (symbol or "").strip().upper()
    if not sym:
        return []
    if _use_plugin():
        try:
            exp_param = _norm_expiry_db(expiry) if expiry else None
            return market_data_client.fetch_option_oi(
                sym,
                expiry=exp_param,
                limit=limit,
                date_from=date_from[:10] if date_from else None,
                date_to=date_to[:10] if date_to else None,
            )
        except Exception as e:
            logger.warning("Plugin API failed for get_option_open_interest_daily, falling back to SQL: %s", e)
    return _sql_get_option_open_interest_daily(status_config, sym, expiry=expiry, limit=limit, date_from=date_from, date_to=date_to)


def _sql_get_option_open_interest_daily(
    status_config: dict,
    symbol: str,
    expiry: Optional[str] = None,
    limit: int = 100,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """SQL fallback: direct PostgreSQL read for option OI."""
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return []
    sym = (symbol or "").strip().upper()
    if not sym:
        return []
    _OI_SELECT = """
        SELECT option_ticker AS contract_key,
               underlying AS symbol,
               expiry::text AS expiry,
               strike,
               option_right,
               trade_date,
               open_interest,
               'polygon' AS source
        FROM market.option_open_interest
    """
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                lim = max(1, min(500, limit))
                exp_d = _expiry_to_date_param(expiry) if expiry else None
                if expiry and exp_d is None:
                    return []
                if expiry and date_from and date_to:
                    cur.execute(
                        _OI_SELECT
                        + """
                        WHERE underlying = %s AND expiry = %s::date
                          AND trade_date >= %s::date AND trade_date <= %s::date
                        ORDER BY trade_date DESC
                        LIMIT %s
                        """,
                        (sym, exp_d, date_from[:10], date_to[:10], lim),
                    )
                elif expiry:
                    cur.execute(
                        _OI_SELECT
                        + """
                        WHERE underlying = %s AND expiry = %s::date
                        ORDER BY trade_date DESC
                        LIMIT %s
                        """,
                        (sym, exp_d, lim),
                    )
                elif date_from and date_to:
                    cur.execute(
                        _OI_SELECT
                        + """
                        WHERE underlying = %s
                          AND trade_date >= %s::date AND trade_date <= %s::date
                        ORDER BY trade_date DESC
                        LIMIT %s
                        """,
                        (sym, date_from[:10], date_to[:10], lim),
                    )
                else:
                    cur.execute(
                        _OI_SELECT
                        + """
                        WHERE underlying = %s
                        ORDER BY trade_date DESC
                        LIMIT %s
                        """,
                        (sym, lim),
                    )
                rows = cur.fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()
    except Exception as e:
        logger.debug("_sql_get_option_open_interest_daily failed: %s", e)
        return []

_SNAPSHOT_LATEST_SELECT = """
    s.snapshot_ts,
    s.iv, s.delta, s.gamma, s.theta, s.vega, s.open_interest,
    s.underlying AS underlying_ticker,
    s.day_open, s.day_high, s.day_low, s.day_close,
    s.day_previous_close,
    NULL::double precision AS day_change,
    s.day_change_percent,
    s.day_volume, s.day_vwap,
    NULL::timestamptz AS day_last_updated,
    NULL::date AS day_last_updated_day,
    'massive' AS source,
    s.fetched_at AS created_at,
    oc.option_ticker AS _option_ticker,
    oc.underlying AS _underlying,
    oc.expiry AS _expiry,
    oc.strike AS _strike,
    oc.option_right AS _option_right
"""

_IB_CONTRACT_JOIN = """
    FROM unnest(%s::text[], %s::date[], %s::text[], %s::float8[], %s::text[])
      AS req(underlying, expiry, option_right, strike, ib_key)
    JOIN market.option_contract oc
      ON oc.underlying = req.underlying
     AND oc.expiry = req.expiry
     AND oc.option_right = req.option_right
     AND abs(oc.strike - req.strike) < 1e-4
"""


def _map_snapshot_row_to_ib_key(
    row: Dict[str, Any],
    *,
    ib_by_identity: Dict[Tuple[str, date_type, float, str], str],
    poly_requested: set,
) -> Optional[Dict[str, Any]]:
    """Attach IB ``contract_key`` for callers; drop internal bridge columns."""
    from bifrost_api.research.contract_key_bridge import (
        ib_contract_key_from_parts,
        identity_key,
    )

    out = dict(row)
    req_ib_key = out.pop("_req_ib_key", None)
    underlying = out.pop("_underlying", None) or out.get("underlying_ticker")
    expiry = out.pop("_expiry", None)
    strike = out.pop("_strike", None)
    option_right = out.pop("_option_right", None)
    option_ticker = out.pop("_option_ticker", None)

    ck: Optional[str] = None
    if req_ib_key:
        ck = str(req_ib_key)
    elif underlying is not None and expiry is not None and strike is not None and option_right:
        try:
            exp_d = expiry if isinstance(expiry, date_type) else date_type.fromisoformat(str(expiry)[:10])
            ident = identity_key(str(underlying), exp_d, float(strike), str(option_right))
            ck = ib_by_identity.get(ident)
            if ck is None and option_ticker in poly_requested:
                ck = ib_contract_key_from_parts(
                    str(underlying), exp_d, float(strike), str(option_right)
                )
        except (TypeError, ValueError):
            ck = None
    if ck is None:
        return None
    out["contract_key"] = ck
    return out


def _fetch_option_snapshots_latest_bridged(cur: Any, keys: List[str]) -> List[Dict[str, Any]]:
    from bifrost_api.research.contract_key_bridge import (
        identity_key,
        split_contract_keys,
    )

    polygon, ib_parts = split_contract_keys(keys)
    if not polygon and not ib_parts:
        return []

    ib_by_identity = {
        identity_key(p.underlying, p.expiry, p.strike, p.option_right): p.original_key
        for p in ib_parts
    }
    poly_requested = set(polygon)

    rows: List[Dict[str, Any]] = []

    def _view_exists() -> bool:
        cur.execute(
            """
            SELECT 1 FROM information_schema.views
            WHERE table_schema = 'market' AND table_name = 'v_option_chain_latest'
            LIMIT 1
            """
        )
        return bool(cur.fetchone())

    if polygon:
        view_ok = False
        try:
            if _view_exists():
                cur.execute(
                    f"""
                    SELECT {_SNAPSHOT_LATEST_SELECT}
                    FROM market.v_option_chain_latest s
                    JOIN market.option_contract oc ON oc.option_ticker = s.option_ticker
                    WHERE s.option_ticker = ANY(%s)
                    """,
                    (polygon,),
                )
                view_ok = True
        except Exception:
            try:
                cur.connection.rollback()
            except Exception:
                pass
            view_ok = False

        if not view_ok:
            cur.execute(
                f"""
                SELECT DISTINCT ON (s.option_ticker)
                    {_SNAPSHOT_LATEST_SELECT}
                FROM market.option_snapshot s
                JOIN market.option_contract oc ON oc.option_ticker = s.option_ticker
                WHERE s.option_ticker = ANY(%s)
                ORDER BY s.option_ticker, s.snapshot_ts DESC
                """,
                (polygon,),
            )
        rows.extend(dict(r) for r in cur.fetchall())

    if ib_parts:
        underlyings = [p.underlying for p in ib_parts]
        expiries = [p.expiry for p in ib_parts]
        strikes = [p.strike for p in ib_parts]
        rights = [p.option_right for p in ib_parts]
        ib_keys = [p.original_key for p in ib_parts]
        ib_params = (underlyings, expiries, rights, strikes, ib_keys)
        select_ib = _SNAPSHOT_LATEST_SELECT + ",\n    req.ib_key AS _req_ib_key"
        view_ok = False
        try:
            if _view_exists():
                cur.execute(
                    f"""
                    SELECT {select_ib}
                    {_IB_CONTRACT_JOIN}
                    JOIN market.v_option_chain_latest s ON s.option_ticker = oc.option_ticker
                    """,
                    ib_params,
                )
                view_ok = True
        except Exception:
            try:
                cur.connection.rollback()
            except Exception:
                pass
            view_ok = False

        if not view_ok:
            cur.execute(
                f"""
                SELECT DISTINCT ON (oc.option_ticker)
                    {select_ib}
                {_IB_CONTRACT_JOIN}
                JOIN market.option_snapshot s ON s.option_ticker = oc.option_ticker
                ORDER BY oc.option_ticker, s.snapshot_ts DESC
                """,
                ib_params,
            )
        rows.extend(dict(r) for r in cur.fetchall())

    out: List[Dict[str, Any]] = []
    seen_ck: set = set()
    for row in rows:
        mapped = _map_snapshot_row_to_ib_key(
            row, ib_by_identity=ib_by_identity, poly_requested=poly_requested
        )
        if mapped is None:
            continue
        ck = mapped["contract_key"]
        if ck in seen_ck:
            continue
        seen_ck.add(ck)
        out.append(mapped)
    return out


def get_option_snapshots_latest(
    status_config: dict,
    contract_keys: List[str],
    source: str = "massive",
) -> List[Dict[str, Any]]:
    """Latest snapshot per contract_key from ``market.v_option_chain_latest``.

    Accepts IB keys (``SYM|OPT|…``) and Polygon tickers (``O:…``). Always returns
    IB-shaped ``contract_key`` so Discovery/Screener ``parse_contract_key`` works.
    ``source`` is accepted for API compatibility but ignored.

    In plugin mode, the Plugin API handles IB↔Polygon key bridging internally.
    contract_key_bridge.py is only needed for the SQL fallback path.
    """
    _ = source  # unused — market.* has no source column
    if not contract_keys:
        return []
    keys = [k for k in contract_keys if k and str(k).strip()][:120]
    if not keys:
        return []
    if _use_plugin():
        try:
            return market_data_client.fetch_option_chain_latest(keys)
        except Exception as e:
            logger.warning("Plugin API failed for get_option_snapshots_latest, falling back to SQL: %s", e)
    return _sql_get_option_snapshots_latest(status_config, keys)


def _sql_get_option_snapshots_latest(
    status_config: dict,
    contract_keys: List[str],
) -> List[Dict[str, Any]]:
    """SQL fallback: direct PostgreSQL read with contract_key_bridge."""
    if not status_config or (
        status_config.get("sink") != "postgres" and not status_config.get("postgres")
    ):
        return []
    if not contract_keys:
        return []
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                return _fetch_option_snapshots_latest_bridged(cur, contract_keys)
        finally:
            conn.close()
    except Exception as e:
        logger.debug("_sql_get_option_snapshots_latest failed: %s", e)
        return []


def _fetch_option_snapshots_eod_bridged(
    cur: Any,
    keys: List[str],
    since_ts: datetime,
) -> List[Dict[str, Any]]:
    from bifrost_api.research.contract_key_bridge import (
        ib_contract_key_from_parts,
        identity_key,
        split_contract_keys,
    )

    polygon, ib_parts = split_contract_keys(keys)
    if not polygon and not ib_parts:
        return []

    ib_by_identity = {
        identity_key(p.underlying, p.expiry, p.strike, p.option_right): p.original_key
        for p in ib_parts
    }
    poly_requested = set(polygon)
    out: List[Dict[str, Any]] = []

    def _append_mapped(raw_rows: List[Any]) -> None:
        for row in raw_rows:
            d = dict(row)
            req_ib_key = d.pop("_req_ib_key", None)
            underlying = d.pop("_underlying", None)
            expiry = d.pop("_expiry", None)
            strike = d.pop("_strike", None)
            option_right = d.pop("_option_right", None)
            option_ticker = d.pop("_option_ticker", None)
            ck: Optional[str] = None
            if req_ib_key:
                ck = str(req_ib_key)
            elif underlying is not None and expiry is not None and strike is not None and option_right:
                try:
                    exp_d = (
                        expiry
                        if isinstance(expiry, date_type)
                        else date_type.fromisoformat(str(expiry)[:10])
                    )
                    ident = identity_key(str(underlying), exp_d, float(strike), str(option_right))
                    ck = ib_by_identity.get(ident)
                    if ck is None and option_ticker in poly_requested:
                        ck = ib_contract_key_from_parts(
                            str(underlying), exp_d, float(strike), str(option_right)
                        )
                except (TypeError, ValueError):
                    ck = None
            if ck is None:
                continue
            d["contract_key"] = ck
            out.append(d)

    if polygon:
        cur.execute(
            """
            SELECT DISTINCT ON (
              (DATE(timezone('America/New_York', v.snapshot_ts))),
              oc.option_ticker
            )
              DATE(timezone('America/New_York', v.snapshot_ts)) AS snap_day,
              v.iv,
              v.underlying_price,
              v.snapshot_ts,
              oc.option_ticker AS _option_ticker,
              oc.underlying AS _underlying,
              oc.expiry AS _expiry,
              oc.strike AS _strike,
              oc.option_right AS _option_right
            FROM market.v_option_snapshot_with_stock v
            JOIN market.option_contract oc ON oc.option_ticker = v.option_ticker
            WHERE v.option_ticker = ANY(%s)
              AND v.snapshot_ts >= %s
            ORDER BY
              DATE(timezone('America/New_York', v.snapshot_ts)),
              oc.option_ticker,
              v.snapshot_ts DESC
            """,
            (polygon, since_ts),
        )
        _append_mapped(cur.fetchall())

    if ib_parts:
        underlyings = [p.underlying for p in ib_parts]
        expiries = [p.expiry for p in ib_parts]
        strikes = [p.strike for p in ib_parts]
        rights = [p.option_right for p in ib_parts]
        ib_keys = [p.original_key for p in ib_parts]
        cur.execute(
            f"""
            SELECT DISTINCT ON (
              (DATE(timezone('America/New_York', v.snapshot_ts))),
              oc.option_ticker
            )
              DATE(timezone('America/New_York', v.snapshot_ts)) AS snap_day,
              v.iv,
              v.underlying_price,
              v.snapshot_ts,
              oc.option_ticker AS _option_ticker,
              oc.underlying AS _underlying,
              oc.expiry AS _expiry,
              oc.strike AS _strike,
              oc.option_right AS _option_right,
              req.ib_key AS _req_ib_key
            {_IB_CONTRACT_JOIN}
            JOIN market.v_option_snapshot_with_stock v ON v.option_ticker = oc.option_ticker
            WHERE v.snapshot_ts >= %s
            ORDER BY
              DATE(timezone('America/New_York', v.snapshot_ts)),
              oc.option_ticker,
              v.snapshot_ts DESC
            """,
            (underlyings, expiries, rights, strikes, ib_keys, since_ts),
        )
        _append_mapped(cur.fetchall())

    return out


def get_option_snapshots_eod_per_day(
    status_config: dict,
    contract_keys: List[str],
    source: str = "massive",
    since_ts: Optional[datetime] = None,
    chunk_size: int = 100,
) -> List[Dict[str, Any]]:
    """Latest snapshot per calendar day (America/New_York) per contract_key.

    Reads ``market.v_option_snapshot_with_stock``. Accepts IB and Polygon keys;
    returns IB-shaped ``contract_key``. ``source`` kept for API compat.

    In plugin mode, the Plugin API handles key bridging and chunking internally.
    """
    _ = source  # unused — market.* has no source column
    if not contract_keys:
        return []
    keys = [k for k in contract_keys if k and str(k).strip()]
    if not keys:
        return []
    if _use_plugin():
        try:
            since_iso: Optional[str] = None
            if since_ts is not None:
                since_iso = since_ts.isoformat() if since_ts.year > 1970 else None
            return market_data_client.fetch_option_chain_eod(keys, since=since_iso)
        except Exception as e:
            logger.warning("Plugin API failed for get_option_snapshots_eod_per_day, falling back to SQL: %s", e)
    return _sql_get_option_snapshots_eod_per_day(status_config, keys, since_ts=since_ts, chunk_size=chunk_size)


def _sql_get_option_snapshots_eod_per_day(
    status_config: dict,
    contract_keys: List[str],
    since_ts: Optional[datetime] = None,
    chunk_size: int = 100,
) -> List[Dict[str, Any]]:
    """SQL fallback: direct PostgreSQL read with contract_key_bridge."""
    if not status_config or (
        status_config.get("sink") != "postgres" and not status_config.get("postgres")
    ):
        return []
    if not contract_keys:
        return []
    if since_ts is None:
        since_ts = datetime(1970, 1, 1)
    chunk_size = max(10, min(120, int(chunk_size)))

    out: List[Dict[str, Any]] = []
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                for i in range(0, len(contract_keys), chunk_size):
                    batch = contract_keys[i : i + chunk_size]
                    out.extend(_fetch_option_snapshots_eod_bridged(cur, batch, since_ts))
            return out
        finally:
            conn.close()
    except Exception as e:
        logger.debug("_sql_get_option_snapshots_eod_per_day failed: %s", e)
        return []
def _norm_expiry_db(expiry: str) -> str:
    e = (expiry or "").strip()
    if len(e) >= 10 and e[4] == "-":
        return e[:4] + e[5:7] + e[8:10]
    return e

def get_stock_day_series_for_sepa(
    status_config: dict,
    symbols: List[str],
    *,
    lookback_days: int = 400,
    source: str = "massive",
) -> Dict[str, List[Dict[str, Any]]]:
    """Batch-read market.stock_daily rows for SEPA phase-1 technical screening.

    Returns an ascending bar series per symbol with keys:
    ``symbol, bar_time, open, high, low, close, volume, source``.
    ``source`` is accepted for API compatibility but ignored.
    """
    if _use_plugin():
        try:
            return market_data_client.fetch_stock_bars_daily(
                [str(s or "").strip().upper() for s in symbols if str(s or "").strip()],
                days=lookback_days,
            )
        except Exception as e:
            logger.warning("Plugin API failed for get_stock_day_series_for_sepa, falling back to SQL: %s", e)
    return _sql_get_stock_day_series_for_sepa(status_config, symbols, lookback_days=lookback_days, source=source)


def _sql_get_stock_day_series_for_sepa(
    status_config: dict,
    symbols: List[str],
    *,
    lookback_days: int = 400,
    source: str = "massive",
) -> Dict[str, List[Dict[str, Any]]]:
    """SQL fallback: direct PostgreSQL read for SEPA stock bars."""
    _ = source
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return {}
    syms = sorted({str(s or "").strip().upper() for s in symbols if str(s or "").strip()})
    if not syms:
        return {}
    lb = max(260, min(int(lookback_days), 3000))
    out: Dict[str, List[Dict[str, Any]]] = {s: [] for s in syms}
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                      UPPER(TRIM(symbol)) AS symbol,
                      bar_date AS bar_time,
                      open,
                      high,
                      low,
                      close,
                      volume,
                      'massive' AS source
                    FROM market.stock_daily
                    WHERE UPPER(TRIM(symbol)) = ANY(%s)
                      AND bar_date >= (CURRENT_DATE - (%s || ' days')::interval)::date
                    ORDER BY UPPER(TRIM(symbol)), bar_date ASC
                    """,
                    (syms, lb),
                )
                rows = cur.fetchall() or []
            for row in rows:
                sym = str((row or {}).get("symbol") or "").strip().upper()
                if not sym:
                    continue
                if sym not in out:
                    out[sym] = []
                out[sym].append(dict(row))
        finally:
            conn.close()
    except Exception as e:
        logger.warning("_sql_get_stock_day_series_for_sepa failed: %s", e)
        return out
    return out


def get_stock_day_close_series_for_crs(
    status_config: dict,
    symbols: List[str],
    *,
    lookback_days: int = 420,
    source: str = "massive",
) -> Dict[str, List[Dict[str, Any]]]:
    """Batch-read market.stock_daily close series for CRS calculation."""
    if _use_plugin():
        try:
            return market_data_client.fetch_stock_bars_daily_close(
                [str(s or "").strip().upper() for s in symbols if str(s or "").strip()],
                days=lookback_days,
            )
        except Exception as e:
            logger.warning("Plugin API failed for get_stock_day_close_series_for_crs, falling back to SQL: %s", e)
    return _sql_get_stock_day_close_series_for_crs(status_config, symbols, lookback_days=lookback_days, source=source)


def _sql_get_stock_day_close_series_for_crs(
    status_config: dict,
    symbols: List[str],
    *,
    lookback_days: int = 420,
    source: str = "massive",
) -> Dict[str, List[Dict[str, Any]]]:
    """SQL fallback: direct PostgreSQL read for CRS close series."""
    _ = source
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return {}
    syms = sorted({str(s or "").strip().upper() for s in symbols if str(s or "").strip()})
    if not syms:
        return {}
    lb = max(260, min(int(lookback_days), 3000))
    out: Dict[str, List[Dict[str, Any]]] = {s: [] for s in syms}
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                      UPPER(TRIM(symbol)) AS symbol,
                      bar_date AS bar_time,
                      close
                    FROM market.stock_daily
                    WHERE UPPER(TRIM(symbol)) = ANY(%s)
                      AND bar_date >= (CURRENT_DATE - (%s || ' days')::interval)::date
                      AND close IS NOT NULL
                    ORDER BY UPPER(TRIM(symbol)), bar_date ASC
                    """,
                    (syms, lb),
                )
                rows = cur.fetchall() or []
            for row in rows:
                sym = str((row or {}).get("symbol") or "").strip().upper()
                if not sym:
                    continue
                out.setdefault(sym, []).append(dict(row))
        finally:
            conn.close()
    except Exception as e:
        logger.warning("_sql_get_stock_day_close_series_for_crs failed: %s", e)
        return out
    return out

def _right_from_ref_contract_type(ct: str) -> str:
    u = (ct or "").upper()
    if u in ("CALL", "C"):
        return "C"
    if u in ("PUT", "P"):
        return "P"
    return "C"

def is_us_equity_regular_session_et(now: Optional[datetime] = None) -> bool:
    """Weekday 09:30–16:00 America/New_York (no holiday calendar)."""
    et = ZoneInfo("America/New_York")
    dt = now or datetime.now(et)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=et)
    else:
        dt = dt.astimezone(et)
    if dt.weekday() >= 5:
        return False
    t = dt.time()
    return time_of_day(9, 30) <= t < time_of_day(16, 0)


def get_option_expirations_from_contracts_db(status_config: dict, symbol: str) -> List[str]:
    """Distinct expirations (YYYYMMDD) from market.option_contract for an underlying."""
    sym = (symbol or "").strip().upper()
    if not sym:
        return []
    if _use_plugin():
        try:
            return market_data_client.fetch_option_expirations_yyyymmdd(sym)
        except Exception as e:
            logger.warning("Plugin API failed for get_option_expirations_from_contracts_db, falling back to SQL: %s", e)
    return _sql_get_option_expirations_from_contracts_db(status_config, sym)


def _sql_get_option_expirations_from_contracts_db(status_config: dict, symbol: str) -> List[str]:
    """SQL fallback: direct PostgreSQL read for option expirations."""
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return []
    sym = (symbol or "").strip().upper()
    if not sym:
        return []
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT expiry FROM market.option_contract
                    WHERE underlying = %s
                    ORDER BY expiry
                    """,
                    (sym,),
                )
                out: List[str] = []
                for r in cur.fetchall():
                    if not r or r[0] is None:
                        continue
                    v = r[0]
                    if hasattr(v, "strftime"):
                        out.append(v.strftime("%Y%m%d"))
                    else:
                        s = str(v).strip()
                        if len(s) >= 10 and s[4] == "-":
                            out.append(s[:4] + s[5:7] + s[8:10])
                        else:
                            out.append(s)
                return out
        finally:
            conn.close()
    except Exception as e:
        logger.debug("_sql_get_option_expirations_from_contracts_db failed: %s", e)
        return []


def get_strikes_for_expiry_from_contracts_db(
    status_config: dict, symbol: str, expiration: str
) -> List[float]:
    """Distinct strikes for symbol + expiry from market.option_contract."""
    sym = (symbol or "").strip().upper()
    exp_raw = (expiration or "").strip()
    if not sym or not exp_raw:
        return []
    if _use_plugin():
        try:
            exp_param = _norm_expiry_db(exp_raw)
            return market_data_client.fetch_option_strikes(sym, exp_param)
        except Exception as e:
            logger.warning("Plugin API failed for get_strikes_for_expiry_from_contracts_db, falling back to SQL: %s", e)
    return _sql_get_strikes_for_expiry_from_contracts_db(status_config, sym, expiration)


def _sql_get_strikes_for_expiry_from_contracts_db(
    status_config: dict, symbol: str, expiration: str
) -> List[float]:
    """SQL fallback: direct PostgreSQL read for option strikes."""
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return []
    sym = (symbol or "").strip().upper()
    exp = _norm_expiry_date((expiration or "").strip())
    if not sym or exp is None:
        return []
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT strike FROM market.option_contract
                    WHERE underlying = %s AND expiry = %s
                    ORDER BY strike
                    """,
                    (sym, exp),
                )
                out: List[float] = []
                for r in cur.fetchall():
                    if r and r[0] is not None:
                        try:
                            out.append(float(r[0]))
                        except (TypeError, ValueError):
                            pass
                return out
        finally:
            conn.close()
    except Exception as e:
        logger.debug("_sql_get_strikes_for_expiry_from_contracts_db failed: %s", e)
        return []


def get_option_expiration_cache_snapshot(
    status_config: dict, symbol: str, source: str = "massive"
) -> Optional[Tuple[List[str], Optional[datetime]]]:
    """Return (sorted expirations, max updated_at) from market.option_expiration.

    ``source`` is accepted for API compatibility but ignored (no source column).
    In plugin mode, fetches from Plugin API and transforms to match expected shape.
    """
    _ = source
    sym = (symbol or "").strip().upper()
    if not sym:
        return None
    if _use_plugin():
        try:
            result = market_data_client.fetch_option_expirations(sym)
            if result is None:
                return None
            exps_raw, updated_at_iso = result
            max_u: Optional[datetime] = None
            if updated_at_iso:
                try:
                    max_u = datetime.fromisoformat(updated_at_iso)
                    if max_u.tzinfo is None:
                        max_u = max_u.replace(tzinfo=ZoneInfo("UTC"))
                except (TypeError, ValueError):
                    max_u = None
            return (exps_raw, max_u)
        except Exception as e:
            logger.warning("Plugin API failed for get_option_expiration_cache_snapshot, falling back to SQL: %s", e)
    return _sql_get_option_expiration_cache_snapshot(status_config, sym)


def _sql_get_option_expiration_cache_snapshot(
    status_config: dict, symbol: str
) -> Optional[Tuple[List[str], Optional[datetime]]]:
    """SQL fallback: direct PostgreSQL read for option expiration cache."""
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return None
    sym = (symbol or "").strip().upper()
    if not sym:
        return None
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT expiry, updated_at FROM market.option_expiration
                    WHERE underlying = %s
                    ORDER BY expiry
                    """,
                    (sym,),
                )
                rows = cur.fetchall()
            if not rows:
                return None
            exps: List[str] = []
            max_u: Optional[datetime] = None
            for r in rows:
                exp_raw = r[0]
                if hasattr(exp_raw, "isoformat"):
                    exps.append(exp_raw.isoformat()[:10])
                else:
                    exps.append(str(exp_raw).strip())
                u = r[1]
                if u is not None:
                    if hasattr(u, "tzinfo") and u.tzinfo is None:
                        u = u.replace(tzinfo=ZoneInfo("UTC"))
                    if max_u is None or u > max_u:
                        max_u = u
            return (exps, max_u)
        finally:
            conn.close()
    except ProgrammingError as e:
        if getattr(e, "pgcode", None) == "42P01":
            return None
        logger.debug("_sql_get_option_expiration_cache_snapshot: %s", e)
        return None
    except Exception as e:
        logger.debug("_sql_get_option_expiration_cache_snapshot failed: %s", e)
        return None

def replace_option_expiration_cache(
    status_config: dict,
    symbol: str,
    expirations: List[str],
    source: str = "massive",
) -> None:
    """Replace full expiration list for a symbol in market.option_expiration.

    ``source`` is accepted for API compatibility but ignored (no source column).
    """
    _ = source
    sym = (symbol or "").strip().upper()
    if not sym or not status_config:
        return
    if not expirations:
        return
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM market.option_expiration WHERE underlying = %s",
                    (sym,),
                )
                for raw in expirations:
                    exp_d = _expiry_to_date_param(str(raw))
                    if not exp_d:
                        continue
                    cur.execute(
                        """
                        INSERT INTO market.option_expiration (underlying, expiry, updated_at)
                        VALUES (%s, %s::date, now())
                        ON CONFLICT (underlying, expiry) DO UPDATE SET updated_at = now()
                        """,
                        (sym, exp_d),
                    )
            conn.commit()
        finally:
            conn.close()
    except ProgrammingError as e:
        if getattr(e, "pgcode", None) == "42P01":
            return
        logger.warning("replace_option_expiration_cache failed: %s", e)
    except Exception as e:
        logger.warning("replace_option_expiration_cache failed: %s", e)

def _ensure_sepa_fundamentals_cache_table(cur: Any) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS research_sepa_fundamentals_cache (
            symbol text NOT NULL,
            rule_version text NOT NULL,
            payload jsonb NOT NULL,
            source text DEFAULT 'massive',
            fetched_at timestamptz NOT NULL DEFAULT now(),
            expire_at timestamptz NOT NULL,
            updated_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (symbol, rule_version)
        )
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_research_sepa_fund_cache_expire
        ON research_sepa_fundamentals_cache (expire_at)
        """
    )


def get_sepa_fundamentals_cache_snapshot(
    status_config: dict,
    symbol: str,
    *,
    rule_version: str,
) -> Optional[Dict[str, Any]]:
    sym = (symbol or "").strip().upper()
    if not sym or not status_config:
        return None
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                _ensure_sepa_fundamentals_cache_table(cur)
                cur.execute(
                    """
                    SELECT payload, fetched_at, expire_at, source
                    FROM research_sepa_fundamentals_cache
                    WHERE symbol = %s AND rule_version = %s AND expire_at > now()
                    LIMIT 1
                    """,
                    (sym, rule_version),
                )
                row = cur.fetchone()
            conn.commit()
            if not row:
                return None
            payload = row.get("payload")
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except Exception:
                    payload = None
            if not isinstance(payload, dict):
                return None
            return {
                "symbol": sym,
                "payload": payload,
                "source": row.get("source"),
                "fetched_at": row.get("fetched_at"),
                "expire_at": row.get("expire_at"),
            }
        finally:
            conn.close()
    except Exception as e:
        logger.debug("get_sepa_fundamentals_cache_snapshot failed: %s", e)
        return None


def upsert_sepa_fundamentals_cache(
    status_config: dict,
    symbol: str,
    payload: Dict[str, Any],
    *,
    rule_version: str,
    source: str = "massive",
    ttl_sec: int = 21600,
) -> bool:
    sym = (symbol or "").strip().upper()
    if not sym or not status_config or not isinstance(payload, dict):
        return False
    ttl = max(60, int(ttl_sec))
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                _ensure_sepa_fundamentals_cache_table(cur)
                cur.execute(
                    """
                    INSERT INTO research_sepa_fundamentals_cache
                        (symbol, rule_version, payload, source, fetched_at, expire_at, updated_at)
                    VALUES (%s, %s, %s::jsonb, %s, now(), now() + (%s || ' seconds')::interval, now())
                    ON CONFLICT (symbol, rule_version) DO UPDATE SET
                        payload = EXCLUDED.payload,
                        source = EXCLUDED.source,
                        fetched_at = EXCLUDED.fetched_at,
                        expire_at = EXCLUDED.expire_at,
                        updated_at = now()
                    """,
                    (sym, rule_version, json.dumps(payload), source, str(ttl)),
                )
            conn.commit()
            return True
        finally:
            conn.close()
    except Exception as e:
        logger.debug("upsert_sepa_fundamentals_cache failed: %s", e)
        return False


# ── Tier 2–4 batch readers (technical_engine) ────────────────────────────────


def get_spy_close_series(
    status_config: dict,
    *,
    lookback_days: int = 420,
    source: str = "massive",
) -> List[float]:
    """Read SPY daily closes (ascending) from market.stock_daily. Shared by all symbols."""
    if _use_plugin():
        try:
            return market_data_client.fetch_spy_close_series(days=lookback_days)
        except Exception as e:
            logger.warning("Plugin API failed for get_spy_close_series, falling back to SQL: %s", e)
    return _sql_get_spy_close_series(status_config, lookback_days=lookback_days, source=source)


def _sql_get_spy_close_series(
    status_config: dict,
    *,
    lookback_days: int = 420,
    source: str = "massive",
) -> List[float]:
    """SQL fallback: direct PostgreSQL read for SPY closes."""
    _ = source
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return []
    lb = max(260, min(int(lookback_days), 3000))
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT close
                    FROM market.stock_daily
                    WHERE UPPER(TRIM(symbol)) = 'SPY'
                      AND bar_date >= (CURRENT_DATE - (%s || ' days')::interval)::date
                      AND close IS NOT NULL
                    ORDER BY bar_date ASC
                    """,
                    (lb,),
                )
                rows = cur.fetchall() or []
            return [float(r[0]) for r in rows if r[0] is not None]
        finally:
            conn.close()
    except Exception as e:
        logger.warning("_sql_get_spy_close_series failed: %s", e)
        return []


def get_short_interest_recent(
    status_config: dict,
    symbols: List[str],
    *,
    settlements: int = 6,
    source: str = "massive",
) -> Dict[str, List[Dict[str, Any]]]:
    """Batch-read recent short interest rows per symbol (settlement_date DESC).

    In plugin mode, fetches from Plugin API ``/stocks/fundamentals/db/short-interest``.
    ``source`` kept for API compat.
    """
    _ = source
    syms = sorted({str(s or "").strip().upper() for s in symbols if str(s or "").strip()})
    if not syms:
        return {}
    if _use_plugin():
        try:
            return market_data_client.fetch_short_interest(syms, settlements=settlements)
        except Exception as e:
            logger.warning("Plugin API failed for get_short_interest_recent, falling back to SQL: %s", e)
    return _sql_get_short_interest_recent(status_config, syms, settlements=settlements)


def _sql_get_short_interest_recent(
    status_config: dict,
    symbols: List[str],
    *,
    settlements: int = 6,
) -> Dict[str, List[Dict[str, Any]]]:
    """SQL fallback: direct PostgreSQL read for short interest."""
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return {}
    syms = sorted({str(s or "").strip().upper() for s in symbols if str(s or "").strip()})
    if not syms:
        return {}
    out: Dict[str, List[Dict[str, Any]]] = {s: [] for s in syms}
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                      UPPER(TRIM(symbol)) AS symbol,
                      period_date AS settlement_date,
                      COALESCE(
                        (data->>'short_interest')::bigint,
                        (data->>'short_interest_shares')::bigint,
                        (data->>'short_shares')::bigint
                      ) AS short_interest,
                      COALESCE(
                        (data->>'avg_daily_volume')::bigint,
                        (data->>'avg_daily_volume_consolidated')::bigint
                      ) AS avg_daily_volume,
                      (data->>'days_to_cover')::double precision AS days_to_cover
                    FROM (
                      SELECT *,
                        ROW_NUMBER() OVER (
                          PARTITION BY UPPER(TRIM(symbol)) ORDER BY period_date DESC
                        ) AS rn
                      FROM market.stock_financials
                      WHERE UPPER(TRIM(symbol)) = ANY(%s)
                        AND report_type = 'short_interest'
                    ) sub
                    WHERE rn <= %s
                    ORDER BY symbol, settlement_date DESC
                    """,
                    (syms, settlements),
                )
                for row in cur.fetchall() or []:
                    sym = str((row or {}).get("symbol") or "").strip().upper()
                    if sym:
                        out.setdefault(sym, []).append(dict(row))
        finally:
            conn.close()
    except Exception as e:
        logger.warning("_sql_get_short_interest_recent failed: %s", e)
    return out


def get_short_volume_recent(
    status_config: dict,
    symbols: List[str],
    *,
    trade_days: int = 60,
    source: str = "massive",
) -> Dict[str, List[Dict[str, Any]]]:
    """Batch-read recent short volume rows per symbol (trade_date DESC).

    In plugin mode, fetches from Plugin API ``/stocks/fundamentals/db/short-volume``.
    ``source`` kept for API compat.
    """
    _ = source
    syms = sorted({str(s or "").strip().upper() for s in symbols if str(s or "").strip()})
    if not syms:
        return {}
    if _use_plugin():
        try:
            return market_data_client.fetch_short_volume(syms, trade_days=trade_days)
        except Exception as e:
            logger.warning("Plugin API failed for get_short_volume_recent, falling back to SQL: %s", e)
    return _sql_get_short_volume_recent(status_config, syms, trade_days=trade_days)


def _sql_get_short_volume_recent(
    status_config: dict,
    symbols: List[str],
    *,
    trade_days: int = 60,
) -> Dict[str, List[Dict[str, Any]]]:
    """SQL fallback: direct PostgreSQL read for short volume."""
    if not status_config or (status_config.get("sink") != "postgres" and not status_config.get("postgres")):
        return {}
    syms = sorted({str(s or "").strip().upper() for s in symbols if str(s or "").strip()})
    if not syms:
        return {}
    out: Dict[str, List[Dict[str, Any]]] = {s: [] for s in syms}
    try:
        params = _get_conn_params(status_config)
        conn = psycopg2.connect(**params)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                      UPPER(TRIM(symbol)) AS symbol,
                      period_date AS trade_date,
                      (data->>'short_volume')::bigint AS short_volume,
                      (data->>'short_volume_ratio')::double precision AS short_volume_ratio,
                      (data->>'total_volume')::bigint AS total_volume
                    FROM (
                      SELECT *,
                        ROW_NUMBER() OVER (
                          PARTITION BY UPPER(TRIM(symbol)) ORDER BY period_date DESC
                        ) AS rn
                      FROM market.stock_financials
                      WHERE UPPER(TRIM(symbol)) = ANY(%s)
                        AND report_type = 'short_volume'
                    ) sub
                    WHERE rn <= %s
                    ORDER BY symbol, trade_date DESC
                    """,
                    (syms, trade_days),
                )
                for row in cur.fetchall() or []:
                    sym = str((row or {}).get("symbol") or "").strip().upper()
                    if sym:
                        out.setdefault(sym, []).append(dict(row))
        finally:
            conn.close()
    except Exception as e:
        logger.warning("_sql_get_short_volume_recent failed: %s", e)
    return out


def get_option_trades(
    status_config: dict,
    symbol: str,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """option_trades public table retired with Massive — return empty."""
    _ = (status_config, symbol, limit)
    return []


def get_report_option_atm_iv_daily(
    status_config: dict,
    symbol: str,
    expirations: List[str],
    source: str,
    since_date: date_type,
) -> List[Dict[str, Any]]:
    """report_option_atm_iv_daily dropped — use Plugin analytics."""
    _ = (status_config, symbol, expirations, source, since_date)
    return []


def refresh_expirations_from_massive_api(
    status_config: dict,
    config: dict,
    symbol: str,
    *,
    expiration_date: Optional[str] = None,
    include_debug: bool = False,
) -> Dict[str, Any]:
    """Live Polygon refresh retired — use market-data plugin ingest."""
    _ = (status_config, config, expiration_date, include_debug)
    sym = (symbol or "").strip().upper()
    return {
        "ok": False,
        "symbol": sym,
        "expirations": [],
        "strikes": [],
        "error": "Massive REST refresh retired — use market-data plugin; DB cache still served when present",
        "reason": "massive_retired",
    }


def upsert_option_contracts_from_reference_rows(
    status_config: dict,
    underlying: str,
    contract_rows: List[Dict[str, Any]],
) -> int:
    _ = (status_config, underlying, contract_rows)
    return 0
