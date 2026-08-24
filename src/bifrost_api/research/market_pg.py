"""Market PostgreSQL readers for research/ops (Wave 7-C).

Retained from the retired Trade ingest reader.
Reads ``market.*`` tables; SEPA job tables remain in public schema.
"""

from __future__ import annotations

import logging
from datetime import date as date_type
from datetime import datetime
from datetime import time as time_of_day
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo



from bifrost_api.research import market_data_client

logger = logging.getLogger(__name__)


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


def _norm_expiry_db(expiry: str) -> str:
    e = (expiry or "").strip()
    if len(e) >= 10 and e[4] == "-":
        return e[:4] + e[5:7] + e[8:10]
    return e


def _expiry_to_date_param(expiry: str) -> Optional[str]:
    """Normalize expiry string to ISO date (YYYY-MM-DD) for market.* date columns."""
    e = _norm_expiry_db(str(expiry or "").strip())
    if len(e) == 8 and e.isdigit():
        return f"{e[:4]}-{e[4:6]}-{e[6:8]}"
    raw = (expiry or "").strip()
    if len(raw) >= 10 and raw[4] == "-":
        return raw[:10]
    return None


# ── SEPA Phase4 job queue (RETIRED — table dropped in bifrost-core 0.10.6) ────


def insert_job_sepa_phase4(
    status_config: dict,
    job_id: str,
    request_payload: Optional[Dict[str, Any]] = None,
    *,
    version: str = "sepa_phase4_v1",
) -> Optional[int]:
    """No-op — job_sepa_phase4 retired; use analytics.sepa_screener_wide."""
    return None


def get_job_sepa_phase4(
    status_config: dict,
    job_id: str,
) -> Optional[Dict[str, Any]]:
    """No-op — job_sepa_phase4 retired."""
    return None


def get_job_sepa_phase4_result(
    status_config: dict,
    job_id: str,
    *,
    offset: int = 0,
    limit: int = 200,
) -> Optional[Dict[str, Any]]:
    """No-op — job_sepa_phase4 retired."""
    return None


def update_job_sepa_phase4(
    status_config: dict,
    job_id: str,
    **fields: Any,
) -> bool:
    """No-op — job_sepa_phase4 retired."""
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
    """No-op — job_sepa_phase4 retired."""
    return []


def delete_job_sepa_phase4(status_config: dict, job_id: str) -> bool:
    """No-op — job_sepa_phase4 retired."""
    return False


# ── Migrated to Plugin API (market-data plugin HTTP client) ───────────────────


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
        logger.warning("get_option_open_interest_daily failed: %s", e)
        return []


def get_option_snapshots_latest(
    status_config: dict,
    contract_keys: List[str],
    source: str = "massive",
) -> List[Dict[str, Any]]:
    """Latest snapshot per contract_key from Plugin API.

    Accepts IB keys (``SYM|OPT|…``) and Polygon tickers (``O:…``). Always returns
    IB-shaped ``contract_key`` so Discovery/Screener ``parse_contract_key`` works.
    ``source`` is accepted for API compatibility but ignored.
    """
    if not contract_keys:
        return []
    keys = [k for k in contract_keys if k and str(k).strip()][:120]
    if not keys:
        return []
    try:
        return market_data_client.fetch_option_chain_latest(keys)
    except Exception as e:
        logger.warning("get_option_snapshots_latest failed: %s", e)
        return []


def get_option_snapshots_eod_per_day(
    status_config: dict,
    contract_keys: List[str],
    source: str = "massive",
    since_ts: Optional[datetime] = None,
    chunk_size: int = 100,
) -> List[Dict[str, Any]]:
    """Latest snapshot per calendar day (America/New_York) per contract_key.

    Returns IB-shaped ``contract_key``. ``source`` kept for API compat.
    """
    if not contract_keys:
        return []
    keys = [k for k in contract_keys if k and str(k).strip()]
    if not keys:
        return []
    try:
        since_iso: Optional[str] = None
        if since_ts is not None:
            since_iso = since_ts.isoformat() if since_ts.year > 1970 else None
        return market_data_client.fetch_option_chain_eod(keys, since=since_iso)
    except Exception as e:
        logger.warning("get_option_snapshots_eod_per_day failed: %s", e)
        return []


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
    syms = [str(s or "").strip().upper() for s in symbols if str(s or "").strip()]
    if not syms:
        return {}
    try:
        return market_data_client.fetch_stock_bars_daily(syms, days=lookback_days)
    except Exception as e:
        logger.warning("get_stock_day_series_for_sepa failed: %s", e)
        return {}


def get_stock_day_close_series_for_crs(
    status_config: dict,
    symbols: List[str],
    *,
    lookback_days: int = 420,
    source: str = "massive",
) -> Dict[str, List[Dict[str, Any]]]:
    """Batch-read market.stock_daily close series for CRS calculation."""
    syms = [str(s or "").strip().upper() for s in symbols if str(s or "").strip()]
    if not syms:
        return {}
    try:
        return market_data_client.fetch_stock_bars_daily_close(syms, days=lookback_days)
    except Exception as e:
        logger.warning("get_stock_day_close_series_for_crs failed: %s", e)
        return {}


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
    try:
        return market_data_client.fetch_option_expirations_yyyymmdd(sym)
    except Exception as e:
        logger.warning("get_option_expirations_from_contracts_db failed: %s", e)
        return []


def get_strikes_for_expiry_from_contracts_db(
    status_config: dict, symbol: str, expiration: str
) -> List[float]:
    """Distinct strikes for symbol + expiry from market.option_contract."""
    sym = (symbol or "").strip().upper()
    exp_raw = (expiration or "").strip()
    if not sym or not exp_raw:
        return []
    try:
        exp_param = _norm_expiry_db(exp_raw)
        return market_data_client.fetch_option_strikes(sym, exp_param)
    except Exception as e:
        logger.warning("get_strikes_for_expiry_from_contracts_db failed: %s", e)
        return []


def get_option_expiration_cache_snapshot(
    status_config: dict, symbol: str, source: str = "massive"
) -> Optional[Tuple[List[str], Optional[datetime]]]:
    """Return (sorted expirations, max updated_at) from market.option_expiration.

    ``source`` is accepted for API compatibility but ignored (no source column).
    """
    sym = (symbol or "").strip().upper()
    if not sym:
        return None
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
        logger.warning("get_option_expiration_cache_snapshot failed: %s", e)
        return None


def replace_option_expiration_cache(
    status_config: dict,
    symbol: str,
    expirations: List[str],
    source: str = "massive",
) -> None:
    """Replace full expiration list for a symbol via Plugin API.

    ``status_config`` and ``source`` are accepted for API compatibility but
    ignored — the write is delegated to the market-data plugin HTTP endpoint.
    """
    _ = (status_config, source)
    sym = (symbol or "").strip().upper()
    if not sym:
        return
    if not expirations:
        return
    iso_dates = [d for d in (_expiry_to_date_param(str(raw)) for raw in expirations) if d]
    if not iso_dates:
        return
    try:
        market_data_client.post_replace_expirations(sym, iso_dates)
    except Exception as e:
        logger.warning("replace_option_expiration_cache failed: %s", e)


def get_sepa_fundamentals_cache_snapshot(
    status_config: dict,
    symbol: str,
    *,
    rule_version: str,
) -> Optional[Dict[str, Any]]:
    """RETIRED: research_sepa_fundamentals_cache dropped; use analytics marts."""
    _ = (status_config, symbol, rule_version)
    logger.debug("get_sepa_fundamentals_cache_snapshot retired")
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
    """RETIRED: research_sepa_fundamentals_cache dropped; use analytics marts."""
    _ = (status_config, symbol, payload, rule_version, source, ttl_sec)
    logger.debug("upsert_sepa_fundamentals_cache retired")
    return False


# ── Tier 2–4 batch readers (technical_engine) ────────────────────────────────


def get_spy_close_series(
    status_config: dict,
    *,
    lookback_days: int = 420,
    source: str = "massive",
) -> List[float]:
    """Read SPY daily closes (ascending) from market.stock_daily. Shared by all symbols."""
    try:
        return market_data_client.fetch_spy_close_series(days=lookback_days)
    except Exception as e:
        logger.warning("get_spy_close_series failed: %s", e)
        return []


def get_short_interest_recent(
    status_config: dict,
    symbols: List[str],
    *,
    settlements: int = 6,
    source: str = "massive",
) -> Dict[str, List[Dict[str, Any]]]:
    """Batch-read recent short interest rows per symbol (settlement_date DESC).

    ``source`` kept for API compat.
    """
    syms = sorted({str(s or "").strip().upper() for s in symbols if str(s or "").strip()})
    if not syms:
        return {}
    try:
        return market_data_client.fetch_short_interest(syms, settlements=settlements)
    except Exception as e:
        logger.warning("get_short_interest_recent failed: %s", e)
        return {}


def get_short_volume_recent(
    status_config: dict,
    symbols: List[str],
    *,
    trade_days: int = 60,
    source: str = "massive",
) -> Dict[str, List[Dict[str, Any]]]:
    """Batch-read recent short volume rows per symbol (trade_date DESC).

    ``source`` kept for API compat.
    """
    syms = sorted({str(s or "").strip().upper() for s in symbols if str(s or "").strip()})
    if not syms:
        return {}
    try:
        return market_data_client.fetch_short_volume(syms, trade_days=trade_days)
    except Exception as e:
        logger.warning("get_short_volume_recent failed: %s", e)
        return {}


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


