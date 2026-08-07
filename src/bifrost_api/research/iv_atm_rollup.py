"""ATM IV daily report upsert — retired (Wave 7-C).

``report_option_atm_iv_daily`` was dropped; use market-data plugin analytics.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


def upsert_report_atm_iv_daily_rows(
    conn: Any,
    symbol: str,
    expiry: str,
    source: str,
    rows: List[Dict[str, Any]],
) -> int:
    """No-op: report table dropped."""
    _ = (conn, symbol, expiry, source, rows)
    return 0


def rebuild_report_atm_iv_daily_for_symbol_expiry(
    status_cfg: dict,
    conn: Any,
    symbol: str,
    expiry_yyyymmdd: str,
    source: str,
    lookback_days: int,
    last_price: float,
) -> int:
    """No-op: report table dropped."""
    _ = (status_cfg, conn, symbol, expiry_yyyymmdd, source, lookback_days, last_price)
    return 0


def norm_expiry_yyyymmdd(raw: str) -> Optional[str]:
    s = (raw or "").strip()
    if len(s) >= 10 and s[4] == "-":
        return s[:4] + s[5:7] + s[8:10]
    if len(s) >= 8 and s[:8].isdigit():
        return s[:8]
    return None
