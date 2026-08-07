"""Polygon/Massive HTTP helpers local to research (Wave 7-C).

Recovered from bifrost-worker data/massive vendor after package deletion.
Live ingest lives in bifrost-platform-plugin-market-data.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

DEFAULT_REST_BASE = "https://api.polygon.io"

_RETIRED_MSG = "Massive client retired; use market-data plugin"


def _as_error_str(err: Any) -> str:
    """Polygon/Massive sometimes returns error as a string, object, or list."""
    if isinstance(err, str):
        return err
    if err is None:
        return "Unknown error"
    try:
        return json.dumps(err, default=str)
    except (TypeError, ValueError):
        return str(err)


def _norm_expiry(s: str) -> str:
    """Normalize expiration to YYYYMMDD or YYYYMM as stored elsewhere."""
    s = (s or "").strip()
    if len(s) >= 10 and s[4] == "-":
        return s[:4] + s[5:7] + s[8:10]
    return s


def _right_from_contract_type(ct: str) -> str:
    u = (ct or "").upper()
    if u in ("CALL", "C"):
        return "C"
    if u in ("PUT", "P"):
        return "P"
    return "C"


def contract_key_from_parts(
    symbol: str, expiry: str, strike: float, option_right: str
) -> str:
    """Match account_positions / DATABASE.md: symbol|OPT|expiry|strike|right."""
    sym = (symbol or "").strip().upper()
    exp = _norm_expiry(expiry)
    r = (option_right or "").strip().upper()
    if r in ("CALL",):
        r = "C"
    if r in ("PUT",):
        r = "P"
    sk = round(float(strike), 8)
    return f"{sym}|OPT|{exp}|{sk}|{r}"


def contract_key_from_reference_result(
    underlying: str, row: Dict[str, Any]
) -> Optional[str]:
    """Build ``option_contracts.contract_key`` from a Polygon reference result row."""
    u = (underlying or "").strip().upper()
    if not u or not isinstance(row, dict):
        return None
    exp = row.get("expiration_date") or row.get("expiration") or ""
    if not exp:
        return None
    ed = _norm_expiry(str(exp)[:10])
    if len(ed) != 8 or not ed.isdigit():
        return None
    sp = row.get("strike_price")
    if sp is None:
        return None
    try:
        strike = float(sp)
    except (TypeError, ValueError):
        return None
    ort = _right_from_contract_type(str(row.get("contract_type") or "call"))
    return contract_key_from_parts(u, ed, strike, ort)


class MassiveClient:
    """Retired HTTP client stub — methods refuse live Polygon calls."""

    def __init__(self, api_key: str, rest_base: str = DEFAULT_REST_BASE) -> None:
        self._api_key = (api_key or "").strip()
        self._base = (rest_base or DEFAULT_REST_BASE).rstrip("/")

    @property
    def configured(self) -> bool:
        return bool(self._api_key)

    def _retired(self, *_args: Any, **_kwargs: Any) -> Dict[str, Any]:
        return {"error": _RETIRED_MSG, "ok": False, "reason": "massive_retired"}

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)

        def _method(*_args: Any, **_kwargs: Any) -> Dict[str, Any]:
            return self._retired()

        return _method


def _daily_full_backfill_years_from_config(m: Dict[str, Any], tier: str) -> float:
    """Empty-DB daily_smart window: calendar years to request (capped by vendor plan separately)."""
    raw = m.get("daily_full_backfill_years")
    if raw is not None:
        try:
            v = float(raw)
            if v > 0:
                return min(50.0, max(1.0, v))
        except (TypeError, ValueError):
            pass
    return 5.0 if tier == "starter" else 20.0


def get_massive_settings(config: Dict[str, Any]) -> Dict[str, Any]:
    """Return api_key, rest_base, tier, trades_enabled, daily_full_backfill_years."""
    m = config.get("massive") or {}
    api_key = (
        os.environ.get("MASSIVE_API_KEY")
        or os.environ.get("POLYGON_API_KEY")
        or m.get("api_key")
        or ""
    ).strip()
    tier = (m.get("tier") or "starter").strip().lower()
    if tier not in ("starter", "developer"):
        tier = "starter"
    feats = m.get("features") or {}
    trades_default = tier == "developer"
    trades_enabled = bool(feats.get("trades_enabled", trades_default))
    rest_base = (m.get("rest_base") or "https://api.polygon.io").rstrip("/")
    ws_url = (m.get("ws_url") or "wss://socket.polygon.io/options").strip()
    daily_years = _daily_full_backfill_years_from_config(m, tier)
    return {
        "api_key": api_key,
        "rest_base": rest_base,
        "ws_url": ws_url,
        "tier": tier,
        "trades_enabled": trades_enabled,
        "daily_full_backfill_years": daily_years,
    }


def get_expiration_cache_settings(config: Dict[str, Any]) -> Dict[str, Any]:
    """TTL and behavior for option expiration list (PostgreSQL cache + REST fallback)."""
    m = config.get("massive") or {}
    ec = m.get("expiration_cache") or {}
    return {
        "enabled": bool(ec.get("enabled", True)),
        "ttl_trading_sec": int(ec.get("ttl_trading_sec", 3600)),
        "ttl_off_hours_sec": int(ec.get("ttl_off_hours_sec", 43200)),
        "stale_while_revalidate": bool(ec.get("stale_while_revalidate", True)),
        "beat_batch_size": int(ec.get("beat_batch_size", 12)),
    }


def massive_delay_notice_english() -> str:
    return "Data delayed by 15 minutes (Options Starter). Not for live trading decisions."
