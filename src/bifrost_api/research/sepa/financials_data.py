"""SEPA fundamentals readers — Plugin HTTP layer.

All reads against ``market.stock_financials`` are routed through the
Plugin Market Data API (``market_data_client``).  The jsonb ``data``
column is returned verbatim by the Plugin; field unpacking stays here.

W2-P2: replaced ~33 direct SQL queries with HTTP calls.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

SOURCE_DEFAULT = "massive"

REPORT_INCOME = "income_statement"
REPORT_BALANCE = "balance_sheet"
REPORT_CASH_FLOW = "cash_flow_statement"
REPORT_RATIOS = "ratios"
REPORT_SHORT_INTEREST = "short_interest"
REPORT_SHORT_VOLUME = "short_volume"

_FQ_TO_PERIOD = {0: "FY", 1: "Q1", 2: "Q2", 3: "Q3", 4: "Q4"}

# dest_field -> candidate keys inside jsonb ``data`` (Polygon vx nested or flat).
_INCOME_FIELDS: Dict[str, Tuple[str, ...]] = {
    "basic_earnings_per_share": ("basic_earnings_per_share",),
    "diluted_earnings_per_share": ("diluted_earnings_per_share",),
    "revenue": ("revenues", "revenue"),
    "revenues": ("revenues", "revenue"),
    "gross_profit": ("gross_profit",),
    "operating_income": ("operating_income_loss", "operating_income"),
    "ebitda": ("ebitda", "earnings_before_interest_taxes_depreciation_amortization"),
    "cost_of_revenue": ("cost_of_revenue",),
    "consolidated_net_income_loss": ("net_income_loss", "consolidated_net_income_loss", "net_income"),
    "interest_expense": ("interest_expense",),
    "diluted_shares_outstanding": ("diluted_average_shares", "diluted_shares_outstanding"),
    "basic_shares_outstanding": ("basic_average_shares", "basic_shares_outstanding"),
}

_BALANCE_FIELDS: Dict[str, Tuple[str, ...]] = {
    "cash_and_equivalents": ("cash", "cash_and_equivalents", "cash_and_cash_equivalents"),
    "short_term_investments": ("short_term_investments",),
    "receivables": ("accounts_receivable", "receivables"),
    "inventories": ("inventory", "inventories"),
    "total_current_assets": ("current_assets", "total_current_assets"),
    "total_current_liabilities": ("current_liabilities", "total_current_liabilities"),
    "total_assets": ("assets", "total_assets"),
    "total_liabilities": ("liabilities", "total_liabilities"),
    "total_equity": ("equity", "total_equity"),
    "debt_current": ("debt_current", "current_debt"),
    "long_term_debt_and_capital_lease_obligations": (
        "long_term_debt",
        "long_term_debt_and_capital_lease_obligations",
    ),
    "goodwill": ("goodwill",),
    "intangible_assets_net": ("intangible_assets", "intangible_assets_net"),
    "property_plant_equipment_net": (
        "property_plant_equipment_net",
        "fixed_assets",
    ),
    "retained_earnings_deficit": ("retained_earnings", "retained_earnings_deficit"),
}

_CASH_FLOW_FIELDS: Dict[str, Tuple[str, ...]] = {
    "net_income": ("net_income_loss", "net_income"),
    "net_cash_from_operating_activities": ("net_cash_from_operating_activities",),
    "net_cash_from_investing_activities": ("net_cash_from_investing_activities",),
    "net_cash_from_financing_activities": ("net_cash_from_financing_activities",),
    "purchase_of_property_plant_and_equipment": (
        "purchase_of_property_plant_and_equipment",
    ),
    "depreciation_depletion_and_amortization": (
        "depreciation_depletion_and_amortization",
        "depreciation_and_amortization",
    ),
    "change_in_cash_and_equivalents": ("change_in_cash_and_equivalents",),
}

_RATIOS_FIELDS: Dict[str, Tuple[str, ...]] = {
    "price_to_earnings": ("price_to_earnings",),
    "price_to_sales": ("price_to_sales",),
    "price_to_book": ("price_to_book",),
    "price_to_free_cash_flow": ("price_to_free_cash_flow",),
    "price_to_cash_flow": ("price_to_cash_flow",),
    "debt_to_equity": ("debt_to_equity",),
    "return_on_equity": ("return_on_equity",),
    "return_on_assets": ("return_on_assets",),
    "market_cap": ("market_cap",),
    "free_cash_flow": ("free_cash_flow",),
    "earnings_per_share": ("earnings_per_share",),
    "average_volume": ("average_volume",),
    "dividend_yield": ("dividend_yield",),
    "enterprise_value": ("enterprise_value",),
    "ev_to_ebitda": ("ev_to_ebitda",),
    "ev_to_sales": ("ev_to_sales",),
    "current_ratio_from_ratios": ("current", "current_ratio"),
    "quick_ratio_from_ratios": ("quick", "quick_ratio"),
}

_SHORT_INTEREST_FIELDS: Dict[str, Tuple[str, ...]] = {
    "short_interest": ("short_interest",),
    "avg_daily_volume": ("avg_daily_volume",),
    "days_to_cover": ("days_to_cover",),
}

_SHORT_VOLUME_FIELDS: Dict[str, Tuple[str, ...]] = {
    "short_volume": ("short_volume",),
    "short_volume_ratio": ("short_volume_ratio",),
    "total_volume": ("total_volume",),
}


def _as_float(v: Any) -> Optional[float]:
    try:
        return float(v) if v is not None and v != "" else None
    except (TypeError, ValueError):
        return None


def short_volume_ratio(flat: Dict[str, Any]) -> Optional[float]:
    """Short volume as a ratio of total volume, from a row unpacked with ``_SHORT_VOLUME_FIELDS``.

    The vendor's own ``short_volume_ratio`` is a percent — AAPL on 2026-09-25 is
    58.33, against a computed 0.5833 — while the Stock Inspector renders this field
    times 100, so passing the vendor's number through showed 5833.0%. Computed
    from the two volumes, falling back to the vendor's percent over 100: the rule
    the plugin's /stocks/fundamentals/db/short-volume (0.41.8) and Research's
    ``stg_short_volume`` use, so the three readers give one number.
    """
    short = _as_float(flat.get("short_volume"))
    total = _as_float(flat.get("total_volume"))
    if short and total:
        return short / total
    pct = _as_float(flat.get("short_volume_ratio"))
    return pct / 100 if pct is not None else None


def _json_scalar(v: Any) -> Any:
    if isinstance(v, dict) and "value" in v:
        return v.get("value")
    return v


def unpack_financial_data(
    data: Any,
    field_map: Dict[str, Tuple[str, ...]],
) -> Dict[str, Any]:
    """Flatten jsonb ``data`` into legacy column names expected by SEPA callers."""
    raw = data if isinstance(data, dict) else {}
    out: Dict[str, Any] = {}
    for dest, keys in field_map.items():
        val = None
        for k in keys:
            if k in raw:
                val = _json_scalar(raw[k])
                break
        out[dest] = val
    return out


def is_legacy_financial_data(data: Any) -> bool:
    """True for the legacy vendor shape, where each field is ``{value, unit, label, ...}``.

    The v1 standardized shape is flat scalars. Both sit in ``stock_financials.data``
    while the plugin moves to v1, and they do not splice period over period: v1
    restates EPS for later splits, the legacy rows are as reported.
    """
    if not isinstance(data, dict):
        return False
    return any(isinstance(v, dict) and "value" in v for v in data.values())


def _one_format(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Drop legacy rows from a series that also has v1 rows, so one table never mixes the two."""
    has_v1 = any(
        isinstance(r.get("data"), dict) and r.get("data") and not is_legacy_financial_data(r.get("data"))
        for r in rows
    )
    if not has_v1:
        return list(rows)
    return [r for r in rows if not is_legacy_financial_data(r.get("data"))]


def _as_int(v: Any) -> Optional[int]:
    try:
        return int(v) if v is not None and v != "" else None
    except (TypeError, ValueError):
        return None


def _as_date(v: Any) -> Optional[date]:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    if isinstance(v, str) and len(v) >= 10:
        try:
            return date.fromisoformat(v[:10])
        except ValueError:
            return None
    return None


_QUARTER_DAYS = 365.25 / 4


def derive_fiscal_quarter(
    fiscal_year: Any,
    period_end: Any,
    fiscal_year_ends: Dict[int, date],
) -> Optional[int]:
    """Fiscal quarter of a quarterly row whose ``fiscal_quarter`` column is empty.

    The legacy vendor response carries the quarter only as ``fiscal_period``, which
    the plugin does not store, so every legacy row has ``fiscal_quarter`` NULL
    (read 2026-09-29, quarterly and annual alike); v1 rows carry it. Counted back
    from the fiscal-year end: that year's annual row, else the neighbouring year's
    moved by a year (the current fiscal year has no annual row until it closes).
    """
    fy = _as_int(fiscal_year)
    pe = _as_date(period_end)
    if fy is None or pe is None:
        return None
    end = fiscal_year_ends.get(fy)
    if end is None and fy - 1 in fiscal_year_ends:
        end = fiscal_year_ends[fy - 1] + timedelta(days=365)
    if end is None and fy + 1 in fiscal_year_ends:
        end = fiscal_year_ends[fy + 1] - timedelta(days=365)
    if end is None:
        return None
    quarter = 4 - round((end - pe).days / _QUARTER_DAYS)
    return quarter if 1 <= quarter <= 4 else None


def _newest_first(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(rows, key=lambda r: str(r.get("period_date") or ""), reverse=True)


def income_rows_for_inspector(
    quarterly: List[Dict[str, Any]],
    annual: List[Dict[str, Any]],
    *,
    quarters: int = 10,
    years: int = 5,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """EPS / revenue rows, newest first, for the Stock Inspector's Source Data tables.

    ``quarterly`` / ``annual`` are income-statement rows as the plugin's
    ``/stocks/fundamentals/sepa/financials`` returns them. Reads the legacy nested
    and the v1 flat ``data`` through ``_INCOME_FIELDS``; a series holding both keeps
    only v1. Rows are unique on the table's row key — (fiscal_year, fiscal_quarter)
    and fiscal_year — which the inspector keys its rows and highlights by.
    """
    fiscal_year_ends: Dict[int, date] = {}
    for r in annual:
        fy = _as_int(r.get("fiscal_year"))
        pe = _as_date(r.get("period_date"))
        if fy is not None and pe is not None:
            fiscal_year_ends[fy] = pe

    q_out: List[Dict[str, Any]] = []
    q_seen: set = set()
    for r in _newest_first(_one_format(quarterly)):
        fy = _as_int(r.get("fiscal_year"))
        fq = _as_int(r.get("fiscal_quarter")) or derive_fiscal_quarter(
            fy, r.get("period_date"), fiscal_year_ends
        )
        if fq is not None:
            if (fy, fq) in q_seen:
                continue
            q_seen.add((fy, fq))
        flat = unpack_financial_data(r.get("data"), _INCOME_FIELDS)
        q_out.append({
            "fiscal_year": fy,
            "fiscal_quarter": fq,
            "eps": _as_float(flat.get("basic_earnings_per_share")),
            "revenues": _as_float(flat.get("revenue")),
        })
        if len(q_out) >= quarters:
            break

    a_out: List[Dict[str, Any]] = []
    a_seen: set = set()
    for r in _newest_first(_one_format(annual)):
        fy = _as_int(r.get("fiscal_year"))
        if fy in a_seen:
            continue
        a_seen.add(fy)
        flat = unpack_financial_data(r.get("data"), _INCOME_FIELDS)
        a_out.append({
            "fiscal_year": fy,
            "eps": _as_float(flat.get("basic_earnings_per_share")),
            "revenues": _as_float(flat.get("revenue")),
        })
        if len(a_out) >= years:
            break

    return q_out, a_out


def fetch_income_rows_for_sepa_from_pg(
    status_config: dict,
    symbol: str,
    *,
    min_quarterly: int = 5,
    min_annual: int = 4,
) -> Optional[Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]]:
    """Build quarterly/annual row dicts for ``evaluate_fundamentals`` via Plugin HTTP.

    Returns None if coverage is insufficient.
    ``status_config`` is accepted for signature compatibility but ignored.
    """
    from bifrost_api.research.market_data_client import fetch_sepa_income_rows

    sym = (symbol or "").strip().upper()
    if not sym:
        return None
    try:
        result = fetch_sepa_income_rows(sym)
    except Exception as e:
        logger.debug("fetch_income_rows_for_sepa_from_pg HTTP failed: %s", e)
        return None

    q_db = result.get("quarterly") or []
    a_db = result.get("annual") or []
    if len(q_db) < min_quarterly or len(a_db) < min_annual:
        return None

    def _enrich(r: Dict[str, Any]) -> Dict[str, Any]:
        flat = unpack_financial_data(r.get("data"), _INCOME_FIELDS)
        return {**r, **flat}

    def _map_q(r: Dict[str, Any]) -> Dict[str, Any]:
        r = _enrich(r)
        fq = int(r.get("fiscal_quarter") or 0)
        fp = _FQ_TO_PERIOD.get(fq, f"Q{fq}" if fq else "FY")
        pe = r.get("period_end")
        pe_s = pe[:10] if isinstance(pe, str) and pe else (
            pe.isoformat() if hasattr(pe, "isoformat") else (str(pe)[:10] if pe else None)
        )
        return {
            "fiscal_year": int(r.get("fiscal_year") or 0),
            "fiscal_period": fp,
            "filing_date": None,
            "timeframe": "quarterly",
            "start_date": pe_s,
            "end_date": pe_s,
            "basic_earnings_per_share": r.get("basic_earnings_per_share"),
            "diluted_earnings_per_share": r.get("diluted_earnings_per_share"),
            "revenues": r.get("revenue"),
        }

    def _map_a(r: Dict[str, Any]) -> Dict[str, Any]:
        r = _enrich(r)
        pe = r.get("period_end")
        pe_s = pe[:10] if isinstance(pe, str) and pe else (
            pe.isoformat() if hasattr(pe, "isoformat") else (str(pe)[:10] if pe else None)
        )
        return {
            "fiscal_year": int(r.get("fiscal_year") or 0),
            "fiscal_period": "FY",
            "filing_date": None,
            "timeframe": "annual",
            "start_date": pe_s,
            "end_date": pe_s,
            "basic_earnings_per_share": r.get("basic_earnings_per_share"),
            "diluted_earnings_per_share": r.get("diluted_earnings_per_share"),
            "revenues": r.get("revenue"),
        }

    return ([_map_q(r) for r in q_db], [_map_a(r) for r in a_db])


# Retired feed upserts and job runners retained for API/test compatibility.
from bifrost_api.research.financials_feed import (  # noqa: E402
    SOURCE_DEFAULT as FEED_SOURCE_DEFAULT,
    run_feed_stocks_balance_sheets_job,  # noqa: F401
    run_feed_stocks_cash_flows_job,  # noqa: F401
    run_feed_stocks_income_statements_job,  # noqa: F401
    run_feed_stocks_ratios_job,  # noqa: F401
    run_feed_stocks_short_interest_job,  # noqa: F401
    run_feed_stocks_short_volume_job,  # noqa: F401
    upsert_balance_sheet_rows,  # noqa: F401
    upsert_cash_flow_rows,  # noqa: F401
    upsert_income_statement_rows,  # noqa: F401
    upsert_ratios_rows,  # noqa: F401
    upsert_short_interest_rows,  # noqa: F401
    upsert_short_volume_rows,  # noqa: F401
)

SOURCE_DEFAULT = FEED_SOURCE_DEFAULT


# ── Gap count / detail functions (via Plugin HTTP) ───────────────────────────

def _fetch_gaps(report_type: str, limit: int = 5000) -> Dict[str, Any]:
    """Shared helper: call Plugin gaps endpoint and return {count, symbols}."""
    from bifrost_api.research.market_data_client import fetch_sepa_gaps

    try:
        return fetch_sepa_gaps(report_type, limit=limit)
    except Exception as e:
        logger.warning("Plugin gaps HTTP failed for %s: %s", report_type, e)
        return {"count": 0, "symbols": []}


def count_income_statements_gaps(cur: Any = None) -> int:
    """``cur`` kept for signature compat but ignored."""
    return _fetch_gaps(REPORT_INCOME).get("count", 0)


def get_income_statements_gap_details(cur: Any = None, *, limit: int = 2000) -> Tuple[List[Dict[str, Any]], int]:
    """``cur`` kept for signature compat but ignored."""
    g = _fetch_gaps(REPORT_INCOME, limit=limit)
    syms = g.get("symbols", [])
    total = g.get("count", len(syms))
    rows = [{"symbol": s} for s in syms[:limit]]
    return rows, total


def count_balance_sheet_gaps(cur: Any = None) -> int:
    return _fetch_gaps(REPORT_BALANCE).get("count", 0)


def get_balance_sheet_gap_details(cur: Any = None, *, limit: int = 2000) -> Tuple[List[Dict[str, Any]], int]:
    g = _fetch_gaps(REPORT_BALANCE, limit=limit)
    syms = g.get("symbols", [])
    total = g.get("count", len(syms))
    return [{"symbol": s} for s in syms[:limit]], total


def count_cash_flow_gaps(cur: Any = None) -> int:
    return _fetch_gaps(REPORT_CASH_FLOW).get("count", 0)


def get_cash_flow_gap_details(cur: Any = None, *, limit: int = 2000) -> Tuple[List[Dict[str, Any]], int]:
    g = _fetch_gaps(REPORT_CASH_FLOW, limit=limit)
    syms = g.get("symbols", [])
    total = g.get("count", len(syms))
    return [{"symbol": s} for s in syms[:limit]], total


def count_ratios_gaps(cur: Any = None) -> int:
    return _fetch_gaps(REPORT_RATIOS).get("count", 0)


def get_ratios_gap_details(cur: Any = None, *, limit: int = 2000) -> Tuple[List[Dict[str, Any]], int]:
    g = _fetch_gaps(REPORT_RATIOS, limit=limit)
    syms = g.get("symbols", [])
    total = g.get("count", len(syms))
    return [{"symbol": s} for s in syms[:limit]], total


def count_short_interest_gaps(cur: Any = None) -> int:
    return _fetch_gaps(REPORT_SHORT_INTEREST).get("count", 0)


def get_short_interest_gap_details(cur: Any = None, *, limit: int = 2000) -> Tuple[List[Dict[str, Any]], int]:
    g = _fetch_gaps(REPORT_SHORT_INTEREST, limit=limit)
    syms = g.get("symbols", [])
    total = g.get("count", len(syms))
    return [{"symbol": s} for s in syms[:limit]], total


def count_short_volume_gaps(cur: Any = None) -> int:
    return _fetch_gaps(REPORT_SHORT_VOLUME).get("count", 0)


def get_short_volume_gap_details(cur: Any = None, *, limit: int = 2000) -> Tuple[List[Dict[str, Any]], int]:
    g = _fetch_gaps(REPORT_SHORT_VOLUME, limit=limit)
    syms = g.get("symbols", [])
    total = g.get("count", len(syms))
    return [{"symbol": s} for s in syms[:limit]], total


_KIND_TO_REPORT_TYPE: Dict[str, str] = {
    "feed_stocks_income_statements": REPORT_INCOME,
    "feed_stocks_balance_sheets": REPORT_BALANCE,
    "feed_stocks_cash_flows": REPORT_CASH_FLOW,
    "feed_stocks_ratios": REPORT_RATIOS,
    "feed_stocks_short_interest": REPORT_SHORT_INTEREST,
    "feed_stocks_short_volume": REPORT_SHORT_VOLUME,
}


def financials_gap_symbols_from_db(cur: Any = None, kind: str = "", *, batch_size: int = 50) -> Dict[str, Any]:
    """Return gap symbol batches for a fundamentals feed kind (via Plugin HTTP).

    ``cur`` kept for signature compat but ignored.
    """
    k = (kind or "").strip().lower()
    rt = _KIND_TO_REPORT_TYPE.get(k)
    if not rt:
        return {"ok": False, "error": f"unknown fundamentals kind: {kind}"}

    g = _fetch_gaps(rt, limit=5000)
    syms = g.get("symbols", [])
    bs = max(1, min(int(batch_size), 200))
    batches = [syms[i : i + bs] for i in range(0, len(syms), bs)]
    return {"ok": True, "gap_count": len(syms), "batches": batches}


# ── Batch readers for fundamentals extension evaluators (via Plugin HTTP) ────


def fetch_income_ext_rows_batch(
    cur: Any = None,
    symbols: Optional[List[str]] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """Batch-read quarterly income-statement rows with extra columns for ext evaluators.

    Returns symbol -> list of dicts (ascending period_end).
    ``cur`` kept for signature compat but ignored.
    """
    from bifrost_api.research.market_data_client import fetch_sepa_income_ext

    syms = symbols or []
    if not syms:
        return {}
    raw = fetch_sepa_income_ext(syms)
    out: Dict[str, List[Dict[str, Any]]] = {}
    for sym, rows in raw.items():
        enriched: List[Dict[str, Any]] = []
        for r in rows:
            d = {k: v for k, v in r.items() if k != "data"}
            flat = unpack_financial_data(r.get("data"), _INCOME_FIELDS)
            enriched.append({**d, **flat})
        out[sym] = enriched
    return out


def fetch_balance_sheet_rows_for_ext_batch(
    cur: Any = None,
    symbols: Optional[List[str]] = None,
    *,
    max_quarters: int = 6,
) -> Dict[str, List[Dict[str, Any]]]:
    """Batch-read latest N quarterly balance-sheet rows for each symbol.

    Returns symbol -> list of dicts (ascending period_end).
    ``cur`` kept for signature compat but ignored.
    """
    from bifrost_api.research.market_data_client import fetch_sepa_balance_sheet_ext

    syms = symbols or []
    if not syms:
        return {}
    raw = fetch_sepa_balance_sheet_ext(syms, max_quarters=max_quarters)
    out: Dict[str, List[Dict[str, Any]]] = {}
    for sym, rows in raw.items():
        enriched: List[Dict[str, Any]] = []
        for r in rows:
            d = {k: v for k, v in r.items() if k not in ("data", "rn")}
            flat = unpack_financial_data(r.get("data"), _BALANCE_FIELDS)
            enriched.append({**d, **flat})
        out[sym] = enriched
    return out


def fetch_cash_flow_rows_for_ext_batch(
    cur: Any = None,
    symbols: Optional[List[str]] = None,
    *,
    max_quarters: int = 6,
) -> Dict[str, List[Dict[str, Any]]]:
    """Batch-read latest N quarterly cash-flow rows for each symbol.

    Returns symbol -> list of dicts (ascending period_end).
    ``cur`` kept for signature compat but ignored.
    """
    from bifrost_api.research.market_data_client import fetch_sepa_cash_flow_ext

    syms = symbols or []
    if not syms:
        return {}
    raw = fetch_sepa_cash_flow_ext(syms, max_quarters=max_quarters)
    out: Dict[str, List[Dict[str, Any]]] = {}
    for sym, rows in raw.items():
        enriched: List[Dict[str, Any]] = []
        for r in rows:
            d = {k: v for k, v in r.items() if k not in ("data", "rn")}
            flat = unpack_financial_data(r.get("data"), _CASH_FLOW_FIELDS)
            enriched.append({**d, **flat})
        out[sym] = enriched
    return out


def fetch_ratios_latest_for_ext_batch(
    cur: Any = None,
    symbols: Optional[List[str]] = None,
) -> Dict[str, Dict[str, Any]]:
    """Batch-read the latest ratios row per symbol.

    Returns symbol -> single dict.
    ``cur`` kept for signature compat but ignored.
    """
    from bifrost_api.research.market_data_client import fetch_sepa_ratios_latest

    syms = symbols or []
    if not syms:
        return {}
    raw = fetch_sepa_ratios_latest(syms)
    out: Dict[str, Dict[str, Any]] = {}
    for sym, row in raw.items():
        d = {k: v for k, v in row.items() if k != "data"}
        flat = unpack_financial_data(row.get("data"), _RATIOS_FIELDS)
        out[sym] = {**d, **flat}
    return out


def fetch_short_interest_latest_batch(
    cur: Any = None,
    symbols: Optional[List[str]] = None,
    *,
    max_rows: int = 2,
) -> Dict[str, List[Dict[str, Any]]]:
    """Batch-read latest N short-interest rows per symbol.

    Returns symbol -> list of dicts (ascending settlement_date).
    ``cur`` kept for signature compat but ignored.
    """
    from bifrost_api.research.market_data_client import fetch_sepa_short_interest_latest

    syms = symbols or []
    if not syms:
        return {}
    raw = fetch_sepa_short_interest_latest(syms, max_rows=max_rows)
    out: Dict[str, List[Dict[str, Any]]] = {}
    for sym, rows in raw.items():
        enriched: List[Dict[str, Any]] = []
        for r in rows:
            d = {k: v for k, v in r.items() if k != "data"}
            flat = unpack_financial_data(r.get("data"), _SHORT_INTEREST_FIELDS)
            enriched.append({**d, **flat})
        out[sym] = enriched
    return out
