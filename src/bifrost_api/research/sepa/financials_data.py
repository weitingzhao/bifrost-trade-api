"""SEPA fundamentals readers — Plugin HTTP layer.

All reads against ``market.stock_financials`` are routed through the
Plugin Market Data API (``market_data_client``).  The jsonb ``data``
column is returned verbatim by the Plugin; field unpacking stays here.

W2-P2: replaced ~33 direct SQL queries with HTTP calls.
"""

from __future__ import annotations

import logging
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


def fetch_short_volume_recent_batch(
    cur: Any = None,
    symbols: Optional[List[str]] = None,
    *,
    max_days: int = 10,
) -> Dict[str, List[Dict[str, Any]]]:
    """Batch-read latest N short-volume rows per symbol.

    Returns symbol -> list of dicts (ascending trade_date).
    ``cur`` kept for signature compat but ignored.
    """
    from bifrost_api.research.market_data_client import fetch_sepa_short_volume_recent

    syms = symbols or []
    if not syms:
        return {}
    raw = fetch_sepa_short_volume_recent(syms, max_days=max_days)
    out: Dict[str, List[Dict[str, Any]]] = {}
    for sym, rows in raw.items():
        enriched: List[Dict[str, Any]] = []
        for r in rows:
            d = {k: v for k, v in r.items() if k != "data"}
            flat = unpack_financial_data(r.get("data"), _SHORT_VOLUME_FIELDS)
            enriched.append({**d, **flat})
        out[sym] = enriched
    return out
