"""Extended fundamental condition evaluators beyond the 8 SEPA core conditions.

Each group evaluator returns a GroupResult dict with its own conditions[], pass_count,
total, pass flag, and insufficient flag.  ``merge_extension_into_eval`` folds these
into the base ``evaluate_fundamentals`` output so that the JSONB written to
``stock_readiness_daily.fundamental_eval`` carries a flat ``conditions[]`` list
(backward-compatible) plus a ``groups`` summary map.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from bifrost_api.research.sepa.fundamentals_engine import make_condition, to_float

EXTENSION_VERSION = "ext_v1"

# ── Group names ──────────────────────────────────────────────────────────────

GROUP_SEPA_CORE = "sepa_core"
GROUP_QUALITY = "quality"
GROUP_BALANCE = "balance"
GROUP_CASHFLOW = "cashflow"
GROUP_VALUATION = "valuation"
GROUP_PROFITABILITY = "profitability"
GROUP_EFFICIENCY = "efficiency"
GROUP_SENTIMENT = "sentiment"

ALL_EXT_GROUPS = (
    GROUP_QUALITY,
    GROUP_BALANCE,
    GROUP_CASHFLOW,
    GROUP_VALUATION,
    GROUP_PROFITABILITY,
    GROUP_EFFICIENCY,
    GROUP_SENTIMENT,
)


@dataclass
class FundamentalsExtConfig:
    # quality
    gross_margin_threshold: float = 0.30
    operating_margin_threshold: float = 0.10
    net_margin_threshold: float = 0.05
    ocf_to_ni_threshold: float = 0.70
    interest_coverage_threshold: float = 5.0
    # balance
    current_ratio_threshold: float = 1.5
    quick_ratio_threshold: float = 1.0
    debt_to_equity_threshold: float = 1.0
    net_debt_to_ebitda_threshold: float = 3.0
    # cashflow
    fcf_margin_threshold: float = 0.05
    fcf_yield_threshold: float = 0.03
    capex_intensity_threshold: float = 0.15
    # valuation
    pe_threshold: float = 60.0
    ps_threshold: float = 15.0
    pb_threshold: float = 8.0
    ev_to_ebitda_threshold: float = 30.0
    # profitability
    roe_threshold: float = 0.15
    roa_threshold: float = 0.05
    # efficiency
    asset_turnover_threshold: float = 0.5
    dso_threshold: float = 75.0
    dio_threshold: float = 120.0
    # sentiment
    days_to_cover_threshold: float = 5.0
    short_volume_ratio_threshold: float = 0.30
    short_interest_pct_threshold: float = 0.15


def _group_result(
    name: str,
    conditions: List[Dict[str, Any]],
    *,
    insufficient: bool = False,
) -> Dict[str, Any]:
    pc = sum(1 for c in conditions if c.get("pass"))
    total = len(conditions)
    return {
        "name": name,
        "conditions": conditions,
        "pass_count": pc,
        "total": total,
        "pass": (pc == total and not insufficient),
        "insufficient": insufficient,
    }


def _sum_last_n(rows: List[Dict[str, Any]], key: str, n: int = 4) -> Optional[float]:
    vals = [to_float(r.get(key)) for r in rows[-n:]]
    clean = [v for v in vals if v is not None]
    return sum(clean) if len(clean) == n else None


def _avg_last_n(rows: List[Dict[str, Any]], key: str, n: int = 4) -> Optional[float]:
    s = _sum_last_n(rows, key, n)
    return (s / n) if s is not None else None


# ── Quality group ────────────────────────────────────────────────────────────

def evaluate_quality_group(
    income_q_rows: List[Dict[str, Any]],
    cf_q_rows: List[Dict[str, Any]],
    *,
    cfg: Optional[FundamentalsExtConfig] = None,
) -> Dict[str, Any]:
    """Margins + OCF/NI earnings quality + interest coverage.

    ``income_q_rows``: quarterly rows sorted ascending by period_end; need
    fields ``gross_profit, operating_income, consolidated_net_income_loss,
    revenue, interest_expense``.
    ``cf_q_rows``: quarterly cash-flow rows sorted ascending; need
    ``net_cash_from_operating_activities``.
    """
    conf = cfg or FundamentalsExtConfig()
    g = GROUP_QUALITY
    conditions: List[Dict[str, Any]] = []
    has_data = len(income_q_rows) >= 4

    # gross margin
    rev_ttm = _sum_last_n(income_q_rows, "revenue")
    gp_ttm = _sum_last_n(income_q_rows, "gross_profit")
    if rev_ttm and rev_ttm > 0 and gp_ttm is not None:
        gm = gp_ttm / rev_ttm
        conditions.append(make_condition(
            "gross_margin_ge_30pct", gm >= conf.gross_margin_threshold,
            round(gm, 4), conf.gross_margin_threshold,
            "Trailing 4-quarter gross margin", group=g,
        ))
    else:
        conditions.append(make_condition(
            "gross_margin_ge_30pct", False, None, conf.gross_margin_threshold,
            "Insufficient gross profit / revenue data", group=g,
        ))

    # operating margin
    oi_ttm = _sum_last_n(income_q_rows, "operating_income")
    if rev_ttm and rev_ttm > 0 and oi_ttm is not None:
        om = oi_ttm / rev_ttm
        conditions.append(make_condition(
            "operating_margin_ge_10pct", om >= conf.operating_margin_threshold,
            round(om, 4), conf.operating_margin_threshold,
            "Trailing 4-quarter operating margin", group=g,
        ))
    else:
        conditions.append(make_condition(
            "operating_margin_ge_10pct", False, None, conf.operating_margin_threshold,
            "Insufficient operating income / revenue data", group=g,
        ))

    # net margin
    ni_ttm = _sum_last_n(income_q_rows, "consolidated_net_income_loss")
    if rev_ttm and rev_ttm > 0 and ni_ttm is not None:
        nm = ni_ttm / rev_ttm
        conditions.append(make_condition(
            "net_margin_ge_5pct", nm >= conf.net_margin_threshold,
            round(nm, 4), conf.net_margin_threshold,
            "Trailing 4-quarter net margin", group=g,
        ))
    else:
        conditions.append(make_condition(
            "net_margin_ge_5pct", False, None, conf.net_margin_threshold,
            "Insufficient net income / revenue data", group=g,
        ))

    # OCF / NI (earnings quality)
    ocf_ttm = _sum_last_n(cf_q_rows, "net_cash_from_operating_activities")
    if ocf_ttm is not None and ni_ttm is not None and ni_ttm != 0:
        ratio = ocf_ttm / ni_ttm
        conditions.append(make_condition(
            "ocf_to_ni_ge_0_7", ratio >= conf.ocf_to_ni_threshold,
            round(ratio, 4), conf.ocf_to_ni_threshold,
            "OCF-to-net-income ratio (earnings quality)", group=g,
        ))
    else:
        conditions.append(make_condition(
            "ocf_to_ni_ge_0_7", False, None, conf.ocf_to_ni_threshold,
            "Insufficient OCF or net-income data", group=g,
        ))

    # interest coverage
    ie_ttm = _sum_last_n(income_q_rows, "interest_expense")
    if oi_ttm is not None and ie_ttm is not None:
        if ie_ttm == 0:
            conditions.append(make_condition(
                "interest_coverage_ge_5x", True, None, conf.interest_coverage_threshold,
                "No debt burden (zero interest expense)", group=g,
            ))
        else:
            ic = oi_ttm / abs(ie_ttm)
            conditions.append(make_condition(
                "interest_coverage_ge_5x", ic >= conf.interest_coverage_threshold,
                round(ic, 2), conf.interest_coverage_threshold,
                "Interest coverage ratio", group=g,
            ))
    else:
        conditions.append(make_condition(
            "interest_coverage_ge_5x", False, None, conf.interest_coverage_threshold,
            "Insufficient operating income or interest expense data", group=g,
        ))

    return _group_result(g, conditions, insufficient=not has_data)


# ── Balance group ────────────────────────────────────────────────────────────

def evaluate_balance_group(
    bs_q_rows: List[Dict[str, Any]],
    ratios_row: Optional[Dict[str, Any]],
    income_q_rows: List[Dict[str, Any]],
    *,
    cfg: Optional[FundamentalsExtConfig] = None,
) -> Dict[str, Any]:
    conf = cfg or FundamentalsExtConfig()
    g = GROUP_BALANCE
    conditions: List[Dict[str, Any]] = []
    latest_bs = bs_q_rows[-1] if bs_q_rows else {}
    has_bs = bool(latest_bs)

    # current ratio
    tca = to_float(latest_bs.get("total_current_assets"))
    tcl = to_float(latest_bs.get("total_current_liabilities"))
    if tca is not None and tcl is not None and tcl > 0:
        cr = tca / tcl
        conditions.append(make_condition(
            "current_ratio_ge_1_5", cr >= conf.current_ratio_threshold,
            round(cr, 2), conf.current_ratio_threshold,
            "Current ratio (latest quarter)", group=g,
        ))
    else:
        conditions.append(make_condition(
            "current_ratio_ge_1_5", False, None, conf.current_ratio_threshold,
            "Insufficient balance sheet data", group=g,
        ))

    # quick ratio
    inv = to_float(latest_bs.get("inventories")) or 0.0
    if tca is not None and tcl is not None and tcl > 0:
        qr = (tca - inv) / tcl
        conditions.append(make_condition(
            "quick_ratio_ge_1_0", qr >= conf.quick_ratio_threshold,
            round(qr, 2), conf.quick_ratio_threshold,
            "Quick ratio (latest quarter)", group=g,
        ))
    else:
        conditions.append(make_condition(
            "quick_ratio_ge_1_0", False, None, conf.quick_ratio_threshold,
            "Insufficient balance sheet data", group=g,
        ))

    # debt to equity from ratios
    de = to_float(ratios_row.get("debt_to_equity")) if ratios_row else None
    if de is not None:
        conditions.append(make_condition(
            "debt_to_equity_le_1", de <= conf.debt_to_equity_threshold,
            round(de, 2), conf.debt_to_equity_threshold,
            "Debt-to-equity ratio (latest ratios)", group=g,
        ))
    else:
        conditions.append(make_condition(
            "debt_to_equity_le_1", False, None, conf.debt_to_equity_threshold,
            "No ratios D/E data", group=g,
        ))

    # net debt / EBITDA
    dc = to_float(latest_bs.get("debt_current")) or 0.0
    ltd = to_float(latest_bs.get("long_term_debt_and_capital_lease_obligations")) or 0.0
    cash = to_float(latest_bs.get("cash_and_equivalents")) or 0.0
    sti = to_float(latest_bs.get("short_term_investments")) or 0.0
    net_debt = dc + ltd - cash - sti
    ebitda_ttm = _sum_last_n(income_q_rows, "ebitda")
    if ebitda_ttm is not None and ebitda_ttm > 0 and has_bs:
        nde = net_debt / ebitda_ttm
        conditions.append(make_condition(
            "net_debt_to_ebitda_le_3", nde <= conf.net_debt_to_ebitda_threshold,
            round(nde, 2), conf.net_debt_to_ebitda_threshold,
            "Net debt / EBITDA", group=g,
        ))
    else:
        conditions.append(make_condition(
            "net_debt_to_ebitda_le_3", False, None, conf.net_debt_to_ebitda_threshold,
            "Insufficient EBITDA or balance sheet data" if not (ebitda_ttm and ebitda_ttm > 0)
            else "Insufficient balance sheet data", group=g,
        ))

    return _group_result(g, conditions, insufficient=not has_bs)


# ── Cash-flow group ──────────────────────────────────────────────────────────

def evaluate_cashflow_group(
    income_q_rows: List[Dict[str, Any]],
    cf_q_rows: List[Dict[str, Any]],
    ratios_row: Optional[Dict[str, Any]],
    *,
    cfg: Optional[FundamentalsExtConfig] = None,
) -> Dict[str, Any]:
    conf = cfg or FundamentalsExtConfig()
    g = GROUP_CASHFLOW
    conditions: List[Dict[str, Any]] = []
    has_cf = len(cf_q_rows) >= 4

    ocf_ttm = _sum_last_n(cf_q_rows, "net_cash_from_operating_activities")
    capex_ttm = _sum_last_n(cf_q_rows, "purchase_of_property_plant_and_equipment")
    rev_ttm = _sum_last_n(income_q_rows, "revenue")
    mcap = to_float(ratios_row.get("market_cap")) if ratios_row else None

    fcf = (ocf_ttm + capex_ttm) if (ocf_ttm is not None and capex_ttm is not None) else None

    # FCF positive
    if fcf is not None:
        conditions.append(make_condition(
            "fcf_positive", fcf > 0,
            round(fcf, 0), 0.0,
            "Free cash flow positive (OCF + CapEx)", group=g,
        ))
    else:
        conditions.append(make_condition(
            "fcf_positive", False, None, 0.0,
            "Insufficient cash-flow data", group=g,
        ))

    # FCF margin
    if fcf is not None and rev_ttm and rev_ttm > 0:
        fm = fcf / rev_ttm
        conditions.append(make_condition(
            "fcf_margin_ge_5pct", fm >= conf.fcf_margin_threshold,
            round(fm, 4), conf.fcf_margin_threshold,
            "FCF margin (FCF / revenue TTM)", group=g,
        ))
    else:
        conditions.append(make_condition(
            "fcf_margin_ge_5pct", False, None, conf.fcf_margin_threshold,
            "Insufficient FCF or revenue data", group=g,
        ))

    # FCF yield
    if fcf is not None and mcap and mcap > 0:
        fy = fcf / mcap
        conditions.append(make_condition(
            "fcf_yield_ge_3pct", fy >= conf.fcf_yield_threshold,
            round(fy, 4), conf.fcf_yield_threshold,
            "FCF yield (FCF / market cap)", group=g,
        ))
    else:
        conditions.append(make_condition(
            "fcf_yield_ge_3pct", False, None, conf.fcf_yield_threshold,
            "Insufficient FCF or market cap data", group=g,
        ))

    # CapEx intensity
    if capex_ttm is not None and rev_ttm and rev_ttm > 0:
        ci = abs(capex_ttm) / rev_ttm
        conditions.append(make_condition(
            "capex_intensity_le_15pct", ci <= conf.capex_intensity_threshold,
            round(ci, 4), conf.capex_intensity_threshold,
            "CapEx intensity (|CapEx| / revenue)", group=g,
        ))
    else:
        conditions.append(make_condition(
            "capex_intensity_le_15pct", False, None, conf.capex_intensity_threshold,
            "Insufficient CapEx or revenue data", group=g,
        ))

    return _group_result(g, conditions, insufficient=not has_cf)


# ── Valuation group ──────────────────────────────────────────────────────────

def evaluate_valuation_group(
    ratios_row: Optional[Dict[str, Any]],
    *,
    cfg: Optional[FundamentalsExtConfig] = None,
) -> Dict[str, Any]:
    conf = cfg or FundamentalsExtConfig()
    g = GROUP_VALUATION
    conditions: List[Dict[str, Any]] = []
    has_ratios = ratios_row is not None

    def _ratio_cond(cid: str, key: str, threshold: float, label: str, *, le: bool = True) -> None:
        val = to_float(ratios_row.get(key)) if ratios_row else None
        if val is not None:
            if val < 0:
                conditions.append(make_condition(cid, False, round(val, 2), threshold,
                                                 f"Negative {label}", group=g))
            else:
                passed = val <= threshold if le else val >= threshold
                conditions.append(make_condition(cid, passed, round(val, 2), threshold, label, group=g))
        else:
            conditions.append(make_condition(cid, False, None, threshold,
                                             f"No {label} data", group=g))

    _ratio_cond("pe_le_60", "price_to_earnings", conf.pe_threshold, "Price-to-earnings")
    _ratio_cond("ps_le_15", "price_to_sales", conf.ps_threshold, "Price-to-sales")
    _ratio_cond("pb_le_8", "price_to_book", conf.pb_threshold, "Price-to-book")
    _ratio_cond("ev_to_ebitda_le_30", "ev_to_ebitda", conf.ev_to_ebitda_threshold, "EV / EBITDA")

    return _group_result(g, conditions, insufficient=not has_ratios)


# ── Profitability group ──────────────────────────────────────────────────────

def evaluate_profitability_group(
    ratios_row: Optional[Dict[str, Any]],
    *,
    cfg: Optional[FundamentalsExtConfig] = None,
) -> Dict[str, Any]:
    conf = cfg or FundamentalsExtConfig()
    g = GROUP_PROFITABILITY
    conditions: List[Dict[str, Any]] = []
    has_ratios = ratios_row is not None

    roe = to_float(ratios_row.get("return_on_equity")) if ratios_row else None
    if roe is not None:
        conditions.append(make_condition(
            "roe_ge_15pct", roe >= conf.roe_threshold,
            round(roe, 4), conf.roe_threshold,
            "Return on equity", group=g,
        ))
    else:
        conditions.append(make_condition(
            "roe_ge_15pct", False, None, conf.roe_threshold,
            "No ROE data", group=g,
        ))

    roa = to_float(ratios_row.get("return_on_assets")) if ratios_row else None
    if roa is not None:
        conditions.append(make_condition(
            "roa_ge_5pct", roa >= conf.roa_threshold,
            round(roa, 4), conf.roa_threshold,
            "Return on assets", group=g,
        ))
    else:
        conditions.append(make_condition(
            "roa_ge_5pct", False, None, conf.roa_threshold,
            "No ROA data", group=g,
        ))

    return _group_result(g, conditions, insufficient=not has_ratios)


# ── Efficiency group ─────────────────────────────────────────────────────────

def evaluate_efficiency_group(
    income_q_rows: List[Dict[str, Any]],
    bs_q_rows: List[Dict[str, Any]],
    *,
    cfg: Optional[FundamentalsExtConfig] = None,
) -> Dict[str, Any]:
    conf = cfg or FundamentalsExtConfig()
    g = GROUP_EFFICIENCY
    conditions: List[Dict[str, Any]] = []
    has_data = len(income_q_rows) >= 4 and len(bs_q_rows) >= 4

    rev_ttm = _sum_last_n(income_q_rows, "revenue")
    avg_ta = _avg_last_n(bs_q_rows, "total_assets")
    avg_recv = _avg_last_n(bs_q_rows, "receivables")
    avg_inv = _avg_last_n(bs_q_rows, "inventories")
    cor_ttm = _sum_last_n(income_q_rows, "cost_of_revenue")

    # asset turnover
    if rev_ttm and avg_ta and avg_ta > 0:
        at = rev_ttm / avg_ta
        conditions.append(make_condition(
            "asset_turnover_ge_0_5", at >= conf.asset_turnover_threshold,
            round(at, 2), conf.asset_turnover_threshold,
            "Asset turnover (revenue / avg total assets)", group=g,
        ))
    else:
        conditions.append(make_condition(
            "asset_turnover_ge_0_5", False, None, conf.asset_turnover_threshold,
            "Insufficient revenue or total assets data", group=g,
        ))

    # DSO
    if rev_ttm and rev_ttm > 0 and avg_recv is not None:
        dso = 365.0 * avg_recv / rev_ttm
        conditions.append(make_condition(
            "dso_le_75_days", dso <= conf.dso_threshold,
            round(dso, 1), conf.dso_threshold,
            "Days sales outstanding", group=g,
        ))
    else:
        conditions.append(make_condition(
            "dso_le_75_days", False, None, conf.dso_threshold,
            "Insufficient receivables or revenue data", group=g,
        ))

    # DIO
    if cor_ttm and cor_ttm > 0 and avg_inv is not None:
        dio = 365.0 * avg_inv / cor_ttm
        conditions.append(make_condition(
            "dio_le_120_days", dio <= conf.dio_threshold,
            round(dio, 1), conf.dio_threshold,
            "Days inventory outstanding", group=g,
        ))
    else:
        conditions.append(make_condition(
            "dio_le_120_days", False, None, conf.dio_threshold,
            "Insufficient inventory or COGS data", group=g,
        ))

    return _group_result(g, conditions, insufficient=not has_data)


# ── Sentiment group ──────────────────────────────────────────────────────────

def evaluate_sentiment_group(
    si_rows: List[Dict[str, Any]],
    sv_rows: List[Dict[str, Any]],
    diluted_shares: Optional[float],
    *,
    cfg: Optional[FundamentalsExtConfig] = None,
) -> Dict[str, Any]:
    """Short-interest / short-volume derived conditions.

    ``si_rows``: stock_short_interest rows sorted by settlement_date ASC.
    ``sv_rows``: stock_short_volume rows sorted by trade_date ASC (last ~10 days).
    ``diluted_shares``: latest diluted_shares_outstanding from income statement.
    """
    conf = cfg or FundamentalsExtConfig()
    g = GROUP_SENTIMENT
    conditions: List[Dict[str, Any]] = []
    has_si = len(si_rows) > 0

    # days to cover
    latest_si = si_rows[-1] if si_rows else {}
    dtc = to_float(latest_si.get("days_to_cover"))
    if dtc is not None:
        conditions.append(make_condition(
            "days_to_cover_le_5", dtc <= conf.days_to_cover_threshold,
            round(dtc, 2), conf.days_to_cover_threshold,
            "Days to cover (latest short interest report)", group=g,
        ))
    else:
        conditions.append(make_condition(
            "days_to_cover_le_5", False, None, conf.days_to_cover_threshold,
            "No short interest data", group=g,
        ))

    # short volume ratio avg
    sv_vals = [to_float(r.get("short_volume_ratio")) for r in sv_rows]
    sv_clean = [v for v in sv_vals if v is not None]
    if sv_clean:
        avg_svr = sum(sv_clean) / len(sv_clean)
        conditions.append(make_condition(
            "short_volume_ratio_recent_le_30pct", avg_svr <= conf.short_volume_ratio_threshold,
            round(avg_svr, 4), conf.short_volume_ratio_threshold,
            f"Avg short-volume ratio (last {len(sv_clean)} days)", group=g,
        ))
    else:
        conditions.append(make_condition(
            "short_volume_ratio_recent_le_30pct", False, None, conf.short_volume_ratio_threshold,
            "No short volume data", group=g,
        ))

    # short interest % of float
    si_val = to_float(latest_si.get("short_interest"))
    if si_val is not None and diluted_shares and diluted_shares > 0:
        pct = si_val / diluted_shares
        conditions.append(make_condition(
            "short_interest_pct_of_float_le_15pct", pct <= conf.short_interest_pct_threshold,
            round(pct, 4), conf.short_interest_pct_threshold,
            "Short interest as % of shares outstanding", group=g,
        ))
    else:
        conditions.append(make_condition(
            "short_interest_pct_of_float_le_15pct", False, None, conf.short_interest_pct_threshold,
            "Insufficient short interest or share count data", group=g,
        ))

    return _group_result(g, conditions, insufficient=not has_si)


# ── Merge ────────────────────────────────────────────────────────────────────

def merge_extension_into_eval(
    base_eval: Dict[str, Any],
    group_results: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Merge group evaluator outputs into the ``evaluate_fundamentals`` result dict.

    Preserves all original keys.  Adds ``extension_version``, ``groups`` map,
    and stamps every original condition with ``group='sepa_core'``.
    """
    out = deepcopy(base_eval)
    out["extension_version"] = EXTENSION_VERSION

    # tag original conditions
    for c in out.get("conditions", []):
        if "group" not in c:
            c["group"] = GROUP_SEPA_CORE

    # build groups summary; start with sepa_core from base
    core_conds = [c for c in out.get("conditions", []) if c.get("group") == GROUP_SEPA_CORE]
    core_pc = sum(1 for c in core_conds if c.get("pass"))
    groups: Dict[str, Any] = {
        GROUP_SEPA_CORE: {
            "total": len(core_conds),
            "pass_count": core_pc,
            "pass": core_pc == len(core_conds) and not out.get("insufficient_data", False),
            "insufficient": bool(out.get("insufficient_data", False)),
        },
    }

    # merge each extension group
    ext_metrics: Dict[str, Any] = {}
    for gr in group_results:
        gname = gr["name"]
        for c in gr.get("conditions", []):
            out["conditions"].append(c)
            if c.get("actual") is not None:
                ext_metrics[c["id"]] = c["actual"]
        groups[gname] = {
            "total": gr["total"],
            "pass_count": gr["pass_count"],
            "pass": gr["pass"],
            "insufficient": gr["insufficient"],
        }

    out["groups"] = groups

    # merge metrics
    base_metrics = out.get("metrics") or {}
    base_metrics.update(ext_metrics)
    out["metrics"] = base_metrics

    return out
