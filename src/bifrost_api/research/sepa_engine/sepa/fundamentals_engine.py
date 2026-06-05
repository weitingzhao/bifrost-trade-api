from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

FUNDAMENTALS_RULE_VERSION = "sepa_fundamentals_v1"
_FISCAL_Q_MAP = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}


@dataclass
class FundamentalsConfig:
    eps_q2q_threshold: float = 0.25
    rev_q2q_threshold: float = 0.25
    eps_3y_threshold: float = 0.15
    rev_3y_threshold: float = 0.15


def make_condition(
    cond_id: str,
    passed: bool,
    actual: Optional[float],
    threshold: Optional[float],
    reason: str,
    *,
    group: Optional[str] = None,
) -> Dict[str, Any]:
    d: Dict[str, Any] = {
        "id": cond_id,
        "pass": bool(passed),
        "actual": actual,
        "threshold": threshold,
        "reason": reason,
    }
    if group is not None:
        d["group"] = group
    return d


_condition = make_condition


def to_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _sort_quarterly(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def _key(r: Dict[str, Any]) -> Tuple[int, int]:
        fy = int(r.get("fiscal_year") or 0)
        fp = str(r.get("fiscal_period") or "").upper()
        fq = _FISCAL_Q_MAP.get(fp, 0)
        return fy, fq

    return sorted(rows, key=_key)


def _sort_annual(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(rows, key=lambda r: int(r.get("fiscal_year") or 0))


_to_float = to_float


def _build_quarterly_growth_series(
    qrows: List[Dict[str, Any]],
    field: str,
) -> Tuple[List[float], Optional[str]]:
    by_key: Dict[Tuple[int, int], float] = {}
    for r in qrows:
        fy = int(r.get("fiscal_year") or 0)
        fq = _FISCAL_Q_MAP.get(str(r.get("fiscal_period") or "").upper(), 0)
        val = _to_float(r.get(field))
        if fy <= 0 or fq <= 0 or val is None:
            continue
        by_key[(fy, fq)] = val

    growth: List[float] = []
    issue: Optional[str] = None
    for fy, fq in sorted(by_key.keys()):
        cur = by_key[(fy, fq)]
        prev = by_key.get((fy - 1, fq))
        if prev is None:
            continue
        if prev == 0:
            continue
        if field == "basic_earnings_per_share" and prev < 0:
            issue = "not_comparable_negative_base"
            continue
        growth.append((cur / prev) - 1.0)
    return growth, issue


def _build_annual_values(arows: List[Dict[str, Any]], field: str) -> List[Tuple[int, float]]:
    out: List[Tuple[int, float]] = []
    for r in arows:
        fy = int(r.get("fiscal_year") or 0)
        val = _to_float(r.get(field))
        if fy <= 0 or val is None:
            continue
        out.append((fy, val))
    return sorted(out, key=lambda x: x[0])


def _cagr_3y(values: List[Tuple[int, float]], *, eps_mode: bool = False) -> Tuple[Optional[float], Optional[str]]:
    if len(values) < 4:
        return None, "insufficient_data"
    v0 = values[-4][1]
    v3 = values[-1][1]
    if v0 == 0:
        return None, "not_comparable_zero_base"
    if eps_mode and v0 < 0:
        return None, "not_comparable_negative_base"
    if v3 <= 0:
        return None, "not_comparable_non_positive_latest"
    cagr = (v3 / v0) ** (1.0 / 3.0) - 1.0
    return cagr, None


def _acc_fy(values: List[Tuple[int, float]], *, eps_mode: bool = False) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    if len(values) < 4:
        return None, None, "insufficient_data"
    # Need latest two YoY growth values: g_latest and g_prev.
    v_n3 = values[-4][1]
    v_n2 = values[-3][1]
    v_n1 = values[-2][1]
    v_n0 = values[-1][1]

    for b in (v_n3, v_n2, v_n1):
        if b == 0:
            return None, None, "not_comparable_zero_base"
    if eps_mode and (v_n3 < 0 or v_n2 < 0):
        return None, None, "not_comparable_negative_base"

    g_prev = (v_n1 / v_n2) - 1.0
    g_latest = (v_n0 / v_n1) - 1.0
    return g_latest, g_prev, None


def evaluate_fundamentals(
    quarterly_rows: List[Dict[str, Any]],
    annual_rows: List[Dict[str, Any]],
    *,
    cfg: Optional[FundamentalsConfig] = None,
) -> Dict[str, Any]:
    conf = cfg or FundamentalsConfig()
    qrows = _sort_quarterly(quarterly_rows)
    arows = _sort_annual(annual_rows)

    eps_q_growth, eps_q_issue = _build_quarterly_growth_series(qrows, "basic_earnings_per_share")
    rev_q_growth, rev_q_issue = _build_quarterly_growth_series(qrows, "revenues")

    latest_eps_q2q = eps_q_growth[-1] if eps_q_growth else None
    latest_rev_q2q = rev_q_growth[-1] if rev_q_growth else None

    eps_q2q_pass = latest_eps_q2q is not None and latest_eps_q2q >= conf.eps_q2q_threshold
    rev_q2q_pass = latest_rev_q2q is not None and latest_rev_q2q >= conf.rev_q2q_threshold

    eps_acc_pass = len(eps_q_growth) >= 3 and eps_q_growth[-1] > eps_q_growth[-2] > eps_q_growth[-3]
    rev_acc_pass = len(rev_q_growth) >= 3 and rev_q_growth[-1] > rev_q_growth[-2] > rev_q_growth[-3]

    eps_annual = _build_annual_values(arows, "basic_earnings_per_share")
    rev_annual = _build_annual_values(arows, "revenues")

    eps_cagr, eps_cagr_issue = _cagr_3y(eps_annual, eps_mode=True)
    rev_cagr, rev_cagr_issue = _cagr_3y(rev_annual, eps_mode=False)
    eps_3y_pass = eps_cagr is not None and eps_cagr >= conf.eps_3y_threshold
    rev_3y_pass = rev_cagr is not None and rev_cagr >= conf.rev_3y_threshold

    eps_acc_latest, eps_acc_prev, eps_acc_issue = _acc_fy(eps_annual, eps_mode=True)
    rev_acc_latest, rev_acc_prev, rev_acc_issue = _acc_fy(rev_annual, eps_mode=False)
    eps_fy_acc_pass = eps_acc_latest is not None and eps_acc_prev is not None and eps_acc_latest > eps_acc_prev
    rev_fy_acc_pass = rev_acc_latest is not None and rev_acc_prev is not None and rev_acc_latest > rev_acc_prev

    conditions = [
        _condition(
            "eps_q2q_ge_25pct",
            eps_q2q_pass,
            latest_eps_q2q,
            conf.eps_q2q_threshold,
            "Latest quarterly EPS YoY growth",
        ),
        _condition(
            "rev_q2q_ge_25pct",
            rev_q2q_pass,
            latest_rev_q2q,
            conf.rev_q2q_threshold,
            "Latest quarterly revenue YoY growth",
        ),
        _condition(
            "eps_acc_2q",
            eps_acc_pass,
            eps_q_growth[-1] if eps_q_growth else None,
            None,
            "EPS YoY growth accelerating for last 2 quarters",
        ),
        _condition(
            "rev_acc_2q",
            rev_acc_pass,
            rev_q_growth[-1] if rev_q_growth else None,
            None,
            "Revenue YoY growth accelerating for last 2 quarters",
        ),
        _condition(
            "eps_3y_ge_15pct",
            eps_3y_pass,
            eps_cagr,
            conf.eps_3y_threshold,
            "EPS 3-year CAGR",
        ),
        _condition(
            "rev_3y_ge_15pct",
            rev_3y_pass,
            rev_cagr,
            conf.rev_3y_threshold,
            "Revenue 3-year CAGR",
        ),
        _condition(
            "eps_acc_fy",
            eps_fy_acc_pass,
            eps_acc_latest,
            eps_acc_prev,
            "EPS annual YoY growth acceleration",
        ),
        _condition(
            "rev_acc_fy",
            rev_fy_acc_pass,
            rev_acc_latest,
            rev_acc_prev,
            "Revenue annual YoY growth acceleration",
        ),
    ]

    issues = [
        x
        for x in [
            eps_q_issue,
            rev_q_issue,
            eps_cagr_issue,
            rev_cagr_issue,
            eps_acc_issue,
            rev_acc_issue,
        ]
        if x
    ]
    insufficient_data = any(i == "insufficient_data" for i in issues)
    not_comparable = any(i and i.startswith("not_comparable") for i in issues)

    pass_count = sum(1 for c in conditions if c["pass"])
    fail_count = len(conditions) - pass_count
    return {
        "fundamental_pass": fail_count == 0,
        "insufficient_data": insufficient_data,
        "not_comparable": not_comparable,
        "conditions": conditions,
        "pass_count": pass_count,
        "fail_count": fail_count,
        "metrics": {
            "latest_eps_q2q": latest_eps_q2q,
            "latest_rev_q2q": latest_rev_q2q,
            "eps_3y_cagr": eps_cagr,
            "rev_3y_cagr": rev_cagr,
            "eps_fy_growth_latest": eps_acc_latest,
            "eps_fy_growth_prev": eps_acc_prev,
            "rev_fy_growth_latest": rev_acc_latest,
            "rev_fy_growth_prev": rev_acc_prev,
            "quarterly_rows_used": len(qrows),
            "annual_rows_used": len(arows),
        },
        "issues": sorted(set(issues)),
    }


def fetch_and_evaluate_fundamentals_batch(
    client: Any,
    symbols: List[str],
    *,
    cfg: Optional[FundamentalsConfig] = None,
    throttle_sec: float = 0.2,
) -> Dict[str, Any]:
    conf = cfg or FundamentalsConfig()
    results: List[Dict[str, Any]] = []
    warnings: Dict[str, str] = {}
    syms = sorted({str(s or "").strip().upper() for s in symbols if str(s or "").strip()})

    for idx, sym in enumerate(syms):
        try:
            qres = client.fetch_stock_income_statements(sym, timeframe="quarterly", limit=12, sort="filing_date.desc")
            ares = client.fetch_stock_income_statements(sym, timeframe="annual", limit=5, sort="filing_date.desc")
            qrows = qres.get("results") if isinstance(qres, dict) else None
            arows = ares.get("results") if isinstance(ares, dict) else None
            if not isinstance(qrows, list):
                qrows = []
            if not isinstance(arows, list):
                arows = []
            eval_out = evaluate_fundamentals(qrows, arows, cfg=conf)
            eval_out["symbol"] = sym
            if qres.get("error"):
                warnings[sym] = str(qres.get("error"))
            elif ares.get("error"):
                warnings[sym] = str(ares.get("error"))
            results.append(eval_out)
        except Exception as exc:
            warnings[sym] = str(exc)
            results.append(
                {
                    "symbol": sym,
                    "fundamental_pass": False,
                    "insufficient_data": True,
                    "not_comparable": False,
                    "conditions": [],
                    "pass_count": 0,
                    "fail_count": 0,
                    "metrics": {},
                    "issues": ["evaluation_failed"],
                }
            )
        if idx < len(syms) - 1 and throttle_sec > 0:
            time.sleep(throttle_sec)

    total = len(results)
    passed = sum(1 for r in results if r.get("fundamental_pass"))
    insufficient = sum(1 for r in results if r.get("insufficient_data"))
    failed = total - passed
    return {
        "results": results,
        "summary": {
            "total": total,
            "passed": passed,
            "failed": failed,
            "insufficient_data": insufficient,
        },
        "warnings": warnings,
        "rule_version": FUNDAMENTALS_RULE_VERSION,
    }

