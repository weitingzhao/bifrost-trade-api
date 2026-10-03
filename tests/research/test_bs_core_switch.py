"""TD-42: the research greeks and screener math moved to bifrost_core.pricing (api 0.6.1).

``fixtures/bs_research_golden.json`` was recorded from the api's own copies before they were
deleted (synthetic grid: S, K, T incl. <= 0, sigma incl. 0, both rates in use, C / P / odd
right strings, IV prices incl. <= 0 and below intrinsic). The functions the routes call now
come from core; this test holds them to the recording bit for bit -- same floats, same
None, same exception class.

Record (only on purpose, with the old code):  PYTHONPATH=src python tests/research/test_bs_core_switch.py
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Callable, Dict, List

FIXTURE = Path(__file__).parent / "fixtures" / "bs_research_golden.json"

S = 100.0
STRIKES = (70.0, 95.0, 100.0, 105.0, 140.0)
TIMES = (-1.0 / 365, 0.0, 1.0 / (365 * 24), 1.0 / 365, 30.0 / 365, 1.0)
SIGMAS = (0.0, 1e-4, 0.05, 0.30, 1.20)
RATES = (0.045, 0.0)
RIGHTS = ("C", "P", "c", "call", "PUT", "")
IV_PRICES = (-1.0, 0.0, 1e-4, 0.5, 2.0, 5.0, 12.0, 31.0, 120.0)
DTES = (-1, 0, 1, 7, 30, 365)


def _safe(fn: Callable[..., Any], *args: Any) -> Any:
    try:
        v = fn(*args)
    except Exception as exc:  # noqa: BLE001 - the class is the recorded behaviour
        return {"error": type(exc).__name__}
    if isinstance(v, float) and math.isnan(v):
        return "nan"
    if isinstance(v, dict):
        return {k: ("nan" if isinstance(x, float) and math.isnan(x) else x) for k, x in v.items()}
    return v


def build() -> Dict[str, List[Any]]:
    from bifrost_api.research.routers import greeks as g
    from bifrost_api.research.routers import screener as sc

    out: Dict[str, List[Any]] = {"greeks": [], "iv": [], "prob_itm_put": []}
    for K in STRIKES:
        for T in TIMES:
            for sigma in SIGMAS:
                for r in RATES:
                    for right in RIGHTS:
                        out["greeks"].append(_safe(g.compute_greeks, S, K, T, r, sigma, right))
                        for p in IV_PRICES:
                            out["iv"].append(_safe(g.implied_vol, p, S, K, T, r, right))
    for K in STRIKES:
        for dte in DTES:
            for iv in SIGMAS:
                out["prob_itm_put"].append(_safe(sc._prob_itm_put, S, K, dte, iv))
    return out


def test_core_reproduces_the_recorded_research_math() -> None:
    recorded = json.loads(FIXTURE.read_text())
    now = json.loads(json.dumps(build()))
    for key in ("greeks", "iv", "prob_itm_put"):
        assert len(now[key]) == len(recorded[key]), key
        bad = [(i, a, b) for i, (a, b) in enumerate(zip(now[key], recorded[key])) if a != b]
        assert not bad, f"{key}: {len(bad)} of {len(now[key])} differ, first {bad[:3]}"


if __name__ == "__main__":
    FIXTURE.write_text(json.dumps(build(), separators=(",", ":")) + "\n")
    print("recorded", FIXTURE, {k: len(v) for k, v in build().items()})
