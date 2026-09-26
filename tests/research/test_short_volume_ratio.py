"""symbol-statements reports short volume as a ratio, not the vendor's percent.

The Stock Inspector renders ``short_volume_ratio`` times 100 with a "%" sign. The
route used to pass the vendor's field straight through, and that field is already
a percent: AAPL on 2026-09-25 came back 58.33 and the table showed 5833.0%.
"""

from __future__ import annotations

from typing import Any

import pytest

from bifrost_api.research import market_data_client
from bifrost_api.research.routers import data_readiness
from bifrost_api.research.sepa.financials_data import (
    REPORT_SHORT_VOLUME,
    _SHORT_VOLUME_FIELDS,
    short_volume_ratio,
    unpack_financial_data,
)

#: The vendor's shape, as the plugin returns it: volumes in fractional shares,
#: and a percent in ``short_volume_ratio``.
AAPL_2026_09_25 = {
    "short_volume": 6485654.248821,
    "total_volume": 11119362.831725,
    "short_volume_ratio": 58.33,
}


def test_ratio_is_computed_from_the_volumes() -> None:
    flat = unpack_financial_data(AAPL_2026_09_25, _SHORT_VOLUME_FIELDS)
    assert short_volume_ratio(flat) == pytest.approx(0.58328, abs=1e-5)


def test_volumes_as_strings_are_read_too() -> None:
    flat = {"short_volume": "4408113.135898", "total_volume": "8953313.23072", "short_volume_ratio": "49.23"}
    assert short_volume_ratio(flat) == pytest.approx(0.49234, abs=1e-5)


@pytest.mark.parametrize(
    "flat",
    [
        {"short_volume": 100.0, "total_volume": None, "short_volume_ratio": 58.33},
        {"short_volume": 100.0, "total_volume": 0, "short_volume_ratio": 58.33},
        {"short_volume": None, "total_volume": 200.0, "short_volume_ratio": 58.33},
    ],
)
def test_without_both_volumes_it_falls_back_to_the_percent_over_100(flat: dict[str, Any]) -> None:
    assert short_volume_ratio(flat) == pytest.approx(0.5833)


def test_nothing_to_go_on_is_none_not_zero() -> None:
    assert short_volume_ratio({"short_volume": None, "total_volume": None, "short_volume_ratio": None}) is None
    assert short_volume_ratio({"short_volume": "n/a", "total_volume": "", "short_volume_ratio": ""}) is None


def test_symbol_statements_serves_the_ratio(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_fetch(symbols: list[str], report_type: str, **_kw: Any) -> dict[str, list[dict[str, Any]]]:
        if report_type != REPORT_SHORT_VOLUME:
            return {}
        return {
            "AAPL": [
                {"period_date": "2026-09-25", "data": dict(AAPL_2026_09_25)},
                {
                    "period_date": "2026-09-24",
                    "data": {"short_volume": 4408113.135898, "total_volume": 8953313.23072, "short_volume_ratio": 49.23},
                },
            ]
        }

    monkeypatch.setattr(market_data_client, "fetch_sepa_financials", fake_fetch)
    out = data_readiness.get_symbol_statements(None, symbol=" aapl ")  # type: ignore[arg-type]
    rows = out["short_volume"]
    assert [r["trade_date"] for r in rows] == ["2026-09-25", "2026-09-24"]
    assert rows[0]["short_volume_ratio"] == pytest.approx(0.58328, abs=1e-5)
    assert rows[1]["short_volume_ratio"] == pytest.approx(0.49234, abs=1e-5)
    # What the table renders: ratio * 100 with one decimal.
    assert f"{rows[0]['short_volume_ratio'] * 100:.1f}%" == "58.3%"
