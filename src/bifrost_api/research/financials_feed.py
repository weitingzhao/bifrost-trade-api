"""SEPA financials feed runners — retired Trade ingest path stub (Wave 7-C)."""

from __future__ import annotations

from typing import Any, Dict

SOURCE_DEFAULT = "massive"
FEED_SOURCE_DEFAULT = SOURCE_DEFAULT

_RETIRED = {
    "ok": False,
    "error": "Massive financials feed retired — use market-data plugin",
    "reason": "massive_retired",
}


def upsert_income_statement_rows(*_args: Any, **_kwargs: Any) -> int:
    return 0


def upsert_balance_sheet_rows(*_args: Any, **_kwargs: Any) -> int:
    return 0


def upsert_cash_flow_rows(*_args: Any, **_kwargs: Any) -> int:
    return 0


def upsert_ratios_rows(*_args: Any, **_kwargs: Any) -> int:
    return 0


def upsert_short_interest_rows(*_args: Any, **_kwargs: Any) -> int:
    return 0


def upsert_short_volume_rows(*_args: Any, **_kwargs: Any) -> int:
    return 0


def run_feed_stocks_income_statements_job(*_args: Any, **_kwargs: Any) -> Dict[str, Any]:
    return dict(_RETIRED)


def run_feed_stocks_balance_sheets_job(*_args: Any, **_kwargs: Any) -> Dict[str, Any]:
    return dict(_RETIRED)


def run_feed_stocks_cash_flows_job(*_args: Any, **_kwargs: Any) -> Dict[str, Any]:
    return dict(_RETIRED)


def run_feed_stocks_ratios_job(*_args: Any, **_kwargs: Any) -> Dict[str, Any]:
    return dict(_RETIRED)


def run_feed_stocks_short_interest_job(*_args: Any, **_kwargs: Any) -> Dict[str, Any]:
    return dict(_RETIRED)


def run_feed_stocks_short_volume_job(*_args: Any, **_kwargs: Any) -> Dict[str, Any]:
    return dict(_RETIRED)
