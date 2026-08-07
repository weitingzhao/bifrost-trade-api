"""Cross-repo integration: research helpers and ops Celery capabilities (Wave 7-C)."""

from __future__ import annotations

from bifrost_api.ops.services.celery_capabilities import build_celery_capabilities_payload
from bifrost_worker.celery.celery_app import app as celery_app


def test_worker_research_lazy_import_chain() -> None:
    """Research helpers remain importable without deleted massive package."""
    from bifrost_api.research import iv_atm_rollup
    from bifrost_api.research.sepa import financials_data

    assert callable(financials_data.fetch_income_rows_for_sepa_from_pg)
    assert callable(iv_atm_rollup.upsert_report_atm_iv_daily_rows)
    assert iv_atm_rollup.upsert_report_atm_iv_daily_rows(None, "X", "20250101", "massive", []) == 0


def test_ops_celery_capabilities_without_massive() -> None:
    import bifrost_worker.data.bars.tasks  # noqa: F401

    caps = build_celery_capabilities_payload(celery_app)
    assert caps["ok"] is True
    assert caps.get("run_massive_job_matrix") == []
    assert caps.get("beat_tasks") == []
    assert caps.get("massive_retired") is True
    names = {t["name"] for t in caps.get("registered_tasks") or []}
    assert "src.bars.tasks.backfill_bars" in names
