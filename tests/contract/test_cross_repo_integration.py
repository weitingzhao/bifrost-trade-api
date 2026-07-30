"""Cross-repo integration: worker ↔ api lazy imports and shared beat schedule."""

from __future__ import annotations

from unittest.mock import MagicMock

from starlette.testclient import TestClient

from bifrost_api.massive.app import create_massive_app
from bifrost_api.massive.routers.routes import get_massive_celery_beat_schedule
from tests.contract.helpers import full_server_config


def test_worker_research_lazy_import_chain() -> None:
    """Massive Celery tasks defer bifrost_api.research imports until job execution."""
    from bifrost_api.research import iv_atm_rollup
    from bifrost_api.research.sepa import financials_data

    assert callable(financials_data.fetch_income_rows_for_sepa_from_pg)
    assert callable(iv_atm_rollup.upsert_report_atm_iv_daily_rows)


def test_massive_beat_schedule_api_matches_worker_source() -> None:
    from bifrost_worker.data.massive.beat_schedule_public import (
        MASSIVE_BEAT_SCHEDULE_SPEC,
        public_celery_beat_schedule_response,
    )

    api_body = get_massive_celery_beat_schedule()
    worker_body = public_celery_beat_schedule_response()
    assert api_body == worker_body
    assert api_body["ok"] is True
    assert len(api_body.get("entries") or []) == len(MASSIVE_BEAT_SCHEDULE_SPEC)


def test_ops_and_massive_beat_tasks_aligned() -> None:
    import bifrost_worker.data.bars.tasks  # noqa: F401
    import bifrost_worker.data.massive.tasks  # noqa: F401
    from bifrost_api.ops.services.celery_capabilities import build_celery_capabilities_payload
    from bifrost_worker.celery.celery_app import app as celery_app
    from bifrost_worker.data.massive.beat_schedule_public import (
        _MASSIVE_BEAT_SCHEDULE_SPEC_FULL,
        beat_tasks_payload_for_capabilities,
    )
    from bifrost_worker.data.massive.celery_queues import MASSIVE_QUEUES_DISABLED

    caps = build_celery_capabilities_payload(celery_app)
    assert caps["beat_tasks"] == beat_tasks_payload_for_capabilities()
    if MASSIVE_QUEUES_DISABLED:
        # P8: plugin CronJobs own EOD; Celery Massive beat is empty.
        assert caps["beat_tasks"] == []
        assert len(_MASSIVE_BEAT_SCHEDULE_SPEC_FULL) == 7
    else:
        assert len(caps["beat_tasks"]) == 7


def test_massive_http_beat_schedule_endpoint() -> None:
    from bifrost_worker.data.massive.celery_queues import MASSIVE_QUEUES_DISABLED

    reader = MagicMock()
    reader._config = full_server_config()
    app = create_massive_app(reader=reader, control_via_db=None)
    client = TestClient(app, raise_server_exceptions=False)
    r = client.get("/research/massive/celery-beat-schedule")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    if MASSIVE_QUEUES_DISABLED:
        assert body.get("massive_queues_disabled") is True
        assert len(body.get("entries") or []) == 0
    else:
        assert len(body.get("entries") or []) == 7
