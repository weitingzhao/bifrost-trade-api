"""Infer dev/prod on Celery workers when Redis presence omits config_profile."""

from __future__ import annotations

from bifrost_api.ops.services.worker_state import WorkerStateService


def _svc(hostnames: dict) -> WorkerStateService:
    return WorkerStateService(
        celery_app=object(),
        broker_url="redis://127.0.0.1:6379/0",
        config={
            "ops": {
                "celery": {
                    "prod_worker_hostnames": hostnames.get("prod", []),
                    "dev_worker_hostnames": hostnames.get("dev", []),
                }
            }
        },
    )


def test_infer_prod_from_configured_hostname() -> None:
    svc = _svc({"prod": ["server-app-ubt"]})
    assert (
        svc._infer_worker_config_profile("workeroptions_massive-416@server-app-ubt")
        == "prod"
    )


def test_infer_dev_from_docker_hex_hostname() -> None:
    svc = _svc({"prod": ["server-app-ubt"]})
    assert svc._infer_worker_config_profile("celery@006224fe530e") == "dev"


def test_apply_inferred_profiles_on_worker_list() -> None:
    from bifrost_api.ops.models.schemas import WorkerStatus, WorkerSummary

    svc = _svc({"prod": ["server-app-ubt"]})
    workers = [
        WorkerSummary(
            worker_id="workerstocks_massive-1@server-app-ubt",
            status=WorkerStatus.RUNNING_HEALTHY,
            queues=["stocks_massive"],
            concurrency=1,
            active_tasks=0,
            reserved_tasks=0,
            last_heartbeat=0.0,
        )
    ]
    out = svc._apply_inferred_worker_profiles(workers)
    assert out[0].worker_config_profile == "prod"
