"""Aggregate Celery self-description: task routes, canonical broker queues, ``run_massive_job`` matrix."""

from __future__ import annotations

import logging
from typing import Any, Dict, List

from bifrost_api.ops.services.celery_supported_tasks import build_supported_tasks_payload
from bifrost_worker.data.massive.beat_schedule_public import beat_tasks_payload_for_capabilities
from bifrost_worker.data.massive.run_massive_job_manifest import RUN_MASSIVE_JOB_MATRIX
from bifrost_worker.celery.celery_queue_names import (
    build_broker_queue_labels,
    load_canonical_broker_queue_names,
    ops_celery_config_validation_errors,
)

logger = logging.getLogger(__name__)


def _config_for_capabilities() -> dict:
    try:
        from bifrost_core.config.startup import read_config

        cfg, _ = read_config()
        return cfg if isinstance(cfg, dict) else {}
    except Exception as e:
        logger.debug("read_config for celery capabilities: %s", e)
        return {}


def build_celery_capabilities_payload(celery_app: Any) -> Dict[str, Any]:
    """Return registered tasks + canonical queue names + ``run_massive_job`` kind/mode matrix + beat tasks."""
    cfg = _config_for_capabilities()
    for msg in ops_celery_config_validation_errors(cfg):
        logger.warning("ops celery config: %s", msg)

    base = build_supported_tasks_payload(celery_app)
    registered: List[Dict[str, str]] = []
    for t in base.get("tasks") or []:
        if not isinstance(t, dict):
            continue
        name = str(t.get("name") or "")
        dq = str(t.get("default_queue") or t.get("task_route_default_queue") or "")
        registered.append(
            {
                "name": name,
                "default_queue": dq,
                "task_route_default_queue": dq,
            }
        )

    beat_tasks = beat_tasks_payload_for_capabilities()
    matrix = [r.to_api_dict() for r in RUN_MASSIVE_JOB_MATRIX]
    broker_queue_labels = build_broker_queue_labels(cfg)

    out: Dict[str, Any] = {
        "ok": bool(base.get("ok")),
        "registered_tasks": registered,
        "count": len(registered),
        "canonical_broker_queues": list(load_canonical_broker_queue_names(cfg)),
        "run_massive_job_matrix": matrix,
        "beat_tasks": beat_tasks,
        "broker_queue_labels": broker_queue_labels,
    }
    return out
