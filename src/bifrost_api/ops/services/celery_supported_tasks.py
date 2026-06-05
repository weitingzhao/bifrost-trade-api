"""Enumerate Celery task names registered on the Ops app (same app as workers)."""

from __future__ import annotations

import importlib
from typing import Any, Dict, List


def build_supported_tasks_payload(celery_app: Any) -> Dict[str, Any]:
    """Return sorted project tasks (``src.*``) with default queue from ``task_routes`` / ``task_default_queue``.

    Ensures bars/massive task modules are imported so ``app.tasks`` is populated (task names stay ``src.*``).
    """
    importlib.import_module("bifrost_worker.data.bars.tasks")
    importlib.import_module("bifrost_worker.data.massive.tasks")

    default_q = str(getattr(celery_app.conf, "task_default_queue", None) or "stocks_ib")
    routes = getattr(celery_app.conf, "task_routes", None) or {}
    if not isinstance(routes, dict):
        routes = {}

    tasks_out: List[Dict[str, str]] = []
    for name in sorted(celery_app.tasks.keys()):
        if name.startswith("celery."):
            continue
        if not name.startswith("src."):
            continue
        q = default_q
        spec = routes.get(name)
        if isinstance(spec, dict) and spec.get("queue"):
            q = str(spec["queue"])
        tasks_out.append(
            {
                "name": name,
                "default_queue": q,
                "task_route_default_queue": q,
            }
        )

    return {"ok": True, "tasks": tasks_out, "count": len(tasks_out)}
