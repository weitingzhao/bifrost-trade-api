"""Response envelopes every Trade API route shares (TD-16, TD-17; Owner decision B).

Two shapes, so a client reads every route the same way:

- **Failure**: a real HTTP status and ``{"detail": <message>}`` -- the field FastAPI's
  own ``HTTPException`` and validation errors send. Statuses: 400 invalid input, 404
  missing, 409 conflict / in use, 503 a dependency is not configured or not reachable
  (PostgreSQL, IB Gateway), 500 an unexpected failure (logged here).
- **List**: ``{"items": [...], "count": len(items), "total": <optional>}`` plus a
  route's own fields. A successful single object keeps its own shape.

Decision B went in two steps. api 0.2.2-0.3.4 also sent ``ok: false`` / ``error`` and
each route's old list key (``executions``, ``attributions``, ``links``,
``transactions``) while the readers moved to ``detail`` / ``items`` (frontend
678e0d2e, Research MCP 0.156.1). api 0.4.0 drops them.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Sequence

from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)

_LIST_KEYS = frozenset({"items", "count", "total"})


def error_body(message: str) -> Dict[str, Any]:
    """The failure body: ``{"detail": message}``."""
    return {"detail": message}


def error_response(status_code: int, message: str) -> JSONResponse:
    """Answer a failure with a real status and :func:`error_body`. A 500 is logged:
    it is the one class the caller cannot act on."""
    if status_code < 400:
        raise ValueError(f"error_response needs a 4xx/5xx status, got {status_code}")
    if status_code == 500:
        logger.warning("500 answered: %s", message)
    return JSONResponse(status_code=status_code, content=error_body(message))


def list_body(
    items: Optional[Sequence[Any]],
    total: Optional[int] = None,
    **extra: Any,
) -> Dict[str, Any]:
    """The list shape: ``items``, ``count``, ``total`` when known, then the route's own fields.

    ``extra`` keeps a route's other fields (``strategy_instance_id``, ``slippage_total`` …);
    none of them can override ``items`` / ``count`` / ``total``.
    """
    rows = list(items or [])
    body: Dict[str, Any] = {"items": rows, "count": len(rows)}
    if total is not None:
        body["total"] = int(total)
    for key, value in extra.items():
        if key not in _LIST_KEYS:
            body[key] = value
    return body
