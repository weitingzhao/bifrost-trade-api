"""Response envelopes every Trade API route shares (TD-16, TD-17; Owner decision B).

Two shapes, so a client reads every route the same way:

- **Failure**: a real HTTP status and ``{"detail": <message>, "ok": false,
  "error": <message>, ...legacy keys}``. ``detail`` is the field clients read
  (it is also what FastAPI's own ``HTTPException`` and validation errors send).
  Statuses: 400 invalid input, 404 missing, 409 conflict / in use, 503 a
  dependency is not configured or not reachable (PostgreSQL, IB Gateway),
  500 an unexpected failure (logged here).
- **List**: ``{"items": [...], "count": len(items), "total": <optional>,
  ...legacy list key(s)}``. A successful single object keeps its own shape.

Decision B is additive first: ``ok`` / ``error`` and each route's old list key
(``executions``, ``attributions``, ``transactions`` …) are still sent so a browser
tab opened before the release keeps working. **They go in the next release**;
after that a failure is ``{"detail": ...}`` and a list is ``items`` / ``count``.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence, Union

from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)

_ERROR_KEYS = frozenset({"detail", "ok", "error"})
_LIST_KEYS = frozenset({"items", "count", "total"})


def error_body(message: str, legacy: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    """The failure body: ``detail`` plus, for one release, ``ok: false`` / ``error`` and ``legacy``."""
    body: Dict[str, Any] = {"detail": message, "ok": False, "error": message}
    for key, value in (legacy or {}).items():
        if key not in _ERROR_KEYS:
            body[key] = value
    return body


def error_response(
    status_code: int,
    message: str,
    legacy: Optional[Mapping[str, Any]] = None,
) -> JSONResponse:
    """Answer a failure with a real status and :func:`error_body`.

    ``legacy`` carries the other keys the route used to send with ``ok: false``
    (``id: null``, ``count: 0``, ``executions: []`` …); they cannot override
    ``detail`` / ``ok`` / ``error``. A 500 is logged: it is the one class the
    caller cannot act on.
    """
    if status_code < 400:
        raise ValueError(f"error_response needs a 4xx/5xx status, got {status_code}")
    if status_code == 500:
        logger.warning("500 answered: %s", message)
    return JSONResponse(status_code=status_code, content=error_body(message, legacy))


def list_body(
    items: Optional[Sequence[Any]],
    legacy_keys: Union[str, Iterable[str], None] = None,
    total: Optional[int] = None,
    **extra: Any,
) -> Dict[str, Any]:
    """The list shape: ``items``, ``count``, ``total`` when known, then the route's old key(s).

    ``legacy_keys`` names the key(s) the route used to send the same list under;
    each gets the same list for one release. ``extra`` keeps a route's other
    fields (``strategy_instance_id``, ``slippage_total`` …); none of them can
    override ``items`` / ``count`` / ``total``.
    """
    rows = list(items or [])
    body: Dict[str, Any] = {"items": rows, "count": len(rows)}
    if total is not None:
        body["total"] = int(total)
    names = (legacy_keys,) if isinstance(legacy_keys, str) else tuple(legacy_keys or ())
    for name in names:
        if name not in _LIST_KEYS:
            body[name] = rows
    for key, value in extra.items():
        if key not in _LIST_KEYS:
            body[key] = value
    return body
