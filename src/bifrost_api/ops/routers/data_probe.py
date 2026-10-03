"""`GET /data-probe` -- what the Ops platform may know about this env's Trade database (D8-A).

The platform's freshness panel and data clone used to name Trade tables (``strategy_instance``
and others) in their own SQL. Trade renames those tables (naming program R3) and the
platform must not learn Trade concepts (D13), so Trade answers by role instead
(core ``monitor.reader.data_probe``, 0.42.0)::

    {
      "generated_at": "2031-03-04T14:30:00Z",
      "activity": [{"source": "trades", "last_ts": "…Z" | null, "detail"?: "missing"}, …],
      "sample": {"label": "trades", "rows": 89},
      "clone_groups": [{"name": "trades", "tables": ["strategy_instance", …], "note": "…"}, …],
      "watchlist": {"label": "optionable_stocks", "symbols": ["AAPL", …], "count": 42}
    }

``clone_groups[].tables`` is each group's seeds plus every table referencing them,
transitively (what ``TRUNCATE … CASCADE`` on the seeds would empty), read from
``pg_constraint`` per request. A read that cannot happen is 503 with the reason --
the platform shows ``unknown`` and never falls back to querying tables itself (R2).

``watchlist`` (core 0.44.0) is this env's optionable stocks (``sec_type = 'STK'``,
``optionable``), upper-cased, distinct and sorted; the platform unions them across envs for
``GET /api/v1/watchlist/union`` instead of selecting from ``public.watchlist`` (Owner
2026-10-03, option A). A missing table is ``symbols: null`` with a ``detail``. The route has
no response model: the reader's dict is the response, so a new reader key reaches the platform.

Served at ``/data-probe`` (``GET /api/ops/data-probe`` once the gateway routes that path)
and at ``/ops/data-probe``, which the gateway's existing ``/api/ops/ops/`` rule already
reaches (``GET /api/ops/ops/data-probe``). Read-only; no role is needed.
"""

from __future__ import annotations

import logging
from typing import Any, Dict

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from bifrost_core.monitor.reader.errors import ReadFailed

logger = logging.getLogger(__name__)

router = APIRouter(tags=["ops-data-probe"])


@router.get("/data-probe")
@router.get("/ops/data-probe")
def data_probe(request: Request) -> Any:
    """Activity per source, a sample count and the selective-clone groups of this env's database."""
    reader = getattr(request.app.state, "reader", None)
    if reader is None or not hasattr(reader, "get_data_probe"):
        return JSONResponse(status_code=503, content={"detail": "No Trade database reader on this service.", "reason": "no_reader"})
    try:
        out: Dict[str, Any] = reader.get_data_probe()
    except ReadFailed as e:
        logger.warning("data probe failed: %s", e)
        return JSONResponse(status_code=503, content={"detail": str(e), "reason": "read_failed"})
    return out
