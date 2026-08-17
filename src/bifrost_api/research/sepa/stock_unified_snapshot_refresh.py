"""Refresh cache_stock_snapshot — retired (Massive API no longer available).

The ``run_refresh_cache_stock_unified_snapshots`` function is kept as a thin
retired stub so callers (``data_readiness.py``) continue to import without error.
"""

from __future__ import annotations

from typing import Any, Dict


def run_refresh_cache_stock_unified_snapshots(
    status_config: dict,
    merged_config: dict,
    **_kwargs: Any,
) -> Dict[str, Any]:
    """Retired: Massive unified snapshot refresh — use market-data plugin."""
    _ = (status_config, merged_config)
    return {
        "ok": False,
        "error": "Massive unified snapshot refresh retired — use market-data plugin",
        "reason": "massive_retired",
    }
