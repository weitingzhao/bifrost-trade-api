"""Orphan lease clearing must not drop control fields while the process is still running."""

from __future__ import annotations

from bifrost_api.ops.routers.market_ingest import _process_counts_as_running


def test_process_counts_as_running_active_states() -> None:
    assert _process_counts_as_running("active") is True
    assert _process_counts_as_running("activating") is True
    assert _process_counts_as_running("Active") is True


def test_process_counts_as_running_stopped_states() -> None:
    assert _process_counts_as_running("inactive") is False
    assert _process_counts_as_running("failed") is False
    assert _process_counts_as_running("dead") is False
    assert _process_counts_as_running("unknown") is False
    assert _process_counts_as_running("") is False
