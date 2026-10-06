"""GET /status: daemon_alive follows the heartbeat interval (TD-76, core 0.35.0).

The window was a fixed 35 s while the daemon's heartbeat interval is configurable 5-120 s;
now it is max(35 s, 3 x interval). Fixtures are invented.
"""

from __future__ import annotations

import time
from typing import Any, Dict, Optional
from unittest.mock import patch

import pytest
from starlette.testclient import TestClient

import bifrost_api.monitor.routers.status as status_mod
from bifrost_api.monitor.app import create_app
from tests.contract.helpers import full_server_config
from tests.reader_mock import reader_mock


def _daemon_heartbeat(age_s: float, interval: Optional[float]) -> Dict[str, Any]:
    reader = reader_mock()
    reader.config = full_server_config()
    reader.get_daemon_heartbeat.return_value = {
        "last_ts": time.time() - age_s,
        "heartbeat_interval_sec": interval,
        "ib_connected": True,
    }
    app = create_app(reader=reader, control_via_db=None, data_lag_threshold_ms=5000, merged_config=reader.config)
    seen: Dict[str, Any] = {}

    def capture(**kw: Any) -> Dict[str, Any]:
        seen.update(kw)
        return {"status": "ok", "status_schema_version": 9}

    status_mod._status_cache.clear()
    with patch.object(status_mod, "_assemble_status_v3", side_effect=capture):
        r = TestClient(app, raise_server_exceptions=False).get("/status")
    status_mod._status_cache.clear()
    assert r.status_code == 200
    return seen["daemon_heartbeat"]


@pytest.mark.parametrize(
    "age,interval,alive",
    [
        (20.0, 10, True),
        (40.0, 10, False),  # default interval: the old 35 s window
        (40.0, None, False),
        (40.0, 30, True),  # 30 s beats: alive until 90 s (was "dead" at 35 s)
        (95.0, 30, False),
        (300.0, 120, True),
    ],
)
def test_daemon_alive_window_scales_with_the_interval(age: float, interval: Any, alive: bool) -> None:
    hb = _daemon_heartbeat(age, interval)
    assert hb["daemon_alive"] is alive
    assert hb["heartbeat_interval_sec"] == interval
