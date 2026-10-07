"""api-monitor exports its request metrics (core 0.55.2).

The monitor process instruments the docs app (only to copy its routes) before itself. Under
core 0.55.0 / 0.55.1 that first call held every request series, so DEV / STG / PROD
api-monitor exported no ``http_requests_total`` and an empty latency histogram, and
``BifrostAPIWithoutHttpMetrics`` fired. Built the way ``run_server`` builds it.
"""

from __future__ import annotations

import re

from fastapi.testclient import TestClient

from bifrost_api.monitor.app import create_app
from tests.contract.helpers import full_server_config
from tests.reader_mock import reader_mock


def _sample(body: str, name: str, **labels: str) -> float:
    for line in body.splitlines():
        m = re.fullmatch(rf"{name}(?:\{{(.*)\}})? (\S+)", line)
        if not m:
            continue
        got = dict(re.findall(r'(\w+)="([^"]*)"', m.group(1) or ""))
        if got == labels:
            return float(m.group(2))
    return 0.0


def test_monitor_records_its_requests() -> None:
    reader = reader_mock()
    reader.config = full_server_config()
    app = create_app(
        reader=reader,
        control_via_db={"sink": "postgres"},
        data_lag_threshold_ms=1000,
        merged_config=reader.config,
    )
    client = TestClient(app)
    for _ in range(2):
        assert client.get("/health").status_code == 200
    assert client.get("/status").status_code == 200

    body = client.get("/metrics").text
    total = "http_requests_total"
    assert _sample(body, total, handler="/health", method="GET", status="2xx") == 2
    assert _sample(body, total, handler="/status", method="GET", status="2xx") == 1
    assert _sample(body, "http_request_duration_seconds_count", handler="/status", method="GET") == 1
    # /health is counted, not timed (TD-194): only /status reaches the latency histograms.
    assert _sample(body, "http_request_duration_highr_seconds_count") == 1
    assert _sample(body, total, handler="/metrics", method="GET", status="2xx") == 0
