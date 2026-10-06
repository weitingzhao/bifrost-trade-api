"""A monitor whose Ops or docs wiring fails does not start (debt TD-27)."""

from __future__ import annotations


import pytest

import bifrost_api.docs_api.app as docs_app
import bifrost_api.ops.app as ops_app
from bifrost_api.monitor.app import create_app
from tests.contract.helpers import full_server_config
from tests.reader_mock import reader_mock


def _build() -> None:
    reader = reader_mock()
    reader.config = full_server_config()
    create_app(
        reader=reader,
        control_via_db={"sink": "postgres"},
        data_lag_threshold_ms=1000,
        merged_config=reader.config,
    )


def test_wires_cleanly_with_a_full_config() -> None:
    _build()


@pytest.mark.parametrize(
    "module, name, message",
    [
        (ops_app, "wire_ops_control_plane", "Ops control plane"),
        (docs_app, "attach_docs_routes", "docs routes"),
    ],
)
def test_a_wiring_failure_stops_the_start(monkeypatch: pytest.MonkeyPatch, module, name, message) -> None:
    def broken(*_a, **_k):
        raise ValueError("missing config")

    monkeypatch.setattr(module, name, broken)
    with pytest.raises(RuntimeError, match=message):
        _build()
