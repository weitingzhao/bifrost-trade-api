"""Gate sets: the defaults a new one starts from (TD-72) and where earnings dates go.

Nothing here reaches a database: the defaults are core's `GateParams`, and the
writer refuses nested dates before it opens a connection.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from starlette.testclient import TestClient

from bifrost_api.account.app import create_account_app
from bifrost_core.monitor.schemas.gate_params import default_gates
from tests.contract.helpers import operator_server_config
from tests.route_listing import route_paths


def _client(control_via_db: Any = None) -> TestClient:
    reader = MagicMock()
    reader.config = operator_server_config()
    app = create_account_app(
        reader=reader,
        control_via_db=control_via_db,
        status_cfg_for_read=control_via_db,
        merged_config=reader.config,
    )
    return TestClient(app, raise_server_exceptions=False)


def test_defaults_are_cores_gates() -> None:
    r = _client().get("/gate-sets/defaults")
    assert r.status_code == 200
    body = r.json()
    assert body == {"gates": default_gates()}
    assert {"strategy", "state", "intent", "guard"} <= set(body["gates"])


def test_defaults_carry_no_earnings_dates() -> None:
    gates = _client().get("/gate-sets/defaults").json()["gates"]
    assert "dates" not in (gates["strategy"].get("earnings") or {})


def test_defaults_is_not_read_as_an_id() -> None:
    """Declared before /gate-sets/{gate_safety_strategy_id:int}; else "defaults" would be a 422."""
    paths = route_paths(_client().app)
    assert paths.index("/gate-sets/defaults") < paths.index(
        "/gate-sets/{gate_safety_strategy_id:int}"
    )


def test_a_viewer_reads_the_defaults() -> None:
    reader = MagicMock()
    reader.config = {**operator_server_config(), "ops": {"auth": {"default_role": "viewer"}}}
    app = create_account_app(reader=reader, control_via_db=None, merged_config=reader.config)
    assert TestClient(app).get("/gate-sets/defaults").status_code == 200


@pytest.mark.parametrize("method, path", [("POST", "/gate-sets"), ("PUT", "/gate-sets/5")])
def test_nested_earnings_dates_are_400(method: str, path: str) -> None:
    gates = default_gates()
    gates["strategy"]["earnings"] = {**(gates["strategy"].get("earnings") or {}), "dates": ["2031-01-15"]}
    r = _client({"sink": "postgres"}).request(method, path, json={"name": "Invented set", "gates": gates})
    assert r.status_code == 400
    assert "earnings_dates" in r.json()["detail"]
