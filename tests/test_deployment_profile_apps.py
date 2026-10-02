"""Every deployed app names its environment the same way (debt TD-52).

In K3s each pod loaded a file named config.stg.yaml with BIFROST_ENV=stg, PROD
included: account, market and research reported no profile, and monitor got one
only through the ops wiring's fallback.
"""

from __future__ import annotations

from typing import Any, Dict
from unittest.mock import MagicMock

import pytest
from starlette.testclient import TestClient

from bifrost_api.account.app import create_account_app
from bifrost_api.market.app import create_market_app
from bifrost_api.monitor.app import create_app as create_monitor_app
from bifrost_api.research.app import create_research_app
from tests.contract.helpers import full_server_config

MOUNTED = "/app/config/runtime.yaml"


def _config(env: str) -> Dict[str, Any]:
    return {**full_server_config(), "redis": {"enabled": False}, "ops": {"control_profile": env}}


def _apps(env: str) -> Dict[str, Any]:
    def reader() -> MagicMock:
        r = MagicMock()
        r._config = _config(env)
        return r

    m, a, k, s = reader(), reader(), reader(), reader()
    return {
        "monitor": create_monitor_app(
            reader=m,
            control_via_db=None,
            data_lag_threshold_ms=5000,
            resolved_config_path=MOUNTED,
            merged_config=m._config,
        ),
        "account": create_account_app(reader=a, control_via_db=None, resolved_config_path=MOUNTED, merged_config=a._config),
        "market": create_market_app(reader=k, control_via_db=None, resolved_config_path=MOUNTED, merged_config=k._config),
        "research": create_research_app(reader=s, control_via_db=None, resolved_config_path=MOUNTED, merged_config=s._config),
    }


@pytest.mark.parametrize("env", ["dev", "stg", "prod"])
def test_every_app_reports_its_env(env: str, monkeypatch: pytest.MonkeyPatch) -> None:
    # A stale BIFROST_ENV (every K3s pod carried stg) must not win over the overlay's profile.
    monkeypatch.setenv("BIFROST_ENV", "stg")
    for name, app in _apps(env).items():
        assert app.state.bifrost_config_profile == env, name


@pytest.mark.parametrize("name", ["account", "market", "research"])
def test_health_says_so(name: str) -> None:
    client = TestClient(_apps("prod")[name], raise_server_exceptions=False)
    assert client.get("/health").json().get("config_profile") == "prod"
