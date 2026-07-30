"""API test fixtures."""

from __future__ import annotations

from pathlib import Path

import pytest

_CORE_CONFIG_EXAMPLE = (
    Path(__file__).resolve().parents[2] / "bifrost-trade-core" / "config" / "config.yaml.example"
)


@pytest.fixture(autouse=True)
def bifrost_config_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """Point read_config() at bifrost-trade-core example YAML (api repo has no config/)."""
    if _CORE_CONFIG_EXAMPLE.is_file():
        monkeypatch.setenv("BIFROST_CONFIG", str(_CORE_CONFIG_EXAMPLE))


@pytest.fixture(autouse=True)
def _clear_prometheus_registry() -> None:
    """Allow multiple FastAPI apps per process (instrument_app registers global gauges)."""
    try:
        from prometheus_client import REGISTRY
    except ImportError:
        return
    for collector in list(REGISTRY._collector_to_names.keys()):
        try:
            REGISTRY.unregister(collector)
        except Exception:
            pass


@pytest.fixture
def sample_config():
    return {"postgres": {"host": "localhost", "dbname": "bifrost_dev"}}
