"""API test fixtures."""

from __future__ import annotations

import pytest


@pytest.fixture
def sample_config():
    return {"postgres": {"host": "localhost", "dbname": "bifrost_dev"}}
