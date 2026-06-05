"""Shared fixtures for API contract / parity tests."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from starlette.testclient import TestClient

from tests.contract.helpers import _FULL_SERVER, full_server_config


@pytest.fixture
def mock_reader():
    reader = MagicMock()
    reader._config = {"server": dict(_FULL_SERVER)}
    return reader


@pytest.fixture
def full_server():
    return dict(_FULL_SERVER)
