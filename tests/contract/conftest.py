"""Shared fixtures for API contract / parity tests."""

from __future__ import annotations


import pytest

from tests.contract.helpers import _FULL_SERVER
from tests.reader_mock import reader_mock


@pytest.fixture
def mock_reader():
    reader = reader_mock()
    reader.config = {"server": dict(_FULL_SERVER)}
    return reader


@pytest.fixture
def full_server():
    return dict(_FULL_SERVER)
