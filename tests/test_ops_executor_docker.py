"""Unit tests for Docker Compose Ops executor (Phase 2C-A.1 WP1)."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest

from bifrost_api.ops.docker_compose_map import compose_service_for_systemd_unit
from bifrost_api.ops.services.executor_docker import DockerComposeExecutor


def test_compose_service_mapping():
    assert compose_service_for_systemd_unit("bifrost-engine.service") == "daemon"
    assert compose_service_for_systemd_unit("bifrost-ib-ingestor.service") == "ib-ingestor"
    assert compose_service_for_systemd_unit("bifrost-celery-worker@ib-1.service") == "celery-worker"
    assert compose_service_for_systemd_unit("unknown-unit.service") is None


@pytest.mark.asyncio
async def test_systemctl_is_active_running(tmp_path):
    ex = DockerComposeExecutor(
        workdir=tmp_path,
        compose_files=["docker-compose.yml"],
        allowed_units=["bifrost-ib-ingestor"],
        broker_url="redis://127.0.0.1:6379/1",
        docker_socket="/var/run/docker.sock",
    )
    payload = json.dumps({"State": "running"})
    with patch.object(ex, "_run_compose", new_callable=AsyncMock) as mock_run:
        mock_run.return_value = (0, payload, "")
        state = await ex.systemctl_is_active("bifrost-ib-ingestor.service")
    assert state == "active"
    mock_run.assert_awaited_once()


@pytest.mark.asyncio
async def test_systemctl_is_active_exited(tmp_path):
    ex = DockerComposeExecutor(
        workdir=tmp_path,
        compose_files=["docker-compose.yml"],
        allowed_units=["bifrost-massive-ws"],
        broker_url="redis://127.0.0.1:6379/1",
        docker_socket="/var/run/docker.sock",
    )
    payload = json.dumps({"State": "exited"})
    with patch.object(ex, "_run_compose", new_callable=AsyncMock) as mock_run:
        mock_run.return_value = (0, payload, "")
        state = await ex.systemctl_is_active("bifrost-massive-ws.service")
    assert state == "inactive"


@pytest.mark.asyncio
async def test_systemctl_start_invokes_compose(tmp_path):
    ex = DockerComposeExecutor(
        workdir=tmp_path,
        compose_files=["docker-compose.yml"],
        allowed_units=["bifrost-ib-ingestor"],
        broker_url="redis://127.0.0.1:6379/1",
        docker_socket="/var/run/docker.sock",
    )
    with patch.object(ex, "_run_compose", new_callable=AsyncMock) as mock_run:
        mock_run.return_value = (0, "", "")
        result = await ex._systemctl("start", "bifrost-ib-ingestor.service")
    assert result["method"] == "docker-compose"
    assert result["compose_service"] == "ib-ingestor"
    mock_run.assert_awaited_once_with(["start", "ib-ingestor"], timeout=120)


@pytest.mark.asyncio
async def test_unknown_unit_not_mapped(tmp_path):
    ex = DockerComposeExecutor(
        workdir=tmp_path,
        compose_files=["docker-compose.yml"],
        allowed_units=["bifrost-ib-ingestor"],
        broker_url="redis://127.0.0.1:6379/1",
        docker_socket="/var/run/docker.sock",
    )
    state = await ex.systemctl_is_active("not-a-real-unit.service")
    assert state == "unknown"
