"""Unit tests for Docker Compose Ops executor (Phase 2C-A.1 WP1)."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest

from bifrost_api.ops.docker_compose_map import compose_service_for_systemd_unit
from bifrost_api.ops.services.executor_docker import DockerComposeExecutor


def test_compose_service_mapping():
    assert compose_service_for_systemd_unit("bifrost-engine.service") == "daemon"
    assert compose_service_for_systemd_unit("bifrost-ib-ingestor.service") == "ib-market-gateway"
    assert compose_service_for_systemd_unit("bifrost-ib-market-gateway.service") == "ib-market-gateway"
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
    assert result["compose_service"] == "ib-market-gateway"
    mock_run.assert_awaited_once_with(["start", "ib-market-gateway"], timeout=120)


@pytest.mark.asyncio
async def test_list_instances_from_pgrep(tmp_path):
    ex = DockerComposeExecutor(
        workdir=tmp_path,
        compose_files=["docker-compose.yml"],
        allowed_units=["bifrost-celery-worker@stocks_ib-1.service"],
        broker_url="redis://127.0.0.1:6379/1",
        docker_socket="/var/run/docker.sock",
    )
    pgrep_out = "42 python scripts/systemd/run_celery.py --instance stocks_ib-1\n"
    with patch.object(ex, "_pgrep_celery_lines", new_callable=AsyncMock) as mock_pgrep:
        mock_pgrep.return_value = pgrep_out.splitlines()
        rows = await ex.list_instances()
    assert len(rows) == 1
    assert rows[0]["unit"] == "bifrost-celery-worker@stocks_ib-1.service"


@pytest.mark.asyncio
async def test_ensure_celery_container_uses_start_not_up(tmp_path):
    ex = DockerComposeExecutor(
        workdir=tmp_path,
        compose_files=["docker-compose.yml"],
        allowed_units=["bifrost-celery-worker@stocks_ib-1.service"],
        broker_url="redis://127.0.0.1:6379/1",
        docker_socket="/var/run/docker.sock",
    )
    with patch.object(ex, "_service_state", new_callable=AsyncMock) as mock_state:
        with patch.object(ex, "_run_compose", new_callable=AsyncMock) as mock_run:
            mock_state.side_effect = ["exited", "running"]
            mock_run.return_value = (0, "", "")
            await ex._ensure_celery_container()
    mock_run.assert_awaited_once_with(["start", "celery-worker"])


@pytest.mark.asyncio
async def test_start_celery_instance_uses_compose_exec(tmp_path):
    ex = DockerComposeExecutor(
        workdir=tmp_path,
        compose_files=["docker-compose.yml"],
        allowed_units=["bifrost-celery-worker@stocks_massive-1.service"],
        broker_url="redis://127.0.0.1:6379/1",
        docker_socket="/var/run/docker.sock",
    )
    with patch.object(ex, "_ensure_celery_container", new_callable=AsyncMock):
        with patch.object(ex, "_compose_exec", new_callable=AsyncMock) as mock_exec:
            with patch.object(ex, "_pgrep_instance_active", new_callable=AsyncMock) as mock_active:
                mock_exec.return_value = (0, "", "")
                mock_active.return_value = True
                result = await ex._systemctl(
                    "start",
                    "bifrost-celery-worker@stocks_massive-1.service",
                )
    assert result["method"] == "docker-compose-exec"
    mock_exec.assert_awaited_once()
    args = mock_exec.await_args[0][0]
    assert args == ["python", "scripts/systemd/run_celery.py", "--instance", "stocks_massive-1"]


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
