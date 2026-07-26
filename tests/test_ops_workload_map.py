"""Unit tests for systemd-unit → K8s Deployment mapping."""

from __future__ import annotations

from bifrost_api.ops.workload_map import deployment_for_unit, is_managed_unit


def test_deployment_for_unit_maps_engine_to_daemon():
    assert deployment_for_unit("bifrost-engine") == "daemon"
    assert deployment_for_unit("bifrost-engine.service") == "daemon"


def test_deployment_for_unit_maps_celery_template_instance():
    assert deployment_for_unit("bifrost-celery-worker@stocks_massive-1.service") == "celery-worker"


def test_deployment_for_unit_unknown():
    assert deployment_for_unit("not-a-unit") is None
    assert deployment_for_unit("") is None


def test_is_managed_unit():
    assert is_managed_unit("bifrost-account-sync-daemon.service") is True
    assert is_managed_unit("redis.service") is True
    assert is_managed_unit("random") is False
