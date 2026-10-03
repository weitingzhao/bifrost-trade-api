"""Every /health names the bifrost-core it runs: version and commit (TD-37).

One core version string has named several commits, so the delivery pipelines bake
the cloned core SHA into the image as BIFROST_CORE_SHA and /health reports it.
"""

from __future__ import annotations

from importlib.metadata import version

import pytest
from starlette.testclient import TestClient

from bifrost_api.common import build_info
from bifrost_api.docs_api.app import DOCS_PATH_PREFIX
from tests.test_deployment_profile_apps import _apps

SHA = "0123456789abcdef0123456789abcdef01234567"

# monitor serves the docs and ops routes in-process; each has its own health payload.
HEALTH_PATHS = [
    ("monitor", "/health"),
    ("monitor", "/ops/health"),
    ("monitor", f"{DOCS_PATH_PREFIX}/health"),
    ("account", "/health"),
    ("market", "/health"),
    ("research", "/health"),
]


@pytest.mark.parametrize(("app_name", "path"), HEALTH_PATHS)
def test_health_reports_core_version_and_sha(app_name: str, path: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BIFROST_CORE_SHA", SHA)
    client = TestClient(_apps("stg")[app_name], raise_server_exceptions=False)
    r = client.get(path)
    assert r.status_code == 200, (app_name, path, r.text)
    body = r.json()
    assert body["status"] == "ok"
    assert body["core_version"] == version("bifrost-core")
    assert body["core_sha"] == SHA


@pytest.mark.parametrize(("app_name", "path"), HEALTH_PATHS)
def test_core_sha_is_null_when_the_image_did_not_record_it(
    app_name: str, path: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("BIFROST_CORE_SHA", raising=False)
    body = TestClient(_apps("stg")[app_name], raise_server_exceptions=False).get(path).json()
    assert "core_sha" in body
    assert body["core_sha"] is None


@pytest.mark.parametrize("raw", ["", "   "])
def test_blank_sha_reads_as_unset(raw: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BIFROST_CORE_SHA", raw)
    assert build_info.core_sha() is None


def test_sha_is_stripped(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BIFROST_CORE_SHA", f" {SHA}\n")
    assert build_info.core_sha() == SHA


def test_missing_core_distribution_reads_as_none(monkeypatch: pytest.MonkeyPatch) -> None:
    def _absent(_name: str) -> str:
        raise build_info.PackageNotFoundError(_name)

    build_info.core_version.cache_clear()
    monkeypatch.setattr(build_info, "version", _absent)
    try:
        assert build_info.core_version() is None
    finally:
        build_info.core_version.cache_clear()
