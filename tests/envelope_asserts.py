"""Assertions for the shared response envelopes (``bifrost_api.common.envelopes``, TD-16/17)."""

from __future__ import annotations

from typing import Any, Dict, Optional


def assert_error(resp: Any, status: int, contains: str = "", legacy: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """A real status, ``detail`` to read, and this release's ``ok: false`` / ``error`` beside it."""
    assert resp.status_code == status, (resp.status_code, resp.text)
    body = resp.json()
    assert isinstance(body["detail"], str) and body["detail"]
    assert contains in body["detail"], body
    assert body["ok"] is False
    assert body["error"] == body["detail"]
    for key, value in (legacy or {}).items():
        assert body[key] == value, (key, body)
    return body


def assert_list(resp: Any, legacy_key: Optional[str] = None, expected: Optional[list] = None) -> Dict[str, Any]:
    """``items`` and ``count``, and the route's old list key carrying the same list."""
    assert resp.status_code == 200, (resp.status_code, resp.text)
    body = resp.json()
    assert isinstance(body["items"], list)
    assert body["count"] == len(body["items"])
    if expected is not None:
        assert body["items"] == expected
    if legacy_key is not None:
        assert body[legacy_key] == body["items"]
    return body
