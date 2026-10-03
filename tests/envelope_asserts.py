"""Assertions for the shared response envelopes (``bifrost_api.common.envelopes``, TD-16/17).

Since api 0.4.0 a failure is ``{"detail": ...}`` and nothing else, and a list is
``items`` / ``count`` without the route's old list key. The ``legacy`` / ``legacy_key``
arguments name what 0.2.2-0.3.4 also sent, so each test proves it is gone.
"""

from __future__ import annotations

from typing import Any, Dict, Optional


def assert_error(resp: Any, status: int, contains: str = "", legacy: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """A real status and ``detail`` to read -- no ``ok`` / ``error`` / old keys beside it."""
    assert resp.status_code == status, (resp.status_code, resp.text)
    body = resp.json()
    assert isinstance(body["detail"], str) and body["detail"]
    assert contains in body["detail"], body
    assert set(body) == {"detail"}, body
    for key in legacy or {}:
        assert key not in body, (key, body)
    return body


def assert_list(resp: Any, legacy_key: Optional[str] = None, expected: Optional[list] = None) -> Dict[str, Any]:
    """``items`` and ``count`` -- and no longer the route's old list key."""
    assert resp.status_code == 200, (resp.status_code, resp.text)
    body = resp.json()
    assert isinstance(body["items"], list)
    assert body["count"] == len(body["items"])
    if expected is not None:
        assert body["items"] == expected
    if legacy_key is not None:
        assert legacy_key not in body, body
    assert "ok" not in body, body
    return body
