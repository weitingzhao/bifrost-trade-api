"""The shared failure and list envelopes (TD-16, TD-17; Owner decision B, legacy keys gone in 0.4.0)."""

from __future__ import annotations

import json

import pytest

from bifrost_api.common.envelopes import error_body, error_response, list_body


def test_error_body_is_detail_only() -> None:
    assert error_body("name is required.") == {"detail": "name is required."}


def test_error_response_carries_the_status() -> None:
    r = error_response(409, "in use")
    assert r.status_code == 409
    assert json.loads(r.body) == {"detail": "in use"}


def test_error_response_refuses_a_success_status() -> None:
    with pytest.raises(ValueError):
        error_response(200, "not an error")


def test_error_response_takes_no_legacy_keys() -> None:
    with pytest.raises(TypeError):
        error_response(400, "bad", {"id": None})  # type: ignore[call-arg]


def test_list_body_counts() -> None:
    rows = [{"id": 1}, {"id": 2}]
    assert list_body(rows) == {"items": rows, "count": 2}


def test_list_body_total_extra_and_none() -> None:
    body = list_body(None, total=40, slippage_total=None, count=99)
    assert body == {"items": [], "count": 0, "total": 40, "slippage_total": None}
