"""The shared failure and list envelopes (TD-16, TD-17; Owner decision B)."""

from __future__ import annotations

import json

import pytest

from bifrost_api.common.envelopes import error_body, error_response, list_body


def test_error_body_reads_detail_and_keeps_the_legacy_keys() -> None:
    assert error_body("name is required.", {"id": None}) == {
        "detail": "name is required.",
        "ok": False,
        "error": "name is required.",
        "id": None,
    }


def test_legacy_keys_cannot_override_the_contract() -> None:
    body = error_body("boom", {"ok": True, "detail": "other", "error": "other", "count": 0})
    assert body == {"detail": "boom", "ok": False, "error": "boom", "count": 0}


def test_error_response_carries_the_status() -> None:
    r = error_response(409, "in use")
    assert r.status_code == 409
    assert json.loads(r.body) == {"detail": "in use", "ok": False, "error": "in use"}


def test_error_response_refuses_a_success_status() -> None:
    with pytest.raises(ValueError):
        error_response(200, "not an error")


def test_list_body_counts_and_mirrors_the_legacy_key() -> None:
    rows = [{"id": 1}, {"id": 2}]
    assert list_body(rows, "executions") == {"items": rows, "count": 2, "executions": rows}


def test_list_body_total_extra_and_none() -> None:
    body = list_body(None, ("a", "b"), total=40, slippage_total=None, count=99)
    assert body == {"items": [], "count": 0, "total": 40, "a": [], "b": [], "slippage_total": None}
