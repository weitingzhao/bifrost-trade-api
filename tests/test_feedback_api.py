"""Feedback API (K5) — contract rules with the store's connection patched out."""

from __future__ import annotations

import base64
from unittest.mock import MagicMock

import pytest

from bifrost_api.research import feedback_store as store


def test_public_id_round_trip() -> None:
    assert store.public_id(143) == "FB-0143"
    assert store.parse_public_id("FB-0143") == 143
    assert store.parse_public_id("143") == 143
    assert store.parse_public_id("fb-7") == 7
    assert store.parse_public_id("nope") is None
    assert store.parse_public_id("-3") is None


def test_decode_images_enforces_the_designs_caps() -> None:
    png = base64.b64encode(b"tiny").decode()
    out = store.decode_images([{"mime": "image/png", "data_b64": png}])
    assert out[0]["mime"] == "image/png" and out[0]["bytes"] == b"tiny"

    with pytest.raises(store.FeedbackValidationError, match="at most 4"):
        store.decode_images([{"mime": "image/png", "data_b64": png}] * 5)
    with pytest.raises(store.FeedbackValidationError, match="image/\\*"):
        store.decode_images([{"mime": "text/html", "data_b64": png}])
    with pytest.raises(store.FeedbackValidationError, match="bad base64"):
        store.decode_images([{"mime": "image/png", "data_b64": "%%%"}])


def test_insert_report_refuses_off_contract_rows() -> None:
    conn = MagicMock()
    with pytest.raises(store.FeedbackValidationError, match="kind"):
        store.insert_report(conn, kind="rant", title="x")
    with pytest.raises(store.FeedbackValidationError, match="title"):
        store.insert_report(conn, kind="bug", title="   ")


def test_set_status_validates_the_dictionary() -> None:
    conn = MagicMock()
    with pytest.raises(store.FeedbackValidationError, match="status"):
        store.set_status(conn, 1, "done")


def test_row_shape_carries_the_public_id() -> None:
    import datetime as dt

    row = store._row(
        {
            "report_id": 7,
            "kind": "bug",
            "title": "t",
            "body_md": "",
            "page_route": "/x",
            "page_label": "X",
            "blocks_trading": True,
            "context": {"theme": "dark"},
            "status": "new",
            "reply_md": None,
            "replied_at": None,
            "unread_reply": False,
            "image_count": 2,
            "created_at": dt.datetime(2026, 9, 27, tzinfo=dt.timezone.utc),
            "updated_at": None,
        }
    )
    assert row["id"] == "FB-0007"
    assert row["images"] == 2
    assert row["blocks_trading"] is True
