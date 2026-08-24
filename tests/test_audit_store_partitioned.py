"""Wave 4: audit_store timestamptz ↔ float epoch conversion."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from bifrost_api.ops.models.schemas import AuditEntry
from bifrost_api.ops.services.audit_store import AuditStore, _epoch_from_db_timestamp


def test_epoch_from_db_timestamp_datetime():
    dt = datetime(2026, 8, 1, 12, 0, 0, tzinfo=timezone.utc)
    assert abs(_epoch_from_db_timestamp(dt) - dt.timestamp()) < 1e-6


def test_epoch_from_db_timestamp_naive_assumes_utc():
    dt = datetime(2026, 8, 1, 12, 0, 0)
    expected = dt.replace(tzinfo=timezone.utc).timestamp()
    assert abs(_epoch_from_db_timestamp(dt) - expected) < 1e-6


def test_epoch_from_db_timestamp_float_passthrough():
    assert _epoch_from_db_timestamp(1719792000.0) == 1719792000.0
    assert _epoch_from_db_timestamp(None) == 0.0


def test_persist_uses_to_timestamp():
    store = AuditStore(dsn=None)
    store._db_available = True
    store._dsn = "host=x"
    entry = AuditEntry(
        timestamp=1719792000.0,
        operator="op",
        action="restart",
        target="api",
        outcome="ok",
    )

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value = mock_cur

    with patch("psycopg2.connect", return_value=mock_conn):
        store._persist(entry)

    sql = mock_cur.execute.call_args[0][0]
    params = mock_cur.execute.call_args[0][1]
    assert "to_timestamp(%s)" in sql
    assert params[0] == 1719792000.0
    mock_conn.commit.assert_called_once()


def test_list_from_db_converts_datetime_to_float():
    store = AuditStore(dsn=None)
    store._db_available = True
    store._dsn = "host=x"
    dt = datetime(2026, 8, 1, 12, 0, 0, tzinfo=timezone.utc)

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value = mock_cur
    mock_cur.fetchall.return_value = [
        (dt, "op", "127.0.0.1", "restart", "api", None, "ok", None),
    ]

    with patch("psycopg2.connect", return_value=mock_conn):
        entries = store._list_from_db(10)

    assert len(entries) == 1
    assert isinstance(entries[0].timestamp, float)
    assert abs(entries[0].timestamp - dt.timestamp()) < 1e-6
    assert entries[0].operator == "op"
