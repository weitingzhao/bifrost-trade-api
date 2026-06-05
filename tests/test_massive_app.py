"""Tests for the Massive FastAPI app health and docs endpoints."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from starlette.testclient import TestClient

from bifrost_api.massive.app import create_massive_app

_FULL_SERVER = {
    "monitor_port": 8765,
    "massive_port": 8766,
    "docs_port": 8767,
    "ops_port": 8768,
    "trading_port": 8769,
    "strategy_port": 8770,
    "portfolio_port": 8771,
    "market_port": 8772,
    "research_port": 8773,
}


def _make_client(resolved_config_path: str | None = None, reader_config: dict | None = None) -> TestClient:
    reader = MagicMock()
    base = {"server": dict(_FULL_SERVER)}
    if reader_config is not None:
        rc = {**base, **reader_config}
        if "server" in reader_config:
            rc["server"] = {**_FULL_SERVER, **reader_config["server"]}
        reader._config = rc
    else:
        reader._config = base
    app = create_massive_app(
        reader=reader,
        control_via_db=None,
        resolved_config_path=resolved_config_path,
    )
    return TestClient(app, raise_server_exceptions=False)


class TestMassiveHealth:
    def test_root_health(self):
        client = _make_client()
        r = client.get("/health")
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "ok"
        assert body["service"] == "bifrost-massive"
        assert "ts" in body

    def test_prefixed_health(self):
        client = _make_client(reader_config={"server": {"massive_port": 9901}})
        r = client.get("/research/massive/health")
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "ok"
        assert body["service"] == "bifrost-massive"
        assert body["port"] == 9901

    def test_config_profile_dev(self, tmp_path):
        fake = tmp_path / "config.dev.yaml"
        fake.write_text("server: {}")
        client = _make_client(resolved_config_path=str(fake))
        body = client.get("/research/massive/health").json()
        assert body["config_profile"] == "dev"
        assert "config_path" in body

    def test_config_profile_prod(self, tmp_path):
        fake = tmp_path / "config.prod.yaml"
        fake.write_text("server: {}")
        client = _make_client(resolved_config_path=str(fake))
        body = client.get("/research/massive/health").json()
        assert body["config_profile"] == "prod"

    def test_config_profile_absent(self):
        client = _make_client()
        body = client.get("/research/massive/health").json()
        assert "config_profile" not in body
        assert body.get("port") == 8766

    def test_stock_reference_search_requires_postgres(self):
        client = _make_client()
        r = client.get("/research/massive/stocks/search?q=AAPL")
        assert r.status_code == 200
        body = r.json()
        assert body.get("ok") is False
        assert "PostgreSQL" in str(body.get("error", ""))

    def test_ticker_types_db_requires_postgres(self):
        client = _make_client()
        r = client.get("/research/massive/reference/ticker-types")
        assert r.status_code == 200
        body = r.json()
        assert body.get("ok") is False
        assert "PostgreSQL" in str(body.get("error", ""))

    def test_db_coverage_summary_requires_postgres(self):
        client = _make_client()
        r = client.get("/research/massive/db-coverage-summary")
        assert r.status_code == 200
        body = r.json()
        assert body.get("ok") is False
        assert "PostgreSQL" in str(body.get("error", ""))

    def test_celery_beat_schedule_ok(self):
        client = _make_client()
        r = client.get("/research/massive/celery-beat-schedule")
        assert r.status_code == 200
        body = r.json()
        assert body.get("ok") is True
        assert body.get("timezone") == "UTC"
        entries = body.get("entries") or []
        assert len(entries) >= 1
        first = entries[0]
        assert "name" in first and "task" in first and "label" in first and "crontab" in first

    def test_watchlist_db_coverage_requires_postgres(self):
        client = _make_client()
        r = client.get("/research/massive/watchlist-db-coverage")
        assert r.status_code == 200
        body = r.json()
        assert body.get("ok") is False
        assert "PostgreSQL" in str(body.get("error", ""))

    @patch("psycopg2.connect")
    @patch("bifrost_worker.data.massive.vendor.reader.get_watchlist_optionable_stk_symbols", return_value=["NVDA"])
    @patch("bifrost_core.persistence.postgres.connection._get_conn_params", return_value={})
    @patch("bifrost_api.massive.routers.routes._db_config", return_value={"host": "h", "database": "d"})
    def test_watchlist_db_coverage_option_contracts_json_shape(self, _mock_db, _gp, _syms, mock_connect):
        """When PostgreSQL returns aggregates, option_contracts includes coverage + age fields."""
        newest = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        cur = MagicMock()
        cur.fetchall.side_effect = [
            [("NVDA", 100, newest, 90, 80, 2, 50, 45, 4, 12, 0, 0)],  # option_contracts (+ per-column SQL NULL row counts)
            [("NVDA", 50, newest, 45, 40, 30, 3)],               # option_snapshots
            [("NVDA", newest.date(), newest)],                   # report_option_atm_iv_daily
            [("NVDA", 200, newest, newest, 99.1, 98.0, 95.0, 180)],  # stock_day aggregate
            [],  # stock_min
            [],  # option_day
            [],  # option_min
            [],  # option_snapshots_with_underlying_day
            [],  # job_ticker_reference_state (contracts check)
            [],  # option_expiration_cache
            [],  # option_open_interest_daily
            [],  # report_option_max_pain_daily
            [("NVDA", 1, newest, newest, newest)],  # tickers + ticker_overview join (no o.created_at in DDL)
        ]
        cursor_cm = MagicMock()
        cursor_cm.__enter__.return_value = cur
        cursor_cm.__exit__.return_value = None
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = cursor_cm
        mock_connect.return_value = mock_conn
        cur.fetchone.return_value = (128, newest)  # ticker_types global COUNT + MAX(created_at)

        client = _make_client()
        r = client.get("/research/massive/watchlist-db-coverage")
        assert r.status_code == 200
        body = r.json()
        assert body.get("ok") is True
        assert body.get("source_scope") == "massive"
        syms = body.get("symbols") or []
        assert len(syms) == 1
        oc = syms[0]["option_contracts"]
        assert oc["has_data"] is True
        assert oc["row_count"] == 100
        assert oc["ticker_pct"] == 90.0
        assert oc["identity_pct"] == 80.0
        assert oc["mapping_mismatch_count"] == 2
        assert oc["exercise_style_pct"] == 50.0
        assert oc["shares_per_contract_pct"] == 45.0
        assert oc["optional_data_fill_avg_pct"] == 47.5
        assert oc["exercise_style_null_row_count"] == 0
        assert oc["shares_per_contract_null_row_count"] == 0
        assert oc["column_gap_count"] == 0
        assert oc["distinct_expirations"] == 4
        assert oc["distinct_strikes"] == 12
        assert oc["newest_created_at"] is not None
        assert oc["contracts_last_at"] == oc["newest_created_at"]
        assert isinstance(oc.get("age_seconds"), int)
        osnap = syms[0]["option_snapshots"]
        assert osnap["has_data"] is True
        assert osnap["row_count"] == 50
        assert osnap["iv_pct"] == 90.0
        assert osnap["full_greeks_pct"] == 80.0
        assert osnap["open_interest_pct"] == 60.0
        assert osnap["optional_data_fill_avg_pct"] == 70.0
        assert osnap["stale_snapshot_rows"] == 3
        assert isinstance(osnap.get("age_seconds"), int)
        row0 = syms[0]
        assert row0.get("option_day", {}).get("has_data") is False
        assert row0.get("report_option_max_pain_daily", {}).get("has_data") is False
        sd = row0["stock_day"]
        assert sd["has_data"] is True
        assert sd["row_count"] == 200
        assert sd["ohlc_complete_pct"] == 99.1
        assert row0["ticker_types"]["has_data"] is True
        assert row0["ticker_types"]["dictionary_row_count"] == 128
        assert row0["tickers"]["has_data"] is True
        assert row0["tickers"]["tickers_id"] == 1
        assert row0["ticker_overview"]["has_data"] is True

    @patch("bifrost_worker.data.massive.vendor.contracts_reference_gap.compute_option_contracts_reference_gap")
    @patch("psycopg2.connect")
    @patch("bifrost_api.massive.routers.routes._db_config", return_value={"host": "h", "database": "d"})
    def test_option_contracts_reference_gap_get_ok(self, _mock_db, mock_connect, mock_compute):
        mock_compute.return_value = {
            "ok": True,
            "symbol": "NVDA",
            "has_rows": True,
            "db_row_count": 10,
            "pg_total": 10,
            "massive_total": 12,
            "gap": 2,
            "coverage_pct": 83.3,
            "compared_at": "2026-01-01T00:00:00Z",
            "expiries": [],
            "truncated": False,
            "expiries_truncated": False,
        }
        cur = MagicMock()
        cursor_cm = MagicMock()
        cursor_cm.__enter__.return_value = cur
        cursor_cm.__exit__.return_value = None
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = cursor_cm
        mock_connect.return_value = mock_conn

        client = _make_client(reader_config={"massive": {"api_key": "test-key"}})
        r = client.get("/research/massive/option-contracts-reference-gap?symbol=NVDA")
        assert r.status_code == 200
        body = r.json()
        assert body.get("ok") is True
        assert body.get("symbol") == "NVDA"
        assert body.get("gap") == 2
        mock_compute.assert_called_once()

    @patch("bifrost_worker.data.massive.vendor.contracts_reference_gap.compute_option_contracts_reference_gap")
    @patch("psycopg2.connect")
    @patch("bifrost_api.massive.routers.routes._db_config", return_value={"host": "h", "database": "d"})
    def test_option_contracts_reference_gap_get_passes_max_expiries(self, _mock_db, mock_connect, mock_compute):
        mock_compute.return_value = {
            "ok": True,
            "symbol": "NVDA",
            "has_rows": True,
            "db_row_count": 10,
            "distinct_expiry_total": 70,
            "expiries_scanned": 60,
            "pg_total": 10,
            "massive_total": 12,
            "gap": 2,
            "coverage_pct": 83.3,
            "compared_at": "2026-01-01T00:00:00Z",
            "expiries": [],
            "truncated": False,
            "expiries_truncated": True,
        }
        cur = MagicMock()
        cursor_cm = MagicMock()
        cursor_cm.__enter__.return_value = cur
        cursor_cm.__exit__.return_value = None
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = cursor_cm
        mock_connect.return_value = mock_conn

        client = _make_client(reader_config={"massive": {"api_key": "test-key"}})
        r = client.get("/research/massive/option-contracts-reference-gap?symbol=NVDA&max_expiries=90&max_pages_per_expiry=25")
        assert r.status_code == 200
        body = r.json()
        assert body.get("ok") is True
        assert body.get("distinct_expiry_total") == 70
        mock_compute.assert_called_once()
        kwargs = mock_compute.call_args[1]
        assert kwargs.get("max_expiries") == 90
        assert kwargs.get("max_pages_per_expiry") == 25

    @patch("bifrost_worker.data.massive.vendor.contracts_reference_gap.compute_option_contracts_reference_gap")
    @patch("psycopg2.connect")
    @patch("bifrost_api.massive.routers.routes._db_config", return_value={"host": "h", "database": "d"})
    def test_option_contracts_reference_gap_batch_ok(self, _mock_db, mock_connect, mock_compute):
        mock_compute.side_effect = [
            {"ok": True, "symbol": "NVDA", "has_rows": True, "pg_total": 1, "massive_total": 1, "gap": 0},
            {"ok": True, "symbol": "AAPL", "has_rows": True, "pg_total": 2, "massive_total": 3, "gap": 1},
        ]
        cur = MagicMock()
        cursor_cm = MagicMock()
        cursor_cm.__enter__.return_value = cur
        cursor_cm.__exit__.return_value = None
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = cursor_cm
        mock_connect.return_value = mock_conn

        client = _make_client(reader_config={"massive": {"api_key": "test-key"}})
        r = client.post(
            "/research/massive/option-contracts-reference-gap/batch",
            json={"symbols": ["NVDA", "AAPL"]},
        )
        assert r.status_code == 200
        body = r.json()
        assert body.get("ok") is True
        res = body.get("results") or {}
        assert res.get("NVDA", {}).get("gap") == 0
        assert res.get("AAPL", {}).get("gap") == 1
        assert mock_compute.call_count == 2

    @patch("bifrost_worker.data.massive.vendor.contracts_reference_gap.compute_option_contracts_reference_gap")
    @patch("psycopg2.connect")
    @patch("bifrost_api.massive.routers.routes._db_config", return_value={"host": "h", "database": "d"})
    def test_option_contracts_reference_gap_batch_passes_max_expiries(self, _mock_db, mock_connect, mock_compute):
        mock_compute.side_effect = [
            {"ok": True, "symbol": "NVDA", "has_rows": True, "pg_total": 1, "massive_total": 1, "gap": 0},
        ]
        cur = MagicMock()
        cursor_cm = MagicMock()
        cursor_cm.__enter__.return_value = cur
        cursor_cm.__exit__.return_value = None
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = cursor_cm
        mock_connect.return_value = mock_conn

        client = _make_client(reader_config={"massive": {"api_key": "test-key"}})
        r = client.post(
            "/research/massive/option-contracts-reference-gap/batch",
            json={"symbols": ["NVDA"], "max_expiries": 100, "max_pages_per_expiry": "bad"},
        )
        assert r.status_code == 200
        body = r.json()
        assert body.get("ok") is True
        kwargs = mock_compute.call_args[1]
        assert kwargs.get("max_expiries") == 100
        assert kwargs.get("max_pages_per_expiry") == 20

    def test_option_contracts_reference_gap_requires_postgres(self):
        client = _make_client()
        r = client.get("/research/massive/option-contracts-reference-gap?symbol=NVDA")
        assert r.status_code == 200
        assert r.json().get("ok") is False
        assert "PostgreSQL" in str(r.json().get("error", ""))

    @patch("bifrost_worker.data.massive.vendor.config.get_massive_settings")
    @patch("bifrost_api.massive.routers.routes._db_config", return_value={"host": "h", "database": "d"})
    def test_option_contracts_reference_gap_requires_api_key(self, _mock_db, mock_ms):
        mock_ms.return_value = {
            "api_key": "",
            "rest_base": "https://api.polygon.io",
            "tier": "starter",
            "ws_url": "wss://x",
            "trades_enabled": False,
            "daily_full_backfill_years": 5.0,
        }
        client = _make_client()
        r = client.get("/research/massive/option-contracts-reference-gap?symbol=NVDA")
        assert r.status_code == 200
        body = r.json()
        assert body.get("ok") is False
        assert "API key" in str(body.get("error", ""))

    @patch("bifrost_worker.data.massive.vendor.snapshots_contracts_gap.compute_option_snapshots_contracts_gap")
    @patch("psycopg2.connect")
    @patch("bifrost_api.massive.routers.routes._db_config", return_value={"host": "h", "database": "d"})
    def test_option_snapshots_contracts_gap_get_ok(self, _mock_db, mock_connect, mock_compute):
        mock_compute.return_value = {
            "ok": True,
            "symbol": "NVDA",
            "has_rows": True,
            "db_row_count": 8,
            "pg_total": 8,
            "massive_total": 10,
            "gap": 2,
            "coverage_pct": 80.0,
            "compared_at": "2026-01-01T00:00:00Z",
            "expiries": [],
            "truncated": False,
            "expiries_truncated": False,
        }
        cur = MagicMock()
        cursor_cm = MagicMock()
        cursor_cm.__enter__.return_value = cur
        cursor_cm.__exit__.return_value = None
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = cursor_cm
        mock_connect.return_value = mock_conn

        client = _make_client(reader_config={"massive": {"api_key": "test-key"}})
        r = client.get("/research/massive/option-snapshots-contracts-gap?symbol=NVDA")
        assert r.status_code == 200
        body = r.json()
        assert body.get("ok") is True
        assert body.get("symbol") == "NVDA"
        assert body.get("gap") == 2
        mock_compute.assert_called_once()

    @patch("bifrost_worker.data.massive.vendor.snapshots_contracts_gap.compute_option_snapshots_contracts_gap")
    @patch("psycopg2.connect")
    @patch("bifrost_api.massive.routers.routes._db_config", return_value={"host": "h", "database": "d"})
    def test_option_snapshots_contracts_gap_batch_ok(self, _mock_db, mock_connect, mock_compute):
        mock_compute.side_effect = [
            {"ok": True, "symbol": "NVDA", "has_rows": True, "pg_total": 1, "massive_total": 1, "gap": 0},
            {"ok": True, "symbol": "AAPL", "has_rows": True, "pg_total": 2, "massive_total": 3, "gap": 1},
        ]
        cur = MagicMock()
        cursor_cm = MagicMock()
        cursor_cm.__enter__.return_value = cur
        cursor_cm.__exit__.return_value = None
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = cursor_cm
        mock_connect.return_value = mock_conn

        client = _make_client(reader_config={"massive": {"api_key": "test-key"}})
        r = client.post(
            "/research/massive/option-snapshots-contracts-gap/batch",
            json={"symbols": ["NVDA", "AAPL"]},
        )
        assert r.status_code == 200
        body = r.json()
        assert body.get("ok") is True
        res = body.get("results") or {}
        assert res.get("NVDA", {}).get("gap") == 0
        assert res.get("AAPL", {}).get("gap") == 1
        assert mock_compute.call_count == 2

    def test_option_snapshots_contracts_gap_requires_postgres(self):
        client = _make_client()
        r = client.get("/research/massive/option-snapshots-contracts-gap?symbol=NVDA")
        assert r.status_code == 200
        assert r.json().get("ok") is False
        assert "PostgreSQL" in str(r.json().get("error", ""))

    @patch("bifrost_worker.data.massive.vendor.config.get_massive_settings")
    @patch("bifrost_api.massive.routers.routes._db_config", return_value={"host": "h", "database": "d"})
    def test_option_snapshots_contracts_gap_requires_api_key(self, _mock_db, mock_ms):
        mock_ms.return_value = {
            "api_key": "",
            "rest_base": "https://api.polygon.io",
            "tier": "starter",
            "ws_url": "wss://x",
            "trades_enabled": False,
            "daily_full_backfill_years": 5.0,
        }
        client = _make_client()
        r = client.get("/research/massive/option-snapshots-contracts-gap?symbol=NVDA")
        assert r.status_code == 200
        body = r.json()
        assert body.get("ok") is False
        assert "API key" in str(body.get("error", ""))

    @patch("bifrost_worker.data.massive.vendor.contracts_reference_column_parity.compute_option_contracts_reference_column_parity")
    @patch("psycopg2.connect")
    @patch("bifrost_api.massive.routers.routes._db_config", return_value={"host": "h", "database": "d"})
    def test_option_contracts_reference_column_parity_get_ok(self, _mock_db, mock_connect, mock_compute):
        mock_compute.return_value = {
            "ok": True,
            "symbol": "NVDA",
            "has_rows": True,
            "db_row_count": 10,
            "api_rows_compared": 10,
            "pg_rows_missing": 0,
            "value_mismatch_rows": 0,
            "field_mismatches": {},
            "truncated": False,
            "expiries_truncated": False,
            "sample_mismatches": [],
            "compared_at": "2026-01-01T00:00:00Z",
        }
        cur = MagicMock()
        cursor_cm = MagicMock()
        cursor_cm.__enter__.return_value = cur
        cursor_cm.__exit__.return_value = None
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = cursor_cm
        mock_connect.return_value = mock_conn

        client = _make_client(reader_config={"massive": {"api_key": "test-key"}})
        r = client.get("/research/massive/option-contracts-reference-column-parity?symbol=NVDA")
        assert r.status_code == 200
        body = r.json()
        assert body.get("ok") is True
        assert body.get("api_rows_compared") == 10
        assert body.get("value_mismatch_rows") == 0
        mock_compute.assert_called_once()

    @patch("bifrost_worker.data.massive.vendor.contracts_reference_column_parity.compute_option_contracts_reference_column_parity")
    @patch("psycopg2.connect")
    @patch("bifrost_api.massive.routers.routes._db_config", return_value={"host": "h", "database": "d"})
    def test_option_contracts_reference_column_parity_batch_ok(self, _mock_db, mock_connect, mock_compute):
        mock_compute.side_effect = [
            {"ok": True, "symbol": "NVDA", "value_mismatch_rows": 0},
            {"ok": True, "symbol": "AAPL", "value_mismatch_rows": 1},
        ]
        cur = MagicMock()
        cursor_cm = MagicMock()
        cursor_cm.__enter__.return_value = cur
        cursor_cm.__exit__.return_value = None
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = cursor_cm
        mock_connect.return_value = mock_conn

        client = _make_client(reader_config={"massive": {"api_key": "test-key"}})
        r = client.post(
            "/research/massive/option-contracts-reference-column-parity/batch",
            json={"symbols": ["NVDA", "AAPL"]},
        )
        assert r.status_code == 200
        body = r.json()
        assert body.get("ok") is True
        res = body.get("results") or {}
        assert res.get("NVDA", {}).get("value_mismatch_rows") == 0
        assert res.get("AAPL", {}).get("value_mismatch_rows") == 1
        assert mock_compute.call_count == 2

    def test_option_contracts_reference_column_parity_requires_postgres(self):
        client = _make_client()
        r = client.get("/research/massive/option-contracts-reference-column-parity?symbol=NVDA")
        assert r.status_code == 200
        assert r.json().get("ok") is False
        assert "PostgreSQL" in str(r.json().get("error", ""))


class TestMassiveDocs:
    def test_openapi_json_reachable(self):
        client = _make_client()
        r = client.get("/research/massive/openapi.json")
        assert r.status_code == 200
        spec = r.json()
        assert "paths" in spec

    def test_swagger_ui_reachable(self):
        client = _make_client()
        r = client.get("/research/massive/docs")
        assert r.status_code == 200
        assert "swagger" in r.text.lower() or "text/html" in r.headers.get("content-type", "")

    def test_redoc_reachable(self):
        client = _make_client()
        r = client.get("/research/massive/redoc")
        assert r.status_code == 200
