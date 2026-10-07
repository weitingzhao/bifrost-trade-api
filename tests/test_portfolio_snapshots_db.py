"""The snapshot routes against a seeded Postgres (TD-138 / TD-139 ratchet, api 0.12.0).

Marked `db`: runs only with PGHOST set to a throwaway server (core's ``scripts/test_db.sh``
starts one; never a shared database). The account app is built with a real ``StatusReader``;
the two snapshot tables are created with core's DDL and seeded with invented rows, and every
row this test writes is deleted at the end.
"""

from __future__ import annotations

import os

import pytest
from starlette.testclient import TestClient

pytestmark = pytest.mark.db

ACCOUNTS = ("UZZ0001", "UZZ0002")


@pytest.fixture
def seeded():
    if not os.environ.get("PGHOST"):
        pytest.skip("Set PGHOST to a throwaway Postgres for db tests")
    import psycopg2

    from bifrost_core.persistence.postgres.snapshot_ddl import ensure_snapshot_tables

    conn = psycopg2.connect()
    with conn.cursor() as cur:
        cur.execute("SHOW bifrost.throwaway")
        assert cur.fetchone()[0] == "on", "not a throwaway server"
        ensure_snapshot_tables(cur)
        cur.execute(
            "INSERT INTO account_nav_daily (snapshot_date, account_id, net_liquidation, total_cash, buying_power, "
            "account_updated_at) VALUES "
            "('2031-03-03', 'UZZ0001', 1000, 100, 1500, '2031-03-03 21:05+00'), "
            "('2031-03-03', 'UZZ0002', 500, 500, 500, '2031-03-03 15:48+00'), "
            "('2031-03-04', 'UZZ0001', 1010, 100, 1500, '2031-03-04 21:05+00')"
        )
        k = "ZZZ|OPT|20310418|50.0|P"
        cur.execute(
            "INSERT INTO position_snapshot_daily (snapshot_date, account_id, contract_key, trade_id, symbol, "
            "sec_type, position_qty, trade_qty, mark, mark_source, underlying_close, delta, gamma, vega, theta, iv, "
            "greeks_asof, strike, option_right) VALUES "
            "('2031-03-03', 'UZZ0001', %(k)s, 7, 'ZZZ', 'OPT', -3, -2, 1.5, 'vendor_eod', 52, -0.3, 0.04, 0.08, -0.02, "
            " 0.4, '2031-03-03 21:00+00', 50, 'P'), "
            "('2031-03-04', 'UZZ0001', %(k)s, 7, 'ZZZ', 'OPT', -3, -2, 1.2, 'vendor_eod', 53, -0.28, 0.04, 0.08, -0.02, "
            " 0.38, '2031-03-04 21:00+00', 50, 'P'), "
            "('2031-03-04', 'UZZ0001', %(k)s, NULL, 'ZZZ', 'OPT', -3, -1, 1.2, 'vendor_eod', 53, NULL, NULL, NULL, NULL, "
            " NULL, NULL, 50, 'P'), "
            "('2031-03-04', 'UZZ0001', 'YYY|STK|||', NULL, 'YYY', 'STK', 10, 10, 20, 'vendor_eod', 20, NULL, NULL, NULL, "
            " NULL, NULL, NULL, NULL, NULL)",
            {"k": k},
        )
    conn.commit()
    try:
        yield conn
    finally:
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute("DELETE FROM account_nav_daily WHERE account_id = ANY(%s)", (list(ACCOUNTS),))
            cur.execute("DELETE FROM position_snapshot_daily WHERE account_id = ANY(%s)", (list(ACCOUNTS),))
        conn.commit()
        conn.close()


def _client():
    from bifrost_api.account.app import create_account_app
    from bifrost_core.monitor.reader.common import StatusReader
    from tests.contract.helpers import full_server_config

    cfg = full_server_config()
    cfg["postgres"] = {
        "host": os.environ["PGHOST"],
        "port": int(os.environ.get("PGPORT", "5432")),
        "user": os.environ.get("PGUSER", "postgres"),
        "database": os.environ.get("PGDATABASE", "postgres"),
    }
    reader = StatusReader(cfg)
    app = create_account_app(reader=reader, control_via_db=None, merged_config=cfg)
    return TestClient(app, raise_server_exceptions=False)


def test_the_three_routes_read_the_seeded_tables(seeded):
    client = _client()

    nav = client.get("/portfolio/nav-history?from_date=2031-03-01&to_date=2031-03-31")
    assert nav.status_code == 200, nav.text
    body = nav.json()
    assert [(i["snapshot_date"], i["account_id"]) for i in body["items"]] == [
        ("2031-03-03", "UZZ0001"),
        ("2031-03-04", "UZZ0001"),
    ]
    assert [(d["snapshot_date"], d["account_id"]) for d in body["dropped"]] == [("2031-03-03", "UZZ0002")]

    pos = client.get("/portfolio/position-snapshots")
    assert pos.status_code == 200, pos.text
    body = pos.json()
    assert body["sessions"] == ["2031-03-04"]
    opt = [i for i in body["items"] if i["sec_type"] == "OPT"]
    assert opt and all(i["greeks_quality"] in ("vendor", "degraded", "missing") for i in opt)
    # TD-139 acceptance: missing == option rows with no delta.
    assert body["greeks_quality"]["2031-03-04"]["missing"] == sum(1 for i in opt if i["delta"] is None) == 1
    assert {t["trade_id"] for t in body["trades"]} == {7, None}

    attr = client.get("/portfolio/pnl-attribution?from_date=2031-03-04&to_date=2031-03-04")
    assert attr.status_code == 200, attr.text
    body = attr.json()
    assert body["sessions"][0]["status"] == "ok" and body["sessions"][0]["prior_date"] == "2031-03-03"
    rows = {(r["contract_key"], r["trade_id"]): r for r in body["items"]}
    assert rows[("ZZZ|OPT|20310418|50.0|P", 7)]["status"] == "ok"
    assert rows[("ZZZ|OPT|20310418|50.0|P", 7)]["greeks_quality"] == "vendor"
    assert rows[("YYY|STK|||", None)]["status"] == "opened_in_session"
