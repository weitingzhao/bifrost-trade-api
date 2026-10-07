"""GET /portfolio/nav-history, /position-snapshots, /pnl-attribution (TD-138 / TD-139, api 0.12.0).

The answers are built by core's real snapshot reader functions over invented rows shaped like
its SQL output, so a reader that changes its shape fails here rather than in the UI. The
seeded-database contract is ``test_portfolio_snapshots_db.py``.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

from starlette.testclient import TestClient

from bifrost_api.account.app import create_account_app
from bifrost_core.monitor.reader.errors import ReadFailed
from bifrost_core.portfolio.reader import snapshots as core_snapshots
from tests.contract.helpers import full_server_config
from tests.reader_mock import reader_mock

D = date(2031, 3, 4)


def _sql_row(**kw):
    row = {
        "snapshot_date": D, "account_id": "UZZ0001", "contract_key": "ZZZ|OPT|20310418|50.0|P", "trade_id": 7,
        "symbol": "ZZZ", "sec_type": "OPT", "expiry": date(2031, 4, 18), "strike": 50.0, "option_right": "P",
        "position_qty": -3.0, "trade_qty": -2.0, "avg_cost": 120.0, "mark": 1.5, "mark_source": "vendor_eod",
        "underlying_close": 52.0, "delta": -0.3, "gamma": 0.04, "vega": 0.08, "theta": -0.02, "iv": 0.4,
        "greeks_asof": datetime(2031, 3, 4, 21, tzinfo=timezone.utc), "greeks_session": D,
        "positions_updated_at": None, "captured_at": datetime(2031, 3, 4, 21, 30, tzinfo=timezone.utc),
    }
    row.update(kw)
    q, why = core_snapshots.greeks_quality(row, D)
    row["greeks_quality"], row["greeks_quality_reason"] = q, why
    return row


def _positions_answer():
    rows = [
        _sql_row(),
        _sql_row(trade_id=None, trade_qty=-1.0, delta=None, gamma=None, vega=None, theta=None, iv=None),
        _sql_row(trade_id=8, mark_source="quote_live"),
        _sql_row(contract_key="YYY|STK|||", symbol="YYY", sec_type="STK", trade_id=None, expiry=None, strike=None,
                 option_right=None, trade_qty=10.0, position_qty=10.0, mark=20.0, delta=None, gamma=None,
                 vega=None, theta=None, iv=None, greeks_asof=None, greeks_session=None),
    ]
    items = [core_snapshots._position_item(r, True) for r in rows]
    return {
        "items": items,
        "trades": core_snapshots._trade_rollup(items),
        "sessions": [D.isoformat()],
        "greeks_quality": {D.isoformat(): core_snapshots._quality_counts(i["greeks_quality"] for i in items)},
    }


def _client(reader):
    reader.config = full_server_config()
    app = create_account_app(reader=reader, control_via_db=None, merged_config=reader.config)
    return TestClient(app, raise_server_exceptions=False)


def test_position_snapshots_carry_greeks_quality_on_every_option_row():
    reader = reader_mock()
    reader.get_position_snapshots.return_value = _positions_answer()
    r = _client(reader).get("/portfolio/position-snapshots")
    assert r.status_code == 200
    body = r.json()
    assert body["count"] == 4 and body["sessions"] == ["2031-03-04"]
    opt = [i for i in body["items"] if i["sec_type"] == "OPT"]
    assert opt and all(i["greeks_quality"] in ("vendor", "degraded", "missing") for i in opt)
    assert sorted(i["greeks_quality"] for i in opt) == ["degraded", "missing", "vendor"]
    missing = sum(1 for i in opt if i["delta"] is None)
    assert body["greeks_quality"]["2031-03-04"]["missing"] == missing
    assert [t["trade_id"] for t in body["trades"]] == [7, 8, None]
    reader.get_position_snapshots.assert_called_once_with(account_id=None, trade_id=None, from_date=None, to_date=None)


def test_dates_and_filters_pass_through_and_bad_input_is_400():
    reader = reader_mock()
    reader.get_nav_history.return_value = {"items": [], "dropped": [], "sessions": []}
    client = _client(reader)
    assert client.get("/portfolio/nav-history?account_id=UZZ0001&from_date=2031-03-01&to_date=2031-03-04").status_code == 200
    reader.get_nav_history.assert_called_once_with(
        account_id="UZZ0001", from_date=date(2031, 3, 1), to_date=date(2031, 3, 4)
    )
    assert client.get("/portfolio/nav-history?from_date=03/01/2031").status_code == 400
    reader.get_nav_history.side_effect = ValueError("from_date is after to_date")
    r = client.get("/portfolio/nav-history?from_date=2031-03-04&to_date=2031-03-01")
    assert r.status_code == 400 and r.json() == {"detail": "from_date is after to_date"}


def test_an_unreadable_database_is_503_not_an_empty_history():
    reader = reader_mock()
    reader.get_nav_history.side_effect = ReadFailed("the daily snapshot could not be read: database unavailable")
    reader.get_pnl_attribution.side_effect = ReadFailed("the daily snapshot could not be read: database unavailable")
    client = _client(reader)
    assert client.get("/portfolio/nav-history").status_code == 503
    assert client.get("/portfolio/pnl-attribution").status_code == 503


def test_pnl_attribution_envelope():
    reader = reader_mock()
    reader.get_pnl_attribution.return_value = {
        "items": [{"trade_id": 7}], "sessions": [{"snapshot_date": "2031-03-04", "status": "ok"}],
        "by_trade": [], "by_symbol": [], "totals": core_snapshots._empty_sums(),
    }
    body = _client(reader).get("/portfolio/pnl-attribution?trade_id=7").json()
    assert set(body) == {"items", "count", "sessions", "by_trade", "by_symbol", "totals"}
    assert reader.get_pnl_attribution.call_args.kwargs["trade_id"] == 7


def test_no_route_takes_a_retired_query_name():
    from bifrost_api.common.query_vocab import RETIRED_QUERY_NAMES

    for path in ("/portfolio/nav-history", "/portfolio/position-snapshots", "/portfolio/pnl-attribution"):
        assert ("GET", path) not in RETIRED_QUERY_NAMES
