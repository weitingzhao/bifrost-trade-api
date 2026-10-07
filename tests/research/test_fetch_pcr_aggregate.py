"""TD-102: PCR for the SEPA page comes from Research's filtered daily metric."""

from __future__ import annotations

from bifrost_api.research import market_data_client as client
from bifrost_api.research.analytics_reader import ResearchUnavailable


def test_maps_research_rows_oldest_first(monkeypatch) -> None:
    def _get(path, params, *, missing_is_none=False):
        assert path == "/analytics/options/pcr"
        assert 1 <= params["lookback_days"] <= 365
        assert missing_is_none is True
        return {
            "rows": [
                {
                    "trade_date": "2026-10-02",
                    "pcr_oi": 0.8,
                    "total_put_oi": 8,
                    "total_call_oi": 10,
                    "pcr_volume": 1.5,
                    "total_put_volume": 3,
                    "total_call_volume": 2,
                },
                {
                    "trade_date": "2026-10-01",
                    "pcr_oi": 0.5,
                    "total_put_oi": 5,
                    "total_call_oi": 10,
                    "pcr_volume": 1.0,
                    "total_put_volume": 1,
                    "total_call_volume": 1,
                },
            ]
        }

    monkeypatch.setattr("bifrost_api.research.analytics_reader._research_get", _get)
    out = client.fetch_pcr_aggregate("spy", pcr_type="oi", lookback_days=900)
    assert out["ok"] is True
    assert out["source"] == "research_filtered_pcr"
    assert out["lookback_days"] == 365
    assert [row["trade_date"] for row in out["trend"]] == ["2026-10-01", "2026-10-02"]
    assert out["trend"][0]["put_value"] == 5
    assert out["latest_ratio"] == 0.8

    vol = client.fetch_pcr_aggregate("SPY", pcr_type="volume", lookback_days=30)
    assert vol["trend"][0]["put_value"] == 1
    assert vol["trend"][0]["call_value"] == 1


def test_a_missing_symbol_is_an_empty_trend(monkeypatch) -> None:
    monkeypatch.setattr(
        "bifrost_api.research.analytics_reader._research_get",
        lambda *_a, **_k: None,
    )
    out = client.fetch_pcr_aggregate("ZZZZ")
    assert out == {
        "ok": False,
        "symbol": "ZZZZ",
        "type": "oi",
        "source": "research_filtered_pcr",
        "trend": [],
        "latest_ratio": None,
    }


def test_research_down_does_not_raise(monkeypatch) -> None:
    def _get(*_a, **_k):
        raise ResearchUnavailable("Research API /analytics/options/pcr: HTTP 503")

    monkeypatch.setattr("bifrost_api.research.analytics_reader._research_get", _get)
    out = client.fetch_pcr_aggregate("SPY")
    assert out["ok"] is False
    assert out["trend"] == []
