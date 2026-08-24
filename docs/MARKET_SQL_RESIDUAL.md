# Residual `market.*` Direct SQL — Audit

> **Date**: 2026-08-14
> **Programs**: `market-data-golden-source` (completed) → `market-data-write-consolidation` (completed)
> **Status**: ✅ **ALL MIGRATED** — Zero `market.*` direct SQL in Trade repos.

## Migration Complete (2026-08-14)

All `market.*` direct SQL has been migrated to Plugin API HTTP calls across three waves:

### Wave 0 — Plugin WRITE API (3 phases)

| Endpoint | Purpose |
|----------|---------|
| `POST /stocks/bars/ingest` | OHLC bar batch write |
| `DELETE /stocks/bars` | Bar deletion |
| `POST /reference/ticker/upsert` | Ticker metadata upsert |
| `POST /reference/ticker/upsert-overview` | Ticker overview upsert |
| `POST /options/expirations/replace` | Option expiration cache refresh |

### Wave 1 — Trade WRITE cutover (4 phases)

| Source | Migration |
|--------|-----------|
| `monitor/reader/market.py` write functions | → `market_write_client.py` HTTP |
| `postgres_sink.py` StatusSink bars | → `market_write_client.py` HTTP |
| `ticker_reference.py` upsert functions | → `ticker_write_client.py` HTTP |
| `market_pg.py` expiration cache | → `market_data_client.py` HTTP |

### Wave 2 — Trade READ cutover (4 phases + residual)

| Source | Migration |
|--------|-----------|
| `financials_data.py` (~33 SQL) | → 9 SEPA aggregate Plugin endpoints |
| `readiness_snapshot.py` | → Plugin readiness/coverage endpoints + temp tables |
| `stock_option_pcr.py` | → Plugin PCR + chain-by-expiry endpoints |
| `greeks.py` | → Plugin option_daily endpoint |
| `data_readiness.py` | → Plugin financials/ticker/coverage endpoints |
| `monitor/reader/market.py` READ | → `market_read_client.py` HTTP |
| `ticker_reference.py` READ | → `ticker_read_client.py` HTTP |
| `massive_jobs.py` | → Plugin option-minute-bars endpoint |

### Database cleanup

| Database | Action | Date |
|----------|--------|------|
| `bifrost_stg` | `market`, `market_analytics`, `data_ops` schemas DROPPED | 2026-08-14 |
| `bifrost_prod` | `market`, `market_analytics`, `data_ops` schemas DROPPED | 2026-08-14 |
| `bifrost_dev` | `market`, `market_analytics`, `data_ops` schemas DROPPED | 2026-08-14 |

### PLUGIN_URL hygiene (market-data-gs-closeout P4)

`MARKET_DATA_PLUGIN_URL` is set on Trade writers/readers that call Plugin: `api-monitor`, `api-market`, `api-research`, `daemon`.

**Skip (no market.* SQL / Plugin client):** legacy split `api-docs`, `api-ops`, `api-trading`, `api-strategy`, `api-portfolio`, `api-massive` — all merged or retired; see `k8s/base/apis/manifest.yaml`.

Write-path operator token (P6): Trade writers send `Authorization: Bearer $MARKET_DATA_WRITE_TOKEN` when that env (or `PLATFORM_OPERATOR_TOKEN`) is set. Plugin enforces only when the token is armed in `market-data-secrets` / Trade secrets.

### DDL legacy views (remediated 2026-08-14)

`public.v_us_equity_universe` is now a backward-compat VIEW over `market.v_us_equity_universe` (FDW-backed from `market.ticker`). The physical `us_equity_universe` and `sepa_symbol_price_readiness` tables and their views have been dropped (core 0.8.3). Price readiness summary is computed at query time via Plugin API `/readiness/bar-aggregate`.

## Final grep audit

```
rg "(SELECT|INSERT|UPDATE|DELETE|FROM|INTO)\s+market\." bifrost-trade-api/src/ bifrost-trade-core/src/ bifrost-trade-worker/src/
```

Result: Only 1 match — guarded DDL view definition (not runtime SQL).
