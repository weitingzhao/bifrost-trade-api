# Residual `market.*` Direct SQL — Audit

> **Date**: 2026-08-14 (updated post-cleanup)
> **Program**: `market-data-golden-source`
> **Status**: Golden Source migration COMPLETE. Feature flag + SQL fallback REMOVED.

## Cleanup completed (2026-08-14)

- `market_pg.py`: 11 migrated functions now call Plugin API exclusively (1639→792 lines)
- `_use_plugin()` feature flag and all `_sql_*` fallback functions deleted
- 17 sql_mode/fallback tests deleted; 30 tests retained and passing
- `bifrost_stg` / `bifrost_prod` market schemas DROPPED
- `plugin-market-data-stg` / `plugin-market-data-prod` K8s namespaces DELETED

## Remaining direct SQL (requires separate program)

The following files still contain direct SQL against `market.*` tables in
`bifrost_dev`. These were NOT in scope for the Golden Source migration and
require a separate program to address.

---

## A) `bifrost-trade-api` — `research/sepa/financials_data.py`

**File**: `src/bifrost_api/research/sepa/financials_data.py` (1004 lines)
**market.* SQL count**: ~33 occurrences across ~20 functions

### Functions with `market.*` SQL

| Function | Lines | Table(s) | Operation |
|----------|-------|----------|-----------|
| `fetch_income_rows_for_sepa_from_pg` | 178–383 | `market.stock_financials` | READ |
| `count_income_statements_gaps` | 385–389 | `market.stock_financials` + `market.ticker` | READ |
| `get_income_statements_gap_details` | 391–440 | `market.stock_financials` + `market.ticker` | READ |
| `count_balance_sheet_gaps` | 442–446 | `market.stock_financials` + `market.ticker` | READ |
| `get_balance_sheet_gap_details` | 447–494 | `market.stock_financials` + `market.ticker` | READ |
| `count_cash_flow_gaps` | 496–500 | `market.stock_financials` + `market.ticker` | READ |
| `get_cash_flow_gap_details` | 501–547 | `market.stock_financials` + `market.ticker` | READ |
| `count_ratios_gaps` | 549–553 | `market.stock_financials` + `market.ticker` | READ |
| `get_ratios_gap_details` | 554–592 | `market.stock_financials` + `market.ticker` | READ |
| `count_short_interest_gaps` | 594–598 | `market.stock_financials` + `market.ticker` | READ |
| `get_short_interest_gap_details` | 599–635 | `market.stock_financials` + `market.ticker` | READ |
| `count_short_volume_gaps` | 637–641 | `market.stock_financials` + `market.ticker` | READ |
| `get_short_volume_gap_details` | 642–647 | `market.stock_financials` + `market.ticker` | READ |
| `financials_gap_symbols_from_db` | 649–788 | `market.stock_financials` + `market.ticker` | READ |
| `fetch_income_ext_rows_batch` | 790–821 | `market.stock_financials` | READ |
| `fetch_balance_sheet_rows_for_ext_batch` | 823–860 | `market.stock_financials` | READ |
| `fetch_cash_flow_rows_for_ext_batch` | 862–899 | `market.stock_financials` | READ |
| `fetch_ratios_latest_for_ext_batch` | 901–929 | `market.stock_financials` | READ |
| `fetch_short_interest_latest_batch` | 931–967 | `market.stock_financials` | READ |
| `fetch_short_volume_recent_batch` | 969–1004 | `market.stock_financials` | READ |

### Reason for deferral

These functions perform extensive **jsonb → column unpacking** via SQL (`data->>'field'`
casting, `COALESCE`, nested subqueries). The `fetch_income_rows_for_sepa_from_pg`
function alone is ~200 lines of complex SQL that unpacks quarterly and annual financial
statement data with custom column aliases matching SEPA evaluation logic.

Migrating would require either:
1. Plugin API replicating all SEPA-specific unpacking (high coupling), or
2. Rewriting to fetch raw jsonb and unpack in Python (significant refactor)

### Suggested future approach

- **Option A**: Plugin API adds SEPA-specific aggregate endpoints that return
  pre-unpacked financial data in the shape SEPA expects
- **Option B**: Trade-API fetches raw `stock_financials` rows from Plugin
  (`GET /stocks/fundamentals/db/raw?...`) and unpacks jsonb in Python
- **Gap analysis queries** (`count_*_gaps`, `get_*_gap_details`,
  `financials_gap_symbols_from_db`) could migrate to Plugin coverage endpoints

---

## B) `bifrost-trade-api` — `research/routers/data_readiness.py`

**File**: `src/bifrost_api/research/routers/data_readiness.py` (2061 lines)
**market.* SQL count**: ~9 occurrences

### Functions with `market.*` SQL

| Function | Lines | Table(s) | Operation |
|----------|-------|----------|-----------|
| `get_sepa_criteria_stats` | 523–529 | `market.stock_financials` + `market.ticker` | READ |
| `get_fundamental_distribution_symbols` | 531–587 | `market.stock_financials` + `market.ticker` | READ |
| `get_technical_distribution_symbols` | 589–644 | `market.stock_daily` + `market.ticker` | READ |
| `get_sepa_data_inventory` | 646–652 | `market.stock_financials` + `market.ticker` | READ |
| `get_fundamental_conditions_by_symbol` | 654–742 | `market.stock_financials` | READ |
| `get_symbol_technical_conditions` | 744–965 | `market.stock_daily` | READ |
| `get_symbol_fundamental_raw_data` | 1324–1435 | `market.stock_financials` | READ |
| `get_symbol_statements` | 1455–1650 | `market.stock_financials` | READ |
| `get_ticker_overview` | 1652–1739 | `market.ticker` | READ |

### Reason for deferral

Same jsonb complexity as `financials_data.py` — these are the **router-level** functions
that call into the financials_data helpers or execute similar coverage analysis SQL.
Several functions also join `market.ticker` for symbol metadata.

### Suggested future approach

- Coverage metrics (`get_sepa_criteria_stats`, `get_sepa_data_inventory`) could
  migrate to Plugin's existing `/coverage/*` endpoints
- Distribution queries could become Plugin analytics endpoints
- `get_ticker_overview` could use a Plugin `/stocks/ticker/overview` endpoint
- Condition evaluation queries require SEPA domain knowledge in Plugin (same as A)

---

## C) `bifrost-trade-core` — `monitor/reader/market.py`

**File**: `src/bifrost_core/monitor/reader/market.py` (1242 lines)
**market.* SQL count**: ~32 occurrences

### Functions with `market.*` SQL

| Function | Lines | Table(s) | Operation |
|----------|-------|----------|-----------|
| `get_bars` | 135–176 | `market.stock_daily` / `market.stock_minute` | READ |
| `get_bars_latest` | 178–212 | `market.stock_daily` / `market.stock_minute` | READ |
| `get_bar_times_in_range` | 214–257 | `market.stock_daily` / `market.stock_minute` | READ |
| `get_bars_benchmark` | 259–301 | `market.stock_daily` / `market.stock_minute` | READ |
| `get_stock_day_fallback_price` | 303–379 | `market.stock_daily` | READ |
| `get_contract_quotes_conn` | 381–420 | `market.option_snapshot` + `market.option_contract` | READ |
| `get_bars_stats` | 422–447 | `market.stock_daily` / `market.stock_minute` | READ |
| `distinct_caret_symbols_in_stock_bars_tables` | 475–497 | `market.stock_daily` / `market.stock_minute` | READ |
| `get_bars_coverage` | 499–589 | `market.stock_daily` / `market.stock_minute` | READ |
| `write_ohlc_bars_to_db` | 591–676 | `market.stock_daily` / `market.stock_minute` | **WRITE** |
| `write_stock_bars` | 678–693 | `market.stock_daily` / `market.stock_minute` | **WRITE** |
| `delete_stock_bars_for_symbol` | 695–740 | `market.stock_daily` / `market.stock_minute` | **WRITE** |
| `insert_job_bars_backfill` | 742–786 | `job_bars_backfill` (public) | WRITE |
| `get_job_bars_backfill_list` | 788–839 | `job_bars_backfill` (public) | READ |
| *(+ 7 more job_bars_backfill functions)* | 841–1130 | `job_bars_backfill` (public) | READ/WRITE |
| `get_is_us_trading_day` (+ conn variant) | 39–61, 1151–1172 | `market.market_holiday` | READ |
| `get_market_holidays` (+ conn variant) | 63–92, 1173–1188 | `market.market_holiday` | READ |
| `add_market_holiday` (+ conn variant) | 94–114, 1189–1206 | `market.market_holiday` | **WRITE** |
| `delete_market_holiday` (+ conn variant) | 116–133, 1207–end | `market.market_holiday` | **WRITE** |

### Reason for deferral

Mixed READ + WRITE — **WRITE functions** (IB bars backfill, OHLC insertion, holiday
management) are part of the Trade data ingestion pipeline and are not market data
plugin territory. **READ functions** serve Monitor API charts with minute-granularity
data (`stock_minute`) that Plugin does not currently expose.

### Suggested future approach

- **READ** functions: migrate when Plugin gains minute-bar and bar coverage endpoints
- **WRITE** functions: remain in Trade domain permanently (data ingestion owned by
  `bifrost-trade-worker`); if Plugin takes over bar ingestion, the write path moves
  to `bifrost-platform-plugin-market-data`
- `market_holiday` operations: could migrate to Plugin if holiday management is
  centralized there
- `job_bars_backfill` is in public schema, not `market.*` — out of scope

---

## D) `bifrost-trade-core` — `persistence/postgres/ticker_reference.py`

**File**: `src/bifrost_core/persistence/postgres/ticker_reference.py` (955 lines)
**market.* SQL count**: ~51 occurrences

### Functions with `market.*` SQL

| Function | Lines | Table(s) | Operation |
|----------|-------|----------|-----------|
| `upsert_ticker_row` | 345–382 | `market.ticker` | **WRITE** |
| `upsert_ticker_overview_row` | 384–455 | `market.ticker_overview` | **WRITE** |
| `get_reference_state` | 493–525 | `market.ticker_reference_state` | READ |
| `upsert_reference_state` | 513–525 | `market.ticker_reference_state` | **WRITE** |
| `replace_ticker_types` | 527–551 | `market.ticker_type` | **WRITE** |
| `replace_related_for_tickers_id` | 553–601 | `market.ticker_related` | **WRITE** |
| `get_tickers_id_for_ticker` | 603–625 | `market.ticker` | READ |
| `search_tickers` | 627–665 | `market.ticker` + `market.ticker_overview` | READ |
| `fetch_ticker_detail_merged` | 667–696 | `market.ticker` + `market.ticker_overview` | READ |
| `fetch_related_with_names` | 698–726 | `market.ticker_related` + `market.ticker` | READ |
| `list_ticker_types` | 728–741 | `market.ticker_type` | READ |
| `symbols_needing_overview` | 743–757 | `market.ticker` + `market.ticker_overview` | READ |
| `symbols_missing_overview_only` | 759–769 | `market.ticker` + `market.ticker_overview` | READ |
| `count_ticker_overview_coverage` | 771–793 | `market.ticker` + `market.ticker_overview` | READ |
| `list_tickers_missing_overview_page` | 795–809 | `market.ticker` + `market.ticker_overview` | READ |
| `count_ticker_related_coverage` | 811–833 | `market.ticker` + `market.ticker_related` | READ |
| `symbols_missing_related_only` | 835–849 | `market.ticker` + `market.ticker_related` | READ |
| `symbols_needing_related_stale` | 851–870 | `market.ticker` + `market.ticker_related` | READ |
| `list_tickers_missing_related_page` | 872–890 | `market.ticker` + `market.ticker_related` | READ |
| `list_tickers_filled_related_page` | 892–906 | `market.ticker` + `market.ticker_related` | READ |
| `count_tickers_rows` | 908–913 | `market.ticker` | READ |
| `count_ticker_types_rows` | 915–920 | `market.ticker_type` | READ |
| `all_ticker_symbols` | 922–925 | `market.ticker` | READ |

### Reason for deferral

This module manages the **ticker reference data lifecycle** — both WRITE (ingestion
from Polygon ticker list/detail API into `market.ticker*` tables) and READ (serving
ticker search, overview, related companies to Trade API consumers).

WRITE functions are integral to the Polygon data ingestion pipeline run by
`bifrost-trade-worker` Celery tasks. They cannot be migrated without moving the
entire ticker sync pipeline to Plugin.

### Suggested future approach

- **WRITE** functions: move to Plugin when Plugin takes over ticker reference
  ingestion (currently in `bifrost-platform-plugin-market-data` roadmap)
- **READ** functions: migrate when Plugin API exposes equivalent ticker metadata
  endpoints (`/stocks/ticker/search`, `/stocks/ticker/detail`, `/stocks/ticker/related`)
- Coverage queries (`count_*_coverage`, `symbols_needing_*`) could become Plugin
  `/coverage/ticker-reference` endpoints

---

## E) `bifrost-trade-api` — `research/sepa_engine/stock_option_pcr.py`

**File**: `src/bifrost_api/research/sepa_engine/stock_option_pcr.py`
**market.* SQL count**: ~8 occurrences

### Functions with `market.*` SQL

| Function | Table(s) | Operation |
|----------|----------|-----------|
| PCR calculation (OI-based) | `market.option_contract`, `market.option_snapshot`, `market.option_open_interest` | READ |
| PCR calculation (volume-based) | `market.option_contract`, `market.v_option_chain_latest`, `market.option_snapshot` | READ |

### Reason for deferral

These are complex multi-table JOINs for Put/Call Ratio calculation used by SEPA phase-2
evaluation. The SQL aggregates OI and volume across option contracts with specific
grouping logic tightly coupled to SEPA scoring.

### Suggested future approach

- Plugin API adds a PCR aggregate endpoint (`GET /options/analytics/pcr?symbol=...`)
- Or fetch raw OI/snapshot rows via existing Plugin endpoints and compute PCR in Python

---

## F) `bifrost-trade-api` — `research/routers/greeks.py`

**File**: `src/bifrost_api/research/routers/greeks.py`
**market.* SQL count**: ~2 occurrences

### Functions with `market.*` SQL

| Function | Table(s) | Operation |
|----------|----------|-----------|
| `get_option_daily_greeks` | `market.option_daily` | READ |
| Historical greeks query | `market.option_daily` | READ |

### Reason for deferral

Reads from `market.option_daily` which is a different table than the option chain
tables already migrated. Plugin does not yet expose `option_daily` data.

### Suggested future approach

- Plugin API adds `GET /options/daily?...` endpoint for historical daily greeks
- Or migrate when `option_daily` is consolidated into Plugin's analytics layer

---

## G) `bifrost-trade-api` — `research/sepa/readiness_snapshot.py`

**File**: `src/bifrost_api/research/sepa/readiness_snapshot.py`
**market.* SQL count**: ~13 occurrences

### Functions with `market.*` SQL

| Function | Table(s) | Operation |
|----------|----------|-----------|
| Readiness snapshot queries | `market.stock_daily`, `market.stock_financials` | READ |
| Coverage and freshness checks | `market.stock_daily`, `market.stock_financials` | READ |

### Reason for deferral

Similar to `financials_data.py` — complex jsonb unpacking and coverage analysis
queries specific to SEPA readiness evaluation. Tightly coupled to the SEPA pipeline's
data quality assessment logic.

### Suggested future approach

- Migrate alongside `financials_data.py` when Plugin gains SEPA-specific aggregate endpoints
- Or use Plugin raw data endpoints and compute readiness in Python

---

## Summary

| File | Repo | market.* SQL | READ | WRITE | Reason |
|------|------|-------------|------|-------|--------|
| `sepa/financials_data.py` | trade-api | ~33 | 33 | 0 | Complex jsonb unpacking |
| `routers/data_readiness.py` | trade-api | ~9 | 9 | 0 | Coverage analysis + jsonb |
| `sepa_engine/stock_option_pcr.py` | trade-api | ~8 | 8 | 0 | PCR aggregate SQL |
| `routers/greeks.py` | trade-api | ~2 | 2 | 0 | option_daily reads |
| `sepa/readiness_snapshot.py` | trade-api | ~13 | 13 | 0 | Readiness jsonb analysis |
| `monitor/reader/market.py` | trade-core | ~32 | ~20 | ~12 | Mixed R/W + minute bars |
| `persistence/.../ticker_reference.py` | trade-core | ~51 | ~30 | ~21 | Ticker lifecycle R/W |
| **Total** | | **~148** | **~115** | **~33** | |

### Already migrated (W1-P1 through W1-P3)

| Function | File | Plugin API endpoint |
|----------|------|---------------------|
| `get_stock_day_series_for_sepa` | `market_pg.py` | `GET /stocks/db/bars/daily` |
| `get_stock_day_close_series_for_crs` | `market_pg.py` | `GET /stocks/db/bars/daily/close` |
| `get_spy_close_series` | `market_pg.py` | `GET /stocks/db/bars/daily/spy-close` |
| `get_option_snapshots_latest` | `market_pg.py` | `GET /options/chain/latest` |
| `get_option_snapshots_eod_per_day` | `market_pg.py` | `GET /options/chain/eod` |
| `get_option_open_interest_daily` | `market_pg.py` | `GET /options/oi` |
| `get_option_expirations_from_contracts_db` | `market_pg.py` | `GET /options/expirations/yyyymmdd` |
| `get_strikes_for_expiry_from_contracts_db` | `market_pg.py` | `GET /options/strikes` |
| `get_option_expiration_cache_snapshot` | `market_pg.py` | `GET /options/expirations` |
| `get_short_interest_recent` | `market_pg.py` | `GET /stocks/fundamentals/db/short-interest` |
| `get_short_volume_recent` | `market_pg.py` | `GET /stocks/fundamentals/db/short-volume` |
