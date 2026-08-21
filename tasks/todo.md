# Current Plan — CoinGecko Crypto Ingestion

## Goal
Build a CoinGecko historical OHLCV ingestion script that can fetch one or more crypto assets at a chosen frequency, write canonical bronze parquet in the same OHLCV column format used by the warehouse, and remain compatible with DuckDB rebuilds.

## Dependency Graph
- task-1-inspect-contract depends_on: []
- task-2-extend-asset-class depends_on: [task-1-inspect-contract]
- task-3-implement-fetch-script depends_on: [task-2-extend-asset-class]
- task-4-add-tests depends_on: [task-3-implement-fetch-script]
- task-5-run-verification depends_on: [task-4-add-tests]

## Tasks

### task-1-inspect-contract
- depends_on: []
- Confirm bronze parquet schema, DuckDB rebuild path, and existing ingestion script conventions.
- Status: done.

### task-2-extend-asset-class
- depends_on: [task-1-inspect-contract]
- Add `crypto` as a bronze parquet asset class using the existing OHLCV schema.
- Add DuckDB rebuild support for `asset_class=crypto` into `md.symbols`/`md.equities_daily` with venue `COINGECKO`.
- Status: pending.

### task-3-implement-fetch-script
- depends_on: [task-2-extend-asset-class]
- Add `scripts/fetch_coingecko_crypto.py`.
- Support `--symbols`, `--coins`, `--preset`, `--frequency`, `--start`, `--end`, `--api-key`, `--warehouse`, and `--dry-run`.
- Write canonical parquet to `~/market-warehouse/data-lake/bronze/asset_class=crypto/symbol=<SYMBOL>/data.parquet`.
- Status: pending.

### task-4-add-tests
- depends_on: [task-3-implement-fetch-script]
- Add unit tests with mocked HTTP responses and temp bronze dirs.
- Verify argument parsing/helpers, CoinGecko row conversion, dry-run no-write behavior, and parquet writes.
- Status: pending.

### task-5-run-verification
- depends_on: [task-4-add-tests]
- Run focused tests and full coverage command if feasible.
- Status: pending.
