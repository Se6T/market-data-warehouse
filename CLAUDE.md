# Market Data Warehouse

Local-first financial data warehouse for quantitative research. Parquet data lake as system of record, DuckDB for analytics, ClickHouse for production benchmarking.

## Project Layout

Two directory trees: this **git repo** and the **data warehouse** at `~/market-warehouse/`.

```
market-data-warehouse/              # Git repo
├── clients/
│   ├── __init__.py                 # Exports BronzeClient, DailyBarFallbackClient, IBClient, DBClient
│   ├── bronze_client.py            # Canonical per-ticker bronze parquet client
│   ├── daily_bar_fallback.py       # Public daily-bar fallback chain for U.S. equities/ETFs
│   ├── ib_client.py                # Interactive Brokers API client (ib_insync)
│   ├── historical_provider.py       # HistoricalProvider abstraction (IBProvider, RadonApiProvider, IBClientAdapter)
│   ├── uw_client.py                # Unusual Whales REST API client (kept, not used for historical)
│   └── db_client.py                # DuckDB client for md.* schema
├── presets/
│   ├── volatility.json             # CBOE Volatility Indices (VIX, VVIX, etc.)
│   ├── futures-index.json          # CME/CBOT Index Futures (ES, NQ, RTY, YM)
│   ├── futures-energy.json         # NYMEX Energy Futures (CL, NG)
│   ├── futures-metals.json         # COMEX Metals Futures (GC, SI)
│   ├── futures-treasuries.json     # CBOT Treasury Futures (ZB, ZN, ZF)
│   └── ...                         # S&P 500, NDX-100, Russell 2000 sector presets
├── scripts/
│   ├── setup_market_warehouse.sh   # One-time system bootstrap
│   ├── fetch_ib_historical.py      # Bulk historical OHLCV ingestion from IB (supports --backfill, --asset-class)
│   ├── run_backfill_all.sh         # Auto-restarting runner for all presets
│   ├── daily_update.py             # Daily parquet-first incremental update
│   ├── run_daily_update_job.py     # Retrying scheduled daily-update runner
│   ├── check_daily_update_watchdog.py # Watchdog for missed/incomplete daily syncs
│   ├── rebuild_duckdb_from_parquet.py # Offline DuckDB rebuild from bronze parquet
│   ├── refresh_all_and_rebuild.py  # Atomic all-universe refresh, validation, manifest, and DB publish
│   ├── run_daily_update.sh         # Shell wrapper for launchd/cron
│   ├── run_daily_update_watchdog.sh # Shell wrapper for the daily-update watchdog
│   ├── cerebras_client.mjs         # Cerebras incident-summary client for failure alerts
│   ├── send_daily_update_failure_email.mjs # Nodemailer failure alert CLI
│   ├── com.market-warehouse.daily-update.plist.example  # macOS launchd template
│   ├── com.market-warehouse.daily-update-watchdog.plist.example # macOS launchd watchdog template
│   └── pre-commit-secrets-scan.sh  # Pre-commit hook: secrets scanner
├── docker/
│   └── ib-gateway/                 # Docker Compose setup for IB Gateway (alternative to native IBC)
├── tests/
│   ├── conftest.py                 # Shared fixtures: tmp_duckdb, db
│   ├── test_daily_bar_fallback.py  # Unit tests for fallback providers
│   ├── test_uw_client.py           # Unit tests — HTTP mocked via `responses`
│   ├── test_db_client.py           # Integration tests — temp DuckDB per test
│   ├── test_fetch_ib_historical.py # Tests for IB fetch script
│   ├── test_daily_update.py        # Tests for daily update script
│   ├── test_ib_client.py           # Focused tests for IB client connect fallback
│   └── test_historical_provider.py # Tests for HistoricalProvider, RadonApiProvider, IBClientAdapter
├── pyproject.toml                  # pytest config, coverage enforcement
├── .env.example
└── README.md

~/market-warehouse/                 # Data warehouse (created by setup script)
├── .venv/                          # Python 3.12 venv
├── data-lake/
│   ├── bronze/asset_class=equity/  # Per-ticker Hive-partitioned Parquet (symbol=AAPL/data.parquet)
│   ├── bronze/asset_class=futures/ # Per-contract Hive-partitioned Parquet (symbol=ES_202506/data.parquet)
│   ├── bronze-delisted/asset_class=equity/  # Archived delisted symbols excluded from future sync/backfill runs
│   ├── silver/                     # Cleaned / adjusted
│   └── gold/                       # Derived analytics / factor tables
├── duckdb/market.duckdb            # Analytical DB
├── clickhouse/                     # Optional ClickHouse data
├── scripts/                        # Bootstrap SQL, helper scripts
└── logs/
```

## Architecture

- **Parquet** is the system of record, not DuckDB
- **Data lake tiers**: bronze (normalized Parquet) -> silver (cleaned) -> gold (derived)
- **DuckDB** is the local query engine for research and backtesting
- **ClickHouse** is optional, for production-style benchmarking and concurrency testing
- **Python env** lives at `~/market-warehouse/.venv/` — activate with `source ~/market-warehouse/.venv/bin/activate`

## Native macOS Client (Extracted)

The native macOS client has been extracted to the standalone **Sift** app at `~/dev/apps/util/sift/`.

See the [Sift CLAUDE.md](~/dev/apps/util/sift/CLAUDE.md) for module layout, build instructions, and testing.

## DuckDB Schema

Schema `md` with four tables:

- `md.symbols` — `symbol_id BIGINT PK`, `symbol`, `asset_class`, `venue`
- `md.equities_daily` — `trade_date DATE`, `symbol_id BIGINT`, OHLCV + `adj_close`; unique index on `(trade_date, symbol_id)` for dedup
- `md.futures_daily` — trade_date, contract_id, root_symbol, expiry_date, OHLCV + settlement + open_interest; unique index on `(trade_date, contract_id)` for dedup; no `md.symbols` entries — self-contained with embedded `root_symbol`
- `md.options_daily` — trade_date, contract_id, underlier_id, expiry, strike, `option_right` (not `right` — reserved keyword), OHLCV + OI + implied_vol

ClickHouse mirrors the same schema with MergeTree engines partitioned by `toYYYYMM(trade_date)`.

## IB Gateway / IBC

IB Gateway is managed by **IBC** (IB Controller) for automated login, reconnection, and daily restarts. For Docker-based Gateway, see the **IB Gateway — Docker** section below.

For macOS workstations, IBC is the native machine-local dependency for IB-backed workflows. The secure service is installed globally under the user's home directory and is not scoped to this repo.

- **IBC install**: `~/ibc-install/` (IBC.jar + scripts)
- **IBC secure config**: `~/ibc/config.secure.ini` (settings only; no credentials)
- **IBC secure service installer**: `python scripts/install_ibc_secure_service.py`
- **IBC machine-local wrappers**: `~/ibc/bin/start-secure-ibc-service.sh`, `stop-secure-ibc-service.sh`, `restart-secure-ibc-service.sh`, `status-secure-ibc-service.sh`
- **IBC LaunchAgent**: `~/Library/LaunchAgents/local.ibc-gateway.plist`
- **IBC logs**: `~/ibc/logs/ibc-gateway-service.log` for the secure LaunchAgent, or `~/ibc/logs/` for the stock wrapper
- **Start Gateway**: installed machine-local secure service (preferred and project-required for IB workflows), repo Keychain launcher for low-level troubleshooting, or `~/ibc-install/gatewaystartmacos.sh` for legacy plaintext config
- **Stop Gateway**: `~/ibc-install/stop.sh`
- **Reconnect data**: `~/ibc-install/reconnectdata.sh`
- **Command server**: port 7462
- **Gateway API port**: 4001
- **Auto-restart**: 11:58 PM daily, cold restart Sundays 07:05

## IB Gateway — Docker (Alternative)

IB Gateway can also run as a Docker container via [gnzsnz/ib-gateway-docker](https://github.com/gnzsnz/ib-gateway-docker). Configuration lives at `docker/ib-gateway/`.

- **Setup**: `cd docker/ib-gateway && cp .env.example .env`, set `TWS_USERID`, create `secrets/ib_password.txt`
- **Start**: `docker compose up -d`
- **2FA**: Via VNC client at `localhost:5900` (opt-in, requires `VNC_SERVER_PASSWORD`) or IBKR mobile app
- **Health check**: `docker compose ps` — shows healthy after ~2 min
- **Ports**: host 4001 → container 4003 (live SOCAT relay), host 4002 → container 4004 (paper), host 5900 → VNC
- **Trading mode**: `TRADING_MODE=paper` (default) or `live` in `.env`
- **Read-only API**: `READ_ONLY_API=yes` (default, recommended for data warehouse)
- **Secrets**: `TWS_USERID` is a plain env var; password uses Docker `secrets:` directive via `TWS_PASSWORD_FILE`
- **Settings**: Persisted in a Docker volume across container restarts
- **Logs**: `docker compose logs -f`
- **Stop**: `docker compose down`

Scripts connect to `127.0.0.1:4001` by default — same endpoint whether Gateway runs natively via IBC or in Docker. Override with `MDW_IB_HOST` / `MDW_IB_PORT` env vars or `--host` / `--port` CLI flags.

## IB Gateway — Cloud (Hetzner VPS + Tailscale)

IB Gateway runs on a Hetzner CPX11 VPS (~$4-6/mo) in Ashburn, VA with Tailscale for secure, WireGuard-encrypted remote access. The existing `docker-compose.yml` works unmodified on the VPS.

- **Canonical endpoint**: `ib-gateway:4001` (Tailscale MagicDNS hostname, survives IP changes)
- **Client config**: `MDW_IB_HOST=ib-gateway` in `.env` or as env var — all scripts use this automatically
- **Security**: All ports blocked on public interface via ufw; IB API, VNC, and SSH accessible only via Tailscale mesh; identity-based ACLs control per-device access
- **TCP proxy**: A socat systemd service bridges Tailscale IP traffic to Docker's localhost-bound ports (`tailscale serve --tcp` adds TLS, which is incompatible with IB's raw TCP protocol)
- **Read-only**: `READ_ONLY_API=no` (current config — supports both read and write operations)
- **Cutover**: Cold cutover required — IB allows only one active session per login; running two gateways causes session displacement
- **Break-glass**: Hetzner web console provides browser-based VNC if Tailscale is unreachable
- **Phone access**: SSH from iOS/Android via Termius to `radon@ib-gateway` for `docker compose stop/start`
- **Full setup**: See `docker/ib-gateway/README.md` for provisioning, hardening, Tailscale ACLs, client enrollment, rollback, and 2FA reauth runbook

## Data Ingestion

Data source: **Interactive Brokers** via `ib_insync`. Requires IB Gateway running on a reachable endpoint (default `127.0.0.1:4001`), either natively via the macOS IBC service or via Docker.

- `IBClient` wraps `ib_insync.IB` with connection management, historical data, and contract qualification
- `IBClient.connect()` defaults to `clientId=0` and automatically retries successive `clientId` values if IB reports error `326` (`client id already in use`)
- `IBClient.get_historical_data()` fetches daily bars via `reqHistoricalData`
- `BronzeClient` is the live service storage client: it discovers symbols from parquet, merges or replaces per-ticker snapshots, and publishes with `temp -> validate -> os.replace()`
- `DailyBarFallbackClient` is a narrow recovery client for unresolved target-day gaps in the current U.S. equity universe. Provider order: Nasdaq `assetclass=stocks`, Nasdaq `assetclass=etf`, then Stooq U.S. daily CSV.
- `DBClient` is now the offline analytical-file client: it can still manage/query `md.*`, and it rebuilds DuckDB from bronze parquet with set-based `INSERT INTO ... SELECT`
- `adj_close` is set to `close` (IB TRADES data doesn't provide adjusted prices)
- **CBOE volatility indices** are fetched directly from CBOE's public API (`cdn.cboe.com/api/global/delayed_quotes/charts/historical/`) via `scripts/fetch_cboe_volatility.py`, not IB. This is the authoritative source for VIX, VVIX, VXHYG, VXSMH, and all other CBOE volatility indices. The writer normalizes stale parquet schemas on merge (drops extra columns from older schema versions) and rewrites files to fix schema drift even when no new data is available.

### IB BarData → Bronze mapping

| IB BarData field | Bronze column | Transform |
|---|---|---|
| `bar.date` | `trade_date` | `str(bar.date)` |
| (from ticker) | `symbol_id` | Read existing parquet ID or derive stable ID |
| `bar.open` | `open` | Already float |
| `bar.high` | `high` | Already float |
| `bar.low` | `low` | Already float |
| `bar.close` | `close` | Already float |
| `bar.close` | `adj_close` | Same value |
| `bar.volume` | `volume` | `int(bar.volume)` |

### IB BarData → Futures Bronze mapping

| IB BarData field | Bronze column | Transform |
|---|---|---|
| `bar.date` | `trade_date` | `str(bar.date)` |
| (from composite ticker) | `contract_id` | Stable hash of composite ticker (e.g. `ES_202506`) |
| (from composite ticker) | `root_symbol` | Parsed from `ticker.rsplit("_", 1)[0]` |
| (from composite ticker) | `expiry_date` | `YYYY-MM-01` derived from expiry code |
| `bar.open` | `open` | Already float |
| `bar.high` | `high` | Already float |
| `bar.low` | `low` | Already float |
| `bar.close` | `close` | Already float |
| `bar.close` | `settlement` | Same value (IB doesn't provide settlement) |
| `bar.volume` | `volume` | `int(bar.volume)` |
| (default) | `open_interest` | `0` (IB BarData doesn't include OI) |

### Running the pipeline

```bash
source ~/market-warehouse/.venv/bin/activate
python scripts/fetch_ib_historical.py                                  # Mag 7 default
python scripts/fetch_ib_historical.py --tickers AAPL NVDA              # Custom tickers
python scripts/fetch_ib_historical.py --preset presets/sp500.json      # From preset with cursor resume
python scripts/fetch_ib_historical.py --years 0 --skip-existing        # Inception, skip existing
python scripts/fetch_ib_historical.py --preset presets/sp500.json --backfill  # Backfill older data
python scripts/fetch_ib_historical.py --preset presets/volatility.json --asset-class volatility  # CBOE vol indices (IB backfill)
python scripts/fetch_cboe_volatility.py                                                        # CBOE vol indices (daily sync, preferred)
python scripts/fetch_ib_historical.py --preset presets/futures-index.json --asset-class futures  # CME/CBOT index futures
python scripts/fetch_ib_historical.py --preset presets/futures-energy.json --asset-class futures  # NYMEX energy futures
python scripts/fetch_ib_historical.py --host 192.168.1.50 --port 4001 --tickers AAPL            # Remote IB Gateway
```

IB connection defaults to `127.0.0.1:4001`, configurable via `--host`/`--port` flags or `MDW_IB_HOST`/`MDW_IB_PORT` environment variables.

Current fetch behavior:
- Normal mode atomically replaces the per-ticker bronze snapshot
- Backfill mode merges older bars into the same per-ticker bronze snapshot
- The live service path does not open `market.duckdb`
- If IB returns an empty head timestamp, the fetcher falls back to `IB_EARLIEST_DATE` instead of skipping the symbol
- `--asset-class volatility` uses `Index('SYMBOL', 'CBOE')` contracts instead of `Stock('SYMBOL', 'SMART')` and writes to `data-lake/bronze/asset_class=volatility/`
- `--asset-class futures` uses `Future(root, expiry, exchange)` contracts with composite tickers (`ES_202506`), writes to `data-lake/bronze/asset_class=futures/`, and uses the futures parquet schema (contract_id, root_symbol, expiry_date, settlement, open_interest)

### Backfill mode

`--backfill` fetches only missing older data for tickers already in bronze parquet:
- Queries each ticker's oldest existing `trade_date` from parquet
- Fetches IB inception → oldest_date gap
- Merges older rows into the canonical parquet snapshot
- Uses separate cursor JSON: `cursor_backfill_{name}.json`
- Skips tickers not in bronze parquet (use normal fetch first)

### Auto-restarting runner

```bash
bash scripts/run_backfill_all.sh   # Runs all presets with stall detection + auto-restart
```

Output: per-ticker bronze Parquet at `data-lake/bronze/asset_class=equity/symbol=<ticker>/data.parquet` (or `asset_class=futures/symbol=ES_202506/data.parquet` for futures). DuckDB is rebuilt separately when needed.

### Atomic all-universe refresh and rebuild

Phase 3 consumers should use `scripts/refresh_all_and_rebuild.py` when they need
a point-in-time DuckDB publication across equity, futures, volatility, and
crypto. The command requires a requested as-of date, a new create-only path for
the pre-refresh inventory, and a manifest path beside the database under the
`current` bundle pointer:

```bash
python scripts/refresh_all_and_rebuild.py \
  --as-of 2026-08-11 \
  --db-path ~/market-warehouse/duckdb/market.duckdb \
  --manifest ~/market-warehouse/duckdb/current/manifest.json
```

The stable database authority is normalized to the immutable-generation-backed
`duckdb/current/market.duckdb` bundle. The pre-refresh inventory is written to
`duckdb/pre-refresh-inventory.json` unless `--inventory-path` is explicitly supplied.

It inventories and hashes every canonical physical Parquet file before source
access, refreshes each identity through its owner-specific pipeline, rejects
identity/date/schema drift, builds all asset classes together in a temporary
DuckDB, validates exact schemas/types/indexes/constraints and per-identity
counts/latest sessions, fsyncs an immutable versioned generation, and atomically
promotes the `current` symlink as the only commit point. Stable readers call
`resolve_current_bundle()` once and use both returned generation paths. No
failed or interrupted run can expose a mismatched database/manifest pair; a
pre-promotion SIGKILL can leave only an unreachable generation. The manifest contains source commit/tree,
per-step status, pre/post inventories, physical schemas and schema hashes,
latest sessions, row counts, and the published database SHA-256.

### Futures preset format

Futures presets use a `contracts` array instead of `tickers`:
```json
{
  "name": "futures-index",
  "asset_class": "futures",
  "contracts": [
    {"root": "ES", "exchange": "CME", "expiry": "202506"},
    {"root": "NQ", "exchange": "CME", "expiry": "202506"}
  ]
}
```
`load_preset()` flattens these into composite tickers (`ES_202506`) and returns an exchange map for contract construction.

Delisted symbols that should no longer participate in future syncs or backfills should be archived outside the canonical sync path under `data-lake/bronze-delisted/asset_class=equity/symbol=<ticker>/data.parquet`.

### Daily updates

`daily_update.py` is a lightweight script for daily scheduled runs (~2,500 tickers). It discovers tickers from bronze parquet, detects gaps vs the latest trading day, fetches only missing bars, validates OHLCV integrity, and atomically rewrites only the affected per-ticker snapshots. If IB leaves unresolved target trading days after validation, the script can recover those dates from the fallback chain before publishing parquet.

```bash
source ~/market-warehouse/.venv/bin/activate
python scripts/daily_update.py                                  # Normal daily run
python scripts/daily_update.py --dry-run                        # Report gaps without fetching
python scripts/daily_update.py --force                          # Run on non-trading day
python scripts/daily_update.py --target-date 2026-03-11        # Recover through a fixed trading date
python scripts/daily_update.py --preset presets/sp500.json      # Limit to preset tickers
python scripts/daily_update.py --host 127.0.0.1 --port 7497 --max-concurrent 4   # Custom IB config
python scripts/daily_update.py --batch-size 25                  # Custom batch size
python scripts/daily_update.py --asset-class volatility          # Daily update for volatility indices
python scripts/daily_update.py --asset-class futures             # Daily update for futures contracts
```

**Scheduling with launchd** (macOS):
```bash
# Copy examples, replace /path/to/repo with your actual repo path
sed "s|/path/to/repo|$(pwd)|g" scripts/com.market-warehouse.daily-update.plist.example > ~/Library/LaunchAgents/com.market-warehouse.daily-update.plist
sed "s|/path/to/repo|$(pwd)|g" scripts/com.market-warehouse.daily-update-watchdog.plist.example > ~/Library/LaunchAgents/com.market-warehouse.daily-update-watchdog.plist
launchctl load ~/Library/LaunchAgents/com.market-warehouse.daily-update.plist
launchctl load ~/Library/LaunchAgents/com.market-warehouse.daily-update-watchdog.plist
```
`scripts/run_daily_update.sh` sources `~/.secrets` (for API keys like `CEREBRAS_API_KEY`), loads `.env` files, activates the warehouse venv, and runs `scripts/run_daily_update_job.py`, which retries failed sync attempts before terminal failure. The `~/.secrets` source is necessary because launchd does not run a login shell, so `~/.zshrc` is never executed. The runner automatically syncs equities and futures via IB, then all volatility indices via CBOE's public API in a single invocation; pass `--asset-class <name>` to run only one IB asset class (skips CBOE volatility sync).

The main sync runs at 13:05 Pacific local time daily (4:05 PM Eastern year-round). The watchdog runs at 18:30 Pacific by default and alerts if the scheduled sync never started or never logged a completion marker. Non-trading days are harmless no-ops.

**Key design:**
- Discovers tickers from parquet via `BronzeClient.get_latest_dates()` — no hardcoded lists
- `--target-date YYYY-MM-DD` lets operators run a fixed-date catch-up and prevents bars later than the requested target from being published
- Live service writes avoid DuckDB file-lock contention
- Bar validation: checks OHLCV relationships, positive prices, valid trading days, duplicate dates
- Atomically rewrites a per-ticker bronze snapshot after each successful merge
- The active sync universe is the canonical bronze tree only; archive delisted symbols outside that tree if they should stop participating in future syncs/backfills
- Recovery path for unresolved target-day gaps (equity only): Nasdaq historical quote API (`stocks`, then `etf`) and then Stooq `symbol.us`; fallback is skipped for non-equity asset classes (volatility, futures)
- Fallback bars use the same validation and bronze merge path as IB bars
- Run summary exposes `Fallback attempts`, `Fallback successes`, and `Fallback symbols`
- Pure-Python NYSE trading calendar — no new dependencies
- Logs to `~/market-warehouse/logs/daily_update_YYYY-MM-DD.log`
- Terminal scheduled failures use the Nodemailer CLI at `scripts/send_daily_update_failure_email.mjs`
- Failure alerts can write a sibling `.human.md` incident report and optionally enrich the email body with a Cerebras-generated summary plus proposed remediation
- Failure emails can include Cerebras-generated human-readable incident summaries and write a sibling `*.human.md` incident report beside the raw log

### Rebuilding DuckDB

```bash
source ~/market-warehouse/.venv/bin/activate
python scripts/rebuild_duckdb_from_parquet.py                           # Rebuild equity data (default)
python scripts/rebuild_duckdb_from_parquet.py --asset-class volatility  # Rebuild volatility data
python scripts/rebuild_duckdb_from_parquet.py --asset-class futures     # Rebuild futures data
```

This repopulates `~/market-warehouse/duckdb/market.duckdb` from the canonical bronze parquet tree when you want a fresh analytical DB file. The rebuild path recreates the analytical tables from scratch on each run, so rerunning it against an existing DuckDB file is safe. The `--asset-class` flag derives the correct bronze directory and sets the `venue` in `md.symbols` (`SMART` for equity, `CBOE` for volatility). Futures use `replace_futures_from_parquet()` which populates `md.futures_daily` directly (no `md.symbols` entries).

### Querying

```bash
duckdb ~/market-warehouse/duckdb/market.duckdb \
  "SELECT s.symbol, count(*) FROM md.symbols s JOIN md.equities_daily e ON s.symbol_id = e.symbol_id GROUP BY s.symbol"
```

## Testing

**All new code in `clients/` and `scripts/` must have tests. Coverage is enforced at 100% for the source currently included by `pyproject.toml`; `clients/ib_client.py` is still omitted from the fail-under gate and covered by focused tests separately.**

```bash
source ~/market-warehouse/.venv/bin/activate
python -m pytest tests/ -v                                                        # Run all
python -m pytest tests/ -v --cov=clients --cov=scripts --cov-report=term-missing  # With coverage
python -m pytest tests/ -v -m "not integration"                                   # Unit tests only
python -m pytest tests/ -v -W error::RuntimeWarning                               # Catch leaked coroutine warnings
# Native macOS tests are now in the standalone Sift repo at ~/dev/apps/util/sift
```

### Rules for new code

1. Add tests in `tests/test_<module>.py`
2. Mock all external I/O (IB connections via `MagicMock`, file paths via `patch`)
3. Use `tmp_duckdb` / `db` fixtures from `conftest.py` for DB tests
4. Mark DB tests with `@pytest.mark.integration`
5. Run coverage and confirm 100% before committing
6. Run `-W error::RuntimeWarning` at least once before committing when script tests mock async runners such as `ib.ib.run(...)`
7. `pyproject.toml` enforces `fail_under = 100`; `if __name__ == "__main__"` blocks are excluded
8. `clients/ib_client.py` is excluded from the coverage fail-under gate, but focused behavior tests now live in `tests/test_ib_client.py`

### Test deps

`pytest`, `pytest-cov`, `responses` (installed in `~/market-warehouse/.venv/`)

## Pre-commit Hook

A secrets scanner runs on every commit, checking staged files for API keys, passwords, private keys, tokens, and credentials. Install with:

```bash
ln -sf ../../scripts/pre-commit-secrets-scan.sh .git/hooks/pre-commit
```

Catches: AWS keys, API key/secret/password assignments, private key headers, GitHub/Slack tokens, Google API keys, connection strings with credentials, hardcoded IB credentials, staged `.env` files. Allowlists test files, placeholders, comments, `os.environ` reads, and error messages to avoid false positives. Bypass with `git commit --no-verify` if needed.

## Key Implementation Details

- IB BarData provides native float/int types — no string parsing needed
- `symbol_id` is now a stable 53-bit hash from `blake2b(symbol)` for new symbols
- Live ingestion writes bronze parquet directly; DuckDB is rebuilt from bronze when needed
- Empty IB head timestamps now fall back to the earliest supported IB historical date instead of skipping the symbol
- Bronze Parquet uses per-ticker Hive-partitioned layout: `data-lake/bronze/asset_class=equity/symbol=AAPL/data.parquet` (futures: `asset_class=futures/symbol=ES_202506/data.parquet`)
- Bronze publication is atomic at the file level: write temp parquet, validate it, then `os.replace()` into place
- `BronzeClient` accepts `asset_class` constructor param (`"equity"`, `"volatility"`, or `"futures"`) to select the appropriate parquet schema. Default `"equity"` preserves all existing behavior.
- `IBClient.connect()` auto-retries successive `clientId` values after IB error `326`, then records the actual connected ID

## Known Environment Gotchas

Common traps that derail debugging sessions — check these before investigating further:

- **IB Gateway availability**: Always check `~/ibc/logs/ibc-gateway-service.log` and port 4001 before assuming IB is reachable. The secure LaunchAgent may not be running.
- **DuckDB file locks**: Never open `market.duckdb` from the live service path. The daily update intentionally avoids DuckDB writes — this is by design, not a bug.
- **Empty IB head timestamps**: IB returns empty head timestamps for some symbols. The fallback to `IB_EARLIEST_DATE` is intentional — don't treat it as an error.
- **IB error 326 (client ID in use)**: Handled by auto-retry in `IBClient.connect()`. Don't manually reassign client IDs.
- **Weekend/holiday runs**: IB returns no data on non-trading days. These are harmless no-ops — don't debug "no data returned" on weekends or holidays.
- **CBOE volatility fetch**: Volatility indices use CBOE's public API, not IB. If VIX data looks stale, check `fetch_cboe_volatility.py`, not IB connectivity.
- **Docker vs native Gateway**: Both bind to `127.0.0.1:4001` by default. Don't run both simultaneously — they'll conflict on the port. Set `MDW_IB_HOST`/`MDW_IB_PORT` only when connecting to a remote Docker host.
- **Cloud gateway + local gateway**: IB allows only one active session per login. Running both causes `EXISTING_SESSION_DETECTED` displacement. Cold cutover only — stop one before starting the other.
- **Cloud gateway connectivity**: If `ib-gateway:4001` is unreachable, check in order: (1) Tailscale connected locally (`tailscale status`), (2) VPS Tailscale online (`ssh radon@ib-gateway`), (3) socat proxy running (`sudo systemctl status ib-gateway-proxy`), (4) Docker container healthy (`docker compose -f /home/radon/radon-cloud/docker-compose.yml ps`). Break-glass: Hetzner web console.
- **Cloud gateway health check**: The `nc` binary is not available inside the IB Gateway container image (`ghcr.io/gnzsnz/ib-gateway`), so Docker health checks using `nc -z` always fail. The container will show `(unhealthy)` even when IB Gateway is running normally. Check connectivity from the host with `nc -z 127.0.0.1 4001` or via the Radon API health endpoint instead.
- **Radon API pool init**: Radon's IB pool connects during the FastAPI lifespan. If the IB Gateway container restarts, kill the Radon API process (`kill $(pgrep -f 'uvicorn scripts.api.server')`) and let systemd restart it so the pool reconnects. The health endpoint (`/health`) may show the pool as connected while route handlers report "IB pool not initialized" if the module was loaded under a different sys.modules key — a full process restart resolves this.
- **tailscale serve vs socat**: `tailscale serve --tcp` adds TLS which breaks IB's raw TCP protocol. The cloud gateway uses socat systemd services to bridge Tailscale IP traffic to Docker's localhost-bound ports instead.

## Radon API Mode

Scripts can fetch IB historical data via the Radon FastAPI server instead of connecting directly to IB Gateway. This is the preferred path — it eliminates the need for a local Tailscale connection, reduces IB client ID contention, and routes through Caddy with TLS on the VPS.

### Configuration

Set both env vars to enable API mode (in `.env` or shell):

```bash
MDW_RADON_API_URL=https://app.radon.run/api/ib
MDW_API_KEY=<64-char-hex-key>
```

When set, `fetch_ib_historical.py` and `daily_update.py` automatically use the Radon API. If the API is unreachable (connectivity/timeout), they fall back to direct IB Gateway. Auth errors (401/403) fail fast — no silent fallback.

### Architecture

```
MDW scripts
  → RadonApiProvider (HTTP, X-API-Key auth)
    → Caddy (TLS, app.radon.run, handle_path /api/ib/* → localhost:8321)
      → Radon FastAPI / uvicorn (/historical/bars, /contract/qualify)
        → IBPool "data" role (client_id=5)
          → IB Gateway (Docker, localhost:4001)
```

### Provider Interface

`clients/historical_provider.py` defines:
- `HistoricalProvider` — abstract interface
- `IBProvider` — direct IB Gateway via ib_insync
- `RadonApiProvider` — HTTP calls to Radon FastAPI
- `IBClientAdapter` — makes RadonApiProvider drop-in compatible with existing script code
- `create_ib_client_or_adapter()` — factory that auto-selects based on env vars

### Date Formats

All dates are ISO format:
- Bar dates: `YYYY-MM-DD` (e.g., `2025-01-02`)
- Head timestamps: ISO 8601 datetime (e.g., `2010-01-04T09:30:00`)
