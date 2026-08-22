# Market Data Warehouse

![Market Data Warehouse](https://raw.githubusercontent.com/joemccann/market-data-warehouse/main/.github/github-banner.png)

A **local-first financial data warehouse** for universe-scale market data.

---

## Overview

Market Data Warehouse is designed for storing and analyzing historical **OHLCV data across equities, futures, and volatility indices**, with a clear path from **daily bars → intraday data → production analytics**.

### Core Stack

* **Parquet data lake** → canonical storage
* **DuckDB** → local analytics, research, backtesting
* **ClickHouse (optional)** → large-scale aggregation & concurrency

### Current Capabilities

* Daily ingestion for:

  * **Equities (IB)**
  * **Futures (IB)**
  * **Volatility indices (CBOE API)**
  * **Crypto daily OHLCV (CoinGecko API)**
* Per-ticker **bronze Parquet snapshots**
* **Atomic writes + validation**
* **Fallback recovery pipeline** for missing data
* On-demand **DuckDB rebuilds** from Parquet

> **In one sentence:**
> A local-first, production-ready market data warehouse for serious quantitative workflows.

---

## Goals

* ⚡ High-performance **local quant research environment**
* 🧱 Scalable **multi-asset data model**
* 🔁 Clean **local → production transition**
* 🌐 **Polyglot workflows** (Python, Rust, Node.js)

---

## Architecture

### Data Flow

```text
Raw → Bronze → Silver → Gold
```

* **Raw** → vendor data
* **Bronze** → canonical Parquet (primary ingestion layer)
* **Silver** → cleaned / adjusted datasets
* **Gold** → analytics, factors, derived tables

### Storage Strategy

* **System of record**: Parquet (`data-lake/`)
* **Local engine**: DuckDB
* **Warehouse (optional)**: ClickHouse

---

## Directory Structure

```text
~/market-warehouse/
├── data-lake/
│   ├── raw/
│   ├── bronze/
│   │   ├── asset_class=equity/symbol=AAPL/data.parquet
│   │   ├── asset_class=volatility/symbol=VIX/data.parquet
│   │   ├── asset_class=crypto/symbol=BTC/data.parquet
│   │   └── asset_class=futures/symbol=ES_202506/data.parquet
│   ├── silver/
│   └── gold/
├── duckdb/
│   └── market.duckdb
├── scripts/
├── logs/
└── .venv/
```

---

## Installation

### Requirements

* macOS (Apple Silicon recommended)
* Homebrew
* Python 3.12+
* Node.js 22+
* DuckDB
* [Interactive Brokers](https://ibkr.com/referral/joseph5632) account
* Docker (recommended for IB Gateway)
* ClickHouse (optional)

---

### Quick Start

```bash
chmod +x setup_market_warehouse.sh
./setup_market_warehouse.sh
```

### Full Bootstrap

```bash
./setup_market_warehouse.sh \
  --start-clickhouse \
  --init-clickhouse \
  --with-sample-data \
  --smoke-test
```

---

## [Interactive Brokers](https://ibkr.com/referral/joseph5632) Gateway

You need a running IB Gateway for ingestion.

---

### Option 1 (Recommended): Docker

Uses [`gnzsnz/ib-gateway-docker`](https://github.com/gnzsnz/ib-gateway-docker)

#### Quick Start

```bash
cd docker/ib-gateway
cp .env.example .env
mkdir -p secrets
echo "YOUR_IB_PASSWORD" > secrets/ib_password.txt
docker compose up -d
```

* Complete 2FA via:

  * VNC (`localhost:5900`)
  * IBKR mobile app

#### Ports

| Host | Purpose   |
| ---- | --------- |
| 4001 | Live API  |
| 4002 | Paper API |
| 5900 | VNC       |

---

### Option 2 (Cloud): Hetzner VPS + Tailscale

Run the same Docker Compose setup on a cloud VPS with Tailscale for secure, WireGuard-encrypted access. No public ports exposed — all traffic flows through a private mesh VPN.

#### Why

* IB Gateway runs 24/5 without tying up a local machine
* Accessible from any Tailscale-enrolled device (Mac, iPhone, other servers)
* ~$4-6/mo (Hetzner) + free (Tailscale)

#### Architecture

```text
Hetzner VPS (all public ports blocked)
├── Tailscale (MagicDNS hostname: ib-gateway)
├── socat proxy: tailscale-ip:4001 → localhost:4001
└── Docker: same docker-compose.yml, unmodified
        └── IB Gateway (127.0.0.1:4001)

Clients connect to ib-gateway:4001 via Tailscale tunnel
```

#### Setup

1. **Provision a VPS** (Hetzner CPX11 or similar, Ubuntu 24.04, US East region)
2. **Harden the host**: non-root user, SSH key-only, disable root login, unattended-upgrades
3. **Install Docker and Tailscale** on the VPS
4. **Configure Tailscale**:
   * Authenticate with a preauth key tagged for the gateway
   * Enable MagicDNS for stable hostname resolution
   * Set up ACLs to restrict which devices can reach port 4001
5. **Bridge Tailscale to Docker**: a socat systemd service forwards traffic from the Tailscale interface to Docker's localhost-bound ports (`tailscale serve --tcp` adds TLS, which is incompatible with IB's raw TCP protocol)
6. **Firewall**: ufw denies all incoming except on the Tailscale interface
7. **Deploy**: `scp` the `docker-compose.yml` and `.env` to the VPS, create the password secret, `docker compose up -d`
8. **2FA**: Approve via IBKR mobile app or VNC through Tailscale

#### Client Configuration

```bash
# Set in .env or export in shell
MDW_IB_HOST=ib-gateway    # Tailscale MagicDNS hostname
MDW_IB_PORT=4001
```

All scripts (`fetch_ib_historical.py`, `daily_update.py`, etc.) read `MDW_IB_HOST`/`MDW_IB_PORT` automatically.

#### Phone Access

SSH from iOS/Android (Termius, Blink) to `mdw@ib-gateway` via Tailscale:

```bash
cd ~/ib-gateway
docker compose stop    # stop gateway
docker compose start   # start gateway
docker compose ps      # check status
```

#### Rollback to Local

```bash
unset MDW_IB_HOST      # falls back to 127.0.0.1
```

See [`docker/ib-gateway/README.md`](docker/ib-gateway/README.md) for the full step-by-step provisioning guide, Tailscale ACL policy, client enrollment, 2FA reauth runbook, and volume backup procedures.

---

### Option 3: Native macOS (IBC)

IBC provides:

* Auto login
* Session recovery
* Daily restarts

#### Install

```bash
python scripts/install_ibc_secure_service.py
```

#### Commands

```bash
~/ibc/bin/start-secure-ibc-service.sh
~/ibc/bin/stop-secure-ibc-service.sh
~/ibc/bin/status-secure-ibc-service.sh
```

> The IBC service is **machine-level**, not repo-scoped.

---

## Data Ingestion

### Prerequisites

* IB Gateway running (`127.0.0.1:4001` by default)
* Configurable via:

  * CLI flags (`--host`, `--port`)
  * Env vars (`MDW_IB_HOST`, `MDW_IB_PORT`)

---

### Fetch Historical Data

```bash
source ~/market-warehouse/.venv/bin/activate

# Default (Mag 7)
python scripts/fetch_ib_historical.py

# Specific tickers
python scripts/fetch_ib_historical.py --tickers AAPL NVDA

# Preset universe
python scripts/fetch_ib_historical.py --preset presets/sp500.json

# Futures
python scripts/fetch_ib_historical.py --preset presets/futures-index.json --asset-class futures

# Volatility (CBOE direct)
python scripts/fetch_cboe_volatility.py

# Crypto daily OHLCV (CoinGecko; optional COINGECKO_API_KEY)
python scripts/fetch_coingecko_crypto.py --symbols BTC ETH --frequency daily --start 2024-01-01 --end 2024-12-31
```

---

### Backfill Missing Data

```bash
python scripts/fetch_ib_historical.py --preset presets/sp500.json --backfill
```

* Fills only missing history
* Preserves existing data
* Independent cursor tracking

---

### Daily Updates

```bash
python scripts/daily_update.py
```

Common flags:

```bash
--dry-run
--force
--target-date YYYY-MM-DD
--preset presets/sp500.json
--asset-class {equity|volatility|futures}
```

#### Key Behavior

* Detects missing trading days
* Fetches only gaps
* Validates OHLCV
* Atomic snapshot updates
* Fallback recovery if IB fails

---

### Rebuild DuckDB

```bash
python scripts/rebuild_duckdb_from_parquet.py
```

For the Phase 3 all-universe gate, use the atomic refresh/rebuild command instead:

```bash
python scripts/refresh_all_and_rebuild.py \
  --as-of 2026-08-11 \
  --db-path ~/market-warehouse/duckdb/market.duckdb \
  --manifest ~/market-warehouse/duckdb/current/manifest.json
```

The stable database authority is normalized to the immutable-generation-backed
`duckdb/current/market.duckdb` bundle. The pre-refresh inventory is written to
`duckdb/pre-refresh-inventory.json` unless `--inventory-path` is explicitly supplied.

This command inventories every active equity, futures, volatility, and crypto
Parquet identity; records physical schemas and SHA-256 hashes; and invokes each
non-empty asset-class owner at most once with a bounded per-identity result
artifact. A dedicated PAPER-only owner dynamically discovers, refreshes, and
publishes the current dated VXM contract before inventory freeze without a
static expiry; older dated VXM contracts remain historical. A nonzero owner
exit or ordinary owner exception is recorded as failed identity evidence while
later identities and asset classes continue. The command may still publish an
immutable `degraded` generation when
each failed identity remains exactly its pre-refresh valid bronze file and the
complete inventory has valid schemas/identities, non-regressing row counts and
latest sessions, and no data beyond the requested as-of date. Successful
identities must meet their ordinary freshness target.

If a failed owner changes, removes, corrupts, or regresses its identity—or if
any global integrity or publication check fails—nothing is published and the
command exits unsuccessfully. A published manifest and its embedded `run_result`
carry the `succeeded` or `degraded` outcome, every terminal step status, and a
canonical failed-identity list without owner stderr or other command output. The
`--preserve-broker-assets` mode continues to mark equity/futures as `preserved`
and never accesses their broker owner.

After those gates pass, the command builds and validates a temporary multi-asset
DuckDB, writes an immutable versioned database/manifest generation, and
atomically promotes the `duckdb/current` symlink as the sole commit point.
The inventory path is create-only, so choose a new immutable path for every run.
Any refresh, schema, count, identity, hash, or publication failure leaves the
complete predecessor generation consumable. Stable readers must call
`resolve_current_bundle()` once and use both resolved paths it returns; opening
`current/market.duckdb` and `current/manifest.json` independently is not a
coherent-reader contract. Candidate files and directories are fsynced before the
pointer rename and its parent is fsynced afterward, so SIGKILL before promotion
can leave only an unreachable generation, never a visible mismatched pair.
`--warehouse` and `--db-path` can target an isolated warehouse; the manifest must
remain beside the database under `current`. Tests mock all owner commands and
perform no external I/O.

---

## Scheduling

### macOS (`launchd`)

```bash
launchctl load ~/Library/LaunchAgents/com.market-warehouse.daily-update.plist
```

### Schedule

* **Daily sync**: 13:05 PT (4:05 PM ET)
* **Watchdog**: 18:30 PT

---

## Alerts & Monitoring

* Automatic retries
* Email alerts on failure
* Optional AI-generated summaries

### Setup

```bash
npm install
```

Example config:

```bash
MDW_ALERT_EMAIL_TO="you@example.com"
MDW_ALERT_SMTP_URL="smtp://user:pass@mail.example.com:587"
```


## Testing

### Run Tests

```bash
python -m pytest tests/ -v
```

### Coverage

```bash
python -m pytest tests/ -v --cov=clients --cov=scripts
```

* ✅ **100% coverage enforced**

---

## Security

### Pre-commit Hook

```bash
ln -sf ../../scripts/pre-commit-secrets-scan.sh .git/hooks/pre-commit
```

Detects:

* API keys
* credentials
* private keys
* `.env` leaks

---

## Data Model Notes

### Split-Adjusted Volume

All volume is **split-adjusted** to ensure consistency across time.

---

## ClickHouse (Optional)

Used for:

* Benchmarking
* Concurrency
* Production simulation

### Commands

```bash
~/market-warehouse/scripts/start_clickhouse.sh
~/market-warehouse/scripts/init_clickhouse.sh
~/market-warehouse/scripts/stop_clickhouse.sh
```

---

## Sample Data

```bash
python scripts/write_sample_parquet.py
```

---

## Recommended Workflow

1. Store all data in **Parquet (bronze)**
2. Use **DuckDB** for:

   * research
   * backtesting
3. Use **ClickHouse** for:

   * large-scale queries
   * production-like workloads

---

## Troubleshooting

### DuckDB Errors

* Use inline `PRIMARY KEY`
* Avoid reserved keywords (`right` → `option_right`)

### ClickHouse Issues

```bash
clickhouse-client --query "SELECT version()"
```

---

## Recommended Command

```bash
./setup_market_warehouse.sh \
  --start-clickhouse \
  --init-clickhouse \
  --with-sample-data \
  --smoke-test
```

---

## License

MIT
