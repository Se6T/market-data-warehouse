# IBKR Paper Gateway Login Fix Notes

Date: 2026-06-02

## Current target

- Docker compose directory: `/home/sebastian/projects/market-data-warehouse/docker/ib-gateway`
- Host API target: `127.0.0.1:4002`
- Trading mode: `paper`
- Paper account expected by trading code: `DUA983463`
- Paper login user: `mjrlsy847`
- Password file: `secrets/ib_password.txt` (never print this)

## What was verified

1. Docker config is structurally correct for `gnzsnz/ib-gateway` single-paper mode:
   - `TRADING_MODE=paper`
   - `TWS_USERID=mjrlsy847`
   - `TWS_PASSWORD_FILE=/run/secrets/ib_password`
   - `READ_ONLY_API=no`
   - host `127.0.0.1:4002` maps to container paper relay `4004`.
2. In this image, `TWS_USERID_PAPER` / `TWS_PASSWORD_PAPER*` are only used after the first process when `DUAL_MODE=yes` or `TRADING_MODE=both`. For single paper mode the effective login is `TWS_USERID` + `TWS_PASSWORD_FILE`.
3. Main-user login (`sebtsch59`) with paper mode is not accepted for Gateway API; IBKR returns:
   - `The specified user has multiple Paper Trading users associated with it. Please log on using one of the Paper Trading users and corresponding password.`
4. Earlier attempts used the wrong paper username prefix (`AMEMJRLSY847`). Sebastian corrected the paper username to `mjrlsy847` for paper account `DUA983463`.
5. The `.env` file has been updated to the corrected values:
   - `TWS_USERID=mjrlsy847`
   - `TWS_USERID_PAPER=mjrlsy847`
   - `IBKR_PAPER_ACCOUNT=DUA983463`
   - `PAPER_ACCOUNT=DUA983463`
   - `TRADING_MODE=paper`
   - `READ_ONLY_API=no`
   - `IB_PAPER_PORT=4002`
6. Clean restart with sudo and fresh Gateway settings volume succeeded. Logs showed:
   - `IBC: Login has completed`
   - configuration dialog title: `DUA983463 Trader Workstation Configuration (Simulated Trading)`
   - `Read-Only API checkbox is now set to: false`
7. API handshake on `127.0.0.1:4002` succeeded:
   - `managedAccounts()` returned `['DUA983463']`
   - `AAPL` qualified on SMART/USD.
8. Paper entry and exit trade succeeded using `/home/sebastian/projects/trend-engine/scripts/ibkr_paper_roundtrip_smoke.py`:
   - Command: `uv run python scripts/ibkr_paper_roundtrip_smoke.py --host 127.0.0.1 --port 4002 --account DUA983463 --symbol AAPL --quantity 1 --timeout 90`
   - Entry: BUY 1 AAPL, status `Filled`, avg fill `310.42`, order id `12`
   - Exit: SELL 1 AAPL, status `Filled`, avg fill `310.37`, order id `13`

## Root cause

The root cause was a wrong paper username. The target config is now corrected to `mjrlsy847` + `DUA983463`; after a clean Docker restart and stale volume removal, IBKR Gateway logged into the paper account and accepted API paper orders on `127.0.0.1:4002`.

## Exact working procedure

1. Keep the paper user's actual password in `secrets/ib_password.txt` without printing it:
   ```bash
   cd /home/sebastian/projects/market-data-warehouse/docker/ib-gateway
   install -d -m 700 secrets
   # paste/type password via a non-logging method, then chmod 600 secrets/ib_password.txt
   ```
2. Ensure `.env` has:
   ```dotenv
   TWS_USERID=mjrlsy847
   TWS_USERID_PAPER=mjrlsy847
   IBKR_PAPER_ACCOUNT=DUA983463
   PAPER_ACCOUNT=DUA983463
   TRADING_MODE=paper
   READ_ONLY_API=no
   IB_PAPER_PORT=4002
   IB_PASSWORD_FILE=./secrets/ib_password.txt
   ```
3. Clean restart so stale Gateway settings do not survive:
   ```bash
   cd /home/sebastian/projects/market-data-warehouse/docker/ib-gateway
   sudo docker compose down --remove-orphans
   sudo docker volume rm ib-gateway_ib-gateway-settings || true
   sudo docker compose up -d
   ```
4. Wait for real API readiness (not just the socat port), then verify:
   ```bash
   cd /home/sebastian/projects/trend-engine
   uv run python scripts/ibkr_paper_roundtrip_smoke.py --host 127.0.0.1 --port 4002 --account DUA983463 --symbol AAPL --quantity 1 --timeout 90
   ```
5. Success criterion: the script prints entry and exit fill summaries for `DUA983463`.

## Notes / pitfalls

- Do not use the earlier mistaken username `amemjrlsy847` / `AMEMJRLSY847`.
- A listening `4002` socket is not sufficient; use an `ib_insync` handshake and confirm `managedAccounts()`.
- The smoke test uses AAPL instead of SPY/EURUSD because SPY hit EU KID restrictions and EURUSD hit account FX-leverage restrictions.
- The smoke test sets `tif="DAY"` explicitly because relying on Gateway presets caused API order error `10349`.
