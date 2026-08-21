# IBKR Paper Gateway Working Procedure

Date: 2026-06-02

## Problem

The Docker IB Gateway was reachable on host port `127.0.0.1:4002`, but the IBKR API was not usable. Socket checks could pass while the actual Java Gateway was still blocked at login, so `ib_insync` calls timed out and the trading runner could not load account state or place paper orders.

The earlier diagnosis was confused by an incorrect paper username. The wrong username attempted was `amemjrlsy847` / `AMEMJRLSY847`. The correct paper login username is `mjrlsy847`, and the target paper account is `DUA983463`.

## Root cause

For `ghcr.io/gnzsnz/ib-gateway:stable` in single paper mode (`TRADING_MODE=paper`), the effective login is:

- `TWS_USERID`
- `TWS_PASSWORD_FILE`

`TWS_USERID_PAPER` / `TWS_PASSWORD_PAPER_FILE` are useful to keep aligned, but they are not the primary login path in single-paper mode. The Gateway was therefore logging in with the wrong effective user until `.env` was corrected and the container was clean-restarted.

## Solution applied

In `/home/sebastian/projects/market-data-warehouse/docker/ib-gateway/.env`, set:

```dotenv
TWS_USERID=mjrlsy847
TWS_USERID_PAPER=mjrlsy847
TRADING_MODE=paper
READ_ONLY_API=no
IB_PAPER_PORT=4002
IB_PASSWORD_FILE=./secrets/ib_password.txt
IBKR_PAPER_ACCOUNT=DUA983463
PAPER_ACCOUNT=DUA983463
```

Password remains in:

```text
/home/sebastian/projects/market-data-warehouse/docker/ib-gateway/secrets/ib_password.txt
```

Never print or paste that password into logs.

Then clean-restart the Gateway and remove the stale settings volume:

```bash
cd /home/sebastian/projects/market-data-warehouse/docker/ib-gateway
sudo docker compose down --remove-orphans
sudo docker volume rm ib-gateway_ib-gateway-settings || true
sudo docker compose up -d
```

## Evidence that it worked

Docker/Gateway logs showed:

```text
IBC: Login has completed
DUA983463 Trader Workstation Configuration (Simulated Trading)
Read-Only API checkbox is now set to: false
```

`ib_insync` API handshake returned the expected paper account:

```text
managedAccounts() -> ['DUA983463']
```

A tiny paper round-trip trade succeeded:

```bash
cd /home/sebastian/projects/trend-engine
uv run python scripts/ibkr_paper_roundtrip_smoke.py \
  --host 127.0.0.1 \
  --port 4002 \
  --account DUA983463 \
  --symbol AAPL \
  --quantity 1 \
  --timeout 90
```

Successful output:

```text
BUY 1 AAPL:  Filled, avg fill 310.42, order id 12
SELL 1 AAPL: Filled, avg fill 310.37, order id 13
```

## Exact API health test

Use this when checking whether the Interactive Brokers API is actually working. Do not rely on `nc`, open ports, or Docker health alone.

```bash
cd /home/sebastian/projects/trend-engine
uv run python - <<'PY'
from ib_insync import IB, Stock

ib = IB()
ib.connect('127.0.0.1', 4002, clientId=9846, timeout=20)
try:
    accounts = ib.managedAccounts()
    print('connected', ib.isConnected())
    print('accounts', accounts)
    assert 'DUA983463' in accounts, accounts

    contract = ib.qualifyContracts(Stock('AAPL', 'SMART', 'USD'))[0]
    print('qualified', contract.conId, contract.symbol, contract.exchange, contract.currency)
finally:
    ib.disconnect()
PY
```

A working API prints `connected True`, includes `DUA983463`, and qualifies the AAPL contract.

## Optional paper order smoke test

Only run this when a tiny paper order is acceptable:

```bash
cd /home/sebastian/projects/trend-engine
uv run python scripts/ibkr_paper_roundtrip_smoke.py \
  --host 127.0.0.1 \
  --port 4002 \
  --account DUA983463 \
  --symbol AAPL \
  --quantity 1 \
  --timeout 90
```

## Pitfalls discovered

- Open `4002` socket does not prove API readiness. The Gateway can still be stuck at the login dialog behind a relay.
- Do not use `amemjrlsy847` / `AMEMJRLSY847`; the corrected paper username is `mjrlsy847`.
- `READ_ONLY_API=no` is required for paper order routing.
- Remove the stale Docker volume after credential/login changes, otherwise old Gateway state can survive.
- SPY was rejected due to EU KID restrictions.
- EURUSD was rejected due to FX leverage restrictions.
- AAPL worked for the paper round-trip smoke test.
- The smoke test must set `tif="DAY"` explicitly; relying on Gateway presets caused IBKR API error `10349`.
