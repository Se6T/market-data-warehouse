#!/usr/bin/env python3
"""IBKR-first daily volatility refresh; CBOE is an explicit per-symbol fallback.

Preserves existing canonical identities and uses the existing atomic volatility
writer. Historical rows already stored are not silently replaced by another
provider. No trading or account mutations are performed.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ib_insync import IB, Index
from scripts._refresh_result import write_result
from scripts.daily_update import is_trading_day
from scripts.fetch_cboe_volatility import (
    bars_to_table, fetch_cboe_historical, write_bronze_parquet,
)


async def fetch_ib_daily(symbols: list[str], target: date, host: str, port: int) -> dict[str, list[dict]]:
    """Bounded, read-only IB session. Unresolved identities fall back individually."""
    ib = IB()
    results: dict[str, list[dict]] = {}
    try:
        # Nonzero, dedicated market-data namespace; never bind the order client.
        await ib.connectAsync(host, port, clientId=839280, readonly=True, timeout=12)
        semaphore = asyncio.Semaphore(3)

        async def fetch(symbol: str) -> None:
            async with semaphore:
                try:
                    qualified = await asyncio.wait_for(
                        ib.qualifyContractsAsync(Index(symbol, 'CBOE', 'USD')), 15,
                    )
                    if not qualified:
                        return
                    bars = await asyncio.wait_for(ib.reqHistoricalDataAsync(
                        qualified[0], endDateTime='', durationStr='2 Y',
                        barSizeSetting='1 day', whatToShow='TRADES', useRTH=True,
                        formatDate=1, timeout=25,
                    ), 30)
                    results[symbol] = [
                        {'date': str(b.date), 'open': b.open, 'high': b.high,
                         'low': b.low, 'close': b.close, 'volume': 0}
                        for b in bars if date.fromisoformat(str(b.date)) <= target
                    ]
                except Exception as exc:
                    print(f'{symbol}: IB failed ({type(exc).__name__}: {exc}); trying CBOE', flush=True)
        await asyncio.gather(*(fetch(symbol) for symbol in symbols))
    except Exception as exc:
        print(f'IB volatility session unavailable ({type(exc).__name__}: {exc}); trying CBOE', flush=True)
    finally:
        ib.disconnect()
    return results


def refresh(symbols: list[str], target: date, warehouse: Path, host: str, port: int) -> dict[str, str]:
    expected = target
    from datetime import timedelta
    while not is_trading_day(expected):
        expected -= timedelta(days=1)
    ib_bars = asyncio.run(fetch_ib_daily(symbols, target, host, port))
    statuses = {symbol: 'failed' for symbol in symbols}
    for symbol in symbols:
        try:
            bars = ib_bars.get(symbol, [])
            source = 'IBKR'
            if not bars or max(date.fromisoformat(b['date']) for b in bars) != expected:
                source = 'CBOE-fallback'
                bars = [b for b in fetch_cboe_historical(symbol) if date.fromisoformat(b['date']) <= target]
            if not bars or max(date.fromisoformat(b['date']) for b in bars) != expected:
                raise ValueError(f'no completed {expected} bar from IBKR or CBOE')
            table = bars_to_table(symbol, bars)
            if table is None:
                raise ValueError('no canonical daily rows')
            write_bronze_parquet(table, symbol, warehouse)
            statuses[symbol] = 'succeeded'
            print(json.dumps({'symbol': symbol, 'source': source, 'latest_session': str(expected), 'status': 'succeeded'}), flush=True)
        except Exception as exc:
            print(f'{symbol}: refresh failed ({type(exc).__name__}: {exc})', flush=True)
    return statuses


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--symbols', nargs='+', required=True)
    parser.add_argument('--end', type=date.fromisoformat, required=True)
    parser.add_argument('--warehouse', type=Path, required=True)
    parser.add_argument('--host', default=os.getenv('MDW_IB_HOST', '127.0.0.1'))
    parser.add_argument('--port', type=int, default=int(os.getenv('MDW_IB_PORT', '4002')))
    parser.add_argument('--result-json', type=Path)
    args = parser.parse_args()
    statuses = refresh(args.symbols, args.end, args.warehouse, args.host, args.port)
    if args.result_json is not None:
        write_result(args.result_json, 'volatility', args.symbols, statuses)
    return 0 if all(value == 'succeeded' for value in statuses.values()) else 1


from scripts._entrypoint import run_main
run_main(__name__, main, exit_with_result=True)
