#!/usr/bin/env python3
"""Fetch completed Binance major-crypto daily OHLCV into canonical bronze."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any

import httpx
from rich.console import Console

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
from clients.bronze_client import BronzeClient  # noqa: E402

console = Console()
DEFAULT_WAREHOUSE = Path.home() / "market-warehouse"
DEFAULT_BASE_URL = "https://api.binance.com/api/v3"
SYMBOL_TO_BINANCE = {
    "BTC": "BTCUSDT",
    "ETH": "ETHUSDT",
    "SOL": "SOLUSDT",
    "BNB": "BNBUSDT",
    "XRP": "XRPUSDT",
    "ADA": "ADAUSDT",
    "DOGE": "DOGEUSDT",
}


def end_of_day_ms(value: date) -> int:
    boundary = datetime.combine(value, time(23, 59, 59, 999000), tzinfo=timezone.utc)
    return int(boundary.timestamp() * 1000)


def fetch_klines(
    pair: str,
    *,
    end: date,
    limit: int = 1000,
    base_url: str = DEFAULT_BASE_URL,
) -> list[list[Any]]:
    response = httpx.get(
        f"{base_url.rstrip('/')}/klines",
        params={
            "symbol": pair,
            "interval": "1d",
            "limit": limit,
            "endTime": end_of_day_ms(end),
        },
        timeout=45,
    )
    response.raise_for_status()
    return list(response.json())


def klines_to_rows(payload: list[list[Any]], *, end: date) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in payload:
        if len(item) < 6:
            continue
        trade_date = datetime.fromtimestamp(int(item[0]) / 1000, tz=timezone.utc).date()
        if trade_date > end:
            continue
        rows.append(
            {
                "trade_date": trade_date.isoformat(),
                "open": float(item[1]),
                "high": float(item[2]),
                "low": float(item[3]),
                "close": float(item[4]),
                "adj_close": float(item[4]),
                "volume": int(round(float(item[5]))),
            }
        )
    return rows


def write_rows(symbol: str, rows: list[dict[str, Any]], warehouse: Path) -> int:
    root = warehouse / "data-lake" / "bronze" / "asset_class=crypto"
    with BronzeClient(root, asset_class="crypto") as client:
        return client.merge_ticker_rows(symbol, rows)


def load_symbols(path: Path) -> list[str]:
    values = json.loads(path.read_text(encoding="utf-8")).get("symbols", [])
    symbols = [str(value).strip().upper() for value in values if str(value).strip()]
    unknown = sorted(set(symbols) - set(SYMBOL_TO_BINANCE))
    if not symbols or unknown:
        raise ValueError(f"invalid major-crypto preset; unknown={unknown}")
    return list(dict.fromkeys(symbols))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--symbols", nargs="+")
    source.add_argument("--preset", type=Path)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--warehouse", type=Path, default=DEFAULT_WAREHOUSE)
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if not 253 <= args.limit <= 1000:
        raise ValueError("--limit must cover the 252-day momentum lookback and be <=1000")
    symbols = (
        load_symbols(args.preset)
        if args.preset
        else [str(value).strip().upper() for value in (args.symbols or SYMBOL_TO_BINANCE)]
    )
    unknown = sorted(set(symbols) - set(SYMBOL_TO_BINANCE))
    if unknown:
        raise ValueError(f"unknown major crypto symbols: {unknown}")
    for symbol in symbols:
        payload = fetch_klines(
            SYMBOL_TO_BINANCE[symbol], end=args.end, limit=args.limit, base_url=args.base_url
        )
        rows = klines_to_rows(payload, end=args.end)
        if len(rows) < 253:
            raise RuntimeError(f"{symbol}: only {len(rows)} completed daily rows")
        inserted = write_rows(symbol, rows, args.warehouse)
        console.print(
            f"{symbol}: rows={len(rows)} inserted={inserted} "
            f"range={rows[0]['trade_date']}..{rows[-1]['trade_date']}"
        )


if __name__ == "__main__":
    main()
