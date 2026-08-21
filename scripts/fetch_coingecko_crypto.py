#!/usr/bin/env python3
"""Fetch CoinGecko crypto OHLCV data into canonical bronze parquet.

The warehouse's current canonical OHLCV schema is daily-bar oriented:

    trade_date, symbol_id, open, high, low, close, adj_close, volume

This script therefore writes CoinGecko data as daily bars under:

    ~/market-warehouse/data-lake/bronze/asset_class=crypto/symbol=<SYMBOL>/data.parquet

CoinGecko OHLC endpoints do not include volume, so volume is joined from the
market_chart/range total_volumes series in the same quote currency. For USD
runs, volume is USD quote volume rounded to an integer.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import httpx
from rich.console import Console

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from clients.bronze_client import BronzeClient

console = Console()

ASSET_CLASS = "crypto"
DEFAULT_WAREHOUSE = Path.home() / "market-warehouse"
DEFAULT_BASE_URL = "https://api.coingecko.com/api/v3"
DEFAULT_VS_CURRENCY = "usd"

# Operator convenience map. Unknown symbols can still be fetched with
# --coins <coingecko-id>:<warehouse-symbol>.
SYMBOL_TO_COIN_ID = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "SOL": "solana",
    "BNB": "binancecoin",
    "XRP": "ripple",
    "ADA": "cardano",
    "DOGE": "dogecoin",
    "AVAX": "avalanche-2",
    "LINK": "chainlink",
    "LTC": "litecoin",
    "BCH": "bitcoin-cash",
    "DOT": "polkadot",
    "MATIC": "matic-network",
    "POL": "polygon-ecosystem-token",
}


@dataclass(frozen=True)
class CoinRequest:
    """A CoinGecko coin ID mapped to a warehouse symbol."""

    coin_id: str
    symbol: str


def parse_date(value: str) -> date:
    """Parse YYYY-MM-DD into a date."""
    return date.fromisoformat(value)


def date_to_unix_seconds(value: date, *, end_of_day: bool = False) -> int:
    """Convert a UTC date boundary to Unix seconds."""
    if end_of_day:
        dt = datetime(value.year, value.month, value.day, 23, 59, 59, tzinfo=timezone.utc)
    else:
        dt = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    return int(dt.timestamp())


def normalize_symbol(symbol: str) -> str:
    """Normalize a warehouse crypto symbol."""
    return symbol.strip().upper().replace("/", "_").replace("-", "_")


def parse_coin_spec(spec: str) -> CoinRequest:
    """Parse '--coins' entries.

    Supported forms:
    - bitcoin:BTC  -> CoinGecko ID bitcoin, warehouse symbol BTC
    - bitcoin      -> CoinGecko ID bitcoin, warehouse symbol BITCOIN
    """
    raw = spec.strip()
    if not raw:
        raise ValueError("empty coin spec")
    if ":" in raw:
        coin_id, symbol = raw.split(":", 1)
        return CoinRequest(coin_id=coin_id.strip(), symbol=normalize_symbol(symbol))
    return CoinRequest(coin_id=raw, symbol=normalize_symbol(raw))


def requests_from_symbols(symbols: Iterable[str]) -> list[CoinRequest]:
    """Map common ticker symbols to CoinGecko IDs."""
    requests: list[CoinRequest] = []
    for item in symbols:
        symbol = normalize_symbol(item)
        try:
            coin_id = SYMBOL_TO_COIN_ID[symbol]
        except KeyError as exc:
            raise ValueError(
                f"No built-in CoinGecko mapping for symbol {symbol!r}; "
                f"use --coins <coingecko-id>:{symbol} instead"
            ) from exc
        requests.append(CoinRequest(coin_id=coin_id, symbol=symbol))
    return requests


def load_preset(path: Path) -> list[CoinRequest]:
    """Load CoinGecko crypto requests from a preset JSON file.

    Accepted shapes:
    - {"symbols": ["BTC", "ETH"]}
    - {"tickers": ["BTC", "ETH"]}
    - {"coins": [{"id": "bitcoin", "symbol": "BTC"}, ...]}
    - {"coins": ["bitcoin:BTC", "ethereum:ETH"]}
    """
    with path.open() as f:
        data = json.load(f)

    if "coins" in data:
        requests: list[CoinRequest] = []
        for item in data["coins"]:
            if isinstance(item, str):
                requests.append(parse_coin_spec(item))
            else:
                requests.append(
                    CoinRequest(
                        coin_id=str(item["id"]),
                        symbol=normalize_symbol(str(item.get("symbol", item["id"]))),
                    )
                )
        return requests

    symbols = data.get("symbols", data.get("tickers", []))
    return requests_from_symbols(symbols)


def build_headers(api_key: str | None, api_tier: str) -> dict[str, str]:
    """Build CoinGecko auth headers without logging the key."""
    headers = {"accept": "application/json"}
    if not api_key:
        return headers
    header_name = "x-cg-pro-api-key" if api_tier == "pro" else "x-cg-demo-api-key"
    headers[header_name] = api_key
    return headers


def coingecko_get(
    path: str,
    *,
    params: dict[str, Any],
    api_key: str | None,
    api_tier: str,
    base_url: str = DEFAULT_BASE_URL,
    timeout: float = 45.0,
) -> Any:
    """GET a CoinGecko endpoint and return decoded JSON."""
    url = f"{base_url.rstrip('/')}/{path.lstrip('/')}"
    response: httpx.Response | None = None
    for attempt in range(4):
        response = httpx.get(
            url,
            params=params,
            headers=build_headers(api_key, api_tier),
            timeout=timeout,
        )
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError:
            if response.status_code != 429 or attempt == 3:
                raise
            retry_after = response.headers.get("Retry-After")
            delay = float(retry_after) if retry_after is not None else 15.0 * (attempt + 1)
            time.sleep(max(delay, 0.0))
            continue
        return response.json()
    raise RuntimeError("CoinGecko retry loop exhausted")


def fetch_ohlc_range(
    coin_id: str,
    *,
    vs_currency: str,
    start: date,
    end: date,
    frequency: str,
    api_key: str | None,
    api_tier: str,
    base_url: str = DEFAULT_BASE_URL,
) -> list[list[float]]:
    """Fetch CoinGecko OHLC range data."""
    params = {
        "vs_currency": vs_currency,
        "from": date_to_unix_seconds(start),
        "to": date_to_unix_seconds(end, end_of_day=True),
        "interval": frequency,
    }
    data = coingecko_get(
        f"/coins/{coin_id}/ohlc/range",
        params=params,
        api_key=api_key,
        api_tier=api_tier,
        base_url=base_url,
    )
    return list(data)


def fetch_market_chart_range(
    coin_id: str,
    *,
    vs_currency: str,
    start: date,
    end: date,
    api_key: str | None,
    api_tier: str,
    base_url: str = DEFAULT_BASE_URL,
) -> dict[str, Any]:
    """Fetch CoinGecko market chart range data for volumes."""
    params = {
        "vs_currency": vs_currency,
        "from": date_to_unix_seconds(start),
        "to": date_to_unix_seconds(end, end_of_day=True),
    }
    return coingecko_get(
        f"/coins/{coin_id}/market_chart/range",
        params=params,
        api_key=api_key,
        api_tier=api_tier,
        base_url=base_url,
    )


def fetch_market_chart_daily(
    coin_id: str,
    *,
    vs_currency: str,
    api_key: str | None,
    api_tier: str,
    base_url: str = DEFAULT_BASE_URL,
) -> dict[str, Any]:
    """Fetch the free 365-day daily close/volume series.

    CoinGecko's OHLC range endpoint requires a paid plan for long windows. The
    public market-chart endpoint still supplies enough history for the
    operational 252-day momentum lookback. OHLC fields are equal in the
    fallback because the payload contains one daily close, not intraday extrema.
    """
    return coingecko_get(
        f"/coins/{coin_id}/market_chart",
        params={"vs_currency": vs_currency, "days": 365, "interval": "daily"},
        api_key=api_key,
        api_tier=api_tier,
        base_url=base_url,
    )


def market_chart_to_close_rows(
    market_chart: dict[str, Any], *, start: date, end: date
) -> list[dict[str, Any]]:
    """Convert market-chart closes to canonical daily rows within [start, end]."""
    volume_lookup = volumes_by_date(market_chart)
    prices: dict[date, float] = {}
    for timestamp_ms, value in market_chart.get("prices", []):
        trade_date = _timestamp_ms_to_date(timestamp_ms)
        if start <= trade_date <= end:
            prices[trade_date] = float(value)
    return [
        {
            "trade_date": trade_date.isoformat(),
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "adj_close": close,
            "volume": volume_lookup.get(trade_date, 0),
        }
        for trade_date, close in sorted(prices.items())
    ]


def _timestamp_ms_to_date(timestamp_ms: int | float) -> date:
    return datetime.fromtimestamp(float(timestamp_ms) / 1000.0, tz=timezone.utc).date()


def volumes_by_date(market_chart: dict[str, Any]) -> dict[date, int]:
    """Convert CoinGecko total_volumes series to date -> integer quote volume.

    If CoinGecko returns multiple observations for a date, the latest observation
    for that date wins. For daily range calls that is normally one point/day.
    """
    volumes: dict[date, int] = {}
    for timestamp_ms, volume in market_chart.get("total_volumes", []):
        volumes[_timestamp_ms_to_date(timestamp_ms)] = int(round(float(volume)))
    return volumes


def ohlcv_to_rows(
    ohlc: list[list[float]],
    market_chart: dict[str, Any],
) -> list[dict[str, Any]]:
    """Convert CoinGecko OHLC + market_chart volume into bronze rows."""
    volume_lookup = volumes_by_date(market_chart)
    rows: list[dict[str, Any]] = []
    for item in ohlc:
        if len(item) < 5:
            continue
        timestamp_ms, open_, high, low, close = item[:5]
        trade_date = _timestamp_ms_to_date(timestamp_ms)
        rows.append(
            {
                "trade_date": trade_date.isoformat(),
                "open": float(open_),
                "high": float(high),
                "low": float(low),
                "close": float(close),
                "adj_close": float(close),
                "volume": volume_lookup.get(trade_date, 0),
            }
        )
    return rows


def fetch_coin_rows(
    request: CoinRequest,
    *,
    vs_currency: str,
    start: date,
    end: date,
    frequency: str,
    api_key: str | None,
    api_tier: str,
    base_url: str = DEFAULT_BASE_URL,
) -> list[dict[str, Any]]:
    """Fetch and convert rows for one coin."""
    try:
        ohlc = fetch_ohlc_range(
            request.coin_id,
            vs_currency=vs_currency,
            start=start,
            end=end,
            frequency=frequency,
            api_key=api_key,
            api_tier=api_tier,
            base_url=base_url,
        )
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code not in {401, 403, 404}:
            raise
        market_chart = fetch_market_chart_daily(
            request.coin_id,
            vs_currency=vs_currency,
            api_key=api_key,
            api_tier=api_tier,
            base_url=base_url,
        )
        return market_chart_to_close_rows(market_chart, start=start, end=end)
    market_chart = fetch_market_chart_range(
        request.coin_id,
        vs_currency=vs_currency,
        start=start,
        end=end,
        api_key=api_key,
        api_tier=api_tier,
        base_url=base_url,
    )
    return ohlcv_to_rows(ohlc, market_chart)


def resolve_requests(args: argparse.Namespace) -> list[CoinRequest]:
    """Resolve CLI request selection."""
    if args.symbols:
        return requests_from_symbols(args.symbols)
    if args.coins:
        return [parse_coin_spec(spec) for spec in args.coins]
    if args.preset:
        return load_preset(args.preset)
    return requests_from_symbols(["BTC", "ETH"])


def write_rows(symbol: str, rows: list[dict[str, Any]], warehouse: Path) -> int:
    """Merge rows into canonical crypto bronze parquet."""
    bronze_dir = warehouse / "data-lake" / "bronze" / f"asset_class={ASSET_CLASS}"
    with BronzeClient(bronze_dir=bronze_dir, asset_class=ASSET_CLASS) as bronze:
        return bronze.merge_ticker_rows(symbol, rows)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--symbols", nargs="+", help="Common ticker symbols, e.g. BTC ETH SOL")
    source.add_argument(
        "--coins",
        nargs="+",
        help="CoinGecko coin specs: bitcoin:BTC ethereum:ETH, or bare IDs",
    )
    source.add_argument("--preset", type=Path, help="JSON preset with symbols/tickers/coins")
    parser.add_argument(
        "--frequency",
        choices=["daily"],
        default="daily",
        help="Bar frequency. Current warehouse-compatible bronze schema supports daily bars.",
    )
    parser.add_argument("--start", required=True, type=parse_date, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, type=parse_date, help="End date YYYY-MM-DD")
    parser.add_argument("--vs-currency", default=DEFAULT_VS_CURRENCY, help="Quote currency (default: usd)")
    parser.add_argument("--warehouse", type=Path, default=DEFAULT_WAREHOUSE, help=f"Warehouse root (default: {DEFAULT_WAREHOUSE})")
    parser.add_argument("--api-key", default=None, help="CoinGecko API key. Prefer COINGECKO_API_KEY env var.")
    parser.add_argument(
        "--api-tier",
        choices=["demo", "pro"],
        default=os.getenv("COINGECKO_API_TIER", "demo"),
        help="Header type for API key (default: demo)",
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Override CoinGecko base URL for tests")
    parser.add_argument("--dry-run", action="store_true", help="Resolve and print planned requests without fetching or writing")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if args.end < args.start:
        raise ValueError("--end must be on or after --start")

    requests = resolve_requests(args)
    api_key = args.api_key or os.getenv("COINGECKO_API_KEY")

    console.print(
        f"[bold]CoinGecko crypto ingestion[/bold]: {len(requests)} symbols, "
        f"{args.frequency}, {args.start} → {args.end}, vs={args.vs_currency.upper()}"
    )
    for request in requests:
        console.print(f"  {request.symbol}: coin_id={request.coin_id}")

    if args.dry_run:
        console.print("[yellow]Dry run: no HTTP requests and no parquet writes.[/yellow]")
        return

    for request in requests:
        rows = fetch_coin_rows(
            request,
            vs_currency=args.vs_currency,
            start=args.start,
            end=args.end,
            frequency=args.frequency,
            api_key=api_key,
            api_tier=args.api_tier,
            base_url=args.base_url,
        )
        if not rows:
            console.print(f"  [yellow]{request.symbol}: no rows returned[/yellow]")
            continue
        inserted = write_rows(request.symbol, rows, args.warehouse)
        console.print(
            f"  [green]{request.symbol}: merged {inserted} new rows "
            f"({rows[0]['trade_date']} → {rows[-1]['trade_date']})[/green]"
        )

    console.print("[bold green]Done.[/bold green]")


if __name__ == "__main__":
    main()
