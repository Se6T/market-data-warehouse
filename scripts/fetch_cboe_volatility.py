#!/usr/bin/env python3
"""Fetch CBOE volatility index historical data directly from CBOE's API.

Primary daily sync source for all CBOE volatility indices. Also used for
historical backfill of indices not available via IB (e.g., VXHYG, VXSMH).
Writes to bronze parquet in the standard warehouse format.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from datetime import date
from pathlib import Path
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from rich.console import Console

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:  # pragma: no cover - standalone script path
    sys.path.insert(0, str(PROJECT_ROOT))
from clients.symbol_ids import stable_symbol_id
from scripts._refresh_result import write_result

console = Console()

_XNYS_CALENDAR: Any = None


def _is_canonical_session(day: str) -> bool:
    """True when the date is a canonical XNYS session.

    CBOE publishes several volatility indices on exchange holidays; the
    admission calendar treats those dates as non-sessions, so a holiday bar
    would make session coverage non-canonical. exchange_calendars is the same
    library the portfolio-engine admission path uses (XNYS).
    """
    global _XNYS_CALENDAR
    if _XNYS_CALENDAR is None:
        import exchange_calendars

        _XNYS_CALENDAR = exchange_calendars.get_calendar(
            "XNYS", start="1990-01-01", end="2050-12-31"
        )
    return bool(_XNYS_CALENDAR.is_session(day))

CBOE_HISTORICAL_URL = "https://cdn.cboe.com/api/global/delayed_quotes/charts/historical/_{symbol}.json"

# CBOE's public history for these indices has unrecoverable source-side holes
# (days the index simply did not publish) before these dates. Bronze coverage
# is clamped to the first complete-coverage session on/after this date so the
# merge path can never re-introduce the pre-clamp holes.
COVERAGE_STARTS: dict[str, str] = {
    "VIX": "2000-01-03",
    "VVIX": "2013-05-14",
    "COR3M": "2020-11-18",
    "OVX": "2020-10-19",
    "RVX": "2020-10-19",
    "VXEEM": "2020-10-19",
}

DEFAULT_WAREHOUSE = Path.home() / "market-warehouse"
SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_PRESET = SCRIPT_DIR.parent / "presets" / "volatility.json"
ASSET_CLASS = "volatility"


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_write_table(table: pa.Table, parquet_path: Path) -> None:
    temporary = parquet_path.with_name(f".{parquet_path.name}.{uuid.uuid4().hex}.tmp")
    try:
        pq.write_table(table, temporary)
        with temporary.open("rb") as handle:
            os.fsync(handle.fileno())
        os.replace(temporary, parquet_path)
        _fsync_directory(parquet_path.parent)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _symbol_id(symbol: str) -> int:
    """Generate a stable numeric ID from symbol string."""
    return stable_symbol_id(symbol)


def fetch_cboe_historical(symbol: str) -> list[dict[str, Any]]:
    """Fetch historical OHLCV data from CBOE's public API."""
    url = CBOE_HISTORICAL_URL.format(symbol=symbol)
    console.print(f"  Fetching {symbol} from {url}")
    
    resp = httpx.get(url, timeout=30)
    resp.raise_for_status()
    
    data = resp.json()
    bars = data.get("data", [])
    console.print(f"  {symbol}: received {len(bars)} bars")
    return bars


def bars_to_table(symbol: str, bars: list[dict[str, Any]]) -> pa.Table:
    """Convert CBOE JSON bars to PyArrow table matching bronze schema.
    
    Note: asset_class and symbol are NOT included in the parquet file;
    they're encoded in the hive partition path (asset_class=X/symbol=Y/).
    """
    if not bars:
        return None
    
    symbol_id = _symbol_id(symbol)
    
    coverage_start = COVERAGE_STARTS.get(symbol)
    if coverage_start is not None:
        clamp = date.fromisoformat(coverage_start)
        bars = [bar for bar in bars if date.fromisoformat(bar["date"]) >= clamp]
        if not bars:
            console.print(f"  {symbol}: no bars on/after coverage start {coverage_start}")
            return None

    # CBOE publishes several volatility indices on exchange holidays; the
    # admission calendar treats those as non-sessions, so a holiday bar makes
    # coverage non-canonical. Keep only canonical equity-session dates.
    bars = [bar for bar in bars if _is_canonical_session(bar["date"])]
    if not bars:
        return None
    
    records = []
    for bar in bars:
        open_price = float(bar["open"])
        reported_high = float(bar["high"])
        reported_low = float(bar["low"])
        close_price = float(bar["close"])
        records.append({
            "trade_date": date.fromisoformat(bar["date"]),
            "symbol_id": symbol_id,
            "open": open_price,
            "high": max(open_price, reported_high, reported_low, close_price),
            "low": min(open_price, reported_high, reported_low, close_price),
            "close": close_price,
            "adj_close": close_price,  # No adjustment for indices
            "volume": int(float(bar["volume"])),
        })
    
    schema = pa.schema([
        ("trade_date", pa.date32()),
        ("symbol_id", pa.int64()),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("adj_close", pa.float64()),
        ("volume", pa.int64()),
    ])
    
    return pa.Table.from_pylist(records, schema=schema)


def write_bronze_parquet(
    table: pa.Table,
    symbol: str,
    warehouse_dir: Path,
) -> Path:
    """Write table to bronze parquet, merging with existing data."""
    bronze_dir = warehouse_dir / "data-lake" / "bronze" / f"asset_class={ASSET_CLASS}" / f"symbol={symbol}"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = bronze_dir / "data.parquet"
    
    # Merge with existing data if present
    if parquet_path.exists():
        existing = pq.ParquetFile(parquet_path).read()

        coverage_start = COVERAGE_STARTS.get(symbol)
        if coverage_start is not None:
            clamp = date.fromisoformat(coverage_start)
            clamped_dates = pa.array([clamp], type=pa.date32())
            kept_mask = pc.greater_equal(existing.column("trade_date"), clamped_dates[0])
            kept_rows = pc.sum(kept_mask).as_py() or 0
            dropped_rows = existing.num_rows - kept_rows
            if dropped_rows > 0:
                existing = existing.filter(kept_mask)
                console.print(
                    f"  {symbol}: dropped {dropped_rows} pre-coverage rows "
                    f"(coverage starts {coverage_start})"
                )

        # Drop holiday bars that CBOE published but the canonical session
        # calendar does not recognize (see _is_canonical_session).
        existing_dates_list = existing.column("trade_date").to_pylist()
        non_session_rows = [
            i
            for i, day in enumerate(existing_dates_list)
            if not _is_canonical_session(day.isoformat())
        ]
        if non_session_rows:
            session_mask = pc.invert(
                pa.array(
                    [i in non_session_rows for i in range(existing.num_rows)],
                    type=pa.bool_(),
                )
            )
            existing = existing.filter(session_mask)
            console.print(
                f"  {symbol}: dropped {len(non_session_rows)} non-session rows"
            )

        # Normalize existing schema to match expected columns (handles schema drift)
        expected_columns = table.column_names
        extra_cols = set(existing.column_names) - set(expected_columns)
        if extra_cols:
            existing = existing.select(expected_columns)
        canonical_id = _symbol_id(symbol)
        identity_changed = set(existing.column("symbol_id").to_pylist()) != {
            canonical_id
        }
        if identity_changed:
            existing = existing.set_column(
                existing.column_names.index("symbol_id"),
                "symbol_id",
                pa.array([canonical_id] * existing.num_rows, type=pa.int64()),
            )
        existing_rows = existing.to_pylist()
        normalized_highs = [
            max(row["open"], row["high"], row["low"], row["close"])
            for row in existing_rows
        ]
        normalized_lows = [
            min(row["open"], row["high"], row["low"], row["close"])
            for row in existing_rows
        ]
        envelope_changed = any(
            row["high"] != high or row["low"] != low
            for row, high, low in zip(existing_rows, normalized_highs, normalized_lows)
        )
        if envelope_changed:
            existing = existing.set_column(
                existing.column_names.index("high"),
                "high",
                pa.array(normalized_highs, type=pa.float64()),
            ).set_column(
                existing.column_names.index("low"),
                "low",
                pa.array(normalized_lows, type=pa.float64()),
            )

        existing_dates = set(
            d.as_py() for d in existing.column("trade_date")
        )

        # Filter to only new dates
        new_dates_mask = pc.invert(
            pc.is_in(
                table.column("trade_date"),
                pa.array(list(existing_dates), type=pa.date32()),
            )
        )
        new_rows = table.filter(new_dates_mask)

        if new_rows.num_rows > 0:
            table = pa.concat_tables([existing, new_rows])
            console.print(f"  {symbol}: merged {new_rows.num_rows} new rows with {existing.num_rows} existing")
        elif extra_cols or identity_changed or envelope_changed:
            # Rewrite stale schema/identity metadata even without new data.
            table = existing
            console.print(f"  {symbol}: rewriting canonical metadata")
        else:
            console.print(f"  {symbol}: no new rows to add")
            return parquet_path
    
    # Sort by date
    indices = pc.sort_indices(table, sort_keys=[("trade_date", "ascending")])
    table = table.take(indices)
    
    _atomic_write_table(table, parquet_path)
    console.print(f"  {symbol}: wrote {table.num_rows} rows to {parquet_path}")
    return parquet_path


def load_preset(preset_path: Path) -> list[str]:
    """Load ticker symbols from a preset JSON file."""
    with preset_path.open() as f:
        data = json.load(f)
    return data.get("tickers", [])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--symbols",
        nargs="+",
        help="CBOE volatility index symbols to fetch",
    )
    group.add_argument(
        "--preset",
        type=Path,
        help=f"Path to preset JSON file (default: {DEFAULT_PRESET})",
    )
    parser.add_argument(
        "--warehouse",
        type=Path,
        default=DEFAULT_WAREHOUSE,
        help=f"Warehouse directory (default: {DEFAULT_WAREHOUSE})",
    )
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument("--result-json", type=Path)
    args = parser.parse_args()
    
    # Determine symbols to fetch
    if args.symbols:
        symbols = args.symbols
    elif args.preset:
        symbols = load_preset(args.preset)
    elif DEFAULT_PRESET.exists():
        symbols = load_preset(DEFAULT_PRESET)
    else:
        symbols = ["VIX", "VVIX"]  # Minimal fallback
    
    console.print(f"\n[bold]Fetching CBOE volatility indices: {symbols}[/bold]\n")
    
    failed = 0
    statuses = {symbol: "failed" for symbol in symbols}
    for symbol in symbols:
        try:
            bars = fetch_cboe_historical(symbol)
            if not bars:
                console.print(f"  [yellow]{symbol}: no data returned[/yellow]")
                failed += 1
                continue
            if args.end is not None:
                bars = [bar for bar in bars if date.fromisoformat(bar["date"]) <= args.end]
                if not bars:
                    console.print(f"  [yellow]{symbol}: no data through {args.end}[/yellow]")
                    failed += 1
                    continue
            
            table = bars_to_table(symbol, bars)
            write_bronze_parquet(table, symbol, args.warehouse)
            statuses[symbol] = "succeeded"
            
            # Show date range
            dates = [date.fromisoformat(b["date"]) for b in bars]
            console.print(f"  {symbol}: {min(dates)} → {max(dates)}\n")
            
        except Exception as e:
            console.print(f"  [red]{symbol}: error - {e}[/red]")
            failed += 1
    
    console.print("[bold green]Done.[/bold green]")
    if args.result_json is not None:
        write_result(args.result_json, ASSET_CLASS, symbols, statuses)
    return 1 if failed else 0


from scripts._entrypoint import run_main

run_main(__name__, main, exit_with_result=True)
