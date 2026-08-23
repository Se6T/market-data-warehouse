#!/usr/bin/env python3
"""Refresh ordinary and dynamically selected VXM futures in one owner process."""
from __future__ import annotations

import argparse
import json
import sys
import tempfile
from datetime import date
from pathlib import Path
from typing import Callable, Sequence

# Resolve project root for sealed-environment import safety.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scripts import daily_update
from scripts._refresh_result import write_result
from scripts.fetch_vxm_current import refresh_current_vxm


def _preset_symbols(path: Path) -> list[str]:
    document = json.loads(path.read_text())
    return [
        f"{contract['root']}_{contract['expiry']}"
        for contract in document["contracts"]
    ]


def _statuses(path: Path, symbols: list[str]) -> dict[str, str]:
    document = json.loads(path.read_text())
    if (
        document.get("schema_version") != 1
        or document.get("asset_class") != "futures"
        or document.get("requested_symbols") != symbols
    ):
        raise ValueError("futures sub-result is invalid")
    results = document.get("results")
    statuses = {
        item["symbol"]: item["status"]
        for item in results
        if set(item) == {"symbol", "status"}
        and item["status"] in {"succeeded", "failed"}
    }
    if list(statuses) != symbols or len(results) != len(symbols):
        raise ValueError("futures sub-result is invalid")
    return statuses


def refresh_futures_batch(
    *,
    warehouse: Path,
    as_of: date,
    preset: Path,
    result_json: Path,
    mapping_json: Path,
    prior_vxm_symbols: Sequence[str],
    roll_days: int,
    provider: str,
    host: str,
    port: int,
    daily_main: Callable[[list[str]], int] = daily_update.main,
    vxm_refresh: Callable[..., object] = refresh_current_vxm,
) -> int:
    """Run both futures refresh paths and emit one exact combined result."""
    if provider != "direct-ib" or host != "127.0.0.1" or port != 4002:
        raise ValueError("futures owner requires direct-ib at 127.0.0.1:4002")
    ordinary_symbols = _preset_symbols(preset)
    statuses: dict[str, str] = {}
    selected_symbol: str | None = None
    with tempfile.TemporaryDirectory(prefix="mdw-futures-owner-") as temporary_name:
        temporary = Path(temporary_name)
        ordinary_result = temporary / "ordinary-result.json"
        vxm_result = temporary / "vxm-result.json"
        try:
            selected = vxm_refresh(
                warehouse=warehouse,
                as_of=as_of,
                result_json=vxm_result,
                mapping_json=mapping_json,
                roll_days=roll_days,
                host=host,
                port=port,
            )
            selected_symbol = str(getattr(selected, "symbol"))
            statuses.update(_statuses(vxm_result, [selected_symbol]))
        except Exception:
            mapping_json.unlink(missing_ok=True)
            statuses.update({symbol: "failed" for symbol in prior_vxm_symbols})

        if ordinary_symbols:
            ordinary_argv = [
                "--asset-class", "futures",
                "--target-date", as_of.isoformat(),
                "--force",
                "--preset", str(preset),
                "--result-json", str(ordinary_result),
                "--provider", provider,
                "--host", host,
                "--port", str(port),
            ]
            try:
                ordinary_exit = daily_main(ordinary_argv)
                ordinary_statuses = _statuses(ordinary_result, ordinary_symbols)
                if (ordinary_exit == 0) != all(
                    status == "succeeded" for status in ordinary_statuses.values()
                ):
                    raise ValueError("ordinary futures exit status mismatch")
            except Exception:
                ordinary_statuses = {symbol: "failed" for symbol in ordinary_symbols}
            statuses = {**ordinary_statuses, **statuses}

    requested = [*ordinary_symbols]
    if selected_symbol is not None:
        requested.append(selected_symbol)
    else:
        requested.extend(prior_vxm_symbols)
    ordered = {symbol: statuses.get(symbol, "failed") for symbol in requested}
    write_result(result_json, "futures", requested, ordered)
    return 0 if ordered and all(status == "succeeded" for status in ordered.values()) else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--warehouse", type=Path, required=True)
    parser.add_argument("--as-of", type=date.fromisoformat, required=True)
    parser.add_argument("--preset", type=Path, required=True)
    parser.add_argument("--result-json", type=Path, required=True)
    parser.add_argument("--mapping-json", type=Path, required=True)
    parser.add_argument("--prior-vxm-symbols", nargs="*", default=[])
    parser.add_argument("--roll-days", type=int, required=True)
    parser.add_argument("--provider", choices=["direct-ib"], required=True)
    parser.add_argument("--host", choices=["127.0.0.1"], required=True)
    parser.add_argument("--port", type=int, choices=[4002], required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return refresh_futures_batch(
        warehouse=args.warehouse,
        as_of=args.as_of,
        preset=args.preset,
        result_json=args.result_json,
        mapping_json=args.mapping_json,
        prior_vxm_symbols=args.prior_vxm_symbols,
        roll_days=args.roll_days,
        provider=args.provider,
        host=args.host,
        port=args.port,
    )


from scripts._entrypoint import run_main

run_main(__name__, main, exit_with_result=True)
