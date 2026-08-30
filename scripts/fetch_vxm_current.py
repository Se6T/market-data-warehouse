#!/usr/bin/env python3
"""Own the dynamically selected current Mini VIX futures bronze identity.

This owner has read-only market-data authority only.  It discovers the dated VXM
contract through IB contract details, requires PAPER port 4002, and publishes the
exact-expiry bronze identity plus a bounded owner result and contract mapping.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
import uuid
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable, Sequence

from ib_insync import Contract, IB

# Resolve project root for sealed-environment import safety.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:  # pragma: no cover - isolated subprocess bootstrap
    sys.path.insert(0, str(_PROJECT_ROOT))

from clients.bronze_client import BronzeClient
from clients.symbol_ids import stable_symbol_id
from scripts._refresh_result import write_result

PAPER_CLIENT_ID = 71
PAPER_PORT = 4002
ROOT = "VXM"
EXCHANGE = "CFE"
CURRENCY = "USD"
TRADING_CLASS = "VXM"
MULTIPLIER = "100"


class VXMRefreshError(RuntimeError):
    """Fail-closed dynamic VXM discovery or publication failure."""


@dataclass(frozen=True)
class SelectedContract:
    symbol: str
    con_id: int
    expiry_date: date
    local_symbol: str
    sec_type: str
    exchange: str
    currency: str
    trading_class: str
    multiplier: str
    broker_contract: Any


def select_current_contract(
    contracts: Sequence[object], *, as_of: date, roll_days: int,
) -> SelectedContract:
    """Validate all broker candidates and choose nearest ``(expiry, conId)``."""
    if type(roll_days) is not int or roll_days < 0:
        raise VXMRefreshError("roll_days must be a non-negative integer")
    boundary = as_of + timedelta(days=roll_days)
    candidates: list[tuple[date, int, object]] = []
    seen_con_ids: set[int] = set()
    expected = (
        ("symbol", ROOT, "root"),
        ("secType", "FUT", "security type"),
        ("exchange", EXCHANGE, "exchange"),
        ("currency", CURRENCY, "currency"),
        ("tradingClass", TRADING_CLASS, "trading class"),
        ("multiplier", MULTIPLIER, "multiplier"),
    )
    for contract in contracts:
        for attribute, value, label in expected:
            if getattr(contract, attribute, None) != value:
                raise VXMRefreshError(f"VXM contract {label} mismatch")
        local_symbol = getattr(contract, "localSymbol", None)
        if type(local_symbol) is not str or not local_symbol.startswith(ROOT):
            raise VXMRefreshError("VXM contract local symbol mismatch")
        con_id = getattr(contract, "conId", None)
        if type(con_id) is not int or con_id <= 0:
            raise VXMRefreshError("VXM contract conId must be a positive integer")
        if con_id in seen_con_ids:
            raise VXMRefreshError("VXM contract details contain duplicate conId")
        seen_con_ids.add(con_id)
        expiry_text = getattr(contract, "lastTradeDateOrContractMonth", None)
        if type(expiry_text) is not str or re.fullmatch(r"\d{8}", expiry_text) is None:
            raise VXMRefreshError("VXM contract requires an exact expiry date")
        try:
            expiry = date.fromisoformat(
                f"{expiry_text[:4]}-{expiry_text[4:6]}-{expiry_text[6:8]}"
            )
        except ValueError as exc:
            raise VXMRefreshError("VXM contract requires an exact expiry date") from exc
        if expiry > boundary:
            candidates.append((expiry, con_id, contract))
    if not candidates:
        raise VXMRefreshError("no VXM contract exists beyond roll window")
    expiry, con_id, contract = min(candidates, key=lambda item: (item[0], item[1]))
    return SelectedContract(
        symbol=f"{ROOT}_{expiry:%Y%m%d}",
        con_id=con_id,
        expiry_date=expiry,
        local_symbol=str(getattr(contract, "localSymbol")),
        sec_type=str(getattr(contract, "secType")),
        exchange=str(getattr(contract, "exchange")),
        currency=str(getattr(contract, "currency")),
        trading_class=str(getattr(contract, "tradingClass")),
        multiplier=str(getattr(contract, "multiplier")),
        broker_contract=contract,
    )


def _completed_session(as_of: date) -> date:
    candidate = as_of
    while candidate.weekday() >= 5:
        candidate -= timedelta(days=1)
    return candidate


def _bar_rows(bars: Sequence[object], selected: SelectedContract, as_of: date) -> list[dict]:
    rows: dict[str, dict] = {}
    for bar in bars:
        try:
            trade_date = date.fromisoformat(str(getattr(bar, "date"))[:10])
            values = tuple(float(getattr(bar, field)) for field in ("open", "high", "low", "close"))
            volume = int(getattr(bar, "volume"))
        except (TypeError, ValueError, AttributeError) as exc:
            raise VXMRefreshError("VXM historical bar is malformed") from exc
        open_, high, low, close = values
        if (
            trade_date > as_of
            or not all(math.isfinite(value) and value > 0 for value in values)
            or high < max(open_, close)
            or low > min(open_, close)
            or volume < 0
        ):
            raise VXMRefreshError("VXM historical bar is invalid")
        rows[trade_date.isoformat()] = {
            "trade_date": trade_date.isoformat(),
            "root_symbol": ROOT,
            "expiry_date": selected.expiry_date.isoformat(),
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "settlement": close,
            "volume": volume,
            "open_interest": 0,
        }
    required = _completed_session(as_of).isoformat()
    if required not in rows:
        raise VXMRefreshError(f"VXM completed session {required} is unavailable")
    return [rows[key] for key in sorted(rows)]


def _write_mapping(path: Path, document: dict[str, object]) -> None:
    if not path.is_absolute() or not path.parent.is_dir() or path.is_symlink():
        raise VXMRefreshError("mapping path must be an absolute non-symlink")
    payload = (json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode()
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        parent = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(parent)
        finally:
            os.close(parent)
    finally:
        os.close(descriptor)
        temporary.unlink(missing_ok=True)


def refresh_current_vxm(
    *, warehouse: Path, as_of: date, result_json: Path, mapping_json: Path,
    roll_days: int, host: str, port: int, ib_factory: Callable[[], object] = IB,
) -> SelectedContract:
    """Discover, refresh, and publish the exact current VXM dated contract."""
    if type(port) is not int or port != PAPER_PORT:
        raise VXMRefreshError("dynamic VXM refresh requires PAPER port 4002")
    if type(host) is not str or not host:
        raise VXMRefreshError("IB host must be non-empty")
    if host != "127.0.0.1":
        raise VXMRefreshError("dynamic VXM refresh requires IB host 127.0.0.1")
    broker = ib_factory()
    try:
        broker.connect(host, port, clientId=PAPER_CLIENT_ID, timeout=10, readonly=True)
        request = Contract(symbol=ROOT, secType="FUT", exchange=EXCHANGE, currency=CURRENCY)
        details = broker.reqContractDetails(request)
        selected = select_current_contract(
            [detail.contract for detail in details], as_of=as_of, roll_days=roll_days,
        )
        end = (as_of + timedelta(days=1)).strftime("%Y%m%d 23:59:59 UTC")
        bars = broker.reqHistoricalData(
            selected.broker_contract,
            endDateTime=end,
            durationStr="10 D",
            barSizeSetting="1 day",
            whatToShow="TRADES",
            useRTH=True,
            formatDate=1,
            keepUpToDate=False,
        )
        rows = _bar_rows(bars, selected, as_of)
        bronze_root = warehouse / "data-lake" / "bronze" / "asset_class=futures"
        with BronzeClient(bronze_root, asset_class="futures") as bronze:
            bronze.merge_ticker_rows(selected.symbol, rows)
        mapping = {
            "schema_version": 1,
            "root": ROOT,
            "symbol": selected.symbol,
            "contract_id": stable_symbol_id(selected.symbol),
            "con_id": selected.con_id,
            "local_symbol": selected.local_symbol,
            "sec_type": selected.sec_type,
            "exchange": selected.exchange,
            "currency": selected.currency,
            "trading_class": selected.trading_class,
            "multiplier": selected.multiplier,
            "expiry_date": selected.expiry_date.isoformat(),
            "as_of": as_of.isoformat(),
            "roll_days": roll_days,
            "latest_session": _completed_session(as_of).isoformat(),
        }
        _write_mapping(mapping_json, mapping)
        write_result(
            result_json, "futures", [selected.symbol], {selected.symbol: "succeeded"},
        )
        return selected
    finally:
        broker.disconnect()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--warehouse", type=Path, default=Path.home() / "market-warehouse")
    parser.add_argument("--as-of", type=date.fromisoformat, required=True)
    parser.add_argument("--result-json", type=Path, required=True)
    parser.add_argument("--mapping-json", type=Path, required=True)
    parser.add_argument("--roll-days", type=int, default=5)
    parser.add_argument("--host", default=os.getenv("MDW_IB_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("MDW_IB_PORT", "4002")))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        refresh_current_vxm(
            warehouse=args.warehouse,
            as_of=args.as_of,
            result_json=args.result_json,
            mapping_json=args.mapping_json,
            roll_days=args.roll_days,
            host=args.host,
            port=args.port,
        )
    except VXMRefreshError as exc:
        print(f"VXM refresh failed: {exc}", file=os.sys.stderr)
        return 1
    return 0


from scripts._entrypoint import run_main

run_main(__name__, main, exit_with_result=True)
