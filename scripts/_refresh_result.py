"""Bounded, sanitized, atomic per-symbol refresh result artifacts."""
from __future__ import annotations

import json
import os
import uuid
from pathlib import Path
from typing import Mapping, Sequence

SCHEMA_VERSION = 1
MAX_SYMBOLS = 10_000
MAX_RESULT_BYTES = 1_048_576
TERMINAL_STATUSES = frozenset({"succeeded", "failed"})


def write_result(
    path: Path,
    asset_class: str,
    requested_symbols: Sequence[str],
    statuses: Mapping[str, str],
) -> None:
    """Atomically emit one sanitized terminal status per requested symbol."""
    symbols = list(requested_symbols)
    if (
        not isinstance(asset_class, str)
        or not asset_class
        or not symbols
        or len(symbols) > MAX_SYMBOLS
        or any(type(symbol) is not str or not symbol for symbol in symbols)
        or len(symbols) != len(set(symbols))
        or set(statuses) != set(symbols)
        or any(type(statuses[symbol]) is not str or statuses[symbol] not in TERMINAL_STATUSES for symbol in symbols)
    ):
        raise ValueError("invalid refresh result identities or statuses")
    destination = Path(path)
    if not destination.is_absolute() or not destination.parent.is_dir() or destination.is_symlink():
        raise ValueError("result path must be an absolute non-symlink in an existing directory")
    document = {
        "schema_version": SCHEMA_VERSION,
        "asset_class": asset_class,
        "requested_symbols": symbols,
        "results": [
            {"symbol": symbol, "status": statuses[symbol]} for symbol in symbols
        ],
    }
    payload = (json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    if len(payload) > MAX_RESULT_BYTES:
        raise ValueError("refresh result exceeds size bound")
    temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
        parent = os.open(destination.parent, os.O_RDONLY)
        try:
            os.fsync(parent)
        finally:
            os.close(parent)
    finally:
        os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
