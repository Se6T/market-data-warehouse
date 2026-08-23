#!/usr/bin/env python3
"""Atomically refresh every canonical bronze identity and rebuild DuckDB."""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import io
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import uuid
import zipfile
from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Mapping, Sequence

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:  # pragma: no cover - standalone script path
    sys.path.insert(0, str(PROJECT_ROOT))

from clients.db_client import DBClient  # noqa: E402
from clients.symbol_ids import stable_symbol_id  # noqa: E402
from scripts.daily_update import is_trading_day  # noqa: E402
from scripts._refresh_result import (  # noqa: E402
    MAX_RESULT_BYTES as MAX_OWNER_RESULT_BYTES,
    MAX_SYMBOLS as MAX_OWNER_SYMBOLS,
)

ASSET_CLASSES = ("crypto", "equity", "futures", "volatility")
VENUES = {"crypto": "BINANCE", "equity": "SMART", "volatility": "CBOE"}
SYMBOL_COLUMNS = (
    "trade_date", "symbol_id", "open", "high", "low", "close", "adj_close", "volume"
)
FUTURES_COLUMNS = (
    "trade_date", "contract_id", "root_symbol", "expiry_date", "open", "high", "low",
    "close", "settlement", "volume", "open_interest",
)
SYMBOL_SCHEMA = (
    "trade_date:date32[day]", "symbol_id:int64", "open:double", "high:double",
    "low:double", "close:double", "adj_close:double", "volume:int64",
)
FUTURES_SCHEMA = (
    "trade_date:date32[day]", "contract_id:int64", "root_symbol:string",
    "expiry_date:date32[day]", "open:double", "high:double", "low:double",
    "close:double", "settlement:double", "volume:int64", "open_interest:int64",
)
DB_SCHEMAS = {
    "symbols": ("symbol_id", "symbol", "asset_class", "venue"),
    "equities_daily": SYMBOL_COLUMNS,
    "futures_daily": FUTURES_COLUMNS,
}
DB_PHYSICAL_SCHEMAS = {
    "symbols": (
        ("symbol_id", "BIGINT", True, True),
        ("symbol", "VARCHAR", False, False),
        ("asset_class", "VARCHAR", False, False),
        ("venue", "VARCHAR", False, False),
    ),
    "equities_daily": tuple(
        (name, physical_type, False, False)
        for name, physical_type in zip(
            SYMBOL_COLUMNS,
            ("DATE", "BIGINT", "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE", "BIGINT"),
        )
    ),
    "futures_daily": tuple(
        (name, physical_type, False, False)
        for name, physical_type in zip(
            FUTURES_COLUMNS,
            (
                "DATE", "BIGINT", "VARCHAR", "DATE", "DOUBLE", "DOUBLE", "DOUBLE",
                "DOUBLE", "DOUBLE", "BIGINT", "BIGINT",
            ),
        )
    ),
}
DB_INDEXES = {
    ("idx_equities_daily_dedup", True, "[trade_date, symbol_id]"),
    ("idx_futures_daily_dedup", True, "[trade_date, contract_id]"),
}
DB_CONSTRAINTS = {("symbols", "PRIMARY KEY", ("symbol_id",)), ("symbols", "NOT NULL", ("symbol_id",))}


class RefreshFailure(RuntimeError):
    """A fail-closed M0 refresh failure safe to report to an operator."""


class InjectedFailure(RefreshFailure):
    """Test-only phase-boundary failure."""


@dataclass(frozen=True)
class InventoryEntry:
    asset_class: str
    symbol: str
    path: str
    sha256: str
    rows: int
    latest_session: str
    identity_id: int
    schema: tuple[str, ...]
    schema_sha256: str


@dataclass(frozen=True)
class PublishedBundle:
    """A reader-pinned immutable database/manifest generation."""

    root: Path
    database: Path
    manifest: Path


@dataclass(frozen=True)
class RefreshConfig:
    warehouse: Path
    db_path: Path
    manifest_path: Path
    inventory_path: Path
    as_of: date
    python: Path
    repo_root: Path = PROJECT_ROOT
    command_timeout: int = 3600
    source_archive_fd: int | None = None
    refresh_broker_assets: bool = True
    bootstrap_current_vxm: bool = True
    vxm_roll_days: int = 5
    vxm_host: str = "127.0.0.1"
    vxm_port: int = 4002


CommandRunner = Callable[[list[str]], subprocess.CompletedProcess[str]]
PhaseHook = Callable[[str], None]
SourceIdentity = Callable[[Path], Mapping[str, str]]


def _trusted_git() -> Path:
    executable = shutil.which("git", path=os.defpath)
    if executable is None:
        raise RefreshFailure("trusted Git executable is unavailable")
    trusted = Path(executable).resolve(strict=True)
    if not trusted.is_absolute() or not trusted.is_file() or not os.access(trusted, os.X_OK):
        raise RefreshFailure("trusted Git executable is invalid")
    return trusted


def _git_environment() -> dict[str, str]:
    return {
        "PATH": os.defpath,
        "HOME": "/nonexistent",
        "XDG_CONFIG_HOME": "/nonexistent",
        "LC_ALL": "C",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_COUNT": "0",
    }


def _run_git(
    repo_root: Path, *args: str, text: bool = True,
) -> subprocess.CompletedProcess:
    return subprocess.run(
        [str(_trusted_git()), "--no-replace-objects", *args],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=text,
        env=_git_environment(),
    )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical_bytes(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _entry_document(entry: InventoryEntry) -> dict[str, object]:
    document: dict[str, object] = asdict(entry)
    document["schema"] = list(entry.schema)
    return document


def _path_identity(path: Path) -> tuple[int, int] | None:
    try:
        stat = path.stat()
    except FileNotFoundError:
        return None
    return stat.st_dev, stat.st_ino


def _validate_config(config: RefreshConfig) -> None:
    if type(config.refresh_broker_assets) is not bool:
        raise RefreshFailure("refresh_broker_assets must be an exact bool")
    if type(config.bootstrap_current_vxm) is not bool:
        raise RefreshFailure("bootstrap_current_vxm must be an exact bool")
    if type(config.vxm_roll_days) is not int or config.vxm_roll_days < 0:
        raise RefreshFailure("vxm_roll_days must be a non-negative integer")
    if type(config.vxm_host) is not str or not config.vxm_host:
        raise RefreshFailure("vxm_host must be non-empty")
    if type(config.vxm_port) is not int or config.vxm_port != 4002:
        raise RefreshFailure("dynamic VXM owner requires PAPER port 4002")
    paths = (config.db_path, config.manifest_path, config.inventory_path)
    lexical = {os.path.abspath(path) for path in paths}
    if len(lexical) != 3:
        raise RefreshFailure("database, manifest, and inventory output paths must be distinct")
    if config.db_path.parent != config.manifest_path.parent:
        raise RefreshFailure("database and manifest must share one current bundle directory")
    for path in paths:
        if path.is_symlink():
            raise RefreshFailure(f"output path must not be a symlink: {path}")
    identities = [identity for path in paths if (identity := _path_identity(path)) is not None]
    if len(identities) != len(set(identities)):
        raise RefreshFailure("output paths are filesystem aliases")
    bronze = (config.warehouse / "data-lake" / "bronze").resolve()
    for path in paths:
        resolved = path.resolve(strict=False)
        if resolved == bronze or bronze in resolved.parents:
            raise RefreshFailure("outputs must remain outside canonical bronze")


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_immutable(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        payload = _canonical_bytes(value)
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        os.close(descriptor)
    _fsync_directory(path.parent)


def discover_inventory(
    bronze_root: Path,
    *,
    require_all_asset_classes: bool = True,
    allow_legacy_volatility_ids: bool = False,
) -> list[InventoryEntry]:
    """Discover and validate every active canonical bronze parquet identity."""
    entries: list[InventoryEntry] = []
    if len(ASSET_CLASSES) != len(set(ASSET_CLASSES)):
        raise RefreshFailure("duplicate canonical bronze identities")
    bronze_real = bronze_root.resolve(strict=True)
    if bronze_root.is_symlink():
        raise RefreshFailure(f"canonical bronze root must not be a symlink: {bronze_root}")
    seen_files: dict[tuple[int, int], Path] = {}
    seen_ids: dict[int, tuple[str, str]] = {}
    for asset_class in ASSET_CLASSES:
        root = bronze_root / f"asset_class={asset_class}"
        if root.is_symlink():
            raise RefreshFailure(f"canonical asset root must not be a symlink: {root}")
        expected_columns = FUTURES_COLUMNS if asset_class == "futures" else SYMBOL_COLUMNS
        expected_schema = FUTURES_SCHEMA if asset_class == "futures" else SYMBOL_SCHEMA
        id_column = "contract_id" if asset_class == "futures" else "symbol_id"
        for identity_dir in sorted(root.glob("symbol=*")):
            if identity_dir.is_symlink():
                raise RefreshFailure(f"canonical identity directory must not be a symlink: {identity_dir}")
            path = identity_dir / "data.parquet"
            if not path.exists():
                continue
            if path.is_symlink():
                raise RefreshFailure(f"canonical parquet must not be a symlink: {path}")
            resolved = path.resolve(strict=True)
            if bronze_real not in resolved.parents:
                raise RefreshFailure(f"canonical parquet escapes bronze root: {path}")
            file_identity = _path_identity(path)
            if file_identity in seen_files:
                raise RefreshFailure(
                    f"canonical parquet hard-link alias: {seen_files[file_identity]} and {path}"
                )
            assert file_identity is not None
            seen_files[file_identity] = path
            symbol = path.parent.name.removeprefix("symbol=")
            if not symbol or path.parent.name != f"symbol={symbol}":
                raise RefreshFailure(f"invalid bronze identity path: {path}")
            table = pq.ParquetFile(path).read()
            schema = tuple(f"{field.name}:{field.type}" for field in table.schema)
            if tuple(table.column_names) != expected_columns or schema != expected_schema:
                raise RefreshFailure(f"{asset_class}:{symbol} schema mismatch")
            if table.num_rows <= 0:
                raise RefreshFailure(f"{asset_class}:{symbol} has no rows")
            dates = table.column("trade_date").to_pylist()
            if dates != sorted(dates) or len(dates) != len(set(dates)):
                raise RefreshFailure(f"{asset_class}:{symbol} has invalid trade_date ordering")
            ids = set(table.column(id_column).to_pylist())
            if len(ids) != 1:
                raise RefreshFailure(f"{asset_class}:{symbol} has inconsistent identity IDs")
            identity_id = int(next(iter(ids)))
            canonical_id = stable_symbol_id(symbol)
            if identity_id != canonical_id:
                legacy_id = int(hashlib.sha256(symbol.encode("utf-8")).hexdigest()[:14], 16)
                if not (
                    allow_legacy_volatility_ids
                    and asset_class == "volatility"
                    and identity_id == legacy_id
                ):
                    raise RefreshFailure(
                        f"{asset_class}:{symbol} canonical identity ID mismatch: "
                        f"expected {canonical_id}, observed {identity_id}"
                    )
            previous_identity = seen_ids.get(canonical_id)
            if previous_identity is not None and previous_identity != (asset_class, symbol):
                raise RefreshFailure(
                    "canonical identity ID collision: "
                    f"{previous_identity[0]}:{previous_identity[1]} and {asset_class}:{symbol}"
                )
            seen_ids[canonical_id] = (asset_class, symbol)
            entries.append(
                InventoryEntry(
                    asset_class=asset_class,
                    symbol=symbol,
                    path=path.relative_to(bronze_root).as_posix(),
                    sha256=_sha256_file(path),
                    rows=table.num_rows,
                    latest_session=dates[-1].isoformat(),
                    identity_id=canonical_id,
                    schema=schema,
                    schema_sha256=hashlib.sha256(_canonical_bytes(schema)).hexdigest(),
                )
            )
    present = {entry.asset_class for entry in entries}
    missing = sorted(set(ASSET_CLASSES) - present)
    if require_all_asset_classes and missing:
        raise RefreshFailure(f"canonical bronze inventory missing asset classes: {', '.join(missing)}")
    identities = [(entry.asset_class, entry.symbol) for entry in entries]
    if len(identities) != len(set(identities)):
        raise RefreshFailure("duplicate canonical bronze identities")
    return sorted(entries, key=lambda item: (item.asset_class, item.symbol))


def _git_text(repo_root: Path, *args: str) -> str:
    return str(_run_git(repo_root, *args).stdout).strip()


def _validate_recorded_source(repo_root: Path, expected: Mapping[str, str]) -> None:
    commit = expected.get("commit")
    tree = expected.get("tree")
    if not isinstance(commit, str) or not isinstance(tree, str):
        raise RefreshFailure("source identity is incomplete")
    if _git_text(repo_root, "cat-file", "-t", commit) != "commit":
        raise RefreshFailure("recorded source object is not a commit")
    committed_tree = _git_text(repo_root, "rev-parse", f"{commit}^{{tree}}")
    if committed_tree != tree:
        raise RefreshFailure("recorded source commit tree mismatch")


def _source_identity(repo_root: Path) -> Mapping[str, str]:
    status = _git_text(repo_root, "status", "--porcelain=v1", "-z", "--untracked-files=all")
    relevant_untracked: list[str] = []
    tracked_drift: list[str] = []
    for line in status.split("\0"):
        if not line:
            continue
        code, path_text = line[:2], line[3:]
        if code == "??":
            path = Path(path_text)
            if path.suffix in {".py", ".pyi"} or path.parts[:1] in {("clients",), ("scripts",)}:
                relevant_untracked.append(path_text)
        else:
            tracked_drift.append(path_text)
    if tracked_drift or relevant_untracked:
        raise RefreshFailure("source tree is dirty; commit executed source before running M0")
    commit = _git_text(repo_root, "rev-parse", "--verify", "HEAD")
    if _git_text(repo_root, "cat-file", "-t", commit) != "commit":
        raise RefreshFailure("recorded source object is not a commit")
    committed_tree = _git_text(repo_root, "rev-parse", f"{commit}^{{tree}}")
    tree = _git_text(repo_root, "write-tree")
    if tree != committed_tree:
        raise RefreshFailure("source index does not match HEAD tree")
    return {"commit": commit, "tree": tree}


def _archive_members(archive: bytes) -> dict[str, bytes]:
    members: dict[str, bytes] = {}
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as source:
        for member in source.getmembers():
            if member.isdir():
                continue
            if not member.isfile():
                raise RefreshFailure(f"committed source contains unsupported link: {member.name}")
            extracted = source.extractfile(member)
            if extracted is None:
                raise RefreshFailure("committed source archive is incomplete")
            members[member.name] = extracted.read()
    return members


def _git_tree_members(
    repo_root: Path, expected: Mapping[str, str],
) -> dict[str, tuple[int, bytes]]:
    """Read ordinary committed blobs directly, independent of archive attributes."""
    _validate_recorded_source(repo_root, expected)
    commit = expected["commit"]
    listing = bytes(
        _run_git(repo_root, "ls-tree", "-rz", "--full-tree", commit, text=False).stdout
    )
    members: dict[str, tuple[int, bytes]] = {}
    for record in listing.split(b"\0"):
        if not record:
            continue
        metadata, separator, raw_path = record.partition(b"\t")
        fields = metadata.split()
        if not separator or len(fields) != 3:
            raise RefreshFailure("recorded Git tree inventory is malformed")
        mode_text, object_type, object_id = fields
        if object_type != b"blob" or mode_text not in {b"100644", b"100755"}:
            raise RefreshFailure("recorded Git tree contains unsupported entries")
        path = os.fsdecode(raw_path)
        if path in members:
            raise RefreshFailure("recorded Git tree contains duplicate paths")
        payload = bytes(
            _run_git(repo_root, "cat-file", "blob", object_id.decode("ascii"), text=False).stdout
        )
        members[path] = (int(mode_text, 8), payload)
    return members


def _archive_tree_members(archive: bytes) -> dict[str, tuple[int, bytes]]:
    members: dict[str, tuple[int, bytes]] = {}
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as source:
        for member in source.getmembers():
            if member.isdir():
                continue
            if not member.isfile() or member.name in members:
                raise RefreshFailure(f"committed source contains unsupported link: {member.name}")
            extracted = source.extractfile(member)
            if extracted is None:
                raise RefreshFailure("committed source archive is incomplete")
            mode = 0o100755 if member.mode & 0o111 else 0o100644
            members[member.name] = (mode, extracted.read())
    return members


def _source_archive(repo_root: Path, expected: Mapping[str, str]) -> bytes:
    _validate_recorded_source(repo_root, expected)
    commit = expected["commit"]
    archive = bytes(
        _run_git(repo_root, "archive", "--format=tar", commit, text=False).stdout
    )
    if _archive_tree_members(archive) != _git_tree_members(repo_root, expected):
        raise RefreshFailure("Git archive differs from recorded Git tree")
    return archive


def _verify_materialized_source_tree(
    repo_root: Path, expected: Mapping[str, str], destination: Path,
) -> None:
    expected_members = _git_tree_members(repo_root, expected)
    actual_members = {
        path.relative_to(destination).as_posix(): (
            0o100755 if path.stat().st_mode & 0o111 else 0o100644,
            path.read_bytes(),
        )
        for path in destination.rglob("*")
        if path.is_file()
    }
    if actual_members != expected_members:
        raise RefreshFailure("materialized source differs from recorded Git tree")


def _materialize_source_tree(
    repo_root: Path, expected: Mapping[str, str], destination: Path,
) -> None:
    """Extract the recorded Git commit into a private, verified execution root."""
    archive = _source_archive(repo_root, expected)
    destination.mkdir(mode=0o700)
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as source:
        for member in source.getmembers():
            if not (member.isfile() or member.isdir()):
                raise RefreshFailure(f"committed source contains unsupported link: {member.name}")
            target = (destination / member.name).resolve(strict=False)
            if destination.resolve() not in target.parents and target != destination.resolve():
                raise RefreshFailure("committed source archive escapes materialization root")
        source.extractall(destination, filter="data")
    _verify_materialized_source_tree(repo_root, expected, destination)
    committed = _git_tree_members(repo_root, expected)
    for path in sorted(destination.rglob("*"), reverse=True):
        relative = path.relative_to(destination).as_posix()
        executable = not path.is_dir() and committed[relative][0] == 0o100755
        path.chmod(0o500 if path.is_dir() or executable else 0o400)
    destination.chmod(0o500)


@contextmanager
def _sealed_execution_source(repo_root: Path, expected: Mapping[str, str]):
    """Yield an unlinked ZIP fd containing only blob-verified committed bytes."""
    members = _git_tree_members(repo_root, expected)
    with tempfile.TemporaryFile(mode="w+b") as sealed:
        with zipfile.ZipFile(sealed, mode="w", compression=zipfile.ZIP_STORED) as archive:
            for path, (_mode, payload) in sorted(members.items()):
                archive.writestr(path, payload)
        sealed.flush()
        os.fsync(sealed.fileno())
        sealed.seek(0)
        yield sealed


def _verify_source_identity(
    repo_root: Path, expected: Mapping[str, str], source_identity: SourceIdentity,
) -> None:
    if dict(source_identity(repo_root)) != dict(expected):
        raise RefreshFailure("source identity changed during M0 refresh")


def _seal_owner_argv(config: RefreshConfig, owner_argv: list[str]) -> list[str]:
    if config.source_archive_fd is None:
        return owner_argv
    script = Path(owner_argv[1]).relative_to(config.repo_root).as_posix()
    archive = f"/dev/fd/{config.source_archive_fd}"
    bootstrap = (
        "import os,sys,sysconfig,zipfile;"
        "a,s=sys.argv[1:3];sys.argv=sys.argv[2:];"
        "v=f'python{sys.version_info.major}.{sys.version_info.minor}';"
        "p=os.path.dirname(os.path.dirname(sys.executable));"
        "x=sysconfig.get_paths();"
        "sys.path[:0]=[a,p+'/lib/'+v+'/site-packages',x['purelib'],x['platlib']];"
        "P=type('SealedImportPath',(list,),{'insert':lambda q,i,v:list.insert(q,1 if i==0 else i,v)});"
        "sys.path=P(sys.path);"
        "g={'__name__':'__main__','__file__':s,'__package__':None,'__cached__':None};"
        "exec(compile(zipfile.ZipFile(a).read(s),s,'exec'),g)"
    )
    return [str(config.python), "-I", "-S", "-c", bootstrap, archive, script, *owner_argv[2:]]


def _update_argv(
    config: RefreshConfig,
    entries: Sequence[InventoryEntry],
    preset: Path,
    result_json: Path,
) -> list[str]:
    if not entries:
        raise RefreshFailure("cannot build owner argv for an empty asset class")
    asset_class = entries[0].asset_class
    if any(entry.asset_class != asset_class for entry in entries):
        raise RefreshFailure("owner argv asset class mismatch")
    symbols = [entry.symbol for entry in entries]
    python = str(config.python)
    if asset_class in {"equity", "futures"}:
        if asset_class == "futures":
            preset_document = {
                "name": "m0-futures-batch",
                "contracts": [
                    {"root": symbol.rsplit("_", 1)[0], "expiry": symbol.rsplit("_", 1)[1]}
                    for symbol in symbols
                ],
            }
        else:
            preset_document = {
                "name": "m0-equity-batch",
                "tickers": symbols,
            }
        preset.write_bytes(_canonical_bytes(preset_document))
        owner_argv = [
            python,
            str(config.repo_root / "scripts" / "daily_update.py"),
            "--asset-class", asset_class,
            "--target-date", config.as_of.isoformat(),
            "--force",
            "--preset", str(preset),
            "--provider", "direct-ib",
            "--host", config.vxm_host,
            "--port", str(config.vxm_port),
        ]
    elif asset_class == "volatility":
        owner_argv = [
            python,
            str(config.repo_root / "scripts" / "fetch_cboe_volatility.py"),
            "--symbols", *symbols,
            "--end", config.as_of.isoformat(),
            "--warehouse", str(config.warehouse),
        ]
    else:
        owner_argv = [
            python,
            str(config.repo_root / "scripts" / "fetch_binance_crypto.py"),
            "--symbols", *symbols,
            "--end", config.as_of.isoformat(),
            "--warehouse", str(config.warehouse),
        ]
    owner_argv.extend(["--result-json", str(result_json)])
    return _seal_owner_argv(config, owner_argv)


def _read_owner_result(
    path: Path, asset_class: str, requested_symbols: Sequence[str],
) -> dict[str, str]:
    """Validate and return exact bounded per-symbol owner statuses."""
    message = f"{asset_class} owner result is invalid"
    try:
        symbols = list(requested_symbols)
        if (
            not symbols
            or len(symbols) > MAX_OWNER_SYMBOLS
            or any(type(symbol) is not str or not symbol for symbol in symbols)
            or len(symbols) != len(set(symbols))
            or path.is_symlink()
        ):
            raise ValueError
        metadata = path.stat()
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > MAX_OWNER_RESULT_BYTES:
            raise ValueError
        raw = path.read_bytes()
        if len(raw) > MAX_OWNER_RESULT_BYTES:
            raise ValueError
        document = json.loads(raw)
        if type(document) is not dict or set(document) != {
            "schema_version", "asset_class", "requested_symbols", "results"
        }:
            raise ValueError
        if document["schema_version"] != 1 or type(document["schema_version"]) is not int:
            raise ValueError
        if document["asset_class"] != asset_class or type(document["asset_class"]) is not str:
            raise ValueError
        reported_symbols = document["requested_symbols"]
        results = document["results"]
        if (
            type(reported_symbols) is not list
            or reported_symbols != symbols
            or len(reported_symbols) != len(set(reported_symbols))
            or type(results) is not list
            or len(results) != len(symbols)
        ):
            raise ValueError
        statuses: dict[str, str] = {}
        for item in results:
            if type(item) is not dict or set(item) != {"symbol", "status"}:
                raise ValueError
            symbol, status = item["symbol"], item["status"]
            if (
                type(symbol) is not str
                or symbol in statuses
                or type(status) is not str
                or status not in {"succeeded", "failed"}
            ):
                raise ValueError
            statuses[symbol] = status
        if list(statuses) != symbols:
            raise ValueError
        return statuses
    except (OSError, UnicodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        raise RefreshFailure(message) from exc


def _futures_owner_argv(
    config: RefreshConfig,
    entries: Sequence[InventoryEntry],
    preset: Path,
    result_json: Path,
    mapping_json: Path,
) -> list[str]:
    ordinary = [entry for entry in entries if not entry.symbol.startswith("VXM_")]
    preset.write_bytes(_canonical_bytes({
        "name": "m0-futures-batch",
        "contracts": [
            {
                "root": entry.symbol.rsplit("_", 1)[0],
                "expiry": entry.symbol.rsplit("_", 1)[1],
            }
            for entry in ordinary
        ],
    }))
    prior_vxm = [entry.symbol for entry in entries if entry.symbol.startswith("VXM_")]
    owner_argv = [
        str(config.python),
        str(config.repo_root / "scripts" / "refresh_futures_batch.py"),
        "--warehouse", str(config.warehouse),
        "--as-of", config.as_of.isoformat(),
        "--preset", str(preset),
        "--result-json", str(result_json),
        "--mapping-json", str(mapping_json),
        "--roll-days", str(config.vxm_roll_days),
        "--provider", "direct-ib",
        "--host", config.vxm_host,
        "--port", str(config.vxm_port),
        "--prior-vxm-symbols", *prior_vxm,
    ]
    return _seal_owner_argv(config, owner_argv)


def _read_vxm_mapping(path: Path, config: RefreshConfig) -> dict[str, object]:
    message = "dynamic VXM owner mapping is invalid"
    expected_keys = {
        "schema_version", "root", "symbol", "contract_id", "con_id", "local_symbol",
        "sec_type", "exchange", "currency", "trading_class", "multiplier",
        "expiry_date", "as_of", "roll_days", "latest_session",
    }
    try:
        if path.is_symlink() or path.stat().st_size > MAX_OWNER_RESULT_BYTES:
            raise ValueError
        document = json.loads(path.read_bytes())
        if type(document) is not dict or set(document) != expected_keys:
            raise ValueError
        symbol = document["symbol"]
        expiry = date.fromisoformat(document["expiry_date"])
        if (
            document["schema_version"] != 1
            or type(document["schema_version"]) is not int
            or document["root"] != "VXM"
            or type(symbol) is not str
            or symbol != f"VXM_{expiry:%Y%m%d}"
            or document["contract_id"] != stable_symbol_id(symbol)
            or type(document["contract_id"]) is not int
            or type(document["con_id"]) is not int
            or document["con_id"] <= 0
            or type(document["local_symbol"]) is not str
            or not document["local_symbol"].startswith("VXM")
            or document["sec_type"] != "FUT"
            or document["exchange"] != "CFE"
            or document["currency"] != "USD"
            or document["trading_class"] != "VXM"
            or document["multiplier"] != "100"
            or document["as_of"] != config.as_of.isoformat()
            or document["roll_days"] != config.vxm_roll_days
            or document["latest_session"] != expected_latest_session(
                "futures", config.as_of
            ).isoformat()
            or expiry <= config.as_of + timedelta(days=config.vxm_roll_days)
        ):
            raise ValueError
        return document
    except (OSError, UnicodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        raise RefreshFailure(message) from exc


def _refresh_futures_owner(
    config: RefreshConfig,
    entries: Sequence[InventoryEntry],
    command_runner: CommandRunner,
    scratch: Path,
    verify_source: Callable[[], None],
) -> tuple[dict[str, object] | None, list[dict[str, object]]]:
    result_path = scratch / "futures-owner-result.json"
    mapping_path = scratch / "vxm-contract-mapping.json"
    preset_path = scratch / "futures-preset.json"
    verify_source()
    started = _utc_now()
    argv = _futures_owner_argv(
        config, entries, preset_path, result_path, mapping_path,
    )
    argv_sha256 = hashlib.sha256(_canonical_bytes(argv)).hexdigest()
    try:
        result = command_runner(argv)
    except (OSError, subprocess.TimeoutExpired):
        result = subprocess.CompletedProcess(argv, 1, "", "")
    if result.args != argv:
        raise RefreshFailure("futures owner argv mismatch")
    exit_code = int(result.returncode)
    mapping = _read_vxm_mapping(mapping_path, config) if mapping_path.exists() else None
    if exit_code != 0 and mapping is not None:
        raise RefreshFailure("failed futures owner published a VXM mapping")
    ordinary = [entry.symbol for entry in entries if not entry.symbol.startswith("VXM_")]
    prior_vxm = [entry.symbol for entry in entries if entry.symbol.startswith("VXM_")]
    requested = [*ordinary, *([str(mapping["symbol"])] if mapping else prior_vxm)]
    if result_path.exists():
        statuses = _read_owner_result(result_path, "futures", requested)
    elif exit_code != 0:
        statuses = {symbol: "failed" for symbol in requested}
    else:
        raise RefreshFailure("futures owner result is invalid")
    expected_success = bool(statuses) and all(
        status == "succeeded" for status in statuses.values()
    )
    if (exit_code == 0) != expected_success:
        raise RefreshFailure("futures owner result exit status mismatch")
    if exit_code == 0 and mapping is None:
        raise RefreshFailure("successful futures owner omitted VXM mapping")
    verify_source()
    ended = _utc_now()
    return mapping, [
        {
            "asset_class": "futures",
            "symbol": symbol,
            "argv_sha256": argv_sha256,
            "started_at": started,
            "ended_at": ended,
            "exit_code": exit_code,
            "status": status,
        }
        for symbol, status in statuses.items()
    ]


def _default_runner(config: RefreshConfig) -> CommandRunner:
    def run(argv: list[str]) -> subprocess.CompletedProcess[str]:
        env = {
            "PATH": os.defpath,
            "MDW_WAREHOUSE": str(config.warehouse),
        }
        return subprocess.run(
            argv,
            cwd=config.repo_root,
            check=False,
            capture_output=True,
            text=True,
            timeout=config.command_timeout,
            env=env,
            pass_fds=(
                () if config.source_archive_fd is None else (config.source_archive_fd,)
            ),
        )

    return run


def _refresh_inventory(
    config: RefreshConfig,
    inventory: Sequence[InventoryEntry],
    command_runner: CommandRunner,
    scratch: Path,
    verify_source: Callable[[], None] = lambda: None,
    steps: list[dict[str, object]] | None = None,
) -> list[dict[str, object]]:
    if steps is None:
        steps = []
    preservation_since_verification = False
    unsafe_failures: list[str] = []
    grouped = [
        (asset_class, [entry for entry in inventory if entry.asset_class == asset_class])
        for asset_class in ASSET_CLASSES
    ]
    for ordinal, (asset_class, entries) in enumerate(grouped):
        if not entries:
            continue
        if not config.refresh_broker_assets and asset_class in {"equity", "futures"}:
            timestamp = _utc_now()
            for entry in entries:
                steps.append({
                    "asset_class": entry.asset_class,
                    "symbol": entry.symbol,
                    "argv_sha256": None,
                    "started_at": timestamp,
                    "ended_at": timestamp,
                    "exit_code": None,
                    "status": "preserved",
                })
            preservation_since_verification = True
            continue
        verify_source()
        preservation_since_verification = False
        started = _utc_now()
        argv_sha256: str | None = None
        exit_code: int | None = None
        statuses = {entry.symbol: "failed" for entry in entries}
        result_path = scratch / f"owner-result-{ordinal}-{asset_class}.json"
        try:
            argv = _update_argv(
                config, entries, scratch / f"preset-{ordinal}.json", result_path,
            )
            argv_sha256 = hashlib.sha256(_canonical_bytes(argv)).hexdigest()
            result = command_runner(argv)
            argv_matches = result.args == argv
            exit_code = int(result.returncode)
            statuses = _read_owner_result(
                result_path, asset_class, [entry.symbol for entry in entries],
            )
            expected_exit_success = all(status == "succeeded" for status in statuses.values())
            if (exit_code == 0) != expected_exit_success:
                raise RefreshFailure(f"{asset_class} owner result exit status mismatch")
            if not argv_matches:
                raise RefreshFailure(f"{asset_class} owner reported argv mismatch")
        except (OSError, subprocess.TimeoutExpired):
            # A transport-level owner failure provides class-level terminal evidence.
            # Every identity in this batch remains failed; post-refresh integrity
            # validation decides whether unchanged predecessor data may publish degraded.
            pass
        except Exception as exc:
            unsafe_failures.append(
                str(exc)
                if isinstance(exc, RefreshFailure)
                else f"{asset_class} owner result unavailable"
            )
        ended = _utc_now()
        for entry in entries:
            steps.append({
                "asset_class": entry.asset_class,
                "symbol": entry.symbol,
                "argv_sha256": argv_sha256,
                "started_at": started,
                "ended_at": ended,
                "exit_code": exit_code,
                "status": statuses[entry.symbol],
            })
        verify_source()
    if preservation_since_verification:
        verify_source()
    if unsafe_failures:
        raise RefreshFailure(unsafe_failures[0])
    return steps


def _validate_post_inventory(
    before: Sequence[InventoryEntry],
    after: Sequence[InventoryEntry],
    as_of: date,
    *,
    refresh_broker_assets: bool = True,
    failed_identities: Sequence[tuple[str, str]] = (),
) -> None:
    before_map = {(entry.asset_class, entry.symbol): entry for entry in before}
    after_map = {(entry.asset_class, entry.symbol): entry for entry in after}
    failed = set(failed_identities)
    if set(before_map) != set(after_map):
        raise RefreshFailure("inventory identities changed during refresh")
    for identity, current in after_map.items():
        previous = before_map[identity]
        label = f"{identity[0]}:{identity[1]}"
        if current.rows < previous.rows:
            raise RefreshFailure(f"{label} row count regressed")
        if current.latest_session < previous.latest_session:
            raise RefreshFailure(f"{label} latest session regressed")
        if current.latest_session > as_of.isoformat():
            raise RefreshFailure(f"{label} latest session exceeds requested as-of")
        if identity in failed:
            if current.sha256 != previous.sha256:
                raise RefreshFailure(f"{label} failed identity changed from predecessor")
            continue
        if not refresh_broker_assets and current.asset_class in {"equity", "futures"}:
            continue
        expected = expected_latest_session(current.asset_class, as_of).isoformat()
        if (
            not refresh_broker_assets
            and current.asset_class == "volatility"
            and current.latest_session != expected
        ):
            prior = date.fromisoformat(expected) - timedelta(days=1)
            while not is_trading_day(prior):
                prior -= timedelta(days=1)
            if current.latest_session == prior.isoformat():
                continue
        if current.latest_session != expected:
            raise RefreshFailure(
                f"{label} expected latest session {expected}, observed {current.latest_session}"
            )


def expected_latest_session(asset_class: str, as_of: date) -> date:
    """Return the deterministic completed session required for an asset class.

    Crypto has a UTC daily session.  The currently supported futures universe is
    session-dated Monday through Friday.  Equity and CBOE volatility identities
    use the repository's deterministic NYSE holiday calendar.
    """
    if asset_class == "crypto":
        return as_of
    if asset_class == "futures":
        candidate = as_of
        while candidate.weekday() >= 5:
            candidate -= timedelta(days=1)
        return candidate
    if asset_class in {"equity", "volatility"}:
        candidate = as_of
        while not is_trading_day(candidate):
            candidate -= timedelta(days=1)
        return candidate
    raise RefreshFailure(f"unsupported asset calendar: {asset_class}")


def _build_database(config: RefreshConfig, temp_db: Path) -> None:
    bronze = config.warehouse / "data-lake" / "bronze"
    with DBClient(temp_db) as db:
        first = True
        for asset_class in ("equity", "volatility", "crypto"):
            db.load_equities_from_parquet(
                bronze / f"asset_class={asset_class}",
                asset_class=asset_class,
                venue=VENUES[asset_class],
                reset=first,
            )
            first = False
        db.load_futures_from_parquet(bronze / "asset_class=futures", reset=True)


def validate_database(temp_db: Path, inventory: Sequence[InventoryEntry]) -> dict[str, int]:
    """Validate exact table schemas, identity sets, row counts, and latest sessions."""
    expected_symbol_entries = [entry for entry in inventory if entry.asset_class != "futures"]
    expected_counts = {
        "md.symbols": len(expected_symbol_entries),
        "md.equities_daily": sum(entry.rows for entry in expected_symbol_entries),
        "md.futures_daily": sum(entry.rows for entry in inventory if entry.asset_class == "futures"),
    }
    connection = duckdb.connect(str(temp_db), read_only=True)
    try:
        for table, expected_columns in DB_SCHEMAS.items():
            rows = connection.execute(f"PRAGMA table_info('md.{table}')").fetchall()
            physical = tuple(
                (row[1], row[2], bool(row[3]), bool(row[5])) for row in rows
            )
            if (
                tuple(row[1] for row in rows) != expected_columns
                or physical != DB_PHYSICAL_SCHEMAS[table]
            ):
                raise RefreshFailure(f"md.{table} schema mismatch")
        indexes = {
            (name, unique, expressions)
            for name, unique, expressions in connection.execute(
                "SELECT index_name, is_unique, expressions FROM duckdb_indexes() "
                "WHERE schema_name='md'"
            ).fetchall()
        }
        if indexes != DB_INDEXES:
            raise RefreshFailure("database index mismatch")
        constraints = {
            (table, kind, tuple(columns))
            for table, kind, columns in connection.execute(
                "SELECT table_name, constraint_type, constraint_column_names "
                "FROM duckdb_constraints() WHERE schema_name='md'"
            ).fetchall()
        }
        if constraints != DB_CONSTRAINTS:
            raise RefreshFailure("database constraint mismatch")
        actual_counts = {
            f"md.{table}": connection.execute(f"SELECT count(*) FROM md.{table}").fetchone()[0]
            for table in DB_SCHEMAS
        }
        if actual_counts != expected_counts:
            raise RefreshFailure(
                f"database row count mismatch: expected={expected_counts} actual={actual_counts}"
            )
        symbol_identities = set(
            connection.execute("SELECT asset_class, symbol FROM md.symbols").fetchall()
        )
        expected_symbol_identities = {
            (entry.asset_class, entry.symbol) for entry in expected_symbol_entries
        }
        if symbol_identities != expected_symbol_identities:
            raise RefreshFailure("database symbol inventory mismatch")
        actual_symbol_ids = set(
            connection.execute(
                "SELECT asset_class, symbol, symbol_id FROM md.symbols"
            ).fetchall()
        )
        expected_symbol_ids = {
            (entry.asset_class, entry.symbol, entry.identity_id)
            for entry in expected_symbol_entries
        }
        if actual_symbol_ids != expected_symbol_ids:
            raise RefreshFailure("database canonical ID mismatch for symbol inventory")
        actual_futures = set(
            connection.execute(
                "SELECT DISTINCT root_symbol, expiry_date, contract_id FROM md.futures_daily"
            ).fetchall()
        )
        expected_futures = set()
        for entry in inventory:
            if entry.asset_class != "futures":
                continue
            root, suffix = entry.symbol.rsplit("_", 1)
            if re.fullmatch(r"\d{8}", suffix):
                expiry = date.fromisoformat(f"{suffix[:4]}-{suffix[4:6]}-{suffix[6:8]}")
            elif re.fullmatch(r"\d{6}", suffix):
                expiry = date.fromisoformat(f"{suffix[:4]}-{suffix[4:6]}-01")
            else:
                raise RefreshFailure("database futures inventory has invalid identity")
            expected_futures.add((root, expiry, entry.identity_id))
        if actual_futures != expected_futures:
            raise RefreshFailure(
                "database futures inventory mismatch or canonical ID mismatch: "
                f"expected={sorted(expected_futures)} actual={sorted(actual_futures)}"
            )
        actual_per_identity = {
            (asset_class, symbol): (rows, latest.isoformat() if latest is not None else None)
            for asset_class, symbol, rows, latest in connection.execute(
                "SELECT s.asset_class, s.symbol, count(e.trade_date), max(e.trade_date) "
                "FROM md.symbols s LEFT JOIN md.equities_daily e USING (symbol_id) "
                "GROUP BY s.asset_class, s.symbol"
            ).fetchall()
        }
        expected_per_identity = {
            (entry.asset_class, entry.symbol): (entry.rows, entry.latest_session)
            for entry in expected_symbol_entries
        }
        futures_identity_by_contract = {
            (root, expiry): entry.symbol
            for entry in inventory
            if entry.asset_class == "futures"
            for root, suffix in [entry.symbol.rsplit("_", 1)]
            for expiry in [
                date.fromisoformat(
                    f"{suffix[:4]}-{suffix[4:6]}-"
                    f"{suffix[6:8] if len(suffix) == 8 else '01'}"
                )
            ]
        }
        actual_per_identity.update({
            ("futures", futures_identity_by_contract[(root, expiry)]): (
                rows, latest.isoformat()
            )
            for root, expiry, rows, latest in connection.execute(
                "SELECT root_symbol, expiry_date, count(*), max(trade_date) "
                "FROM md.futures_daily GROUP BY 1, 2"
            ).fetchall()
            if (root, expiry) in futures_identity_by_contract
        })
        expected_per_identity.update({
            (entry.asset_class, entry.symbol): (entry.rows, entry.latest_session)
            for entry in inventory if entry.asset_class == "futures"
        })
        if actual_per_identity != expected_per_identity:
            raise RefreshFailure(
                "database per-identity mismatch: "
                f"expected={expected_per_identity} actual={actual_per_identity}"
            )
        return actual_counts
    finally:
        connection.close()


def _prepare_file(path: Path, payload: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    with temporary.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    return temporary


def _bundle_layout(db_path: Path, manifest_path: Path) -> tuple[Path, Path]:
    if db_path.parent != manifest_path.parent:
        raise RefreshFailure("database and manifest must share one current bundle directory")
    pointer = db_path.parent
    return pointer, pointer.parent / "bundles"


def _recovery_marker(pointer: Path) -> Path:
    return pointer.parent / f".{pointer.name}.recovery.json"


def _reject_symlink_components(path: Path, label: str) -> None:
    """Reject symlinks in ``path`` or any existing ancestor without resolving."""
    absolute = path.absolute()
    current = Path(absolute.anchor)
    for component in absolute.parts[1:]:
        current /= component
        if current.is_symlink():
            raise RefreshFailure(f"{label} must not contain symlink components")


def _validate_bundle_root(
    root: Path, bundles: Path, db_path: Path, manifest_path: Path,
) -> PublishedBundle:
    _reject_symlink_components(bundles, "immutable bundles root")
    _reject_symlink_components(root, "current bundle generation")
    if bundles.is_symlink() or not bundles.is_dir():
        raise RefreshFailure("immutable bundles root must be a real directory, not a symlink")
    if root.parent != bundles:
        raise RefreshFailure("current bundle pointer escapes immutable bundle root")
    if root.is_symlink():
        raise RefreshFailure("current bundle generation must not be a symlink")
    bundles_real = bundles.resolve(strict=True)
    root_real = root.resolve(strict=True)
    if root_real.parent != bundles_real or not root_real.is_dir():
        raise RefreshFailure("current bundle pointer escapes immutable bundle root")
    database = root / db_path.name
    manifest = root / manifest_path.name
    _reject_symlink_components(database, "current bundle database")
    _reject_symlink_components(manifest, "current bundle manifest")
    if database.is_symlink() or manifest.is_symlink():
        raise RefreshFailure("current bundle generation files must not be symlinks")
    if not database.is_file() or not manifest.is_file():
        raise RefreshFailure("current bundle generation is incomplete")
    if _path_identity(database) == _path_identity(manifest):
        raise RefreshFailure("current database and manifest are filesystem aliases")
    document = json.loads(manifest.read_text(encoding="utf-8"))
    expected_hash = document.get("database_sha256")
    if not isinstance(expected_hash, str) or _sha256_file(database) != expected_hash:
        raise RefreshFailure("current bundle database hash mismatch")
    return PublishedBundle(root=root_real, database=database.resolve(), manifest=manifest.resolve())


def resolve_current_bundle(db_path: Path, manifest_path: Path) -> PublishedBundle:
    """Pin and validate one immutable generation for a stable reader.

    Readers must call this once and use the returned resolved paths for both
    files.  Opening the two configured ``current/...`` paths independently can
    cross a concurrent pointer promotion and is intentionally not supported.
    """
    pointer, bundles = _bundle_layout(db_path, manifest_path)
    _reject_symlink_components(pointer.parent, "publication directory")
    _reject_symlink_components(bundles, "immutable bundles root")
    recovery = _recovery_marker(pointer)
    if recovery.exists() or recovery.is_symlink():
        if recovery.is_symlink() or not recovery.is_file():
            raise RefreshFailure("bundle recovery marker is invalid")
        try:
            marker = json.loads(recovery.read_text(encoding="utf-8"))
        except (OSError, ValueError, AttributeError) as exc:
            raise RefreshFailure("bundle recovery marker is invalid") from exc
        if not isinstance(marker, Mapping):
            raise RefreshFailure("bundle recovery marker is invalid")
        state = marker.get("state", "pending")
        if state == "committed":
            successor = marker.get("successor")
            if not isinstance(successor, str) or Path(successor).name != successor:
                raise RefreshFailure("bundle recovery successor is invalid")
            if not pointer.is_symlink():
                raise RefreshFailure("committed bundle recovery pointer is invalid")
            try:
                target = Path(os.readlink(pointer))
            except OSError as exc:
                raise RefreshFailure("committed bundle recovery pointer is invalid") from exc
            pointed_root = target if target.is_absolute() else pointer.parent / target
            if pointed_root != bundles / successor:
                raise RefreshFailure("committed bundle recovery pointer does not match successor")
            return _validate_bundle_root(
                bundles / successor, bundles, db_path, manifest_path,
            )
        if state != "pending":
            raise RefreshFailure("bundle recovery marker state is invalid")
        predecessor = marker.get("predecessor")
        if predecessor is None:
            raise RefreshFailure("bundle publication recovery is required; no predecessor exists")
        if not isinstance(predecessor, str) or Path(predecessor).name != predecessor:
            raise RefreshFailure("bundle recovery predecessor is invalid")
        published = _validate_bundle_root(
            bundles / predecessor, bundles, db_path, manifest_path,
        )
        # A completed rollback (or a failure before pointer replacement) leaves
        # both the marker and pointer naming the predecessor.  A reader may
        # safely retire that stale guard; if cleanup fails, pinning still works.
        if pointer.is_symlink():
            target = Path(os.readlink(pointer))
            pointed_root = target if target.is_absolute() else pointer.parent / target
            if pointed_root == bundles / predecessor:
                try:
                    recovery.unlink()
                    _fsync_directory(pointer.parent)
                except BaseException:
                    pass
        return published
    if not pointer.is_symlink():
        raise RefreshFailure("current bundle pointer is missing or is not a symlink")
    try:
        target = Path(os.readlink(pointer))
    except OSError as exc:
        raise RefreshFailure("current bundle pointer is invalid") from exc
    root = target if target.is_absolute() else pointer.parent / target
    return _validate_bundle_root(root, bundles, db_path, manifest_path)


def _atomic_publish_bundle(
    temp_db: Path,
    db_path: Path,
    manifest_path: Path,
    manifest: object,
    *,
    verify_source: Callable[[], None] = lambda: None,
) -> None:
    """Durably create a generation and publish it behind a recovery guard.

    The pending recovery marker is durable before the only pointer mutation and
    remains reader-visible through pointer fsync and final source verification.
    Publication commits only after that marker is atomically replaced by a
    durable reader-recognized committed state naming the successor.  Marker
    deletion is then idempotent housekeeping: a crash-restored marker still
    resolves the committed successor, so cleanup faults cannot negate success.
    """
    pointer, bundles = _bundle_layout(db_path, manifest_path)
    if (
        not isinstance(manifest, Mapping)
        or manifest.get("database_sha256") != _sha256_file(temp_db)
    ):
        raise RefreshFailure("candidate manifest database hash mismatch")
    _reject_symlink_components(pointer.parent, "publication directory")
    _reject_symlink_components(bundles, "immutable bundles root")
    pointer.parent.mkdir(parents=True, exist_ok=True)
    _reject_symlink_components(pointer.parent, "publication directory")
    bundles.mkdir(mode=0o700, exist_ok=True)
    _reject_symlink_components(bundles, "immutable bundles root")
    if bundles.is_symlink() or not bundles.is_dir():
        raise RefreshFailure("immutable bundles root must be a real directory, not a symlink")
    if pointer.exists() and not pointer.is_symlink():
        raise RefreshFailure("current bundle pointer must not be a directory or regular file")
    generation = bundles / uuid.uuid4().hex
    generation.mkdir(mode=0o700)
    database = generation / db_path.name
    published_manifest = generation / manifest_path.name
    pointer_temp = pointer.parent / f".{pointer.name}.{uuid.uuid4().hex}.tmp"
    rollback_temp = pointer.parent / f".{pointer.name}.{uuid.uuid4().hex}.rollback"
    recovery = _recovery_marker(pointer)
    if recovery.exists() or recovery.is_symlink():
        raise RefreshFailure("unresolved bundle publication recovery marker")
    predecessor: str | None = None
    if pointer.is_symlink():
        predecessor = resolve_current_bundle(db_path, manifest_path).root.name
    promoted = False
    committed = False
    pending_marker = {"state": "pending", "predecessor": predecessor}
    committed_marker_temp: Path | None = None
    marker_transition_started = False
    try:
        with temp_db.open("rb") as source, database.open("xb") as destination:
            shutil.copyfileobj(source, destination)
            destination.flush()
            os.fsync(destination.fileno())
        _write_immutable(published_manifest, manifest)
        _fsync_directory(generation)
        _fsync_directory(bundles)
        pointer_temp.symlink_to(generation.relative_to(pointer.parent), target_is_directory=True)
        _write_immutable(recovery, pending_marker)
        verify_source()
        os.replace(pointer_temp, pointer)
        promoted = True
        verify_source()
        _fsync_directory(pointer.parent)
        verify_source()
        committed_marker_temp = _prepare_file(
            recovery,
            _canonical_bytes({"state": "committed", "successor": generation.name}),
        )
        marker_transition_started = True
        os.replace(committed_marker_temp, recovery)
        committed_marker_temp = None
        _fsync_directory(pointer.parent)
        committed = True
    except BaseException:
        if marker_transition_started and not committed:
            # A failed committed-marker fsync leaves its rename durability
            # uncertain.  Restore the durable predecessor guard before pointer
            # rollback where possible; either surviving state is fail closed.
            try:
                if recovery.exists() and not recovery.is_symlink():
                    restored = _prepare_file(recovery, _canonical_bytes(pending_marker))
                    os.replace(restored, recovery)
                    _fsync_directory(pointer.parent)
            except BaseException:
                pass
        if promoted:
            try:
                if predecessor is None:
                    if pointer.is_symlink():
                        pointer.unlink()
                else:
                    rollback_temp.symlink_to(
                        (bundles / predecessor).relative_to(pointer.parent),
                        target_is_directory=True,
                    )
                    os.replace(rollback_temp, pointer)
                _fsync_directory(pointer.parent)
            except BaseException:
                # The marker was already durable and remains readable.  It is
                # the authoritative reader guard if restoration also fails.
                pass
        try:
            if pointer_temp.is_symlink():
                pointer_temp.unlink()
        except BaseException:
            pass
        try:
            if rollback_temp.is_symlink():
                rollback_temp.unlink()
        except BaseException:
            pass
        try:
            if committed_marker_temp is not None and committed_marker_temp.exists():
                committed_marker_temp.unlink()
        except BaseException:
            pass
        if marker_transition_started:
            # Recovery may itself fault after the committed marker and promoted
            # pointer became the coherent reader-visible state.  Reconcile the
            # API outcome with the production resolver before reporting a
            # failure: otherwise the caller would emit failed terminal evidence
            # for the succeeded generation every fresh reader already admits.
            try:
                if resolve_current_bundle(db_path, manifest_path).root == generation.resolve():
                    committed = True
                    return
            except BaseException:
                pass
        if not pointer.is_symlink() or pointer.resolve(strict=False) != generation:
            shutil.rmtree(generation, ignore_errors=True)
        raise
    finally:
        if committed:
            # The durable committed marker is already sufficient for every
            # fresh reader.  Retirement is best-effort housekeeping only.
            try:
                recovery.unlink()
                _fsync_directory(pointer.parent)
            except BaseException:
                pass


@contextmanager
def _warehouse_refresh_lock(warehouse: Path):
    _reject_symlink_components(warehouse, "warehouse refresh lock root")
    directory_flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        directory_flags |= os.O_DIRECTORY
    if hasattr(os, "O_CLOEXEC"):
        directory_flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        directory_flags |= os.O_NOFOLLOW
    try:
        warehouse_fd = os.open(warehouse, directory_flags)
    except OSError as exc:
        raise RefreshFailure("warehouse refresh lock root must be a real directory") from exc
    descriptor: int | None = None
    try:
        warehouse_stat = os.fstat(warehouse_fd)
        try:
            named_warehouse = os.lstat(warehouse)
        except OSError as exc:
            raise RefreshFailure("warehouse refresh lock path changed during acquisition") from exc
        if (
            not stat.S_ISDIR(warehouse_stat.st_mode)
            or warehouse_stat.st_uid != os.geteuid()
            or warehouse_stat.st_mode & 0o022
            or (warehouse_stat.st_dev, warehouse_stat.st_ino)
            != (named_warehouse.st_dev, named_warehouse.st_ino)
        ):
            raise RefreshFailure("warehouse refresh lock root has unsafe ownership or mode")
        try:
            fcntl.flock(warehouse_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RefreshFailure("warehouse refresh is already in progress") from exc

        lock_name = ".mdw-m0-refresh.lock"
        flags = os.O_RDWR | os.O_CREAT
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(lock_name, flags, 0o600, dir_fd=warehouse_fd)
        except OSError as exc:
            raise RefreshFailure("warehouse refresh lock is unsafe or unavailable") from exc
        opened = os.fstat(descriptor)
        try:
            named = os.stat(lock_name, dir_fd=warehouse_fd, follow_symlinks=False)
        except OSError as exc:
            raise RefreshFailure("warehouse refresh lock path changed during acquisition") from exc
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.geteuid()
            or stat.S_IMODE(opened.st_mode) != 0o600
        ):
            raise RefreshFailure("warehouse refresh lock has unsafe ownership or mode")
        if (
            opened.st_nlink != 1
            or (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino)
        ):
            raise RefreshFailure("warehouse refresh lock must be an unaliased regular file")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RefreshFailure("warehouse refresh is already in progress") from exc
        named_after = os.stat(lock_name, dir_fd=warehouse_fd, follow_symlinks=False)
        opened_after = os.fstat(descriptor)
        if (
            (opened_after.st_dev, opened_after.st_ino)
            != (named_after.st_dev, named_after.st_ino)
            or opened_after.st_nlink != 1
        ):
            raise RefreshFailure("warehouse refresh lock path changed during acquisition")
        yield
    finally:
        if descriptor is not None:
            os.close(descriptor)
        os.close(warehouse_fd)


def _run_result_path(config: RefreshConfig, run_id: str) -> Path:
    inventory_key = hashlib.sha256(
        os.path.abspath(config.inventory_path).encode("utf-8")
    ).hexdigest()
    directory = config.warehouse / ".mdw-m0-run-results" / inventory_key
    return directory / f"{run_id}.json"


def _write_run_result(path: Path, document: Mapping[str, object]) -> None:
    _reject_symlink_components(path.parent, "run-result directory")
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    _reject_symlink_components(path.parent, "run-result directory")
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        directory_fd = os.open(path.parent, flags)
    except OSError as exc:
        raise RefreshFailure("run-result directory must be a real directory") from exc
    descriptor: int | None = None
    created = False
    try:
        directory_stat = os.fstat(directory_fd)
        if (
            not stat.S_ISDIR(directory_stat.st_mode)
            or directory_stat.st_uid != os.geteuid()
            or directory_stat.st_mode & 0o022
        ):
            raise RefreshFailure("run-result directory has unsafe ownership or mode")
        create_flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_CLOEXEC"):
            create_flags |= os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            create_flags |= os.O_NOFOLLOW
        payload = _canonical_bytes(document)
        descriptor = os.open(path.name, create_flags, 0o600, dir_fd=directory_fd)
        created = True
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        opened = os.fstat(descriptor)
        named_file = os.stat(path.name, dir_fd=directory_fd, follow_symlinks=False)
        try:
            named_directory = os.lstat(path.parent)
        except OSError as exc:
            raise RefreshFailure("run-result directory changed during write") from exc
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.geteuid()
            or stat.S_IMODE(opened.st_mode) != 0o600
            or opened.st_nlink != 1
            or (opened.st_dev, opened.st_ino) != (named_file.st_dev, named_file.st_ino)
        ):
            raise RefreshFailure("run-result file changed during write")
        if (directory_stat.st_dev, directory_stat.st_ino) != (
            named_directory.st_dev, named_directory.st_ino,
        ):
            raise RefreshFailure("run-result directory changed during write")
        os.fsync(directory_fd)
    except BaseException:
        if created:
            try:
                os.unlink(path.name, dir_fd=directory_fd)
                os.fsync(directory_fd)
            except BaseException:
                pass
        raise
    finally:
        if descriptor is not None:
            os.close(descriptor)
        os.close(directory_fd)


def _terminal_steps(
    inventory: Sequence[InventoryEntry], steps: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    by_identity = {
        (str(step["asset_class"]), str(step["symbol"])): dict(step)
        for step in steps
    }
    terminal: list[dict[str, object]] = []
    for entry in inventory:
        identity = (entry.asset_class, entry.symbol)
        terminal.append(by_identity.get(identity, {
            "asset_class": entry.asset_class,
            "symbol": entry.symbol,
            "argv_sha256": None,
            "started_at": None,
            "ended_at": None,
            "exit_code": None,
            "status": "not_attempted",
        }))
    return terminal


def _failed_identity_statuses(
    steps: Sequence[Mapping[str, object]],
) -> list[dict[str, str]]:
    """Return the non-sensitive failed identity evidence in canonical order."""
    return [
        {
            "asset_class": str(step["asset_class"]),
            "symbol": str(step["symbol"]),
            "status": "failed",
        }
        for step in steps
        if step.get("status") == "failed"
    ]


def _run_result_document(
    config: RefreshConfig,
    *,
    run_id: str,
    started_at: str,
    audit: Mapping[str, object],
    failure: BaseException | None,
) -> dict[str, object]:
    raw_inventory = audit.get("inventory", [])
    inventory = (
        raw_inventory
        if isinstance(raw_inventory, list)
        and all(isinstance(item, InventoryEntry) for item in raw_inventory)
        else []
    )
    raw_steps = audit.get("steps", [])
    steps = raw_steps if isinstance(raw_steps, list) else []
    identity = audit.get("identity")
    terminal_steps = _terminal_steps(inventory, steps)
    failed_identities = _failed_identity_statuses(terminal_steps)
    return {
        "run_id": run_id,
        "requested_as_of": config.as_of.isoformat(),
        "started_at": started_at,
        "ended_at": _utc_now(),
        "outcome": (
            "failed" if failure is not None
            else "degraded" if failed_identities
            else "succeeded"
        ),
        "failure_type": type(failure).__name__ if failure is not None else None,
        "source_identity": dict(identity) if isinstance(identity, Mapping) else None,
        "inventory_path": str(config.inventory_path),
        "steps": terminal_steps,
        "failed_identities": failed_identities,
    }


def _refresh_all_and_rebuild_locked(
    config: RefreshConfig,
    *,
    audit: dict[str, object],
    run_id: str,
    started_at: str,
    command_runner: CommandRunner | None = None,
    phase_hook: PhaseHook = lambda _phase: None,
    source_identity: SourceIdentity = _source_identity,
) -> dict[str, object]:
    """Execute the complete M0 transaction; publish nothing until all gates pass."""
    _validate_config(config)
    bronze_root = config.warehouse / "data-lake" / "bronze"
    identity = source_identity(config.repo_root)
    audit["identity"] = dict(identity)
    steps: list[dict[str, object]] = []
    audit["steps"] = steps
    current_futures_contracts: dict[str, dict[str, object]] = {}

    before = discover_inventory(
        bronze_root,
        require_all_asset_classes=False,
        allow_legacy_volatility_ids=True,
    )
    if not before:
        raise RefreshFailure("canonical bronze inventory is empty")
    if config.bootstrap_current_vxm and config.refresh_broker_assets:
        initial_before = tuple(before)
        prior_vxm = tuple(
            entry
            for entry in initial_before
            if entry.asset_class == "futures" and entry.symbol.startswith("VXM_")
        )
        futures_entries = tuple(
            entry for entry in initial_before if entry.asset_class == "futures"
        )
        with tempfile.TemporaryDirectory(prefix="mdw-futures-owner-") as bootstrap_name:
            with _sealed_execution_source(config.repo_root, identity) as sealed_source:
                execution_config = replace(
                    config, source_archive_fd=sealed_source.fileno(),
                )
                runner = command_runner or _default_runner(execution_config)
                verify_source = lambda: _verify_source_identity(
                    config.repo_root, identity, source_identity
                )
                mapping, futures_steps = _refresh_futures_owner(
                    execution_config,
                    futures_entries,
                    runner,
                    Path(bootstrap_name),
                    verify_source,
                )
                steps.extend(futures_steps)
        try:
            post_bootstrap = discover_inventory(
                bronze_root,
                require_all_asset_classes=False,
                allow_legacy_volatility_ids=True,
            )
        except (RefreshFailure, OSError, pa.ArrowInvalid) as exc:
            raise RefreshFailure("dynamic VXM owner mutated bronze before failing") from exc
        initial_map = {
            (entry.asset_class, entry.symbol): entry for entry in initial_before
        }
        post_map = {
            (entry.asset_class, entry.symbol): entry for entry in post_bootstrap
        }
        failed = {
            (str(step["asset_class"]), str(step["symbol"]))
            for step in futures_steps
            if step["status"] == "failed"
        }
        unsafe_mutation = any(
            identity_key not in initial_map
            or identity_key not in post_map
            or initial_map[identity_key].sha256 != post_map[identity_key].sha256
            for identity_key in failed
        )
        if mapping is None:
            post_vxm = tuple(
                entry
                for entry in post_bootstrap
                if entry.asset_class == "futures" and entry.symbol.startswith("VXM_")
            )
            unsafe_mutation = unsafe_mutation or prior_vxm != post_vxm
            if unsafe_mutation:
                raise RefreshFailure("dynamic VXM owner mutated bronze before failing")
            if not prior_vxm:
                raise RefreshFailure(
                    "dynamic VXM owner failed with no preservable VXM predecessor"
                )
        else:
            mapped_identity = ("futures", str(mapping["symbol"]))
            if mapped_identity not in post_map:
                raise RefreshFailure("dynamic VXM owner mapping identity is absent")
            current_futures_contracts["VXM"] = mapping
        before = post_bootstrap
    if current_futures_contracts:
        current_vxm_symbol = str(current_futures_contracts["VXM"]["symbol"])
        timestamp = _utc_now()
        for entry in before:
            if (
                entry.asset_class == "futures"
                and entry.symbol.startswith("VXM_")
                and entry.symbol != current_vxm_symbol
            ):
                steps.append({
                    "asset_class": "futures",
                    "symbol": entry.symbol,
                    "argv_sha256": None,
                    "started_at": timestamp,
                    "ended_at": timestamp,
                    "exit_code": None,
                    "status": "preserved",
                })
    audit["inventory"] = before
    inventory_document = {
        "requested_as_of": config.as_of.isoformat(),
        "script_commit": identity["commit"],
        "script_tree": identity["tree"],
        "inventory": [_entry_document(entry) for entry in before],
    }
    _write_immutable(config.inventory_path, inventory_document)
    phase_hook("inventory")

    temp_db_parent = config.db_path.parent.parent
    temp_db_parent.mkdir(parents=True, exist_ok=True)
    temp_db = temp_db_parent / f".{config.db_path.name}.{uuid.uuid4().hex}.tmp"
    try:
        with tempfile.TemporaryDirectory(prefix="mdw-m0-") as scratch_name:
            scratch = Path(scratch_name)
            source_root = scratch / "mdw-source-tree"
            _materialize_source_tree(config.repo_root, identity, source_root)
            with _sealed_execution_source(config.repo_root, identity) as sealed_source:
                execution_config = replace(
                    config,
                    repo_root=source_root,
                    source_archive_fd=sealed_source.fileno(),
                )
                runner = command_runner or _default_runner(execution_config)
                verify_source = lambda: _verify_source_identity(
                    config.repo_root, identity, source_identity
                )
                def verify_execution_source() -> None:
                    verify_source()
                    _verify_materialized_source_tree(config.repo_root, identity, source_root)
                presets = scratch / "presets"
                presets.mkdir()
                try:
                    pre_refreshed = {
                        (entry.asset_class, entry.symbol)
                        for entry in before
                        if config.bootstrap_current_vxm
                        and config.refresh_broker_assets
                        and entry.asset_class == "futures"
                    }
                    refresh_inventory = [
                        entry for entry in before
                        if (entry.asset_class, entry.symbol) not in pre_refreshed
                    ]
                    _refresh_inventory(
                        execution_config, refresh_inventory, runner, presets,
                        verify_execution_source, steps,
                    )
                    steps.sort(key=lambda step: (str(step["asset_class"]), str(step["symbol"])))
                    verify_execution_source()
                finally:
                    for path in source_root.rglob("*"):
                        if path.is_dir():
                            path.chmod(0o700)
                    source_root.chmod(0o700)
        phase_hook("refresh")

        after = discover_inventory(bronze_root, require_all_asset_classes=False)
        before_identities = {(entry.asset_class, entry.symbol) for entry in before}
        _validate_post_inventory(
            before,
            after,
            config.as_of,
            refresh_broker_assets=config.refresh_broker_assets,
            failed_identities=[
                (str(step["asset_class"]), str(step["symbol"]))
                for step in steps
                if step["status"] in {"failed", "preserved"}
                and (str(step["asset_class"]), str(step["symbol"]))
                in before_identities
            ],
        )
        _build_database(config, temp_db)
        phase_hook("rebuild")

        row_counts = validate_database(temp_db, after)
        phase_hook("validation")
        with temp_db.open("rb") as handle:
            os.fsync(handle.fileno())
        db_sha256 = _sha256_file(temp_db)
        failed_identities = _failed_identity_statuses(steps)
        outcome = "degraded" if failed_identities else "succeeded"
        manifest: dict[str, object] = {
            "script_commit": identity["commit"],
            "script_tree": identity["tree"],
            "requested_as_of": config.as_of.isoformat(),
            "outcome": outcome,
            "steps": steps,
            "failed_identities": failed_identities,
            "current_futures_contracts": current_futures_contracts,
            "pre_refresh_inventory": [_entry_document(entry) for entry in before],
            "post_refresh_inventory": [_entry_document(entry) for entry in after],
            "latest_sessions": {
                f"{entry.asset_class}:{entry.symbol}": entry.latest_session for entry in after
            },
            "row_counts": row_counts,
            "schemas": {
                "bronze": {
                    asset_class: list(
                        FUTURES_SCHEMA if asset_class == "futures" else SYMBOL_SCHEMA
                    )
                    for asset_class in ASSET_CLASSES
                },
                "duckdb": {name: list(columns) for name, columns in DB_SCHEMAS.items()},
            },
            "database_sha256": db_sha256,
            "publication": {
                "db_path": str(config.db_path),
                "published": True,
                "sha256": db_sha256,
            },
        }
        manifest["run_result"] = _run_result_document(
            config,
            run_id=run_id,
            started_at=started_at,
            audit=audit,
            failure=None,
        )
        _verify_source_identity(config.repo_root, identity, source_identity)
        _validate_config(config)
        _atomic_publish_bundle(
            temp_db,
            config.db_path,
            config.manifest_path,
            manifest,
            verify_source=verify_source,
        )
        return manifest
    finally:
        if temp_db.exists():
            temp_db.unlink()


def refresh_all_and_rebuild(
    config: RefreshConfig,
    *,
    command_runner: CommandRunner | None = None,
    phase_hook: PhaseHook = lambda _phase: None,
    source_identity: SourceIdentity = _source_identity,
) -> dict[str, object]:
    """Run one warehouse-scoped M0 transaction with immutable terminal evidence."""
    with _warehouse_refresh_lock(config.warehouse):
        run_id = uuid.uuid4().hex
        result_path = _run_result_path(config, run_id)
        started_at = _utc_now()
        audit: dict[str, object] = {}
        try:
            return _refresh_all_and_rebuild_locked(
                config,
                audit=audit,
                run_id=run_id,
                started_at=started_at,
                command_runner=command_runner,
                phase_hook=phase_hook,
                source_identity=source_identity,
            )
        except BaseException as failure:
            document = _run_result_document(
                config,
                run_id=run_id,
                started_at=started_at,
                audit=audit,
                failure=failure,
            )
            try:
                _write_run_result(result_path, document)
            except BaseException as evidence_failure:
                raise failure from evidence_failure
            raise


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    warehouse = Path.home() / "market-warehouse"
    parser.add_argument("--warehouse", type=Path, default=warehouse)
    parser.add_argument(
        "--db-path", type=Path, default=warehouse / "duckdb" / "current" / "market.duckdb"
    )
    parser.add_argument("--manifest", "--manifest-path", dest="manifest_path", type=Path, required=True)
    parser.add_argument("--inventory-path", type=Path)
    parser.add_argument("--as-of", type=date.fromisoformat, required=True)
    parser.add_argument("--python", type=Path, default=warehouse / ".venv" / "bin" / "python")
    parser.add_argument("--command-timeout", type=int, default=3600)
    parser.add_argument(
        "--preserve-broker-assets",
        action="store_true",
        help="preserve equity/futures bronze without accessing an IB-backed provider",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    db_path = args.db_path
    if db_path.name == "market.duckdb" and db_path.parent.name == "duckdb":
        db_path = db_path.parent / "current" / "market.duckdb"
    inventory_path = args.inventory_path
    if inventory_path is None:
        inventory_path = (
            args.manifest_path.parent.parent
            / "inventories"
            / f"pre-refresh-{args.as_of.isoformat()}-{uuid.uuid4().hex}.json"
        )
    config = RefreshConfig(
        warehouse=args.warehouse,
        db_path=db_path,
        manifest_path=args.manifest_path,
        inventory_path=inventory_path,
        as_of=args.as_of,
        python=args.python,
        command_timeout=args.command_timeout,
        refresh_broker_assets=not args.preserve_broker_assets,
    )
    try:
        refresh_all_and_rebuild(config)
    except RefreshFailure as exc:
        print(f"refresh failed: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"refresh failed: {type(exc).__name__}", file=sys.stderr)
        return 1
    return 0


from scripts._entrypoint import run_main


run_main(__name__, main, exit_with_result=True)
