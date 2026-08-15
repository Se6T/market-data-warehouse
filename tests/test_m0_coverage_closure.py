"""Focused fail-closed branch coverage for the Phase 3 M0 publisher."""
from __future__ import annotations

import hashlib
import io
import json
import os
import subprocess
import tarfile
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from scripts import refresh_all_and_rebuild as m0


def _config(tmp_path: Path) -> m0.RefreshConfig:
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir(mode=0o700)
    current = warehouse / "duckdb" / "current"
    return m0.RefreshConfig(
        warehouse=warehouse,
        db_path=current / "market.duckdb",
        manifest_path=current / "manifest.json",
        inventory_path=tmp_path / "inventory.json",
        as_of=date(2025, 1, 2),
        python=Path("/safe/python"),
        repo_root=tmp_path,
    )


def _entry(asset_class: str = "crypto", symbol: str = "BTC") -> m0.InventoryEntry:
    return m0.InventoryEntry(
        asset_class=asset_class,
        symbol=symbol,
        path=f"asset_class={asset_class}/symbol={symbol}/data.parquet",
        sha256="0" * 64,
        rows=1,
        latest_session="2025-01-02",
        identity_id=1,
        schema=("trade_date:date32[day]",),
        schema_sha256="1" * 64,
    )


def _publish(tmp_path: Path, payload: bytes = b"database") -> tuple[Path, Path, m0.PublishedBundle]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    current = tmp_path / "published" / "current"
    db_path, manifest_path = current / "market.duckdb", current / "manifest.json"
    candidate = tmp_path / "candidate.tmp"
    candidate.write_bytes(payload)
    m0._atomic_publish_bundle(
        candidate,
        db_path,
        manifest_path,
        {"database_sha256": hashlib.sha256(payload).hexdigest()},
    )
    return db_path, manifest_path, m0.resolve_current_bundle(db_path, manifest_path)


def _tar_with(member: tarfile.TarInfo, payload: bytes = b"") -> bytes:
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w") as archive:
        archive.addfile(member, io.BytesIO(payload) if member.isfile() else None)
    return output.getvalue()


def test_cboe_end_date_filters_and_fails_closed_when_nothing_is_eligible(
    tmp_path: Path,
) -> None:
    from scripts import fetch_cboe_volatility as cboe

    bars = [
        {"date": "2025-01-02", "open": "10", "high": "11", "low": "9", "close": "10", "volume": "0"},
        {"date": "2025-01-04", "open": "12", "high": "13", "low": "11", "close": "12", "volume": "0"},
    ]
    with patch("sys.argv", ["fetch", "--symbols", "VIX", "--end", "2025-01-02", "--warehouse", str(tmp_path)]), patch.object(
        cboe, "fetch_cboe_historical", return_value=bars,
    ), patch.object(cboe, "write_bronze_parquet") as write:
        assert cboe.main() == 0
    written_table = write.call_args.args[0]
    assert written_table.num_rows == 1
    assert written_table.column("trade_date").to_pylist() == [date(2025, 1, 2)]

    with patch("sys.argv", ["fetch", "--symbols", "VVIX", "--end", "2025-01-01", "--warehouse", str(tmp_path)]), patch.object(
        cboe, "fetch_cboe_historical", return_value=bars,
    ), patch.object(cboe, "write_bronze_parquet") as write:
        assert cboe.main() == 1
    write.assert_not_called()


def test_trusted_git_rejects_missing_and_non_executable_binary(tmp_path: Path) -> None:
    with patch.object(m0.shutil, "which", return_value=None), pytest.raises(
        m0.RefreshFailure, match="unavailable",
    ):
        m0._trusted_git()

    fake = tmp_path / "git"
    fake.write_text("not executable")
    with patch.object(m0.shutil, "which", return_value=str(fake)), pytest.raises(
        m0.RefreshFailure, match="invalid",
    ):
        m0._trusted_git()


@pytest.mark.parametrize("attack", ["split_bundle", "symlink_output", "inside_bronze"])
def test_config_rejects_unsafe_output_layouts(tmp_path: Path, attack: str) -> None:
    config = _config(tmp_path)
    if attack == "split_bundle":
        object.__setattr__(config, "manifest_path", tmp_path / "elsewhere" / "manifest.json")
        message = "must share"
    elif attack == "symlink_output":
        config.db_path.parent.mkdir(parents=True)
        target = tmp_path / "target"
        target.write_text("outside")
        config.db_path.symlink_to(target)
        message = "must not be a symlink"
    else:
        bronze = config.warehouse / "data-lake" / "bronze"
        bronze.mkdir(parents=True)
        object.__setattr__(config, "inventory_path", bronze / "inventory.json")
        message = "outside canonical bronze"
    with pytest.raises(m0.RefreshFailure, match=message):
        m0._validate_config(config)


@pytest.mark.parametrize("attack", ["bronze", "asset", "parquet"])
def test_inventory_rejects_symlinked_canonical_layers(tmp_path: Path, attack: str) -> None:
    real = tmp_path / "real"
    real.mkdir()
    bronze = tmp_path / "bronze"
    if attack == "bronze":
        bronze.symlink_to(real, target_is_directory=True)
    else:
        bronze.mkdir()
        asset = bronze / "asset_class=crypto"
        if attack == "asset":
            asset.symlink_to(real, target_is_directory=True)
        else:
            identity = asset / "symbol=BTC"
            identity.mkdir(parents=True)
            outside = tmp_path / "outside.parquet"
            outside.write_bytes(b"not read")
            identity.joinpath("data.parquet").symlink_to(outside)
    with pytest.raises(m0.RefreshFailure, match="symlink"):
        m0.discover_inventory(bronze, require_all_asset_classes=False)


def test_inventory_rejects_resolved_escape_and_duplicate_discovery(tmp_path: Path) -> None:
    bronze = tmp_path / "bronze"
    identity = bronze / "asset_class=crypto" / "symbol=BTC"
    identity.mkdir(parents=True)
    parquet = identity / "data.parquet"
    parquet.write_bytes(b"not read")
    real_resolve = Path.resolve

    def escaped(path: Path, *args, **kwargs):
        if path == parquet:
            return tmp_path / "outside.parquet"
        return real_resolve(path, *args, **kwargs)

    with patch.object(Path, "resolve", escaped), pytest.raises(m0.RefreshFailure, match="escapes"):
        m0.discover_inventory(bronze, require_all_asset_classes=False)

    entry = _entry()
    with patch.object(m0, "ASSET_CLASSES", ("crypto",)), patch.object(
        Path, "glob", return_value=[identity, identity],
    ), patch.object(m0, "_path_identity", side_effect=[(1, 1), (1, 2)]), patch.object(
        m0.pq, "ParquetFile",
    ) as parquet_file, patch.object(m0, "stable_symbol_id", return_value=1), patch.object(
        m0, "_sha256_file", return_value=entry.sha256,
    ), pytest.raises(m0.RefreshFailure, match="duplicate canonical bronze identities"):
        table = MagicMock()
        table.column_names = list(m0.SYMBOL_COLUMNS)
        table.schema = [SimpleNamespace(name=name, type=kind) for name, kind in [
            ("trade_date", "date32[day]"), ("symbol_id", "int64"), ("open", "double"),
            ("high", "double"), ("low", "double"), ("close", "double"),
            ("adj_close", "double"), ("volume", "int64"),
        ]]
        table.num_rows = 1
        table.column.side_effect = lambda name: MagicMock(
            to_pylist=lambda: [date(2025, 1, 2)] if name == "trade_date" else [1]
        )
        parquet_file.return_value.read.return_value = table
        m0.discover_inventory(bronze, require_all_asset_classes=False)


def test_source_identity_rejects_incomplete_noncommit_and_index_mismatch(tmp_path: Path) -> None:
    with pytest.raises(m0.RefreshFailure, match="incomplete"):
        m0._validate_recorded_source(tmp_path, {"commit": "only"})

    completed = [
        subprocess.CompletedProcess([], 0, "", ""),
        subprocess.CompletedProcess([], 0, "head", ""),
        subprocess.CompletedProcess([], 0, "tree", ""),
    ]
    with patch.object(m0, "_git_text", side_effect=["", "head", "tree"]), pytest.raises(
        m0.RefreshFailure, match="not a commit",
    ):
        m0._source_identity(tmp_path)

    with patch.object(m0, "_git_text", side_effect=["", "head", "commit", "committed", "index"]), pytest.raises(
        m0.RefreshFailure, match="index does not match",
    ):
        m0._source_identity(tmp_path)


def test_archive_parsing_and_materialization_reject_links_incomplete_files_and_escape(
    tmp_path: Path,
) -> None:
    link = tarfile.TarInfo("link")
    link.type = tarfile.SYMTYPE
    link.linkname = "target"
    with pytest.raises(m0.RefreshFailure, match="unsupported link"):
        m0._archive_members(_tar_with(link))

    source = MagicMock()
    regular = MagicMock()
    regular.isdir.return_value = False
    regular.isfile.return_value = True
    regular.name = "missing"
    source.__enter__.return_value.getmembers.return_value = [regular]
    source.__enter__.return_value.extractfile.return_value = None
    with patch.object(m0.tarfile, "open", return_value=source), pytest.raises(
        m0.RefreshFailure, match="incomplete",
    ):
        m0._archive_members(b"archive")

    for member, message in [(link, "unsupported link"), (tarfile.TarInfo("../escape"), "escapes")]:
        if member.name == "../escape":
            member.size = 0
        destination = tmp_path / f"materialized-{message.replace(' ', '-')}"
        with patch.object(m0, "_source_archive", return_value=_tar_with(member)), pytest.raises(
            m0.RefreshFailure, match=message,
        ):
            m0._materialize_source_tree(tmp_path, {"commit": "c", "tree": "t"}, destination)


def test_source_verifier_rejects_midrun_identity_change(tmp_path: Path) -> None:
    with pytest.raises(m0.RefreshFailure, match="changed during"):
        m0._verify_source_identity(tmp_path, {"commit": "a"}, lambda _root: {"commit": "b"})


def test_refresh_inventory_initializes_steps_and_calendar_is_fail_closed(tmp_path: Path) -> None:
    config = _config(tmp_path)
    assert m0._refresh_inventory(config, [], lambda argv: subprocess.CompletedProcess(argv, 0), tmp_path) == []
    assert m0.expected_latest_session("futures", date(2025, 1, 5)) == date(2025, 1, 3)
    with pytest.raises(m0.RefreshFailure, match="unsupported asset calendar"):
        m0.expected_latest_session("options", date(2025, 1, 2))


def test_database_constraint_contract_mismatch_is_rejected(tmp_path: Path) -> None:
    from tests.test_refresh_all_and_rebuild import _built_database

    db_path, inventory = _built_database(tmp_path)
    with patch.object(m0, "DB_CONSTRAINTS", set()), pytest.raises(
        m0.RefreshFailure, match="constraint mismatch",
    ):
        m0.validate_database(db_path, inventory)


def test_prepare_file_is_durable_create_only_and_bundle_layout_is_coherent(tmp_path: Path) -> None:
    destination = tmp_path / "nested" / "payload.bin"
    with patch.object(m0.os, "fsync", wraps=os.fsync) as fsync:
        temporary = m0._prepare_file(destination, b"payload")
    assert temporary.parent == destination.parent
    assert temporary.read_bytes() == b"payload"
    assert fsync.call_count == 1
    with pytest.raises(m0.RefreshFailure, match="must share"):
        m0._bundle_layout(tmp_path / "one" / "db", tmp_path / "two" / "manifest")


@pytest.mark.parametrize("attack", ["bundles_missing", "generation_file", "generation_symlink", "nested_symlink", "incomplete", "hardlink", "hash"])
def test_bundle_validation_rejects_corrupt_generation(tmp_path: Path, attack: str) -> None:
    bundles = tmp_path / "bundles"
    root = bundles / "generation"
    db_path = tmp_path / "current" / "market.duckdb"
    manifest_path = tmp_path / "current" / "manifest.json"
    if attack != "bundles_missing":
        bundles.mkdir()
    if attack == "generation_file":
        root.write_text("not a directory")
    elif attack == "generation_symlink":
        real_generation = bundles / "real-generation"
        real_generation.mkdir()
        root.symlink_to(real_generation, target_is_directory=True)
    elif attack != "bundles_missing":
        root.mkdir()
        database = root / db_path.name
        manifest = root / manifest_path.name
        database.write_bytes(b"db")
        manifest.write_text(json.dumps({"database_sha256": hashlib.sha256(b"db").hexdigest()}))
        if attack == "nested_symlink":
            outside = tmp_path / "outside.db"
            outside.write_bytes(b"db")
            database.unlink()
            database.symlink_to(outside)
        elif attack == "incomplete":
            manifest.unlink()
        elif attack == "hardlink":
            manifest.unlink()
            os.link(database, manifest)
        elif attack == "hash":
            manifest.write_text(json.dumps({"database_sha256": "wrong"}))
    messages = {
        "bundles_missing": "bundles root",
        "generation_file": "escapes immutable bundle root",
        "generation_symlink": "generation must not be a symlink",
        "nested_symlink": "generation files must not be symlinks",
        "incomplete": "incomplete",
        "hardlink": "filesystem aliases",
        "hash": "hash mismatch",
    }
    reject = patch.object(m0, "_reject_symlink_components") if attack in {"generation_symlink", "nested_symlink"} else patch.object(
        m0, "_reject_symlink_components", wraps=m0._reject_symlink_components,
    )
    with reject, pytest.raises(m0.RefreshFailure, match=messages[attack]):
        m0._validate_bundle_root(root, bundles, db_path, manifest_path)


def test_resolver_rejects_invalid_recovery_markers_and_pointer_states(tmp_path: Path) -> None:
    current = tmp_path / "published" / "current"
    db_path, manifest_path = current / "market.duckdb", current / "manifest.json"
    recovery = current.parent / ".current.recovery.json"
    current.parent.mkdir(parents=True)

    recovery.mkdir()
    with pytest.raises(m0.RefreshFailure, match="marker is invalid"):
        m0.resolve_current_bundle(db_path, manifest_path)
    recovery.rmdir()

    for payload, message in [("not-json", "marker is invalid"), ('{"predecessor":"../escape"}', "predecessor is invalid")]:
        recovery.write_text(payload)
        with pytest.raises(m0.RefreshFailure, match=message):
            m0.resolve_current_bundle(db_path, manifest_path)
        recovery.unlink()

    with pytest.raises(m0.RefreshFailure, match="pointer is missing"):
        m0.resolve_current_bundle(db_path, manifest_path)
    current.write_text("regular file")
    with patch.object(Path, "is_symlink", lambda path: path == current), patch.object(
        m0.os, "readlink", side_effect=OSError("race"),
    ), pytest.raises(m0.RefreshFailure, match="pointer is invalid"):
        m0.resolve_current_bundle(db_path, manifest_path)


def test_recovery_cleanup_failure_does_not_prevent_reader_pinning(tmp_path: Path) -> None:
    db_path, manifest_path, bundle = _publish(tmp_path)
    recovery = db_path.parent.parent / ".current.recovery.json"
    recovery.write_text(json.dumps({"predecessor": bundle.root.name}))
    with patch.object(m0, "_fsync_directory", side_effect=OSError("cleanup failed")):
        pinned = m0.resolve_current_bundle(db_path, manifest_path)
    assert pinned.database.read_bytes() == b"database"
    assert not recovery.exists()


def test_resolver_rejects_invalid_committed_marker_states(tmp_path: Path) -> None:
    db_path, manifest_path, bundle = _publish(tmp_path)
    pointer = db_path.parent
    recovery = pointer.parent / ".current.recovery.json"

    for marker, message in [
        ([], "marker is invalid"),
        ({"state": "unknown"}, "marker state is invalid"),
        ({"state": "committed", "successor": "../escape"}, "successor is invalid"),
        ({"state": "committed", "successor": "other"}, "does not match successor"),
    ]:
        recovery.write_text(json.dumps(marker))
        with pytest.raises(m0.RefreshFailure, match=message):
            m0.resolve_current_bundle(db_path, manifest_path)
        recovery.unlink()

    marker = {"state": "committed", "successor": bundle.root.name}
    recovery.write_text(json.dumps(marker))
    pointer.unlink()
    with pytest.raises(m0.RefreshFailure, match="pointer is invalid"):
        m0.resolve_current_bundle(db_path, manifest_path)
    pointer.symlink_to(bundle.root.relative_to(pointer.parent), target_is_directory=True)
    with patch.object(m0.os, "readlink", side_effect=OSError("race")), pytest.raises(
        m0.RefreshFailure, match="pointer is invalid",
    ):
        m0.resolve_current_bundle(db_path, manifest_path)


def test_committed_marker_temp_cleanup_fault_preserves_transition_failure(tmp_path: Path) -> None:
    db_path, manifest_path, _bundle = _publish(tmp_path, b"old")
    candidate = tmp_path / "new"
    candidate.write_bytes(b"new")
    real_replace, real_unlink = os.replace, Path.unlink
    replacements = 0

    def fail_transition(source, destination):
        nonlocal replacements
        replacements += 1
        if replacements == 2:
            raise KeyboardInterrupt()
        real_replace(source, destination)

    def fail_marker_temp_cleanup(path: Path, *args, **kwargs):
        if path.name.startswith("..current.recovery.json.") and path.name.endswith(".tmp"):
            raise OSError("marker temp cleanup failed")
        real_unlink(path, *args, **kwargs)

    with patch.object(m0.os, "replace", side_effect=fail_transition), patch.object(
        Path, "unlink", fail_marker_temp_cleanup,
    ), pytest.raises(KeyboardInterrupt):
        m0._atomic_publish_bundle(
            candidate,
            db_path,
            manifest_path,
            {"database_sha256": hashlib.sha256(b"new").hexdigest()},
        )


@pytest.mark.parametrize("attack", ["manifest_hash", "bundles_race", "regular_pointer", "recovery"])
def test_publication_rejects_invalid_preconditions(tmp_path: Path, attack: str) -> None:
    current = tmp_path / "published" / "current"
    db_path, manifest_path = current / "market.duckdb", current / "manifest.json"
    candidate = tmp_path / "candidate"
    candidate.write_bytes(b"db")
    manifest = {"database_sha256": hashlib.sha256(b"db").hexdigest()}
    context = patch.object(m0, "_reject_symlink_components", wraps=m0._reject_symlink_components)
    if attack == "manifest_hash":
        manifest["database_sha256"] = "wrong"
        message = "candidate manifest"
    elif attack == "bundles_race":
        original = Path.is_dir
        context = patch.object(Path, "is_dir", lambda path: False if path.name == "bundles" else original(path))
        message = "bundles root"
    elif attack == "regular_pointer":
        current.parent.mkdir(parents=True)
        current.write_text("not a pointer")
        message = "must not be a directory or regular file"
    else:
        current.parent.mkdir(parents=True)
        (current.parent / ".current.recovery.json").write_text("{}")
        message = "unresolved"
    with context, pytest.raises(m0.RefreshFailure, match=message):
        m0._atomic_publish_bundle(candidate, db_path, manifest_path, manifest)


def test_publication_cleanup_faults_preserve_original_failure(tmp_path: Path) -> None:
    current = tmp_path / "published" / "current"
    db_path, manifest_path = current / "market.duckdb", current / "manifest.json"
    candidate = tmp_path / "candidate"
    candidate.write_bytes(b"db")
    real_unlink = Path.unlink

    def fail_temp_unlink(path: Path, *args, **kwargs):
        if path.name.startswith(".current.") and path.name.endswith(".tmp"):
            raise OSError("temp cleanup failed")
        return real_unlink(path, *args, **kwargs)

    with patch.object(Path, "unlink", fail_temp_unlink), pytest.raises(KeyboardInterrupt):
        m0._atomic_publish_bundle(
            candidate, db_path, manifest_path,
            {"database_sha256": hashlib.sha256(b"db").hexdigest()},
            verify_source=MagicMock(side_effect=KeyboardInterrupt()),
        )

    db_path, manifest_path, _bundle = _publish(tmp_path / "second", b"old")
    candidate = tmp_path / "second" / "new"
    candidate.write_bytes(b"new")
    real_replace = os.replace
    replacements = 0

    def fail_rollback(source, destination):
        nonlocal replacements
        replacements += 1
        if replacements == 2:
            raise OSError("rollback failed")
        real_replace(source, destination)

    def fail_rollback_unlink(path: Path, *args, **kwargs):
        if path.name.endswith(".rollback"):
            raise OSError("rollback cleanup failed")
        return real_unlink(path, *args, **kwargs)

    verifier = MagicMock(side_effect=[None, KeyboardInterrupt()])
    with patch.object(m0.os, "replace", side_effect=fail_rollback), patch.object(
        Path, "unlink", fail_rollback_unlink,
    ), pytest.raises(KeyboardInterrupt):
        m0._atomic_publish_bundle(
            candidate, db_path, manifest_path,
            {"database_sha256": hashlib.sha256(b"new").hexdigest()},
            verify_source=verifier,
        )


def test_refresh_lock_and_run_result_reject_invalid_directory_races(tmp_path: Path) -> None:
    missing = tmp_path / "missing"
    with pytest.raises(m0.RefreshFailure, match="lock root"):
        with m0._warehouse_refresh_lock(missing):
            pass

    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    real_lstat = os.lstat
    with patch.object(m0.os, "lstat", side_effect=OSError("changed")), pytest.raises(
        m0.RefreshFailure, match="path changed",
    ):
        with m0._warehouse_refresh_lock(warehouse):
            pass

    result = tmp_path / "results" / "run.json"
    result.parent.mkdir()
    result.parent.chmod(0o777)
    with pytest.raises(m0.RefreshFailure, match="unsafe ownership or mode"):
        m0._write_run_result(result, {"outcome": "failed"})


def test_run_result_directory_symlink_swap_cannot_redirect_audit(
    tmp_path: Path,
) -> None:
    result = tmp_path / "results" / "run.json"
    result.parent.mkdir()
    result.parent.chmod(0o700)
    outside = tmp_path / "outside"
    outside.mkdir()
    displaced = tmp_path / "displaced-results"
    real_open = os.open
    attacked = False

    def swap_then_open(path, flags, mode=0o777, *, dir_fd=None):
        nonlocal attacked
        if not attacked and Path(path).name == result.name:
            attacked = True
            result.parent.rename(displaced)
            result.parent.symlink_to(outside, target_is_directory=True)
        return real_open(path, flags, mode, dir_fd=dir_fd)

    with patch.object(m0.os, "open", side_effect=swap_then_open), pytest.raises(
        m0.RefreshFailure, match="run-result directory changed",
    ):
        m0._write_run_result(result, {"outcome": "failed"})

    assert attacked
    assert not (outside / result.name).exists()
