"""Focused fail-closed branch coverage for the Phase 3 M0 publisher."""
from __future__ import annotations

import hashlib
import io
import json
import os
import subprocess
import tarfile
import contextlib
import tempfile
from dataclasses import replace
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from scripts import refresh_all_and_rebuild as m0


def test_shared_script_entrypoint_runs_only_for_main_and_preserves_exit_code() -> None:
    from scripts._entrypoint import run_main

    main = MagicMock(return_value=7)
    run_main("scripts.owner", main)
    main.assert_not_called()

    run_main("__main__", main)
    main.assert_called_once_with()

    with pytest.raises(SystemExit) as raised:
        run_main("__main__", main, exit_with_result=True)
    assert raised.value.code == 7


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


def test_config_rejects_non_boolean_broker_refresh_flag(tmp_path: Path) -> None:
    config = _config(tmp_path)
    object.__setattr__(config, "refresh_broker_assets", 1)
    with pytest.raises(m0.RefreshFailure, match="exact bool"):
        m0._validate_config(config)


def test_refresh_rejects_empty_canonical_inventory(tmp_path: Path) -> None:
    config = _config(tmp_path)
    (config.warehouse / "data-lake" / "bronze").mkdir(parents=True)
    identity = lambda _root: {"commit": "a" * 40, "tree": "b" * 40}
    with pytest.raises(m0.RefreshFailure, match="inventory is empty"):
        m0.refresh_all_and_rebuild(config, source_identity=identity)


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

    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w") as target:
        directory = tarfile.TarInfo("scripts")
        directory.type = tarfile.DIRTYPE
        target.addfile(directory)
        regular_member = tarfile.TarInfo("scripts/owner.py")
        regular_member.size = len(b"owner")
        target.addfile(regular_member, io.BytesIO(b"owner"))
    assert m0._archive_members(archive.getvalue()) == {"scripts/owner.py": b"owner"}

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


@pytest.mark.parametrize(
    ("listing", "message"),
    [
        (b"malformed\0", "inventory is malformed"),
        (b"040000 tree deadbeef\tdirectory\0", "unsupported entries"),
        (
            b"100644 blob deadbeef\tduplicate.py\0"
            b"100644 blob deadbeef\tduplicate.py\0",
            "duplicate paths",
        ),
    ],
)
def test_git_tree_inventory_rejects_malformed_unsupported_and_duplicate_entries(
    tmp_path: Path, listing: bytes, message: str,
) -> None:
    def git_result(_repo: Path, *args: str, text: bool = True):
        payload = listing if "ls-tree" in args else b"payload"
        return subprocess.CompletedProcess(args, 0, stdout=payload)

    with (
        patch.object(m0, "_validate_recorded_source"),
        patch.object(m0, "_run_git", side_effect=git_result),
        pytest.raises(m0.RefreshFailure, match=message),
    ):
        m0._git_tree_members(tmp_path, {"commit": "c", "tree": "t"})


def test_archive_inventory_rejects_links_and_incomplete_regular_members() -> None:
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w") as target:
        link = tarfile.TarInfo("linked.py")
        link.type = tarfile.SYMTYPE
        link.linkname = "target.py"
        target.addfile(link)
    with pytest.raises(m0.RefreshFailure, match="unsupported link"):
        m0._archive_tree_members(archive.getvalue())

    member = MagicMock()
    member.isdir.return_value = False
    member.isfile.return_value = True
    member.name = "missing.py"
    source = MagicMock()
    source.__enter__.return_value = source
    source.getmembers.return_value = [member]
    source.extractfile.return_value = None
    with (
        patch.object(m0.tarfile, "open", return_value=source),
        pytest.raises(m0.RefreshFailure, match="archive is incomplete"),
    ):
        m0._archive_tree_members(b"archive")


def test_refresh_lock_rejects_unsafe_root_missing_name_alias_and_post_lock_swap(
    tmp_path: Path,
) -> None:
    unsafe = tmp_path / "unsafe"
    unsafe.mkdir(mode=0o700)
    unsafe.chmod(0o777)
    with pytest.raises(m0.RefreshFailure, match="root has unsafe ownership or mode"):
        with m0._warehouse_refresh_lock(unsafe):
            pass

    missing_name = tmp_path / "missing-name"
    missing_name.mkdir(mode=0o700)
    real_stat = os.stat

    def fail_lock_stat(path, *args, dir_fd=None, follow_symlinks=True):
        if path == ".mdw-m0-refresh.lock" and dir_fd is not None:
            raise OSError("removed")
        return real_stat(path, *args, dir_fd=dir_fd, follow_symlinks=follow_symlinks)

    with (
        patch.object(m0.os, "stat", side_effect=fail_lock_stat),
        pytest.raises(m0.RefreshFailure, match="path changed during acquisition"),
    ):
        with m0._warehouse_refresh_lock(missing_name):
            pass

    aliased = tmp_path / "aliased"
    aliased.mkdir(mode=0o700)
    lock = aliased / ".mdw-m0-refresh.lock"
    lock.write_bytes(b"")
    lock.chmod(0o600)
    os.link(lock, tmp_path / "lock-alias")
    with pytest.raises(m0.RefreshFailure, match="unalias"):
        with m0._warehouse_refresh_lock(aliased):
            pass

    swapped = tmp_path / "swapped"
    swapped.mkdir(mode=0o700)
    calls = 0

    def swap_after_flock(path, *args, dir_fd=None, follow_symlinks=True):
        nonlocal calls
        value = real_stat(path, *args, dir_fd=dir_fd, follow_symlinks=follow_symlinks)
        if path == ".mdw-m0-refresh.lock" and dir_fd is not None:
            calls += 1
            if calls == 2:
                return SimpleNamespace(st_dev=value.st_dev, st_ino=value.st_ino + 1)
        return value

    with (
        patch.object(m0.os, "stat", side_effect=swap_after_flock),
        pytest.raises(m0.RefreshFailure, match="path changed during acquisition"),
    ):
        with m0._warehouse_refresh_lock(swapped):
            pass


def test_run_result_fails_closed_on_directory_file_and_cleanup_faults(
    tmp_path: Path,
) -> None:
    open_failure = tmp_path / "open-failure" / "run.json"
    real_open = os.open

    def fail_directory_open(path, flags, mode=0o777, *, dir_fd=None):
        if Path(path) == open_failure.parent and dir_fd is None:
            raise OSError("cannot open directory")
        return real_open(path, flags, mode, dir_fd=dir_fd)

    with (
        patch.object(m0.os, "open", side_effect=fail_directory_open),
        pytest.raises(m0.RefreshFailure, match="real directory"),
    ):
        m0._write_run_result(open_failure, {"outcome": "failed"})

    changed = tmp_path / "changed" / "run.json"
    real_lstat = os.lstat

    def fail_post_write_lstat(path, *args, **kwargs):
        if Path(path) == changed.parent and changed.exists():
            raise OSError("directory replaced")
        return real_lstat(path, *args, **kwargs)

    with (
        patch.object(m0.os, "lstat", side_effect=fail_post_write_lstat),
        pytest.raises(m0.RefreshFailure, match="directory changed during write"),
    ):
        m0._write_run_result(changed, {"outcome": "failed"})

    altered = tmp_path / "altered" / "run.json"
    real_stat = os.stat

    def alter_named_file(path, *args, dir_fd=None, follow_symlinks=True):
        value = real_stat(path, *args, dir_fd=dir_fd, follow_symlinks=follow_symlinks)
        if path == altered.name and dir_fd is not None:
            return SimpleNamespace(st_dev=value.st_dev, st_ino=value.st_ino + 1)
        return value

    with (
        patch.object(m0.os, "stat", side_effect=alter_named_file),
        pytest.raises(m0.RefreshFailure, match="file changed during write"),
    ):
        m0._write_run_result(altered, {"outcome": "failed"})

    cleanup = tmp_path / "cleanup" / "run.json"

    def fail_cleanup_lstat(path, *args, **kwargs):
        if Path(path) == cleanup.parent and cleanup.exists():
            raise OSError("original directory failure")
        return real_lstat(path, *args, **kwargs)

    with (
        patch.object(m0.os, "lstat", side_effect=fail_cleanup_lstat),
        patch.object(m0.os, "unlink", side_effect=OSError("cleanup failed")),
        pytest.raises(m0.RefreshFailure, match="directory changed during write"),
    ):
        m0._write_run_result(cleanup, {"outcome": "failed"})


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("bootstrap_current_vxm", 1, "exact bool"),
        ("vxm_roll_days", -1, "non-negative integer"),
        ("vxm_host", "", "must be non-empty"),
        ("vxm_port", 4001, "PAPER port 4002"),
    ],
)
def test_dynamic_vxm_config_is_strictly_fail_closed(
    tmp_path: Path, field: str, value: object, message: str,
) -> None:
    config = _config(tmp_path)
    object.__setattr__(config, field, value)
    with pytest.raises(m0.RefreshFailure, match=message):
        m0._validate_config(config)


def test_batch_owner_argv_requires_nonempty_single_asset_class(tmp_path: Path) -> None:
    config = _config(tmp_path)
    with pytest.raises(m0.RefreshFailure, match="empty asset class"):
        m0._update_argv(config, [], tmp_path / "preset.json", tmp_path / "result.json")
    with pytest.raises(m0.RefreshFailure, match="asset class mismatch"):
        m0._update_argv(
            config,
            [_entry("equity", "AAPL"), _entry("crypto", "BTC")],
            tmp_path / "preset.json",
            tmp_path / "result.json",
        )


def test_owner_result_rejects_bad_request_read_race_and_result_order(tmp_path: Path) -> None:
    path = tmp_path / "result.json"
    path.write_text("{}")
    with pytest.raises(m0.RefreshFailure, match="owner result is invalid"):
        m0._read_owner_result(path, "equity", [])

    valid = {
        "schema_version": 1,
        "asset_class": "equity",
        "requested_symbols": ["AAPL"],
        "results": [{"symbol": "AAPL", "status": "succeeded"}],
    }
    path.write_text(json.dumps(valid))
    with patch.object(
        Path, "read_bytes", return_value=b"x" * (m0.MAX_OWNER_RESULT_BYTES + 1),
    ), pytest.raises(m0.RefreshFailure, match="owner result is invalid"):
        m0._read_owner_result(path, "equity", ["AAPL"])

    reversed_results = {
        "schema_version": 1,
        "asset_class": "equity",
        "requested_symbols": ["AAPL", "MSFT"],
        "results": [
            {"symbol": "MSFT", "status": "succeeded"},
            {"symbol": "AAPL", "status": "succeeded"},
        ],
    }
    path.write_text(json.dumps(reversed_results))
    with pytest.raises(m0.RefreshFailure, match="owner result is invalid"):
        m0._read_owner_result(path, "equity", ["AAPL", "MSFT"])


def _valid_vxm_mapping(config: m0.RefreshConfig) -> dict[str, object]:
    symbol = "VXM_20250219"
    return {
        "schema_version": 1,
        "root": "VXM",
        "symbol": symbol,
        "contract_id": m0.stable_symbol_id(symbol),
        "con_id": 123,
        "local_symbol": "VXMG5",
        "sec_type": "FUT",
        "exchange": "CFE",
        "currency": "USD",
        "trading_class": "VXM",
        "multiplier": "100",
        "expiry_date": "2025-02-19",
        "as_of": config.as_of.isoformat(),
        "roll_days": config.vxm_roll_days,
        "latest_session": "2025-01-02",
    }


@pytest.mark.parametrize("failure", ["oversized", "wrong_keys", "invalid_semantics"])
def test_dynamic_vxm_mapping_rejects_bounded_schema_and_semantic_violations(
    tmp_path: Path, failure: str,
) -> None:
    config = _config(tmp_path)
    path = tmp_path / "mapping.json"
    document = _valid_vxm_mapping(config)
    if failure == "oversized":
        path.write_bytes(b" " * (m0.MAX_OWNER_RESULT_BYTES + 1))
    elif failure == "wrong_keys":
        document.pop("root")
        path.write_text(json.dumps(document))
    else:
        document["exchange"] = "CBOE"
        path.write_text(json.dumps(document))
    with pytest.raises(m0.RefreshFailure, match="mapping is invalid"):
        m0._read_vxm_mapping(path, config)


def test_futures_owner_transport_failure_is_an_exact_failed_identity(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    _mapping, steps = m0._refresh_futures_owner(
        config,
        [_entry("futures", "VXM_20250219")],
        lambda _argv: (_ for _ in ()).throw(OSError("transport")),
        tmp_path,
        lambda: None,
    )
    assert [(step["symbol"], step["status"]) for step in steps] == [
        ("VXM_20250219", "failed")
    ]


@pytest.mark.parametrize(
    "failure", ["argv", "status", "mapping_on_failure", "missing_result", "omitted_mapping"]
)
def test_futures_owner_rejects_invalid_process_evidence(tmp_path: Path, failure: str) -> None:
    config = _config(tmp_path)
    mapping = _valid_vxm_mapping(config)

    def runner(argv: list[str]) -> subprocess.CompletedProcess[str]:
        result_path = Path(argv[argv.index("--result-json") + 1])
        mapping_path = Path(argv[argv.index("--mapping-json") + 1])
        if failure != "missing_result":
            result_path.write_text("{}")
        if failure == "mapping_on_failure":
            mapping_path.write_text("{}")
        return subprocess.CompletedProcess(
            ["different"] if failure == "argv" else argv,
            1 if failure == "mapping_on_failure" else 0,
            "",
            "",
        )

    statuses = {
        str(mapping["symbol"]): "succeeded" if failure == "omitted_mapping" else "failed"
    }
    with (
        patch.object(m0, "_read_vxm_mapping", return_value=mapping),
        patch.object(m0, "_read_owner_result", return_value=statuses),
        pytest.raises(m0.RefreshFailure),
    ):
        m0._refresh_futures_owner(
            config, [_entry("futures", "VXM_20250219")], runner, tmp_path, lambda: None
        )


def test_futures_owner_accepts_valid_vxm_mapping_with_unrelated_partial_failure(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    mapping = _valid_vxm_mapping(config)
    mapped_symbol = str(mapping["symbol"])

    def runner(argv: list[str]) -> subprocess.CompletedProcess[str]:
        Path(argv[argv.index("--result-json") + 1]).write_text("{}")
        Path(argv[argv.index("--mapping-json") + 1]).write_text("{}")
        return subprocess.CompletedProcess(argv, 1, "", "")

    statuses = {"ES_202506": "failed", mapped_symbol: "succeeded"}
    with (
        patch.object(m0, "_read_vxm_mapping", return_value=mapping),
        patch.object(m0, "_read_owner_result", return_value=statuses),
    ):
        actual_mapping, steps = m0._refresh_futures_owner(
            config,
            [_entry("futures", "ES_202506"), _entry("futures", "VXM_20250122")],
            runner,
            tmp_path,
            lambda: None,
        )
    assert actual_mapping == mapping
    assert [(step["symbol"], step["status"]) for step in steps] == list(statuses.items())


@pytest.mark.parametrize("failure", ["mutated_failed_predecessor", "mapping_absent"])
def test_vxm_post_owner_inventory_guards(tmp_path: Path, failure: str) -> None:
    config = _config(tmp_path)
    object.__setattr__(config, "bootstrap_current_vxm", True)
    old = _entry("futures", "VXM_20250122")
    post = replace(old, sha256="c" * 64) if failure == "mutated_failed_predecessor" else old
    mapping = _valid_vxm_mapping(config) if failure == "mapping_absent" else None
    step = {
        "asset_class": "futures",
        "symbol": old.symbol if mapping is None else str(mapping["symbol"]),
        "argv_sha256": "a" * 64,
        "started_at": "start",
        "ended_at": "end",
        "exit_code": 1 if mapping is None else 0,
        "status": "failed" if mapping is None else "succeeded",
    }
    identity = {"commit": "a" * 40, "tree": "b" * 40}
    with tempfile.TemporaryFile() as sealed:
        with (
            patch.object(m0, "discover_inventory", side_effect=[[old], [post]]),
            patch.object(m0, "_sealed_execution_source", return_value=contextlib.nullcontext(sealed)),
            patch.object(m0, "_refresh_futures_owner", return_value=(mapping, [step])),
            pytest.raises(m0.RefreshFailure, match="mutated|mapping identity is absent"),
        ):
            m0._refresh_all_and_rebuild_locked(
                config,
                audit={},
                run_id="run",
                started_at="start",
                source_identity=lambda _root: identity,
            )


def test_batch_owner_exit_code_must_match_exact_symbol_statuses(tmp_path: Path) -> None:
    config = _config(tmp_path)
    entry = _entry("crypto", "BTC")

    def mismatched_exit(argv: list[str]) -> subprocess.CompletedProcess[str]:
        result_path = Path(argv[argv.index("--result-json") + 1])
        result_path.write_text(json.dumps({
            "schema_version": 1,
            "asset_class": "crypto",
            "requested_symbols": ["BTC"],
            "results": [{"symbol": "BTC", "status": "succeeded"}],
        }))
        return subprocess.CompletedProcess(argv, 1, "", "")

    with pytest.raises(m0.RefreshFailure, match="exit status mismatch"):
        m0._refresh_inventory(config, [entry], mismatched_exit, tmp_path)


def test_database_rejects_futures_identity_without_exact_expiry_suffix(tmp_path: Path) -> None:
    from tests.test_refresh_all_and_rebuild import _built_database

    db_path, inventory = _built_database(tmp_path)
    futures = next(entry for entry in inventory if entry.asset_class == "futures")
    corrupted = [
        replace(entry, symbol="ES_BAD") if entry is futures else entry
        for entry in inventory
    ]
    with pytest.raises(m0.RefreshFailure, match="invalid identity"):
        m0.validate_database(db_path, corrupted)


def test_dynamic_vxm_argv_mismatch_is_never_degraded(tmp_path: Path) -> None:
    config = _config(tmp_path)
    object.__setattr__(config, "bootstrap_current_vxm", True)
    audit: dict[str, object] = {}
    identity = {"commit": "a" * 40, "tree": "b" * 40}
    with tempfile.TemporaryFile() as sealed:
        with (
            patch.object(m0, "discover_inventory", return_value=[_entry("futures", "VXM_20250219")]),
            patch.object(m0, "_sealed_execution_source", return_value=contextlib.nullcontext(sealed)),
            patch.object(
                m0, "_refresh_futures_owner",
                side_effect=m0.RefreshFailure("dynamic VXM owner argv mismatch"),
            ),
            pytest.raises(m0.RefreshFailure, match="argv mismatch"),
        ):
            m0._refresh_all_and_rebuild_locked(
                config,
                audit=audit,
                run_id="run",
                started_at="start",
                source_identity=lambda _root: identity,
            )


def test_successful_dynamic_vxm_bootstrap_marks_older_contract_preserved(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    object.__setattr__(config, "bootstrap_current_vxm", True)
    old = _entry("futures", "VXM_20250122")
    current = _entry("futures", "VXM_20250219")
    mapping = _valid_vxm_mapping(config)
    owner_step = {
        "asset_class": "futures", "symbol": current.symbol, "argv_sha256": "a" * 64,
        "started_at": "start", "ended_at": "end", "exit_code": 0, "status": "succeeded",
    }
    audit: dict[str, object] = {}
    identity = {"commit": "a" * 40, "tree": "b" * 40}
    with tempfile.TemporaryFile() as sealed:
        with (
            patch.object(m0, "discover_inventory", side_effect=[[old], [old, current]]),
            patch.object(m0, "_sealed_execution_source", return_value=contextlib.nullcontext(sealed)),
            patch.object(m0, "_refresh_futures_owner", return_value=(mapping, [owner_step])),
            patch.object(m0, "_write_immutable", side_effect=RuntimeError("stop after inventory")),
            pytest.raises(RuntimeError, match="stop after inventory"),
        ):
            m0._refresh_all_and_rebuild_locked(
                config,
                audit=audit,
                run_id="run",
                started_at="start",
                source_identity=lambda _root: identity,
            )
    steps = audit["steps"]
    assert isinstance(steps, list)
    assert [(step["symbol"], step["status"]) for step in steps] == [
        ("VXM_20250219", "succeeded"),
        ("VXM_20250122", "preserved"),
    ]
