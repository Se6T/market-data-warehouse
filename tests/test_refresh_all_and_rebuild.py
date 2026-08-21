"""Phase 3 M0 atomic all-universe refresh/rebuild contract tests."""
from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import subprocess
import sys
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from clients.bronze_client import BronzeClient
from clients.db_client import DBClient
from clients.symbol_ids import stable_symbol_id
from scripts import refresh_all_and_rebuild as m0


def _daily_row(day: str, close: float = 10.0) -> dict:
    return {"trade_date": day, "open": close - 1, "high": close + 1, "low": close - 2,
            "close": close, "adj_close": close, "volume": 100}


def _futures_row(day: str) -> dict:
    return {"trade_date": day, "root_symbol": "ES", "expiry_date": "2025-06-01",
            "open": 5000, "high": 5010, "low": 4990, "close": 5005,
            "settlement": 5005, "volume": 1000, "open_interest": 500}


def _warehouse(tmp_path: Path) -> Path:
    warehouse = tmp_path / "market-warehouse"
    bronze = warehouse / "data-lake" / "bronze"
    fixtures = {
        "equity": ("AAPL", _daily_row("2025-01-02", 100)),
        "volatility": ("VIX", _daily_row("2025-01-02", 20)),
        "crypto": ("BTC", _daily_row("2025-01-02", 90000)),
        "futures": ("ES_202506", _futures_row("2025-01-02")),
    }
    for asset_class, (symbol, row) in fixtures.items():
        with BronzeClient(bronze / f"asset_class={asset_class}", asset_class=asset_class) as client:
            client.replace_ticker_rows(symbol, [row])
    warehouse.chmod(0o700)
    return warehouse


def _config(tmp_path: Path, warehouse: Path) -> m0.RefreshConfig:
    current = warehouse / "duckdb" / "current"
    return m0.RefreshConfig(
        warehouse=warehouse,
        db_path=current / "market.duckdb",
        manifest_path=current / "manifest.json",
        inventory_path=tmp_path / "m0-pre-refresh-inventory.json",
        as_of=date(2025, 1, 2),
        python=Path("/safe/python"),
        repo_root=Path(__file__).resolve().parents[1],
    )


def _runner(calls: list[list[str]]):
    def run(argv: list[str]) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 0, "ok", "")
    return run


def _seed_old_db(config: m0.RefreshConfig) -> bytes:
    payload = b"existing-database-must-survive"
    candidate = config.db_path.parent.parent / "old.tmp"
    candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate.write_bytes(payload)
    m0._atomic_publish_bundle(
        candidate, config.db_path, config.manifest_path,
        {"database_sha256": hashlib.sha256(payload).hexdigest(), "generation": "old"},
    )
    return payload


def _identity(_root: Path) -> dict[str, str]:
    return {
        "commit": subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=m0.PROJECT_ROOT, check=True,
            capture_output=True, text=True,
        ).stdout.strip(),
        "tree": subprocess.run(
            ["git", "rev-parse", "HEAD^{tree}"], cwd=m0.PROJECT_ROOT, check=True,
            capture_output=True, text=True,
        ).stdout.strip(),
    }


def _run_result_paths(config: m0.RefreshConfig) -> list[Path]:
    inventory_key = hashlib.sha256(
        os.path.abspath(config.inventory_path).encode("utf-8")
    ).hexdigest()
    return sorted(
        config.warehouse.joinpath(".mdw-m0-run-results", inventory_key).glob("*.json")
    )


def test_discover_inventory_is_complete_canonical_and_content_bound(tmp_path: Path) -> None:
    warehouse = _warehouse(tmp_path)
    inventory = m0.discover_inventory(warehouse / "data-lake" / "bronze")
    assert [(item.asset_class, item.symbol) for item in inventory] == [
        ("crypto", "BTC"), ("equity", "AAPL"), ("futures", "ES_202506"),
        ("volatility", "VIX")]
    assert all(item.rows == 1 and item.latest_session == "2025-01-02" for item in inventory)
    assert all(len(item.sha256) == 64 for item in inventory)
    assert inventory[0].path == "asset_class=crypto/symbol=BTC/data.parquet"


def test_pre_refresh_inventory_accepts_only_legacy_volatility_ids(tmp_path: Path) -> None:
    bronze = _warehouse(tmp_path) / "data-lake" / "bronze"
    path = bronze / "asset_class=volatility" / "symbol=VIX" / "data.parquet"
    table = pq.ParquetFile(path).read()
    legacy_id = int(hashlib.sha256(b"VIX").hexdigest()[:14], 16)
    table = table.set_column(
        table.column_names.index("symbol_id"),
        "symbol_id",
        pa.array([legacy_id], type=pa.int64()),
    )
    pq.write_table(table, path)

    with pytest.raises(m0.RefreshFailure, match="canonical identity ID mismatch"):
        m0.discover_inventory(bronze)
    inventory = m0.discover_inventory(
        bronze,
        allow_legacy_volatility_ids=True,
    )
    vix = next(item for item in inventory if item.symbol == "VIX")
    assert vix.identity_id == stable_symbol_id("VIX")


def test_dbclient_loads_all_asset_classes_without_erasure(tmp_path: Path) -> None:
    bronze = _warehouse(tmp_path) / "data-lake" / "bronze"
    with DBClient(tmp_path / "all.duckdb") as db:
        assert db.load_equities_from_parquet(bronze / "asset_class=equity", "equity", "SMART", reset=True) == {"symbols": 1, "rows": 1}
        assert db.load_equities_from_parquet(bronze / "asset_class=volatility", "volatility", "CBOE") == {"symbols": 1, "rows": 1}
        assert db.load_equities_from_parquet(bronze / "asset_class=crypto", "crypto", "BINANCE") == {"symbols": 1, "rows": 1}
        assert db.load_futures_from_parquet(bronze / "asset_class=futures", reset=True) == {"rows": 1}
        assert db.query("SELECT asset_class, count(*) n FROM md.symbols GROUP BY asset_class ORDER BY asset_class") == [
            {"asset_class": "crypto", "n": 1}, {"asset_class": "equity", "n": 1},
            {"asset_class": "volatility", "n": 1}]
        assert db.query("SELECT count(*) n FROM md.equities_daily") == [{"n": 3}]


def test_success_refreshes_each_identity_and_publishes_manifest(tmp_path: Path) -> None:
    warehouse = _warehouse(tmp_path)
    config = _config(tmp_path, warehouse)
    old = _seed_old_db(config)
    calls: list[list[str]] = []
    manifest = m0.refresh_all_and_rebuild(config, command_runner=_runner(calls), source_identity=_identity)
    text = [" ".join(call) for call in calls]
    assert len(calls) == 4
    assert any("daily_update.py" in call and "--asset-class equity" in call for call in text)
    assert any("daily_update.py" in call and "--asset-class futures" in call for call in text)
    assert any("fetch_cboe_volatility.py" in call and "--symbols VIX" in call for call in text)
    assert any("fetch_binance_crypto.py" in call and "--symbols BTC" in call for call in text)
    assert config.db_path.read_bytes() != old
    sha = hashlib.sha256(config.db_path.read_bytes()).hexdigest()
    assert manifest["publication"] == {"db_path": str(config.db_path), "published": True, "sha256": sha}
    assert manifest["script_commit"] == _identity(config.repo_root)["commit"]
    assert manifest["script_tree"] == _identity(config.repo_root)["tree"]
    assert manifest["requested_as_of"] == "2025-01-02"
    assert manifest["row_counts"] == {"md.equities_daily": 3, "md.futures_daily": 1, "md.symbols": 3}
    assert manifest["latest_sessions"] == {"crypto:BTC": "2025-01-02", "equity:AAPL": "2025-01-02", "futures:ES_202506": "2025-01-02", "volatility:VIX": "2025-01-02"}
    assert all(
        set(step)
        == {
            "asset_class",
            "symbol",
            "argv_sha256",
            "started_at",
            "ended_at",
            "exit_code",
            "status",
        }
        and step["status"] == "succeeded"
        for step in manifest["steps"]
    )
    assert manifest["schemas"]["duckdb"] == {
        name: list(columns) for name, columns in m0.DB_SCHEMAS.items()
    }
    assert all(
        item["schema"] and len(item["schema_sha256"]) == 64
        for item in manifest["post_refresh_inventory"]
    )
    pinned = m0.resolve_current_bundle(config.db_path, config.manifest_path)
    assert pinned.database == config.db_path.resolve()
    assert pinned.manifest == config.manifest_path.resolve()
    assert json.loads(pinned.manifest.read_text()) == manifest
    assert pinned.manifest.read_bytes().endswith(b"\n")
    assert len(json.loads(config.inventory_path.read_text())["inventory"]) == 4
    run_result = manifest["run_result"]
    assert isinstance(run_result, dict)
    assert run_result["outcome"] == "succeeded"
    assert [step["status"] for step in run_result["steps"]] == ["succeeded"] * 4
    assert len(run_result["run_id"]) == 32
    assert _run_result_paths(config) == []


def test_public_only_refresh_preserves_broker_assets_and_all_database_partitions(
    tmp_path: Path,
) -> None:
    warehouse = _warehouse(tmp_path)
    futures_file = next(
        warehouse.glob("data-lake/bronze/asset_class=futures/symbol=*/data.parquet")
    )
    futures_file.unlink()
    futures_file.parent.rmdir()
    futures_file.parent.parent.rmdir()
    config = _config(tmp_path, warehouse)
    object.__setattr__(config, "refresh_broker_assets", False)
    calls: list[list[str]] = []

    manifest = m0.refresh_all_and_rebuild(
        config,
        command_runner=_runner(calls),
        source_identity=_identity,
    )

    assert len(calls) == 2
    assert all("daily_update.py" not in " ".join(call) for call in calls)
    assert {step["status"] for step in manifest["steps"]} == {"preserved", "succeeded"}
    assert manifest["row_counts"] == {
        "md.equities_daily": 3,
        "md.futures_daily": 0,
        "md.symbols": 3,
    }
    connection = duckdb.connect(str(config.db_path), read_only=True)
    try:
        assert connection.execute(
            "SELECT asset_class, count(*) FROM md.symbols GROUP BY 1 ORDER BY 1"
        ).fetchall() == [("crypto", 1), ("equity", 1), ("volatility", 1)]
    finally:
        connection.close()


def test_preserved_broker_inventory_verifies_source_once_per_batch(tmp_path: Path) -> None:
    config = _config(tmp_path, _warehouse(tmp_path))
    object.__setattr__(config, "refresh_broker_assets", False)
    broker_entries = [
        entry
        for entry in m0.discover_inventory(
            config.warehouse / "data-lake" / "bronze"
        )
        if entry.asset_class in {"equity", "futures"}
    ]
    verifier = MagicMock()

    steps = m0._refresh_inventory(
        config,
        broker_entries,
        _runner([]),
        tmp_path,
        verify_source=verifier,
    )

    assert [step["status"] for step in steps] == ["preserved", "preserved"]
    verifier.assert_called_once_with()


def test_success_evidence_is_committed_with_bundle_before_external_audit_can_fail(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, _warehouse(tmp_path))
    old = _seed_old_db(config)

    with patch.object(m0, "_write_run_result", side_effect=OSError("late audit failure")) as writer:
        manifest = m0.refresh_all_and_rebuild(
            config, command_runner=_runner([]), source_identity=_identity,
        )

    writer.assert_not_called()
    assert config.db_path.read_bytes() != old
    committed = json.loads(m0.resolve_current_bundle(
        config.db_path, config.manifest_path,
    ).manifest.read_text())
    assert committed["run_result"] == manifest["run_result"]
    assert committed["run_result"]["outcome"] == "succeeded"
    assert [step["status"] for step in committed["run_result"]["steps"]] == [
        "succeeded",
    ] * 4


def test_reader_visible_success_suppresses_failure_audit_under_compound_faults(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, _warehouse(tmp_path))
    old = _seed_old_db(config)
    real_replace, real_fsync = os.replace, m0._fsync_directory
    replacements = parent_fsyncs = 0

    def replace(source, destination) -> None:
        nonlocal replacements
        replacements += 1
        if replacements == 3:
            raise OSError("pending marker restoration failed")
        if replacements == 4:
            raise OSError("pointer rollback failed")
        real_replace(source, destination)

    def fsync(path: Path) -> None:
        nonlocal parent_fsyncs
        if path == config.db_path.parent.parent:
            parent_fsyncs += 1
            if parent_fsyncs == 3:
                raise OSError("committed marker fsync failed")
        real_fsync(path)

    with patch.object(m0.os, "replace", side_effect=replace), patch.object(
        m0, "_fsync_directory", side_effect=fsync,
    ), patch.object(
        m0, "_write_run_result", side_effect=OSError("failure audit fsync failed"),
    ) as writer:
        manifest = m0.refresh_all_and_rebuild(
            config, command_runner=_runner([]), source_identity=_identity,
        )

    writer.assert_not_called()
    assert config.db_path.read_bytes() != old
    committed = json.loads(m0.resolve_current_bundle(
        config.db_path, config.manifest_path,
    ).manifest.read_text())
    assert committed == manifest
    assert committed["run_result"]["outcome"] == "succeeded"


def test_reported_persistent_fsync_failure_keeps_reader_on_predecessor(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, _warehouse(tmp_path))
    old = _seed_old_db(config)
    publication_failure = OSError("persistent publication fsync failure")
    audit_failure = OSError("failure audit fsync failed")
    real_replace, real_fsync = os.replace, m0._fsync_directory
    real_resolve = m0.resolve_current_bundle
    replacements = parent_fsyncs = resolutions = 0

    def replace(source, destination) -> None:
        nonlocal replacements
        replacements += 1
        if replacements == 4:
            raise OSError("pointer rollback failed")
        real_replace(source, destination)

    def fsync(path: Path) -> None:
        nonlocal parent_fsyncs
        if path == config.db_path.parent.parent:
            parent_fsyncs += 1
            if parent_fsyncs >= 3:
                raise publication_failure
        real_fsync(path)

    def resolve(db_path: Path, manifest_path: Path) -> m0.PublishedBundle:
        nonlocal resolutions
        resolutions += 1
        if resolutions == 2:
            raise OSError("reconciliation read failed")
        return real_resolve(db_path, manifest_path)

    with patch.object(m0.os, "replace", side_effect=replace), patch.object(
        m0, "_fsync_directory", side_effect=fsync,
    ), patch.object(
        m0, "resolve_current_bundle", side_effect=resolve,
    ), patch.object(m0, "_write_run_result", side_effect=audit_failure), pytest.raises(
        OSError, match="persistent publication fsync failure",
    ) as raised:
        m0.refresh_all_and_rebuild(
            config, command_runner=_runner([]), source_identity=_identity,
        )

    assert raised.value is publication_failure
    assert raised.value.__cause__ is audit_failure
    visible = m0.resolve_current_bundle(config.db_path, config.manifest_path)
    assert visible.database.read_bytes() == old
    assert json.loads(visible.manifest.read_text())["generation"] == "old"


def test_failure_evidence_fault_preserves_original_error_and_chains_audit_fault(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, _warehouse(tmp_path))
    old = _seed_old_db(config)
    owner_failure = TimeoutError("owner failed")
    audit_failure = OSError("terminal evidence fsync failed")

    with patch.object(m0, "_write_run_result", side_effect=audit_failure), pytest.raises(
        TimeoutError, match="owner failed",
    ) as raised:
        m0.refresh_all_and_rebuild(
            config,
            command_runner=lambda _argv: (_ for _ in ()).throw(owner_failure),
            source_identity=_identity,
        )

    assert raised.value is owner_failure
    assert raised.value.__cause__ is audit_failure
    assert config.db_path.read_bytes() == old


def test_whole_refresh_lock_rejects_concurrent_run_before_any_effect(tmp_path: Path) -> None:
    warehouse = _warehouse(tmp_path)
    config = _config(tmp_path, warehouse)
    old = _seed_old_db(config)
    lock_path = warehouse / ".mdw-m0-refresh.lock"
    lock_path.touch(mode=0o600)
    with lock_path.open("r+b") as holder:
        fcntl.flock(holder.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        calls: list[list[str]] = []
        with pytest.raises(m0.RefreshFailure, match="already in progress"):
            m0.refresh_all_and_rebuild(
                config, command_runner=_runner(calls), source_identity=_identity,
            )

    assert calls == []
    assert not config.inventory_path.exists()
    assert _run_result_paths(config) == []
    assert config.db_path.read_bytes() == old
    assert json.loads(config.manifest_path.read_text())["generation"] == "old"


def test_stale_unlocked_refresh_lock_is_reusable(tmp_path: Path) -> None:
    config = _config(tmp_path, _warehouse(tmp_path))
    lock_path = config.warehouse / ".mdw-m0-refresh.lock"
    lock_path.write_text("stale pid metadata")
    lock_path.chmod(0o600)

    manifest = m0.refresh_all_and_rebuild(
        config, command_runner=_runner([]), source_identity=_identity,
    )

    assert manifest["publication"]["published"] is True
    with lock_path.open("r+b") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def test_refresh_lock_rejects_replaced_path_and_prevents_dual_acquisition(
    tmp_path: Path,
) -> None:
    warehouse = _warehouse(tmp_path)
    lock_path = warehouse / ".mdw-m0-refresh.lock"

    with m0._warehouse_refresh_lock(warehouse):
        held = warehouse / ".held-lock"
        lock_path.rename(held)
        lock_path.touch(mode=0o600)
        with pytest.raises(m0.RefreshFailure, match="already in progress"):
            with m0._warehouse_refresh_lock(warehouse):
                pass


@pytest.mark.parametrize("mode", [0o644, 0o660, 0o666])
def test_refresh_lock_rejects_nonrestrictive_existing_mode(
    tmp_path: Path, mode: int,
) -> None:
    warehouse = _warehouse(tmp_path)
    lock_path = warehouse / ".mdw-m0-refresh.lock"
    lock_path.touch()
    lock_path.chmod(mode)

    with pytest.raises(m0.RefreshFailure, match="mode"):
        with m0._warehouse_refresh_lock(warehouse):
            pass


@pytest.mark.parametrize("attack", ["symlink", "hardlink"])
def test_refresh_lock_rejects_filesystem_aliases_before_inventory(
    tmp_path: Path, attack: str,
) -> None:
    config = _config(tmp_path, _warehouse(tmp_path))
    lock_path = config.warehouse / ".mdw-m0-refresh.lock"
    outside = tmp_path / "outside-lock"
    outside.write_text("outside")
    if attack == "symlink":
        lock_path.symlink_to(outside)
    else:
        os.link(outside, lock_path)

    with pytest.raises(m0.RefreshFailure, match="refresh lock"):
        m0.refresh_all_and_rebuild(
            config, command_runner=_runner([]), source_identity=_identity,
        )

    assert not config.inventory_path.exists()
    assert outside.read_text() == "outside"


def test_futures_refresh_uses_the_owner_preset_contract(tmp_path: Path) -> None:
    config = _config(tmp_path, _warehouse(tmp_path))
    entry = next(
        item
        for item in m0.discover_inventory(config.warehouse / "data-lake" / "bronze")
        if item.asset_class == "futures"
    )
    preset = tmp_path / "futures.json"
    argv = m0._update_argv(config, entry, preset)
    assert argv[argv.index("--preset") + 1] == str(preset)
    assert json.loads(preset.read_text()) == {
        "contracts": [{"expiry": "202506", "root": "ES"}],
        "name": "m0-futures-ES_202506",
    }


@pytest.mark.parametrize("phase", ["inventory", "refresh", "rebuild", "validation"])
def test_failure_after_each_phase_preserves_db(tmp_path: Path, phase: str) -> None:
    config = _config(tmp_path, _warehouse(tmp_path))
    old = _seed_old_db(config)
    def hook(completed: str) -> None:
        if completed == phase:
            raise m0.InjectedFailure(phase)
    with pytest.raises(m0.InjectedFailure, match=phase):
        m0.refresh_all_and_rebuild(config, command_runner=_runner([]), phase_hook=hook, source_identity=_identity)
    assert config.db_path.read_bytes() == old
    assert not list(config.db_path.parent.glob(".market.duckdb.*.tmp"))


def test_update_failure_and_inventory_drift_preserve_db(tmp_path: Path) -> None:
    warehouse = _warehouse(tmp_path)
    config = _config(tmp_path, warehouse)
    old = _seed_old_db(config)
    def fail(argv: list[str]) -> subprocess.CompletedProcess[str]:
        code = 7 if "fetch_cboe_volatility.py" in " ".join(argv) else 0
        return subprocess.CompletedProcess(argv, code, "", "SECRET")
    with pytest.raises(m0.RefreshFailure, match="volatility:VIX update failed with exit 7"):
        m0.refresh_all_and_rebuild(config, command_runner=fail, source_identity=_identity)
    assert config.db_path.read_bytes() == old
    assert json.loads(config.manifest_path.read_text())["generation"] == "old"
    [result_path] = _run_result_paths(config)
    result_bytes = result_path.read_bytes()
    result = json.loads(result_bytes)
    assert result["outcome"] == "failed"
    assert result["failure_type"] == "RefreshFailure"
    assert [(step["asset_class"], step["symbol"], step["status"]) for step in result["steps"]] == [
        ("crypto", "BTC", "succeeded"),
        ("equity", "AAPL", "succeeded"),
        ("futures", "ES_202506", "succeeded"),
        ("volatility", "VIX", "failed"),
    ]
    assert b"SECRET" not in result_bytes

    config.inventory_path.unlink()
    calls = 0
    def remove(argv: list[str]) -> subprocess.CompletedProcess[str]:
        nonlocal calls
        calls += 1
        if calls == 1:
            (warehouse / "data-lake/bronze/asset_class=crypto/symbol=BTC/data.parquet").unlink()
        return subprocess.CompletedProcess(argv, 0, "", "")
    with pytest.raises(m0.RefreshFailure, match="inventory identities changed"):
        m0.refresh_all_and_rebuild(config, command_runner=remove, source_identity=_identity)
    assert config.db_path.read_bytes() == old
    assert result_path.read_bytes() == result_bytes
    assert len(_run_result_paths(config)) == 2


def test_owner_exception_records_failed_and_not_attempted_identities(tmp_path: Path) -> None:
    config = _config(tmp_path, _warehouse(tmp_path))
    old = _seed_old_db(config)
    calls = 0

    def fail_second(argv: list[str]) -> subprocess.CompletedProcess[str]:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise TimeoutError("SECRET child output")
        return subprocess.CompletedProcess(argv, 0, "SECRET stdout", "SECRET stderr")

    with pytest.raises(TimeoutError, match="SECRET child output"):
        m0.refresh_all_and_rebuild(
            config, command_runner=fail_second, source_identity=_identity,
        )

    [result_path] = _run_result_paths(config)
    result_bytes = result_path.read_bytes()
    result = json.loads(result_bytes)
    assert [(step["asset_class"], step["symbol"], step["status"]) for step in result["steps"]] == [
        ("crypto", "BTC", "succeeded"),
        ("equity", "AAPL", "failed"),
        ("futures", "ES_202506", "not_attempted"),
        ("volatility", "VIX", "not_attempted"),
    ]
    assert b"SECRET" not in result_bytes
    assert config.db_path.read_bytes() == old
    assert json.loads(config.manifest_path.read_text())["generation"] == "old"


def test_zero_exit_noop_owner_cannot_publish_stale_identity(tmp_path: Path) -> None:
    warehouse = _warehouse(tmp_path)
    config = _config(tmp_path, warehouse)
    object.__setattr__(config, "as_of", date(2025, 1, 3))
    old = _seed_old_db(config)
    with pytest.raises(
        m0.RefreshFailure,
        match="crypto:BTC expected latest session 2025-01-03, observed 2025-01-02",
    ):
        m0.refresh_all_and_rebuild(
            config, command_runner=_runner([]), source_identity=_identity,
        )
    assert config.db_path.read_bytes() == old


@pytest.mark.parametrize(
    ("asset_class", "as_of", "expected"),
    [
        ("crypto", date(2025, 1, 5), date(2025, 1, 5)),
        ("equity", date(2025, 1, 5), date(2025, 1, 3)),
        ("volatility", date(2025, 1, 1), date(2024, 12, 31)),
        ("equity", date(2025, 1, 20), date(2025, 1, 17)),
        ("futures", date(2025, 1, 20), date(2025, 1, 20)),
    ],
)
def test_expected_latest_session_has_explicit_asset_calendar_semantics(
    asset_class: str, as_of: date, expected: date,
) -> None:
    assert m0.expected_latest_session(asset_class, as_of) == expected


@pytest.mark.parametrize(("new_day", "message"), [("2025-01-01", "regressed"), ("2025-01-03", "exceeds requested as-of")])
def test_terminal_latest_session_is_fail_closed(tmp_path: Path, new_day: str, message: str) -> None:
    warehouse = _warehouse(tmp_path)
    config = _config(tmp_path, warehouse)
    old = _seed_old_db(config)
    def mutate(argv: list[str]) -> subprocess.CompletedProcess[str]:
        if "fetch_binance_crypto.py" in " ".join(argv):
            root = warehouse / "data-lake/bronze/asset_class=crypto"
            with BronzeClient(root, asset_class="crypto") as client:
                if new_day < "2025-01-02":
                    client.replace_ticker_rows("BTC", [_daily_row(new_day)])
                else:
                    client.merge_ticker_rows("BTC", [_daily_row(new_day)])
        return subprocess.CompletedProcess(argv, 0, "", "")
    with pytest.raises(m0.RefreshFailure, match=message):
        m0.refresh_all_and_rebuild(config, command_runner=mutate, source_identity=_identity)
    assert config.db_path.read_bytes() == old


def test_schema_and_publication_failures_preserve_db(tmp_path: Path) -> None:
    config = _config(tmp_path, _warehouse(tmp_path))
    old = _seed_old_db(config)
    with patch.object(m0, "validate_database", side_effect=m0.RefreshFailure("schema invalid")):
        with pytest.raises(m0.RefreshFailure, match="schema invalid"):
            m0.refresh_all_and_rebuild(config, command_runner=_runner([]), source_identity=_identity)
    assert config.db_path.read_bytes() == old
    config.inventory_path.unlink()
    with patch.object(m0.os, "replace", side_effect=OSError("publish denied")):
        with pytest.raises(OSError, match="publish denied"):
            m0.refresh_all_and_rebuild(config, command_runner=_runner([]), source_identity=_identity)
    assert config.db_path.read_bytes() == old


def test_pre_refresh_inventory_is_immutable(tmp_path: Path) -> None:
    config = _config(tmp_path, _warehouse(tmp_path))
    config.inventory_path.write_text("do-not-overwrite")
    with pytest.raises(FileExistsError):
        m0.refresh_all_and_rebuild(config, command_runner=_runner([]), source_identity=_identity)
    assert config.inventory_path.read_text() == "do-not-overwrite"


@pytest.mark.parametrize(
    ("left", "right"),
    [("db_path", "manifest_path"), ("db_path", "inventory_path"), ("manifest_path", "inventory_path")],
)
def test_output_paths_must_be_distinct(
    tmp_path: Path, left: str, right: str
) -> None:
    config = _config(tmp_path, _warehouse(tmp_path))
    old = _seed_old_db(config)
    object.__setattr__(config, right, getattr(config, left))
    with pytest.raises(m0.RefreshFailure, match="output paths must be distinct"):
        m0.refresh_all_and_rebuild(config, command_runner=_runner([]), source_identity=_identity)
    assert config.db_path.read_bytes() == old


def test_output_paths_reject_hardlink_aliases(tmp_path: Path) -> None:
    config = _config(tmp_path, _warehouse(tmp_path))
    config.db_path.parent.mkdir(parents=True)
    config.db_path.write_bytes(b"same inode")
    os.link(config.db_path, config.manifest_path)
    with pytest.raises(m0.RefreshFailure, match="filesystem aliases"):
        m0.refresh_all_and_rebuild(
            config, command_runner=_runner([]), source_identity=_identity,
        )


def test_inventory_rejects_missing_invalid_duplicate_and_corrupt(tmp_path: Path, monkeypatch) -> None:
    warehouse = _warehouse(tmp_path)
    bronze = warehouse / "data-lake" / "bronze"
    crypto = bronze / "asset_class=crypto/symbol=BTC/data.parquet"
    crypto.unlink()
    with pytest.raises(m0.RefreshFailure, match="missing asset classes: crypto"):
        m0.discover_inventory(bronze)
    source = bronze / "asset_class=equity/symbol=AAPL/data.parquet"
    invalid = bronze / "asset_class=equity/symbol=/data.parquet"
    invalid.parent.mkdir(parents=True)
    invalid.write_bytes(source.read_bytes())
    with pytest.raises(m0.RefreshFailure, match="invalid bronze identity path"):
        m0.discover_inventory(bronze, require_all_asset_classes=False)
    invalid.unlink()
    monkeypatch.setattr(m0, "ASSET_CLASSES", ("equity", "equity"))
    with pytest.raises(m0.RefreshFailure, match="duplicate canonical bronze identities"):
        m0.discover_inventory(bronze, require_all_asset_classes=False)


def test_inventory_rejects_symlink_escape_and_hardlink_alias(tmp_path: Path) -> None:
    warehouse = _warehouse(tmp_path)
    bronze = warehouse / "data-lake" / "bronze"
    outside = tmp_path / "outside" / "symbol=EVIL"
    outside.mkdir(parents=True)
    outside.joinpath("data.parquet").write_bytes(
        bronze.joinpath("asset_class=equity/symbol=AAPL/data.parquet").read_bytes()
    )
    symlink = bronze / "asset_class=equity/symbol=EVIL"
    symlink.symlink_to(outside, target_is_directory=True)
    with pytest.raises(m0.RefreshFailure, match="symlink"):
        m0.discover_inventory(bronze)
    symlink.unlink()

    alias = bronze / "asset_class=equity/symbol=MSFT"
    alias.mkdir()
    os.link(
        bronze / "asset_class=equity/symbol=AAPL/data.parquet",
        alias / "data.parquet",
    )
    with pytest.raises(m0.RefreshFailure, match="hard-link alias"):
        m0.discover_inventory(bronze)


@pytest.mark.parametrize(("corruption", "message"), [("schema", "schema mismatch"), ("type", "schema mismatch"), ("empty", "has no rows"), ("dates", "invalid trade_date ordering"), ("identity", "inconsistent identity IDs")])
def test_inventory_rejects_corrupt_parquet(tmp_path: Path, corruption: str, message: str) -> None:
    warehouse = _warehouse(tmp_path)
    path = warehouse / "data-lake/bronze/asset_class=equity/symbol=AAPL/data.parquet"
    table = pq.ParquetFile(path).read()
    if corruption == "schema":
        table = table.append_column("unexpected", pa.array([1]))
    elif corruption == "type":
        table = table.set_column(6, "adj_close", pa.array(["100.0"]))
    elif corruption == "empty":
        table = table.slice(0, 0)
    elif corruption == "dates":
        table = pa.concat_tables([table, table])
    else:
        table = pa.concat_tables([table, table.set_column(1, "symbol_id", pa.array([999], type=pa.int64()))])
        table = table.set_column(0, "trade_date", pa.array([date(2025, 1, 1), date(2025, 1, 2)], type=pa.date32()))
    pq.write_table(table, path)
    with pytest.raises(m0.RefreshFailure, match=message):
        m0.discover_inventory(warehouse / "data-lake" / "bronze")


def test_source_identity_and_default_runner_capture_output(tmp_path: Path) -> None:
    completed = [
        subprocess.CompletedProcess(["git"], 0, "", ""),
        subprocess.CompletedProcess(["git"], 0, "commit-id\n", ""),
        subprocess.CompletedProcess(["git"], 0, "commit\n", ""),
        subprocess.CompletedProcess(["git"], 0, "tree-id\n", ""),
        subprocess.CompletedProcess(["git"], 0, "tree-id\n", ""),
    ]
    with patch.object(m0.subprocess, "run", side_effect=completed) as run:
        assert m0._source_identity(tmp_path) == {"commit": "commit-id", "tree": "tree-id"}
    assert all(call.kwargs["capture_output"] and call.kwargs["check"] for call in run.call_args_list)
    assert all(Path(call.args[0][0]).is_absolute() for call in run.call_args_list)
    assert all(call.kwargs["env"]["PATH"] == os.defpath for call in run.call_args_list)
    config = m0.RefreshConfig(tmp_path, tmp_path / "db", tmp_path / "manifest", tmp_path / "inventory", date(2025, 1, 2), Path("python"), tmp_path, 17)
    result = subprocess.CompletedProcess(["safe"], 0, "SECRET", "SECRET")
    with patch.object(m0.subprocess, "run", return_value=result) as run:
        assert m0._default_runner(config)(["safe"]) is result
    kwargs = run.call_args.kwargs
    assert {key: kwargs[key] for key in ("cwd", "check", "capture_output", "text", "timeout")} == {
        "cwd": tmp_path,
        "check": False,
        "capture_output": True,
        "text": True,
        "timeout": 17,
    }
    assert kwargs["env"]["MDW_WAREHOUSE"] == str(tmp_path)
    assert not any(key.startswith("PYTHON") for key in kwargs["env"])
    assert not any(key.startswith(("LD_", "DYLD_")) for key in kwargs["env"])


def test_owner_executes_sealed_committed_bytes_not_mutable_materialization(
    tmp_path: Path,
) -> None:
    repo, identity = _real_repo(tmp_path)
    owner = repo / "scripts" / "fetch_binance_crypto.py"
    owner.write_text(
        "import os\nfrom pathlib import Path\n"
        "Path(os.environ['MDW_WAREHOUSE'], 'owner-byte.txt').write_text('committed')\n"
    )
    subprocess.run(["git", "add", "scripts/fetch_binance_crypto.py"], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-qm", "owner"], cwd=repo, check=True)
    identity = dict(m0._source_identity(repo))
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()

    with m0._sealed_execution_source(repo, identity) as sealed:
        owner.write_text(
            "import os\nfrom pathlib import Path\n"
            "Path(os.environ['MDW_WAREHOUSE'], 'owner-byte.txt').write_text('hostile')\n"
        )
        config = m0.RefreshConfig(
            warehouse, tmp_path / "db", tmp_path / "manifest", tmp_path / "inventory",
            date(2025, 1, 2), Path(sys.executable), repo,
            source_archive_fd=sealed.fileno(),
        )
        entry = m0.InventoryEntry(
            "crypto", "BTC", "unused", "0" * 64, 1, "2025-01-02", 1,
            ("trade_date:date32[day]",), "1" * 64,
        )
        argv = m0._update_argv(config, entry, tmp_path / "preset.json")
        result = m0._default_runner(config)(argv)

    assert result.returncode == 0, result.stderr
    assert (warehouse / "owner-byte.txt").read_text() == "committed"
    assert "-I" in argv and "-S" in argv


def test_owner_imports_dependencies_from_sealed_commit_not_materialized_tree(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    (repo / "scripts").mkdir(parents=True)
    (repo / "clients").mkdir()
    (repo / "clients" / "__init__.py").write_text("")
    dependency = repo / "clients" / "payload.py"
    dependency.write_text("VALUE = 'committed'\n")
    owner = repo / "scripts" / "fetch_binance_crypto.py"
    owner.write_text(
        "import os,sys\nfrom pathlib import Path\n"
        "PROJECT_ROOT=Path(__file__).resolve().parent.parent\n"
        "sys.path.insert(0,str(PROJECT_ROOT))\n"
        "from clients.payload import VALUE\n"
        "Path(os.environ['MDW_WAREHOUSE'],'dependency-byte.txt').write_text(VALUE)\n"
    )
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.email", "test@example.invalid"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=repo, check=True)
    subprocess.run(["git", "add", "."], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-qm", "sealed import closure"], cwd=repo, check=True)
    identity = dict(m0._source_identity(repo))
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()

    with m0._sealed_execution_source(repo, identity) as sealed:
        dependency.write_text("VALUE = 'hostile'\n")
        config = m0.RefreshConfig(
            warehouse, tmp_path / "db", tmp_path / "manifest", tmp_path / "inventory",
            date(2025, 1, 2), Path(sys.executable), repo,
            source_archive_fd=sealed.fileno(),
        )
        entry = m0.InventoryEntry(
            "crypto", "BTC", "unused", "0" * 64, 1, "2025-01-02", 1,
            ("trade_date:date32[day]",), "1" * 64,
        )
        result = m0._default_runner(config)(
            m0._update_argv(config, entry, tmp_path / "preset.json")
        )

    assert result.returncode == 0, result.stderr
    assert (warehouse / "dependency-byte.txt").read_text() == "committed"


def test_materialization_rejects_git_archive_attributes_transformations(
    tmp_path: Path,
) -> None:
    repo, identity = _real_repo(tmp_path)
    attributes = repo / ".git" / "info" / "attributes"
    attributes.write_text("scripts/owner.py export-ignore\n")

    with pytest.raises(m0.RefreshFailure, match="archive differs from recorded Git tree"):
        m0._materialize_source_tree(repo, identity, tmp_path / "materialized")


@pytest.mark.parametrize("drift", ["staged", "unstaged", "untracked_source"])
def test_source_identity_rejects_executed_source_drift(tmp_path: Path, drift: str) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.email", "test@example.invalid"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=repo, check=True)
    source = repo / "runner.py"
    source.write_text("VALUE = 1\n")
    subprocess.run(["git", "add", "runner.py"], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-qm", "base"], cwd=repo, check=True)
    if drift == "staged":
        source.write_text("VALUE = 2\n")
        subprocess.run(["git", "add", "runner.py"], cwd=repo, check=True)
    elif drift == "unstaged":
        source.write_text("VALUE = 2\n")
    else:
        (repo / "helper.py").write_text("VALUE = 2\n")
    with pytest.raises(m0.RefreshFailure, match="source tree is dirty"):
        m0._source_identity(repo)


def test_source_identity_allows_committed_tree(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.email", "test@example.invalid"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=repo, check=True)
    (repo / "runner.py").write_text("VALUE = 1\n")
    subprocess.run(["git", "add", "runner.py"], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-qm", "base"], cwd=repo, check=True)
    identity = m0._source_identity(repo)
    assert identity["tree"] == subprocess.run(
        ["git", "rev-parse", "HEAD^{tree}"], cwd=repo, check=True,
        capture_output=True, text=True,
    ).stdout.strip()


def _real_repo(tmp_path: Path) -> tuple[Path, dict[str, str]]:
    repo = tmp_path / "repo"
    (repo / "scripts").mkdir(parents=True)
    (repo / "scripts" / "owner.py").write_text("VALUE = 1\n")
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.email", "test@example.invalid"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=repo, check=True)
    subprocess.run(["git", "add", "."], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-qm", "base"], cwd=repo, check=True)
    return repo, dict(m0._source_identity(repo))


def test_source_attestation_rejects_commit_tree_mismatch_and_noncommit(tmp_path: Path) -> None:
    repo, identity = _real_repo(tmp_path)
    (repo / "other.txt").write_text("other\n")
    subprocess.run(["git", "add", "other.txt"], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-qm", "other"], cwd=repo, check=True)
    other = dict(m0._source_identity(repo))

    with pytest.raises(m0.RefreshFailure, match="commit tree mismatch"):
        m0._materialize_source_tree(
            repo, {"commit": identity["commit"], "tree": other["tree"]},
            tmp_path / "bad-tree",
        )
    with pytest.raises(m0.RefreshFailure, match="not a commit"):
        m0._materialize_source_tree(
            repo, {"commit": other["tree"], "tree": other["tree"]},
            tmp_path / "not-commit",
        )


def test_source_attestation_ignores_hostile_path_and_git_environment(
    tmp_path: Path, monkeypatch,
) -> None:
    repo, expected = _real_repo(tmp_path)
    fake_bin = tmp_path / "fake-bin"
    fake_bin.mkdir()
    fake_git = fake_bin / "git"
    fake_git.write_text("#!/bin/sh\nprintf '%s\\n' fake\n")
    fake_git.chmod(0o755)
    other, _other_identity = _real_repo(tmp_path / "other")
    monkeypatch.setenv("PATH", f"{fake_bin}:{os.environ.get('PATH', '')}")
    monkeypatch.setenv("GIT_DIR", str(other / ".git"))
    monkeypatch.setenv("GIT_OBJECT_DIRECTORY", str(other / ".git" / "objects"))
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", str(tmp_path / "hostile-config"))

    assert m0._source_identity(repo) == expected
    destination = tmp_path / "materialized"
    m0._materialize_source_tree(repo, expected, destination)
    assert (destination / "scripts" / "owner.py").read_text() == "VALUE = 1\n"


def test_materialized_source_tampering_is_detected(tmp_path: Path) -> None:
    repo, identity = _real_repo(tmp_path)
    destination = tmp_path / "materialized"
    m0._materialize_source_tree(repo, identity, destination)
    owner = destination / "scripts" / "owner.py"
    owner.chmod(0o600)
    owner.write_text("VALUE = 'tampered'\n")

    with pytest.raises(m0.RefreshFailure, match="materialized source differs"):
        m0._verify_materialized_source_tree(repo, identity, destination)


def _built_database(tmp_path: Path) -> tuple[Path, list[m0.InventoryEntry]]:
    warehouse = _warehouse(tmp_path)
    config = _config(tmp_path, warehouse)
    db_path = tmp_path / "candidate.duckdb"
    m0._build_database(config, db_path)
    return db_path, m0.discover_inventory(warehouse / "data-lake" / "bronze")


@pytest.mark.parametrize(
    "corruption", ["schema", "type", "index", "count", "symbols", "futures", "redistribute"],
)
def test_database_validation_rejects_integrity_failures(tmp_path: Path, corruption: str) -> None:
    db_path, inventory = _built_database(tmp_path)
    connection = duckdb.connect(str(db_path))
    if corruption == "schema":
        connection.execute("ALTER TABLE md.symbols ADD COLUMN bad INTEGER")
        message = "schema mismatch"
    elif corruption == "type":
        connection.execute("DROP INDEX md.idx_equities_daily_dedup")
        connection.execute("ALTER TABLE md.equities_daily ALTER volume TYPE DOUBLE")
        connection.execute(
            "CREATE UNIQUE INDEX idx_equities_daily_dedup "
            "ON md.equities_daily(trade_date, symbol_id)"
        )
        message = "schema mismatch"
    elif corruption == "index":
        connection.execute("DROP INDEX md.idx_equities_daily_dedup")
        message = "index mismatch"
    elif corruption == "count":
        connection.execute("INSERT INTO md.equities_daily SELECT DATE '2025-01-03', symbol_id, open, high, low, close, adj_close, volume FROM md.equities_daily LIMIT 1")
        message = "row count mismatch"
    elif corruption == "symbols":
        connection.execute("UPDATE md.symbols SET symbol='WRONG' WHERE asset_class='crypto'")
        message = "symbol inventory mismatch"
    elif corruption == "futures":
        connection.execute("UPDATE md.futures_daily SET root_symbol='NQ'")
        message = "futures inventory mismatch"
    else:
        ids = dict(connection.execute(
            "SELECT asset_class, symbol_id FROM md.symbols"
        ).fetchall())
        connection.execute(
            "UPDATE md.equities_daily SET trade_date=DATE '2025-01-01' WHERE symbol_id=?",
            [ids["crypto"]],
        )
        connection.execute(
            "UPDATE md.equities_daily SET symbol_id=? WHERE symbol_id=?",
            [ids["equity"], ids["crypto"]],
        )
        message = "per-identity mismatch"
    connection.close()
    with pytest.raises(m0.RefreshFailure, match=message):
        m0.validate_database(db_path, inventory)


@pytest.mark.parametrize("interruption", [OSError("denied"), KeyboardInterrupt(), SystemExit(9)])
def test_bundle_pointer_is_the_only_atomic_commit_point(
    tmp_path: Path, interruption: BaseException,
) -> None:
    current = tmp_path / "published" / "current"
    db_path, manifest_path = current / "market.duckdb", current / "manifest.json"
    old = tmp_path / "old.tmp"
    old.write_bytes(b"old-db")
    m0._atomic_publish_bundle(old, db_path, manifest_path, {
        "generation": "old", "database_sha256": hashlib.sha256(b"old-db").hexdigest(),
    })
    old_bundle = m0.resolve_current_bundle(db_path, manifest_path)
    candidate = tmp_path / "new.tmp"
    candidate.write_bytes(b"new-db")
    with patch.object(m0.os, "replace", side_effect=interruption):
        with pytest.raises(type(interruption)):
            m0._atomic_publish_bundle(candidate, db_path, manifest_path, {
                "generation": "new",
                "database_sha256": hashlib.sha256(b"new-db").hexdigest(),
            })
    visible = m0.resolve_current_bundle(db_path, manifest_path)
    assert visible.database.read_bytes() == b"old-db"
    assert json.loads(visible.manifest.read_text())["generation"] == "old"
    assert old_bundle.database.read_bytes() == b"old-db"


def test_interrupted_first_publication_has_no_partial_visible_bundle(tmp_path: Path) -> None:
    current = tmp_path / "published" / "current"
    db_path, manifest_path = current / "market.duckdb", current / "manifest.json"
    candidate = tmp_path / "new.tmp"
    candidate.write_bytes(b"new-db")
    with patch.object(m0.os, "replace", side_effect=KeyboardInterrupt()):
        with pytest.raises(KeyboardInterrupt):
            m0._atomic_publish_bundle(candidate, db_path, manifest_path, {
                "generation": "new",
                "database_sha256": hashlib.sha256(b"new-db").hexdigest(),
            })
    assert not current.exists() and not current.is_symlink()
    with pytest.raises(m0.RefreshFailure, match="no predecessor"):
        m0.resolve_current_bundle(db_path, manifest_path)


def test_sigkill_before_pointer_promotion_preserves_predecessor(tmp_path: Path) -> None:
    current = tmp_path / "published" / "current"
    db_path, manifest_path = current / "market.duckdb", current / "manifest.json"
    old = tmp_path / "old.tmp"
    old.write_bytes(b"old-db")
    old_hash = hashlib.sha256(b"old-db").hexdigest()
    m0._atomic_publish_bundle(old, db_path, manifest_path, {
        "generation": "old", "database_sha256": old_hash,
    })
    candidate = tmp_path / "new.tmp"
    candidate.write_bytes(b"new-db")
    program = """
import os, signal, sys
from pathlib import Path
from scripts import refresh_all_and_rebuild as m0

def terminate(_source, _destination):
    os.kill(os.getpid(), signal.SIGKILL)

m0.os.replace = terminate
m0._atomic_publish_bundle(
    Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3]),
    {"generation": "new", "database_sha256": sys.argv[4]},
)
"""
    result = subprocess.run(
        [
            sys.executable, "-c", program, str(candidate), str(db_path),
            str(manifest_path), hashlib.sha256(b"new-db").hexdigest(),
        ],
        cwd=m0.PROJECT_ROOT,
        check=False,
    )
    assert result.returncode == -9
    visible = m0.resolve_current_bundle(db_path, manifest_path)
    assert visible.database.read_bytes() == b"old-db"
    assert json.loads(visible.manifest.read_text())["generation"] == "old"


def test_successful_bundle_publish_preserves_pinned_predecessor(tmp_path: Path) -> None:
    current = tmp_path / "published" / "current"
    db_path, manifest_path = current / "market.duckdb", current / "manifest.json"
    old = tmp_path / "old.tmp"
    old.write_bytes(b"old-db")
    m0._atomic_publish_bundle(old, db_path, manifest_path, {
        "generation": "old", "database_sha256": hashlib.sha256(b"old-db").hexdigest(),
    })
    pinned_old = m0.resolve_current_bundle(db_path, manifest_path)
    new = tmp_path / "new.tmp"
    new.write_bytes(b"new-db")
    m0._atomic_publish_bundle(new, db_path, manifest_path, {
        "generation": "new", "database_sha256": hashlib.sha256(b"new-db").hexdigest(),
    })
    pinned_new = m0.resolve_current_bundle(db_path, manifest_path)
    assert pinned_new.database.read_bytes() == b"new-db"
    assert json.loads(pinned_new.manifest.read_text())["generation"] == "new"
    assert pinned_old.database.read_bytes() == b"old-db"
    assert json.loads(pinned_old.manifest.read_text())["generation"] == "old"


def test_cli_status_and_secret_redaction(tmp_path: Path, capsys) -> None:
    config = _config(tmp_path, _warehouse(tmp_path))
    argv = ["--warehouse", str(config.warehouse), "--db-path", str(config.db_path), "--manifest-path", str(config.manifest_path), "--inventory-path", str(config.inventory_path), "--as-of", "2025-01-02", "--python", "/safe/python", "--command-timeout", "12"]
    with patch.object(m0, "refresh_all_and_rebuild", side_effect=m0.RefreshFailure("safe failure")):
        assert m0.main(argv) == 1
    assert "safe failure" in capsys.readouterr().err
    with patch.object(m0, "refresh_all_and_rebuild", side_effect=OSError("SECRET")):
        assert m0.main(argv) == 1
    captured = capsys.readouterr()
    assert "OSError" in captured.err and "SECRET" not in captured.err
    with patch.object(m0, "refresh_all_and_rebuild", return_value={}):
        assert m0.main(argv) == 0


def test_cli_accepts_exact_portfolio_engine_authority_shape() -> None:
    warehouse = Path("/home/sebastian/market-warehouse")
    manifest = warehouse / "duckdb" / "current" / "manifest.json"
    captured: list[m0.RefreshConfig] = []
    argv = [
        "--as-of",
        "2025-01-02",
        "--db-path",
        str(warehouse / "duckdb" / "market.duckdb"),
        "--manifest",
        str(manifest),
    ]

    with patch.object(m0, "refresh_all_and_rebuild", side_effect=lambda config: captured.append(config)):
        assert m0.main(argv) == 0

    assert len(captured) == 1
    assert captured[0].db_path == warehouse / "duckdb" / "current" / "market.duckdb"
    assert captured[0].manifest_path == manifest
    assert captured[0].inventory_path == warehouse / "duckdb" / "pre-refresh-inventory.json"


@pytest.mark.parametrize("asset_class", m0.ASSET_CLASSES)
def test_inventory_binds_each_path_to_its_canonical_internal_id(
    tmp_path: Path, asset_class: str,
) -> None:
    warehouse = _warehouse(tmp_path)
    bronze = warehouse / "data-lake" / "bronze"
    path = next((bronze / f"asset_class={asset_class}").glob("symbol=*/data.parquet"))
    table = pq.ParquetFile(path).read()
    id_column = "contract_id" if asset_class == "futures" else "symbol_id"
    table = table.set_column(
        table.column_names.index(id_column), id_column,
        pa.array([stable_symbol_id("WRONG")], type=pa.int64()),
    )
    pq.write_table(table, path)
    with pytest.raises(m0.RefreshFailure, match="canonical identity ID mismatch"):
        m0.discover_inventory(bronze)


def test_inventory_rejects_canonical_id_collisions(tmp_path: Path, monkeypatch) -> None:
    warehouse = _warehouse(tmp_path)
    bronze = warehouse / "data-lake" / "bronze"
    source = bronze / "asset_class=equity/symbol=AAPL/data.parquet"
    collision = bronze / "asset_class=equity/symbol=MSFT/data.parquet"
    collision.parent.mkdir()
    collision.write_bytes(source.read_bytes())
    monkeypatch.setattr(m0, "ASSET_CLASSES", ("equity",))
    monkeypatch.setattr(m0, "stable_symbol_id", lambda _symbol: stable_symbol_id("AAPL"))
    with pytest.raises(m0.RefreshFailure, match="canonical identity ID collision"):
        m0.discover_inventory(bronze, require_all_asset_classes=False)


def test_source_mutation_during_owner_call_blocks_publication(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    (repo / "scripts").mkdir(parents=True)
    for name in ("daily_update.py", "fetch_cboe_volatility.py", "fetch_binance_crypto.py"):
        (repo / "scripts" / name).write_text("# committed owner\n")
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.email", "test@example.invalid"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=repo, check=True)
    subprocess.run(["git", "add", "."], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-qm", "base"], cwd=repo, check=True)
    warehouse = _warehouse(tmp_path)
    config = _config(tmp_path, warehouse)
    object.__setattr__(config, "repo_root", repo)
    old = _seed_old_db(config)
    calls = 0

    def mutate(argv: list[str]) -> subprocess.CompletedProcess[str]:
        nonlocal calls
        calls += 1
        assert all(str(repo) not in argument for argument in argv)
        assert argv[1:3] == ["-I", "-S"]
        assert argv[5].startswith("/dev/fd/")
        assert argv[6].startswith("scripts/")
        if calls == 1:
            (repo / "scripts" / "fetch_binance_crypto.py").write_text("# mutated\n")
        return subprocess.CompletedProcess(argv, 0, "", "")

    with pytest.raises(m0.RefreshFailure, match="source tree is dirty"):
        m0.refresh_all_and_rebuild(config, command_runner=mutate)
    assert config.db_path.read_bytes() == old


def test_refresh_rejects_completed_process_argv_mismatch(tmp_path: Path) -> None:
    config = _config(tmp_path, _warehouse(tmp_path))
    old = _seed_old_db(config)

    def mismatch(_argv: list[str]) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(["different"], 0, "", "")

    with pytest.raises(m0.RefreshFailure, match="reported argv mismatch"):
        m0.refresh_all_and_rebuild(
            config, command_runner=mismatch, source_identity=_identity,
        )
    assert config.db_path.read_bytes() == old


@pytest.mark.parametrize("asset_class", m0.ASSET_CLASSES)
def test_database_validation_independently_binds_canonical_ids(
    tmp_path: Path, asset_class: str,
) -> None:
    db_path, inventory = _built_database(tmp_path)
    connection = duckdb.connect(str(db_path))
    if asset_class == "futures":
        connection.execute("UPDATE md.futures_daily SET contract_id=?", [stable_symbol_id("WRONG")])
    else:
        old_id = stable_symbol_id(
            {"crypto": "BTC", "equity": "AAPL", "volatility": "VIX"}[asset_class]
        )
        new_id = stable_symbol_id(f"WRONG-{asset_class}")
        connection.execute(
            "UPDATE md.equities_daily SET symbol_id=? WHERE symbol_id=?", [new_id, old_id],
        )
        connection.execute(
            "UPDATE md.symbols SET symbol_id=? WHERE asset_class=?", [new_id, asset_class],
        )
    connection.close()
    with pytest.raises(m0.RefreshFailure, match="canonical ID mismatch"):
        m0.validate_database(db_path, inventory)


def test_post_replace_fsync_failure_restores_predecessor(tmp_path: Path) -> None:
    current = tmp_path / "published" / "current"
    db_path, manifest_path = current / "market.duckdb", current / "manifest.json"
    old = tmp_path / "old.tmp"
    old.write_bytes(b"old-db")
    m0._atomic_publish_bundle(old, db_path, manifest_path, {
        "generation": "old", "database_sha256": hashlib.sha256(b"old-db").hexdigest(),
    })
    candidate = tmp_path / "new.tmp"
    candidate.write_bytes(b"new-db")
    real_fsync = m0._fsync_directory
    pointer_fsyncs = 0

    def interrupt(path: Path) -> None:
        nonlocal pointer_fsyncs
        if path == current.parent:
            pointer_fsyncs += 1
            if pointer_fsyncs == 2:
                raise KeyboardInterrupt()
        real_fsync(path)

    with patch.object(m0, "_fsync_directory", side_effect=interrupt):
        with pytest.raises(KeyboardInterrupt):
            m0._atomic_publish_bundle(candidate, db_path, manifest_path, {
                "generation": "new",
                "database_sha256": hashlib.sha256(b"new-db").hexdigest(),
            })
    assert m0.resolve_current_bundle(db_path, manifest_path).database.read_bytes() == b"old-db"


def test_failed_rollback_leaves_reader_recovery_bound_to_predecessor(tmp_path: Path) -> None:
    current = tmp_path / "published" / "current"
    db_path, manifest_path = current / "market.duckdb", current / "manifest.json"
    old = tmp_path / "old.tmp"
    old.write_bytes(b"old-db")
    m0._atomic_publish_bundle(old, db_path, manifest_path, {
        "generation": "old", "database_sha256": hashlib.sha256(b"old-db").hexdigest(),
    })
    candidate = tmp_path / "new.tmp"
    candidate.write_bytes(b"new-db")
    real_replace = os.replace
    replacements = 0

    def fail_rollback(source, destination) -> None:
        nonlocal replacements
        replacements += 1
        if replacements == 2:
            raise OSError("rollback failed")
        real_replace(source, destination)

    real_fsync = m0._fsync_directory
    pointer_fsyncs = 0

    def fail_commit(path: Path) -> None:
        nonlocal pointer_fsyncs
        if path == current.parent:
            pointer_fsyncs += 1
            if pointer_fsyncs == 2:
                raise SystemExit(9)
        real_fsync(path)

    with patch.object(m0.os, "replace", side_effect=fail_rollback), patch.object(
        m0, "_fsync_directory", side_effect=fail_commit,
    ):
        with pytest.raises(SystemExit):
            m0._atomic_publish_bundle(candidate, db_path, manifest_path, {
                "generation": "new",
                "database_sha256": hashlib.sha256(b"new-db").hexdigest(),
            })
    visible = m0.resolve_current_bundle(db_path, manifest_path)
    assert visible.database.read_bytes() == b"old-db"
    assert json.loads(visible.manifest.read_text())["generation"] == "old"


@pytest.mark.parametrize("has_predecessor", [False, True])
@pytest.mark.parametrize(("unlink_fails", "cleanup_fsync_fails"), [(True, False), (False, True), (True, True)])
def test_post_commit_marker_cleanup_faults_leave_reader_recognized_success(
    tmp_path: Path, has_predecessor: bool, unlink_fails: bool, cleanup_fsync_fails: bool,
) -> None:
    current = tmp_path / "published" / "current"
    db_path, manifest_path = current / "market.duckdb", current / "manifest.json"
    if has_predecessor:
        old = tmp_path / "old.tmp"
        old.write_bytes(b"old-db")
        m0._atomic_publish_bundle(old, db_path, manifest_path, {
            "generation": "old", "database_sha256": hashlib.sha256(b"old-db").hexdigest(),
        })
    candidate = tmp_path / "new.tmp"
    candidate.write_bytes(b"new-db")
    recovery = current.parent / ".current.recovery.json"
    real_unlink, real_fsync = Path.unlink, m0._fsync_directory
    parent_fsyncs = 0
    retired_marker: bytes | None = None

    def unlink(path: Path, *args, **kwargs) -> None:
        nonlocal retired_marker
        if path == recovery:
            retired_marker = path.read_bytes()
        if unlink_fails and path == recovery:
            raise OSError("marker unlink failed")
        real_unlink(path, *args, **kwargs)

    def fsync(path: Path) -> None:
        nonlocal parent_fsyncs
        if path == current.parent:
            parent_fsyncs += 1
            if cleanup_fsync_fails and parent_fsyncs == 4:
                raise OSError("cleanup fsync failed")
        real_fsync(path)

    with patch.object(Path, "unlink", unlink), patch.object(
        m0, "_fsync_directory", side_effect=fsync,
    ):
        m0._atomic_publish_bundle(candidate, db_path, manifest_path, {
            "generation": "new",
            "database_sha256": hashlib.sha256(b"new-db").hexdigest(),
        })

    assert retired_marker is not None
    marker_document = json.loads(retired_marker)
    assert marker_document["state"] == "committed"
    assert marker_document["successor"] == current.resolve().name
    # Model a crash restoring the exact pre-unlink directory entry after a
    # failed cleanup fsync.  A fresh production reader must still resolve the
    # committed successor, never pin the predecessor.
    if not recovery.exists():
        recovery.write_bytes(retired_marker)
    assert m0.resolve_current_bundle(db_path, manifest_path).database.read_bytes() == b"new-db"


def test_compound_commit_fault_returns_success_when_fresh_reader_admits_successor(
    tmp_path: Path,
) -> None:
    current = tmp_path / "published" / "current"
    db_path, manifest_path = current / "market.duckdb", current / "manifest.json"
    old = tmp_path / "old.tmp"
    old.write_bytes(b"old-db")
    m0._atomic_publish_bundle(old, db_path, manifest_path, {
        "generation": "old", "database_sha256": hashlib.sha256(b"old-db").hexdigest(),
    })
    candidate = tmp_path / "new.tmp"
    candidate.write_bytes(b"new-db")
    real_replace, real_fsync = os.replace, m0._fsync_directory
    replacements = parent_fsyncs = 0

    def replace(source, destination) -> None:
        nonlocal replacements
        replacements += 1
        if replacements == 3:
            raise OSError("guard restoration failed")
        if replacements == 4:
            raise OSError("rollback failed")
        real_replace(source, destination)

    def fsync(path: Path) -> None:
        nonlocal parent_fsyncs
        if path == current.parent:
            parent_fsyncs += 1
            if parent_fsyncs == 3:
                raise OSError("marker commit fsync failed")
        real_fsync(path)

    with patch.object(m0.os, "replace", side_effect=replace), patch.object(
        m0, "_fsync_directory", side_effect=fsync,
    ):
        m0._atomic_publish_bundle(candidate, db_path, manifest_path, {
            "generation": "new",
            "database_sha256": hashlib.sha256(b"new-db").hexdigest(),
        })

    visible = m0.resolve_current_bundle(db_path, manifest_path)
    assert visible.database.read_bytes() == b"new-db"
    assert json.loads(visible.manifest.read_text())["generation"] == "new"


def test_post_replace_validation_and_failed_rollback_leave_durable_reader_guard(tmp_path: Path) -> None:
    current = tmp_path / "published" / "current"
    db_path, manifest_path = current / "market.duckdb", current / "manifest.json"
    old = tmp_path / "old.tmp"
    old.write_bytes(b"old-db")
    m0._atomic_publish_bundle(old, db_path, manifest_path, {
        "generation": "old", "database_sha256": hashlib.sha256(b"old-db").hexdigest(),
    })
    candidate = tmp_path / "new.tmp"
    candidate.write_bytes(b"new-db")
    real_replace = os.replace
    replacements = verifications = 0

    def replace(source, destination) -> None:
        nonlocal replacements
        replacements += 1
        if replacements == 2:
            raise OSError("rollback failed")
        real_replace(source, destination)

    def verify() -> None:
        nonlocal verifications
        verifications += 1
        if verifications == 2:
            raise KeyboardInterrupt()

    with patch.object(m0.os, "replace", side_effect=replace):
        with pytest.raises(KeyboardInterrupt):
            m0._atomic_publish_bundle(
                candidate, db_path, manifest_path,
                {"generation": "new", "database_sha256": hashlib.sha256(b"new-db").hexdigest()},
                verify_source=verify,
            )
    recovery = current.parent / ".current.recovery.json"
    assert recovery.is_file() and not recovery.is_symlink()
    assert m0.resolve_current_bundle(db_path, manifest_path).database.read_bytes() == b"old-db"


def test_first_publication_post_replace_failure_and_failed_removal_stays_fail_closed(
    tmp_path: Path,
) -> None:
    current = tmp_path / "published" / "current"
    db_path, manifest_path = current / "market.duckdb", current / "manifest.json"
    candidate = tmp_path / "new.tmp"
    candidate.write_bytes(b"new-db")
    real_unlink = Path.unlink
    verifications = 0

    def unlink(path: Path, *args, **kwargs) -> None:
        if path == current:
            raise OSError("pointer removal failed")
        real_unlink(path, *args, **kwargs)

    def verify() -> None:
        nonlocal verifications
        verifications += 1
        if verifications == 2:
            raise SystemExit(9)

    with patch.object(Path, "unlink", unlink):
        with pytest.raises(SystemExit):
            m0._atomic_publish_bundle(
                candidate, db_path, manifest_path,
                {"database_sha256": hashlib.sha256(b"new-db").hexdigest()},
                verify_source=verify,
            )
    recovery = current.parent / ".current.recovery.json"
    assert recovery.is_file() and not recovery.is_symlink()
    with pytest.raises(m0.RefreshFailure, match="no predecessor"):
        m0.resolve_current_bundle(db_path, manifest_path)


@pytest.mark.parametrize("mutation", ["tracked", "relevant_untracked"])
@pytest.mark.parametrize("injection", ["publish_entry", "pointer_replace", "commit_fsync"])
def test_source_mutation_in_publication_window_blocks_and_rolls_back(
    tmp_path: Path, mutation: str, injection: str,
) -> None:
    repo = tmp_path / "repo"
    (repo / "scripts").mkdir(parents=True)
    for name in ("daily_update.py", "fetch_cboe_volatility.py", "fetch_binance_crypto.py"):
        (repo / "scripts" / name).write_text("# committed owner\n")
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.email", "test@example.invalid"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=repo, check=True)
    subprocess.run(["git", "add", "."], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-qm", "base"], cwd=repo, check=True)
    config = _config(tmp_path, _warehouse(tmp_path))
    object.__setattr__(config, "repo_root", repo)
    old = _seed_old_db(config)
    mutated = False

    def mutate() -> None:
        nonlocal mutated
        if mutated:
            return
        mutated = True
        if mutation == "tracked":
            (repo / "scripts" / "fetch_binance_crypto.py").write_text("# drifted\n")
        else:
            (repo / "scripts" / "publication_helper.py").write_text("# untracked drift\n")

    if injection == "publish_entry":
        real_publish = m0._atomic_publish_bundle

        def publish(*args, **kwargs) -> None:
            mutate()
            real_publish(*args, **kwargs)

        publication_patch = patch.object(m0, "_atomic_publish_bundle", side_effect=publish)
    elif injection == "pointer_replace":
        real_replace = os.replace

        def replace(source, destination) -> None:
            if Path(destination) == config.db_path.parent:
                mutate()
            real_replace(source, destination)

        publication_patch = patch.object(m0.os, "replace", side_effect=replace)
    else:
        real_replace = os.replace
        real_fsync_directory = m0._fsync_directory
        promoted = False

        def replace(source, destination) -> None:
            nonlocal promoted
            real_replace(source, destination)
            if Path(destination) == config.db_path.parent:
                promoted = True

        def fsync_directory(path: Path) -> None:
            real_fsync_directory(path)
            if promoted and Path(path) == config.db_path.parent.parent:
                mutate()

        publication_patch = contextlib.ExitStack()
        publication_patch.enter_context(patch.object(m0.os, "replace", side_effect=replace))
        publication_patch.enter_context(
            patch.object(m0, "_fsync_directory", side_effect=fsync_directory)
        )

    with publication_patch, pytest.raises(m0.RefreshFailure, match="source tree is dirty"):
        m0.refresh_all_and_rebuild(config, command_runner=_runner([]))
    assert config.db_path.read_bytes() == old


def test_resolver_rejects_generation_and_nested_file_symlinks(tmp_path: Path) -> None:
    current = tmp_path / "published" / "current"
    db_path, manifest_path = current / "market.duckdb", current / "manifest.json"
    candidate = tmp_path / "candidate.tmp"
    candidate.write_bytes(b"db")
    m0._atomic_publish_bundle(candidate, db_path, manifest_path, {
        "database_sha256": hashlib.sha256(b"db").hexdigest(),
    })
    generation = current.resolve()
    real_generation = generation.with_name("real-generation")
    generation.rename(real_generation)
    generation.symlink_to(real_generation, target_is_directory=True)
    with pytest.raises(m0.RefreshFailure, match="symlink"):
        m0.resolve_current_bundle(db_path, manifest_path)
    generation.unlink()
    real_generation.rename(generation)

    outside_db = tmp_path / "outside.db"
    outside_db.write_bytes(db_path.read_bytes())
    generation.joinpath(db_path.name).unlink()
    generation.joinpath(db_path.name).symlink_to(outside_db)
    with pytest.raises(m0.RefreshFailure, match="symlink|incomplete"):
        m0.resolve_current_bundle(db_path, manifest_path)


def test_publication_and_resolver_reject_symlinked_ancestor_without_following(
    tmp_path: Path,
) -> None:
    real_published = tmp_path / "real-published"
    real_published.mkdir()
    alias_published = tmp_path / "alias-published"
    alias_published.symlink_to(real_published, target_is_directory=True)
    current = alias_published / "current"
    db_path, manifest_path = current / "market.duckdb", current / "manifest.json"
    candidate = tmp_path / "candidate.tmp"
    candidate.write_bytes(b"db")

    with pytest.raises(m0.RefreshFailure, match="symlink components"):
        m0._atomic_publish_bundle(candidate, db_path, manifest_path, {
            "database_sha256": hashlib.sha256(b"db").hexdigest(),
        })
    assert tuple(real_published.iterdir()) == ()

    with pytest.raises(m0.RefreshFailure, match="symlink components"):
        m0.resolve_current_bundle(db_path, manifest_path)
    assert tuple(real_published.iterdir()) == ()


def test_resolver_rejects_pointer_target_with_lexical_prefix_escape(tmp_path: Path) -> None:
    published = tmp_path / "published"
    bundles = published / "bundles"
    outside = published / "outside"
    bundles.mkdir(parents=True)
    outside.mkdir()
    outside.joinpath("market.duckdb").write_bytes(b"db")
    outside.joinpath("manifest.json").write_text(json.dumps({
        "database_sha256": hashlib.sha256(b"db").hexdigest(),
    }))
    current = published / "current"
    current.symlink_to("bundles/prefix/../../outside", target_is_directory=True)
    with pytest.raises(m0.RefreshFailure, match="escapes"):
        m0.resolve_current_bundle(current / "market.duckdb", current / "manifest.json")
