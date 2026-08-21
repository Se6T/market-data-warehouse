"""Tests for CBOE volatility index fetcher."""

from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.fetch_cboe_volatility import (
    _symbol_id,
    bars_to_table,
    fetch_cboe_historical,
    load_preset,
    main,
    write_bronze_parquet,
)


class TestLoadPreset:
    def test_loads_tickers_from_preset(self, tmp_path):
        preset = tmp_path / "test.json"
        preset.write_text('{"tickers": ["VIX", "VVIX", "VXHYG"]}')
        
        symbols = load_preset(preset)
        assert symbols == ["VIX", "VVIX", "VXHYG"]

    def test_returns_empty_list_if_no_tickers(self, tmp_path):
        preset = tmp_path / "test.json"
        preset.write_text('{"name": "test"}')
        
        symbols = load_preset(preset)
        assert symbols == []


class TestSymbolId:
    def test_stable_hash(self):
        """Symbol ID should be stable across calls."""
        id1 = _symbol_id("VXHYG")
        id2 = _symbol_id("VXHYG")
        assert id1 == id2

    def test_different_symbols_different_ids(self):
        """Different symbols should have different IDs."""
        assert _symbol_id("VXHYG") != _symbol_id("VXSMH")


class TestFetchCboeHistorical:
    def test_fetch_success(self):
        """Successful API call returns bars."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "data": [
                {"date": "2025-01-02", "open": "10.0", "high": "11.0", "low": "9.0", "close": "10.5", "volume": "0.0"},
            ]
        }
        mock_response.raise_for_status = MagicMock()

        with patch("scripts.fetch_cboe_volatility.httpx.get", return_value=mock_response):
            bars = fetch_cboe_historical("VXHYG")

        assert len(bars) == 1
        assert bars[0]["date"] == "2025-01-02"

    def test_fetch_empty_data(self):
        """Empty data returns empty list."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"data": []}
        mock_response.raise_for_status = MagicMock()

        with patch("scripts.fetch_cboe_volatility.httpx.get", return_value=mock_response):
            bars = fetch_cboe_historical("UNKNOWN")

        assert bars == []


class TestBarsToTable:
    def test_converts_bars_to_table(self):
        """JSON bars are converted to PyArrow table."""
        bars = [
            {"date": "2025-01-02", "open": "10.0", "high": "11.0", "low": "9.0", "close": "10.5", "volume": "0.0"},
            {"date": "2025-01-03", "open": "10.5", "high": "12.0", "low": "10.0", "close": "11.0", "volume": "0.0"},
        ]
        table = bars_to_table("VXHYG", bars)

        assert table.num_rows == 2
        # asset_class and symbol are in hive partition path, not in parquet
        assert set(table.column_names) == {
            "trade_date", "symbol_id", "open", "high", "low",
            "close", "adj_close", "volume"
        }
        assert table.column("close")[0].as_py() == 10.5

    def test_empty_bars_returns_none(self):
        """Empty bars list returns None."""
        assert bars_to_table("VXHYG", []) is None


def _read_single_parquet(path: Path):
    """Read a single parquet file without hive partitioning discovery."""
    return pq.ParquetFile(path).read()


class TestWriteBronzeParquet:
    def test_writes_new_file(self, tmp_path):
        """Creates new parquet file when none exists."""
        bars = [
            {"date": "2025-01-02", "open": "10.0", "high": "11.0", "low": "9.0", "close": "10.5", "volume": "0.0"},
        ]
        table = bars_to_table("VXHYG", bars)
        
        path = write_bronze_parquet(table, "VXHYG", tmp_path)
        
        assert path.exists()
        read_table = _read_single_parquet(path)
        assert read_table.num_rows == 1

    def test_merges_with_existing(self, tmp_path):
        """Merges new data with existing parquet file."""
        # Write initial data
        bars1 = [
            {"date": "2025-01-02", "open": "10.0", "high": "11.0", "low": "9.0", "close": "10.5", "volume": "0.0"},
        ]
        table1 = bars_to_table("VXHYG", bars1)
        write_bronze_parquet(table1, "VXHYG", tmp_path)

        # Write overlapping + new data
        bars2 = [
            {"date": "2025-01-02", "open": "10.0", "high": "11.0", "low": "9.0", "close": "10.5", "volume": "0.0"},  # duplicate
            {"date": "2025-01-03", "open": "10.5", "high": "12.0", "low": "10.0", "close": "11.0", "volume": "0.0"},  # new
        ]
        table2 = bars_to_table("VXHYG", bars2)
        path = write_bronze_parquet(table2, "VXHYG", tmp_path)

        read_table = _read_single_parquet(path)
        assert read_table.num_rows == 2  # Only 2 unique dates

    def test_sorts_by_date(self, tmp_path):
        """Output is sorted by trade_date ascending."""
        bars = [
            {"date": "2025-01-05", "open": "10.0", "high": "11.0", "low": "9.0", "close": "10.5", "volume": "0.0"},
            {"date": "2025-01-02", "open": "10.0", "high": "11.0", "low": "9.0", "close": "10.5", "volume": "0.0"},
            {"date": "2025-01-10", "open": "10.0", "high": "11.0", "low": "9.0", "close": "10.5", "volume": "0.0"},
        ]
        table = bars_to_table("VXHYG", bars)
        path = write_bronze_parquet(table, "VXHYG", tmp_path)

        read_table = _read_single_parquet(path)
        dates = [d.as_py() for d in read_table.column("trade_date")]
        assert dates == sorted(dates)

    def test_normalizes_existing_schema_with_extra_columns(self, tmp_path):
        """Existing parquet with extra columns is normalized before merge."""
        # Simulate old-schema parquet with asset_class and symbol columns
        old_schema = pa.schema([
            ("trade_date", pa.date32()),
            ("symbol_id", pa.int64()),
            ("open", pa.float64()),
            ("high", pa.float64()),
            ("low", pa.float64()),
            ("close", pa.float64()),
            ("adj_close", pa.float64()),
            ("volume", pa.int64()),
            ("asset_class", pa.string()),
            ("symbol", pa.string()),
        ])
        old_table = pa.Table.from_pylist(
            [
                {
                    "trade_date": date(2025, 1, 2),
                    "symbol_id": _symbol_id("VXHYG"),
                    "open": 10.0, "high": 11.0, "low": 9.0,
                    "close": 10.5, "adj_close": 10.5, "volume": 0,
                    "asset_class": "volatility", "symbol": "VXHYG",
                },
            ],
            schema=old_schema,
        )
        bronze_dir = tmp_path / "data-lake" / "bronze" / "asset_class=volatility" / "symbol=VXHYG"
        bronze_dir.mkdir(parents=True)
        pq.write_table(old_table, bronze_dir / "data.parquet")

        # Now merge new data using the correct schema
        new_bars = [
            {"date": "2025-01-03", "open": "10.5", "high": "12.0", "low": "10.0", "close": "11.0", "volume": "0.0"},
        ]
        new_table = bars_to_table("VXHYG", new_bars)
        path = write_bronze_parquet(new_table, "VXHYG", tmp_path)

        result = _read_single_parquet(path)
        assert result.num_rows == 2
        assert set(result.column_names) == {
            "trade_date", "symbol_id", "open", "high", "low",
            "close", "adj_close", "volume",
        }
        assert "asset_class" not in result.column_names
        assert "symbol" not in result.column_names

    def test_rewrites_stale_schema_even_without_new_rows(self, tmp_path):
        """Existing parquet with stale schema is rewritten even when no new data."""
        old_schema = pa.schema([
            ("trade_date", pa.date32()),
            ("symbol_id", pa.int64()),
            ("open", pa.float64()),
            ("high", pa.float64()),
            ("low", pa.float64()),
            ("close", pa.float64()),
            ("adj_close", pa.float64()),
            ("volume", pa.int64()),
            ("asset_class", pa.string()),
            ("symbol", pa.string()),
        ])
        old_table = pa.Table.from_pylist(
            [
                {
                    "trade_date": date(2025, 1, 2),
                    "symbol_id": _symbol_id("VXHYG"),
                    "open": 10.0, "high": 11.0, "low": 9.0,
                    "close": 10.5, "adj_close": 10.5, "volume": 0,
                    "asset_class": "volatility", "symbol": "VXHYG",
                },
            ],
            schema=old_schema,
        )
        bronze_dir = tmp_path / "data-lake" / "bronze" / "asset_class=volatility" / "symbol=VXHYG"
        bronze_dir.mkdir(parents=True)
        pq.write_table(old_table, bronze_dir / "data.parquet")

        # Merge with same date (no new rows) — should still rewrite to fix schema
        same_bars = [
            {"date": "2025-01-02", "open": "10.0", "high": "11.0", "low": "9.0", "close": "10.5", "volume": "0.0"},
        ]
        same_table = bars_to_table("VXHYG", same_bars)
        path = write_bronze_parquet(same_table, "VXHYG", tmp_path)

        result = _read_single_parquet(path)
        assert result.num_rows == 1
        assert "asset_class" not in result.column_names
        assert "symbol" not in result.column_names

    def test_no_new_rows_returns_early(self, tmp_path):
        """Returns early without rewriting when no new rows and schema is fine."""
        bars = [
            {"date": "2025-01-02", "open": "10.0", "high": "11.0", "low": "9.0", "close": "10.5", "volume": "0.0"},
        ]
        table = bars_to_table("VXHYG", bars)
        path = write_bronze_parquet(table, "VXHYG", tmp_path)
        mtime_before = path.stat().st_mtime

        # Write same data again — should return early
        same_table = bars_to_table("VXHYG", bars)
        path2 = write_bronze_parquet(same_table, "VXHYG", tmp_path)
        assert path2 == path
        assert path.stat().st_mtime == mtime_before

    def test_rewrites_legacy_symbol_id_without_new_rows(self, tmp_path):
        bars = [
            {"date": "2025-01-02", "open": "10", "high": "11", "low": "9",
             "close": "10.5", "volume": "100"},
        ]
        table = bars_to_table("VXHYG", bars)
        path = write_bronze_parquet(table, "VXHYG", tmp_path)
        existing = pq.ParquetFile(path).read().set_column(
            1,
            "symbol_id",
            pa.array([123], type=pa.int64()),
        )
        pq.write_table(existing, path)

        write_bronze_parquet(table, "VXHYG", tmp_path)

        rewritten = pq.ParquetFile(path).read()
        assert set(rewritten.column("symbol_id").to_pylist()) == {
            _symbol_id("VXHYG")
        }

    @pytest.mark.parametrize("failure", ["write", "fsync", "replace"])
    def test_atomic_publication_failure_preserves_predecessor_and_cleans_temp(
        self, tmp_path: Path, failure: str,
    ) -> None:
        initial = bars_to_table("VXHYG", [
            {"date": "2025-01-02", "open": "10", "high": "11", "low": "9", "close": "10", "volume": "0"},
        ])
        path = write_bronze_parquet(initial, "VXHYG", tmp_path)
        predecessor = path.read_bytes()
        successor = bars_to_table("VXHYG", [
            {"date": "2025-01-03", "open": "11", "high": "12", "low": "10", "close": "11", "volume": "0"},
        ])

        if failure == "write":
            def partial_write(table, destination, *args, **kwargs):
                Path(destination).write_bytes(b"PARTIAL")
                raise OSError("write failed")

            context = patch(
                "scripts.fetch_cboe_volatility.pq.write_table", side_effect=partial_write,
            )
        elif failure == "fsync":
            context = patch(
                "scripts.fetch_cboe_volatility.os.fsync", side_effect=OSError("fsync failed"),
            )
        else:
            context = patch(
                "scripts.fetch_cboe_volatility.os.replace", side_effect=OSError("replace failed"),
            )

        with context, pytest.raises(OSError, match=f"{failure} failed"):
            write_bronze_parquet(successor, "VXHYG", tmp_path)

        assert path.read_bytes() == predecessor
        assert not list(path.parent.glob(".data.parquet.*.tmp"))

    def test_atomic_publication_fsyncs_file_then_parent(self, tmp_path: Path) -> None:
        table = bars_to_table("VXHYG", [
            {"date": "2025-01-02", "open": "10", "high": "11", "low": "9", "close": "10", "volume": "0"},
        ])
        calls: list[int] = []
        real_fsync = os.fsync

        def record_fsync(fd: int) -> None:
            calls.append(fd)
            real_fsync(fd)

        with patch("scripts.fetch_cboe_volatility.os.fsync", side_effect=record_fsync):
            write_bronze_parquet(table, "VXHYG", tmp_path)

        assert len(calls) == 2


class TestMain:
    """Tests for the main() CLI entry point."""

    _SAMPLE_BARS = [
        {"date": "2025-01-02", "open": "10.0", "high": "11.0", "low": "9.0", "close": "10.5", "volume": "0.0"},
    ]

    def _mock_fetch(self, bars):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"data": bars}
        mock_resp.raise_for_status = MagicMock()
        return mock_resp

    def test_main_with_symbols_flag(self, tmp_path):
        """--symbols fetches specified symbols."""
        with (
            patch("sys.argv", ["prog", "--symbols", "VIX", "--warehouse", str(tmp_path)]),
            patch("scripts.fetch_cboe_volatility.httpx.get", return_value=self._mock_fetch(self._SAMPLE_BARS)),
        ):
            assert main() == 0

        parquet = tmp_path / "data-lake" / "bronze" / "asset_class=volatility" / "symbol=VIX" / "data.parquet"
        assert parquet.exists()

    def test_main_with_preset_flag(self, tmp_path):
        """--preset loads symbols from preset file."""
        preset = tmp_path / "test.json"
        preset.write_text('{"tickers": ["RVX"]}')

        with (
            patch("sys.argv", ["prog", "--preset", str(preset), "--warehouse", str(tmp_path)]),
            patch("scripts.fetch_cboe_volatility.httpx.get", return_value=self._mock_fetch(self._SAMPLE_BARS)),
        ):
            main()

        parquet = tmp_path / "data-lake" / "bronze" / "asset_class=volatility" / "symbol=RVX" / "data.parquet"
        assert parquet.exists()

    def test_main_default_preset(self, tmp_path):
        """Falls back to default preset when it exists."""
        with (
            patch("sys.argv", ["prog", "--warehouse", str(tmp_path)]),
            patch("scripts.fetch_cboe_volatility.DEFAULT_PRESET", tmp_path / "vol.json"),
            patch("scripts.fetch_cboe_volatility.httpx.get", return_value=self._mock_fetch(self._SAMPLE_BARS)),
        ):
            (tmp_path / "vol.json").write_text('{"tickers": ["VVIX"]}')
            main()

        parquet = tmp_path / "data-lake" / "bronze" / "asset_class=volatility" / "symbol=VVIX" / "data.parquet"
        assert parquet.exists()

    def test_main_fallback_symbols(self, tmp_path):
        """Falls back to VIX, VVIX when no preset exists."""
        with (
            patch("sys.argv", ["prog", "--warehouse", str(tmp_path)]),
            patch("scripts.fetch_cboe_volatility.DEFAULT_PRESET", tmp_path / "nonexistent.json"),
            patch("scripts.fetch_cboe_volatility.httpx.get", return_value=self._mock_fetch(self._SAMPLE_BARS)),
        ):
            main()

        assert (tmp_path / "data-lake" / "bronze" / "asset_class=volatility" / "symbol=VIX" / "data.parquet").exists()
        assert (tmp_path / "data-lake" / "bronze" / "asset_class=volatility" / "symbol=VVIX" / "data.parquet").exists()

    def test_main_handles_empty_data(self, tmp_path):
        """Symbols with no data are skipped gracefully."""
        empty_resp = MagicMock()
        empty_resp.json.return_value = {"data": []}
        empty_resp.raise_for_status = MagicMock()

        with (
            patch("sys.argv", ["prog", "--symbols", "MISSING", "--warehouse", str(tmp_path)]),
            patch("scripts.fetch_cboe_volatility.httpx.get", return_value=empty_resp),
        ):
            assert main() == 1

        parquet = tmp_path / "data-lake" / "bronze" / "asset_class=volatility" / "symbol=MISSING" / "data.parquet"
        assert not parquet.exists()

    def test_main_handles_fetch_error(self, tmp_path):
        """Fetch exceptions are caught and logged, not raised."""
        with (
            patch("sys.argv", ["prog", "--symbols", "BAD", "--warehouse", str(tmp_path)]),
            patch("scripts.fetch_cboe_volatility.httpx.get", side_effect=Exception("network error")),
        ):
            assert main() == 1
