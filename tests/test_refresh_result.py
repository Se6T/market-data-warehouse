"""Tests for bounded atomic owner refresh result artifacts."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts import _refresh_result as result_writer
from scripts._refresh_result import write_result


def test_write_result_emits_exact_sanitized_schema_atomically(tmp_path) -> None:
    path = tmp_path / "result.json"
    write_result(path, "crypto", ["BTC", "ETH"], {"BTC": "failed", "ETH": "succeeded"})
    assert json.loads(path.read_text()) == {
        "schema_version": 1,
        "asset_class": "crypto",
        "requested_symbols": ["BTC", "ETH"],
        "results": [
            {"symbol": "BTC", "status": "failed"},
            {"symbol": "ETH", "status": "succeeded"},
        ],
    }
    assert not list(tmp_path.glob(".result.json.*.tmp"))


@pytest.mark.parametrize(
    ("symbols", "statuses"),
    [([], {}), (["BTC", "BTC"], {"BTC": "succeeded"}), (["BTC"], {}), (["BTC"], {"BTC": "SECRET"})],
)
def test_write_result_rejects_invalid_identity_or_status_sets(tmp_path, symbols, statuses) -> None:
    with pytest.raises(ValueError):
        write_result(tmp_path / "result.json", "crypto", symbols, statuses)


def test_write_result_rejects_unsafe_destination_and_oversized_payload(
    tmp_path, monkeypatch,
) -> None:
    relative = Path(tmp_path.name) / "result.json"
    with pytest.raises(ValueError, match="absolute non-symlink"):
        write_result(relative, "crypto", ["BTC"], {"BTC": "succeeded"})

    monkeypatch.setattr(result_writer, "MAX_RESULT_BYTES", 1)
    with pytest.raises(ValueError, match="size bound"):
        write_result(
            tmp_path / "result.json", "crypto", ["BTC"], {"BTC": "succeeded"},
        )