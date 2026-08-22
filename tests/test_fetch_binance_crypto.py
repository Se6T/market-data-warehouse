"""Tests for Binance major-crypto daily bronze ingestion."""
from __future__ import annotations

import json
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from clients.bronze_client import BronzeClient
from scripts import fetch_binance_crypto as bn


def test_major_crypto_mapping_is_complete() -> None:
    assert bn.SYMBOL_TO_BINANCE == {
        "BTC": "BTCUSDT",
        "ETH": "ETHUSDT",
        "SOL": "SOLUSDT",
        "BNB": "BNBUSDT",
        "XRP": "XRPUSDT",
        "ADA": "ADAUSDT",
        "DOGE": "DOGEUSDT",
    }


def test_klines_to_rows_uses_true_ohlcv_and_excludes_after_end() -> None:
    rows = bn.klines_to_rows(
        [
            [1735776000000, "1", "3", "0.5", "2", "10.7"],
            [1735862400000, "2", "4", "1", "3", "20.2"],
        ],
        end=date(2025, 1, 2),
    )
    assert rows == [
        {
            "trade_date": "2025-01-02",
            "open": 1.0,
            "high": 3.0,
            "low": 0.5,
            "close": 2.0,
            "adj_close": 2.0,
            "volume": 11,
        }
    ]


def test_klines_to_rows_ignores_malformed_payload_items() -> None:
    assert bn.klines_to_rows([[1735776000000, "1"]], end=date(2025, 1, 2)) == []


def test_fetch_klines_requests_completed_daily_window() -> None:
    response = MagicMock()
    response.raise_for_status = MagicMock()
    response.json.return_value = [[1735776000000, "1", "2", "0.5", "1.5", "10"]]
    with patch("scripts.fetch_binance_crypto.httpx.get", return_value=response) as request:
        payload = bn.fetch_klines("BTCUSDT", end=date(2025, 1, 2), limit=1000)
    assert len(payload) == 1
    assert request.call_args.kwargs["params"] == {
        "symbol": "BTCUSDT",
        "interval": "1d",
        "limit": 1000,
        "endTime": 1735862399999,
    }


def test_write_rows_uses_crypto_bronze(tmp_path) -> None:
    inserted = bn.write_rows(
        "BTC",
        [{"trade_date": "2025-01-02", "open": 1, "high": 2, "low": 0.5, "close": 1.5, "adj_close": 1.5, "volume": 10}],
        tmp_path,
    )
    assert inserted == 1
    root = tmp_path / "data-lake" / "bronze" / "asset_class=crypto"
    with BronzeClient(root, asset_class="crypto") as client:
        assert client.get_existing_symbols() == {"BTC"}
        assert client.get_latest_dates() == {"BTC": "2025-01-02"}


def test_preset_contains_every_major_crypto(tmp_path) -> None:
    path = tmp_path / "major.json"
    path.write_text(json.dumps({"symbols": list(bn.SYMBOL_TO_BINANCE)}))
    assert bn.load_symbols(path) == list(bn.SYMBOL_TO_BINANCE)


@pytest.mark.parametrize(
    "payload",
    [
        {"symbols": []},
        {"symbols": ["btc", "NOT-A-MAJOR"]},
    ],
)
def test_load_symbols_rejects_empty_or_unknown_presets(tmp_path, payload) -> None:
    path = tmp_path / "invalid.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="invalid major-crypto preset"):
        bn.load_symbols(path)


@pytest.mark.parametrize("limit", [252, 1001])
def test_main_rejects_limits_outside_completed_momentum_window(monkeypatch, limit) -> None:
    monkeypatch.setattr(
        "sys.argv",
        ["fetch_binance_crypto.py", "--end", "2025-01-02", "--limit", str(limit)],
    )
    with pytest.raises(ValueError, match="252-day momentum lookback"):
        bn.main()


def test_main_rejects_unknown_cli_symbols_before_network(monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "fetch_binance_crypto.py",
            "--end",
            "2025-01-02",
            "--symbols",
            "BTC",
            "unknown",
        ],
    )
    with (
        patch("scripts.fetch_binance_crypto.fetch_klines") as fetch,
        pytest.raises(ValueError, match="unknown major crypto symbols"),
    ):
        bn.main()
    fetch.assert_not_called()


def test_main_rejects_preset_symbol_with_insufficient_completed_history(
    tmp_path, monkeypatch
) -> None:
    preset = tmp_path / "major.json"
    preset.write_text(json.dumps({"symbols": ["eth"]}), encoding="utf-8")
    monkeypatch.setattr(
        "sys.argv",
        [
            "fetch_binance_crypto.py",
            "--end",
            "2025-01-02",
            "--preset",
            str(preset),
        ],
    )
    with (
        patch("scripts.fetch_binance_crypto.fetch_klines", return_value=[]),
        pytest.raises(RuntimeError, match="ETH: only 0 completed daily rows"),
    ):
        bn.main()


def test_main_fetches_writes_and_reports_completed_symbol(monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "fetch_binance_crypto.py",
            "--end",
            "2025-01-02",
            "--symbols",
            "btc",
            "--warehouse",
            "/tmp/warehouse",
            "--base-url",
            "https://mirror.example/api/v3/",
        ],
    )
    rows = [
        {
            "trade_date": f"2024-01-{(index % 28) + 1:02d}",
            "open": 1,
            "high": 2,
            "low": 0.5,
            "close": 1.5,
            "adj_close": 1.5,
            "volume": 10,
        }
        for index in range(253)
    ]
    rows[0]["trade_date"] = "2024-01-01"
    rows[-1]["trade_date"] = "2025-01-02"
    with (
        patch("scripts.fetch_binance_crypto.fetch_klines", return_value=[["payload"]]) as fetch,
        patch("scripts.fetch_binance_crypto.klines_to_rows", return_value=rows) as transform,
        patch("scripts.fetch_binance_crypto.write_rows", return_value=253) as write,
        patch("scripts.fetch_binance_crypto.console.print") as report,
    ):
        bn.main()

    fetch.assert_called_once_with(
        "BTCUSDT",
        end=date(2025, 1, 2),
        limit=1000,
        base_url="https://mirror.example/api/v3/",
    )
    transform.assert_called_once_with([["payload"]], end=date(2025, 1, 2))
    write.assert_called_once_with("BTC", rows, bn.Path("/tmp/warehouse"))
    report.assert_called_once_with(
        "BTC: rows=253 inserted=253 range=2024-01-01..2025-01-02"
    )


def test_result_artifact_continues_after_independent_symbol_failure(
    tmp_path, monkeypatch
) -> None:
    result_path = tmp_path / "result.json"
    monkeypatch.setattr("sys.argv", [
        "fetch_binance_crypto.py", "--end", "2025-01-02", "--symbols", "BTC", "ETH",
        "--warehouse", str(tmp_path), "--result-json", str(result_path),
    ])
    rows = [
        {"trade_date": "2025-01-02", "open": 1, "high": 2, "low": 0.5,
         "close": 1.5, "adj_close": 1.5, "volume": 10}
    ] * 253
    with (
        patch("scripts.fetch_binance_crypto.fetch_klines", side_effect=[RuntimeError("SECRET"), [["ok"]]]) as fetch,
        patch("scripts.fetch_binance_crypto.klines_to_rows", return_value=rows),
        patch("scripts.fetch_binance_crypto.write_rows", return_value=1) as write,
    ):
        assert bn.main() == 1

    assert fetch.call_count == 2
    write.assert_called_once()
    assert json.loads(result_path.read_text()) == {
        "schema_version": 1,
        "asset_class": "crypto",
        "requested_symbols": ["BTC", "ETH"],
        "results": [
            {"symbol": "BTC", "status": "failed"},
            {"symbol": "ETH", "status": "succeeded"},
        ],
    }
