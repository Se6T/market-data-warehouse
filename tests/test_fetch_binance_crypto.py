"""Tests for Binance major-crypto daily bronze ingestion."""
from __future__ import annotations

import json
from datetime import date
from unittest.mock import MagicMock, patch

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
