"""Tests for scripts/fetch_coingecko_crypto.py."""

from __future__ import annotations

import json
from datetime import date
from unittest.mock import MagicMock, patch

import httpx
import pytest

from clients.bronze_client import BronzeClient
from scripts import fetch_coingecko_crypto as cg


class TestCoinGeckoHelpers:
    def test_parse_date_and_timestamp_boundaries(self):
        parsed = cg.parse_date("2025-01-02")
        assert parsed == date(2025, 1, 2)
        assert cg.date_to_unix_seconds(parsed) == 1735776000
        assert cg.date_to_unix_seconds(parsed, end_of_day=True) == 1735862399

    def test_normalize_symbol(self):
        assert cg.normalize_symbol(" btc/usd ") == "BTC_USD"
        assert cg.normalize_symbol("eth-usd") == "ETH_USD"

    def test_parse_coin_spec_with_symbol(self):
        assert cg.parse_coin_spec("bitcoin:BTC") == cg.CoinRequest("bitcoin", "BTC")

    def test_parse_coin_spec_without_symbol(self):
        assert cg.parse_coin_spec("bitcoin") == cg.CoinRequest("bitcoin", "BITCOIN")

    def test_parse_coin_spec_rejects_empty(self):
        with pytest.raises(ValueError, match="empty coin spec"):
            cg.parse_coin_spec("  ")

    def test_requests_from_symbols(self):
        assert cg.requests_from_symbols(["btc", "ETH"]) == [
            cg.CoinRequest("bitcoin", "BTC"),
            cg.CoinRequest("ethereum", "ETH"),
        ]

    def test_requests_from_symbols_unknown_requires_coin_spec(self):
        with pytest.raises(ValueError, match="use --coins"):
            cg.requests_from_symbols(["NOTREAL"])

    def test_load_preset_with_string_coins(self, tmp_path):
        preset = tmp_path / "crypto.json"
        preset.write_text(json.dumps({"coins": ["bitcoin:BTC", "ethereum:ETH"]}))
        assert cg.load_preset(preset) == [
            cg.CoinRequest("bitcoin", "BTC"),
            cg.CoinRequest("ethereum", "ETH"),
        ]

    def test_load_preset_with_dict_coins(self, tmp_path):
        preset = tmp_path / "crypto.json"
        preset.write_text(json.dumps({"coins": [{"id": "bitcoin", "symbol": "btc"}]}))
        assert cg.load_preset(preset) == [cg.CoinRequest("bitcoin", "BTC")]

    def test_load_preset_with_symbols(self, tmp_path):
        preset = tmp_path / "crypto.json"
        preset.write_text(json.dumps({"symbols": ["BTC"]}))
        assert cg.load_preset(preset) == [cg.CoinRequest("bitcoin", "BTC")]

    def test_load_preset_with_tickers(self, tmp_path):
        preset = tmp_path / "crypto.json"
        preset.write_text(json.dumps({"tickers": ["ETH"]}))
        assert cg.load_preset(preset) == [cg.CoinRequest("ethereum", "ETH")]

    def test_build_headers(self):
        assert cg.build_headers(None, "demo") == {"accept": "application/json"}
        assert cg.build_headers("demo-key", "demo") == {
            "accept": "application/json",
            "x-cg-demo-api-key": "demo-key",
        }
        assert cg.build_headers("pro-key", "pro") == {
            "accept": "application/json",
            "x-cg-pro-api-key": "pro-key",
        }


class TestCoinGeckoFetch:
    def test_coingecko_get(self):
        response = MagicMock()
        response.json.return_value = {"ok": True}
        response.raise_for_status = MagicMock()
        with patch("scripts.fetch_coingecko_crypto.httpx.get", return_value=response) as mock_get:
            result = cg.coingecko_get(
                "/test",
                params={"a": "b"},
                api_key="key",
                api_tier="demo",
                base_url="https://example.test/api",
                timeout=5,
            )
        assert result == {"ok": True}
        mock_get.assert_called_once()
        assert mock_get.call_args.args[0] == "https://example.test/api/test"
        assert mock_get.call_args.kwargs["params"] == {"a": "b"}
        assert mock_get.call_args.kwargs["headers"]["x-cg-demo-api-key"] == "key"

    def test_coingecko_get_retries_rate_limit(self):
        limited = MagicMock(status_code=429, headers={"Retry-After": "0"})
        limited.raise_for_status.side_effect = httpx.HTTPStatusError(
            "rate limited",
            request=httpx.Request("GET", "https://example.test/api/test"),
            response=httpx.Response(429),
        )
        success = MagicMock(status_code=200, headers={})
        success.raise_for_status = MagicMock()
        success.json.return_value = {"ok": True}
        with (
            patch("scripts.fetch_coingecko_crypto.httpx.get", side_effect=[limited, success]) as mock_get,
            patch("scripts.fetch_coingecko_crypto.time.sleep") as mock_sleep,
        ):
            result = cg.coingecko_get(
                "/test",
                params={},
                api_key=None,
                api_tier="demo",
                base_url="https://example.test/api",
            )
        assert result == {"ok": True}
        assert mock_get.call_count == 2
        mock_sleep.assert_called_once()

    def test_coingecko_get_propagates_non_rate_limit_error(self):
        response = MagicMock(status_code=500, headers={})
        error = httpx.HTTPStatusError(
            "server error",
            request=httpx.Request("GET", "https://example.test/api/test"),
            response=httpx.Response(500),
        )
        response.raise_for_status.side_effect = error
        with patch("scripts.fetch_coingecko_crypto.httpx.get", return_value=response):
            with pytest.raises(httpx.HTTPStatusError):
                cg.coingecko_get(
                    "/test", params={}, api_key=None, api_tier="demo"
                )

    def test_fetch_ohlc_range(self):
        with patch("scripts.fetch_coingecko_crypto.coingecko_get", return_value=[[1, 2, 3, 4, 5]]) as mock_get:
            result = cg.fetch_ohlc_range(
                "bitcoin",
                vs_currency="usd",
                start=date(2025, 1, 1),
                end=date(2025, 1, 2),
                frequency="daily",
                api_key=None,
                api_tier="demo",
                base_url="https://example.test",
            )
        assert result == [[1, 2, 3, 4, 5]]
        assert mock_get.call_args.args[0] == "/coins/bitcoin/ohlc/range"
        assert mock_get.call_args.kwargs["params"]["interval"] == "daily"

    def test_fetch_market_chart_range(self):
        with patch("scripts.fetch_coingecko_crypto.coingecko_get", return_value={"total_volumes": []}) as mock_get:
            result = cg.fetch_market_chart_range(
                "bitcoin",
                vs_currency="usd",
                start=date(2025, 1, 1),
                end=date(2025, 1, 2),
                api_key=None,
                api_tier="demo",
                base_url="https://example.test",
            )
        assert result == {"total_volumes": []}
        assert mock_get.call_args.args[0] == "/coins/bitcoin/market_chart/range"

    def test_fetch_market_chart_daily(self):
        with patch(
            "scripts.fetch_coingecko_crypto.coingecko_get",
            return_value={"prices": []},
        ) as mock_get:
            result = cg.fetch_market_chart_daily(
                "bitcoin", vs_currency="usd", api_key=None, api_tier="demo"
            )
        assert result == {"prices": []}
        assert mock_get.call_args.args[0] == "/coins/bitcoin/market_chart"

    def test_ohlcv_to_rows_joins_latest_volume_by_date(self):
        ohlc = [
            [1735776000000, 100.0, 110.0, 90.0, 105.0],
            [1735862400000, 105.0, 115.0, 95.0, 108.0],
            [1735948800000, 1.0],  # malformed row ignored
        ]
        market_chart = {
            "total_volumes": [
                [1735776000000, 1000.4],
                [1735777000000, 2000.6],
            ]
        }
        rows = cg.ohlcv_to_rows(ohlc, market_chart)
        assert rows == [
            {
                "trade_date": "2025-01-02",
                "open": 100.0,
                "high": 110.0,
                "low": 90.0,
                "close": 105.0,
                "adj_close": 105.0,
                "volume": 2001,
            },
            {
                "trade_date": "2025-01-03",
                "open": 105.0,
                "high": 115.0,
                "low": 95.0,
                "close": 108.0,
                "adj_close": 108.0,
                "volume": 0,
            },
        ]

    def test_fetch_coin_rows_combines_endpoints(self):
        with (
            patch("scripts.fetch_coingecko_crypto.fetch_ohlc_range", return_value=[[1735776000000, 1, 2, 0.5, 1.5]]) as mock_ohlc,
            patch("scripts.fetch_coingecko_crypto.fetch_market_chart_range", return_value={"total_volumes": [[1735776000000, 10]]}) as mock_chart,
        ):
            rows = cg.fetch_coin_rows(
                cg.CoinRequest("bitcoin", "BTC"),
                vs_currency="usd",
                start=date(2025, 1, 1),
                end=date(2025, 1, 2),
                frequency="daily",
                api_key="key",
                api_tier="demo",
                base_url="https://example.test",
            )
        assert rows[0]["trade_date"] == "2025-01-02"
        mock_ohlc.assert_called_once()
        mock_chart.assert_called_once()

    def test_fetch_coin_rows_falls_back_to_free_daily_market_chart(self):
        chart = {
            "prices": [
                [1735689600000, 100.0],
                [1735776000000, 105.0],
                [1735862400000, 108.0],
            ],
            "total_volumes": [
                [1735689600000, 900.0],
                [1735776000000, 1000.4],
                [1735862400000, 1200.6],
            ],
        }
        paid_error = httpx.HTTPStatusError(
            "paid endpoint unavailable",
            request=httpx.Request("GET", "https://example.test/ohlc/range"),
            response=httpx.Response(401),
        )
        with (
            patch("scripts.fetch_coingecko_crypto.fetch_ohlc_range", side_effect=paid_error),
            patch("scripts.fetch_coingecko_crypto.fetch_market_chart_daily", return_value=chart) as mock_chart,
        ):
            rows = cg.fetch_coin_rows(
                cg.CoinRequest("bitcoin", "BTC"),
                vs_currency="usd",
                start=date(2025, 1, 2),
                end=date(2025, 1, 3),
                frequency="daily",
                api_key=None,
                api_tier="demo",
                base_url="https://example.test",
            )
        assert rows == [
            {
                "trade_date": "2025-01-02",
                "open": 105.0,
                "high": 105.0,
                "low": 105.0,
                "close": 105.0,
                "adj_close": 105.0,
                "volume": 1000,
            },
            {
                "trade_date": "2025-01-03",
                "open": 108.0,
                "high": 108.0,
                "low": 108.0,
                "close": 108.0,
                "adj_close": 108.0,
                "volume": 1201,
            },
        ]
        mock_chart.assert_called_once()

    def test_fetch_coin_rows_propagates_unexpected_http_error(self):
        error = httpx.HTTPStatusError(
            "server error",
            request=httpx.Request("GET", "https://example.test/ohlc/range"),
            response=httpx.Response(500),
        )
        with patch(
            "scripts.fetch_coingecko_crypto.fetch_ohlc_range", side_effect=error
        ):
            with pytest.raises(httpx.HTTPStatusError):
                cg.fetch_coin_rows(
                    cg.CoinRequest("bitcoin", "BTC"),
                    vs_currency="usd",
                    start=date(2025, 1, 2),
                    end=date(2025, 1, 3),
                    frequency="daily",
                    api_key=None,
                    api_tier="demo",
                )


class TestCliAndStorage:
    def test_resolve_requests_precedence(self, tmp_path):
        parser = cg.build_parser()
        args = parser.parse_args(["--symbols", "BTC", "--start", "2025-01-01", "--end", "2025-01-02"])
        assert cg.resolve_requests(args) == [cg.CoinRequest("bitcoin", "BTC")]

        args = parser.parse_args(["--coins", "solana:SOL", "--start", "2025-01-01", "--end", "2025-01-02"])
        assert cg.resolve_requests(args) == [cg.CoinRequest("solana", "SOL")]

        preset = tmp_path / "preset.json"
        preset.write_text(json.dumps({"symbols": ["ETH"]}))
        args = parser.parse_args(["--preset", str(preset), "--start", "2025-01-01", "--end", "2025-01-02"])
        assert cg.resolve_requests(args) == [cg.CoinRequest("ethereum", "ETH")]

        args = parser.parse_args(["--start", "2025-01-01", "--end", "2025-01-02"])
        assert cg.resolve_requests(args) == [cg.CoinRequest("bitcoin", "BTC"), cg.CoinRequest("ethereum", "ETH")]

    def test_write_rows_uses_crypto_bronze_layout(self, tmp_path):
        inserted = cg.write_rows(
            "BTC",
            [{"trade_date": "2025-01-02", "open": 1, "high": 2, "low": 0.5, "close": 1.5, "adj_close": 1.5, "volume": 10}],
            tmp_path,
        )
        assert inserted == 1
        bronze_dir = tmp_path / "data-lake" / "bronze" / "asset_class=crypto"
        with BronzeClient(bronze_dir=bronze_dir, asset_class="crypto") as bronze:
            assert bronze.get_existing_symbols() == {"BTC"}
            assert bronze.read_symbol_rows("BTC")[0]["close"] == 1.5

    def test_main_dry_run_does_not_fetch(self, monkeypatch, tmp_path):
        monkeypatch.setattr(
            "sys.argv",
            [
                "fetch_coingecko_crypto.py",
                "--symbols",
                "BTC",
                "--start",
                "2025-01-01",
                "--end",
                "2025-01-02",
                "--warehouse",
                str(tmp_path),
                "--dry-run",
            ],
        )
        with patch("scripts.fetch_coingecko_crypto.fetch_coin_rows") as mock_fetch:
            cg.main()
        mock_fetch.assert_not_called()
        assert not (tmp_path / "data-lake").exists()

    def test_main_fetches_and_writes_rows(self, monkeypatch, tmp_path):
        monkeypatch.setenv("COINGECKO_API_KEY", "env-key")
        monkeypatch.setattr(
            "sys.argv",
            [
                "fetch_coingecko_crypto.py",
                "--coins",
                "bitcoin:BTC",
                "--start",
                "2025-01-01",
                "--end",
                "2025-01-02",
                "--warehouse",
                str(tmp_path),
            ],
        )
        rows = [{"trade_date": "2025-01-02", "open": 1, "high": 2, "low": 0.5, "close": 1.5, "adj_close": 1.5, "volume": 10}]
        with patch("scripts.fetch_coingecko_crypto.fetch_coin_rows", return_value=rows) as mock_fetch:
            cg.main()
        assert mock_fetch.call_args.kwargs["api_key"] == "env-key"
        assert (tmp_path / "data-lake" / "bronze" / "asset_class=crypto" / "symbol=BTC" / "data.parquet").exists()

    def test_main_skips_empty_rows(self, monkeypatch, tmp_path):
        monkeypatch.setattr(
            "sys.argv",
            [
                "fetch_coingecko_crypto.py",
                "--coins",
                "bitcoin:BTC",
                "--start",
                "2025-01-01",
                "--end",
                "2025-01-02",
                "--warehouse",
                str(tmp_path),
            ],
        )
        with patch("scripts.fetch_coingecko_crypto.fetch_coin_rows", return_value=[]):
            cg.main()
        assert not (tmp_path / "data-lake").exists()

    def test_main_rejects_inverted_date_range(self, monkeypatch):
        monkeypatch.setattr(
            "sys.argv",
            [
                "fetch_coingecko_crypto.py",
                "--start",
                "2025-01-03",
                "--end",
                "2025-01-02",
            ],
        )
        with pytest.raises(ValueError, match="--end"):
            cg.main()
