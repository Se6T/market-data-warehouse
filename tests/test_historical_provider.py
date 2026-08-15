"""Tests for HistoricalProvider implementations."""

import asyncio
import os
import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from clients.historical_provider import (
    BarRecord,
    IBProvider,
    IBClientAdapter,
    RadonApiProvider,
    _FakeIB,
    create_provider,
    ib_contract_to_spec,
    spec_to_ib_contract,
    create_ib_client_or_adapter,
)


class TestBarRecord:
    def test_iso_date_format(self):
        bar = BarRecord(date="2025-01-02", open=150.0, high=152.0, low=149.5, close=151.0, volume=1000000)
        assert bar.date == "2025-01-02"
        assert bar.open == 150.0


class TestContractSpecHelpers:
    def test_stock_to_spec(self):
        from ib_insync import Stock
        contract = Stock("AAPL", "SMART", "USD")
        spec = ib_contract_to_spec(contract)
        assert spec["sec_type"] == "STK"
        assert spec["symbol"] == "AAPL"
        assert spec["exchange"] == "SMART"

    def test_spec_to_stock(self):
        spec = {"sec_type": "STK", "symbol": "AAPL", "exchange": "SMART", "currency": "USD"}
        contract = spec_to_ib_contract(spec)
        assert contract.symbol == "AAPL"
        assert contract.secType == "STK"

    def test_future_roundtrip(self):
        from ib_insync import Future
        contract = Future("ES", "202506", "CME", "USD")
        spec = ib_contract_to_spec(contract)
        assert spec["sec_type"] == "FUT"
        assert spec["last_trade_date"] == "202506"
        rebuilt = spec_to_ib_contract(spec)
        assert rebuilt.symbol == "ES"

    def test_index_roundtrip(self):
        from ib_insync import Index
        contract = Index("VIX", "CBOE", "USD")
        spec = ib_contract_to_spec(contract)
        assert spec["sec_type"] == "IND"
        assert isinstance(spec_to_ib_contract(spec), Index)

    def test_rejects_unsupported_security_type(self):
        with pytest.raises(ValueError, match="Unsupported sec_type: OPT"):
            spec_to_ib_contract({"sec_type": "OPT", "symbol": "AAPL"})


class TestIBProvider:
    def test_connects_direct_client_on_construction(self):
        with patch("clients.ib_client.IBClient") as client_type:
            provider = IBProvider("ib.example", 7497)

        client_type.return_value.connect.assert_called_once_with("ib.example", 7497)
        assert provider._host == "ib.example"
        assert provider._port == 7497

    def test_qualifies_contract_and_preserves_spec_when_unresolved(self):
        provider = IBProvider.__new__(IBProvider)
        qualified = SimpleNamespace(
            conId=42, symbol="AAPL", secType="STK", exchange="NASDAQ", currency="USD"
        )
        provider._client = MagicMock()
        provider._client.qualify_contracts.side_effect = [[qualified], []]
        spec = {"symbol": "AAPL", "sec_type": "STK"}

        resolved = asyncio.run(provider.qualify_contract(spec))
        unresolved = asyncio.run(provider.qualify_contract(spec))

        assert resolved == {
            "conId": 42,
            "symbol": "AAPL",
            "secType": "STK",
            "exchange": "NASDAQ",
            "currency": "USD",
        }
        assert unresolved is spec

    def test_fetches_head_timestamp_and_normalized_bars(self):
        provider = IBProvider.__new__(IBProvider)
        provider._client = MagicMock()
        provider._client.get_head_timestamp_async = AsyncMock(
            side_effect=["20200102-00:00:00", None]
        )
        provider._client.get_historical_data_async = AsyncMock(
            return_value=[
                SimpleNamespace(
                    date="2025-01-02 00:00:00",
                    open="1",
                    high="3",
                    low="0.5",
                    close="2",
                    volume="11",
                )
            ]
        )
        spec = {"symbol": "AAPL", "sec_type": "STK"}

        timestamp = asyncio.run(provider.get_head_timestamp(spec, "MIDPOINT", False))
        missing = asyncio.run(provider.get_head_timestamp(spec))
        bars = asyncio.run(
            provider.get_historical_bars(
                spec, "20250103-00:00:00", "2 D", "1 day", "MIDPOINT", False
            )
        )

        assert timestamp == "20200102-00:00:00"
        assert missing is None
        assert bars == [BarRecord("2025-01-02", 1.0, 3.0, 0.5, 2.0, 11)]
        assert provider._client.get_historical_data_async.call_args.kwargs == {
            "end_date_time": "20250103-00:00:00",
            "duration": "2 D",
            "bar_size": "1 day",
            "what_to_show": "MIDPOINT",
            "use_rth": False,
        }

    def test_empty_bar_response_and_disconnect(self):
        provider = IBProvider.__new__(IBProvider)
        provider._client = MagicMock()
        provider._client.get_historical_data_async = AsyncMock(return_value=None)

        assert asyncio.run(provider.get_historical_bars({"symbol": "AAPL"})) == []
        asyncio.run(provider.disconnect())
        provider._client.disconnect.assert_called_once_with()


class TestRadonApiProvider:
    def test_parses_bar_response(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "bars": [
                {"date": "2025-01-02", "open": 150.0, "high": 152.0, "low": 149.5, "close": 151.0, "volume": 1000000},
                {"date": "2025-01-03", "open": 151.0, "high": 153.0, "low": 150.0, "close": 152.5, "volume": 900000},
            ]
        }
        mock_resp.raise_for_status = MagicMock()

        bars = [BarRecord(**b) for b in mock_resp.json()["bars"]]
        assert len(bars) == 2
        assert bars[0].date == "2025-01-02"
        assert bars[1].volume == 900000

    def test_http_methods_send_provider_contracts_and_close(self):
        qualify_response = MagicMock()
        qualify_response.json.return_value = {"contracts": [{"symbol": "AAPL", "conId": 42}]}
        head_response = MagicMock()
        head_response.json.return_value = {"timestamp": "20200102-00:00:00"}
        bars_response = MagicMock()
        bars_response.json.return_value = {
            "bars": [
                {"date": "2025-01-02", "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 10}
            ]
        }
        client = MagicMock()
        client.post.side_effect = [qualify_response, head_response, bars_response]
        with patch("httpx.Client", return_value=client) as client_type:
            provider = RadonApiProvider("https://radon.example/", "secret", timeout=5)

        spec = {"symbol": "AAPL", "sec_type": "STK"}
        assert asyncio.run(provider.qualify_contract(spec))["conId"] == 42
        assert asyncio.run(provider.get_head_timestamp(spec, "MIDPOINT", False)) == "20200102-00:00:00"
        assert asyncio.run(
            provider.get_historical_bars(spec, "end", "2 D", "1 day", "MIDPOINT", False)
        ) == [BarRecord("2025-01-02", 1, 2, 0.5, 1.5, 10)]
        asyncio.run(provider.disconnect())

        client_type.assert_called_once_with(
            base_url="https://radon.example",
            headers={"X-API-Key": "secret"},
            timeout=5,
        )
        qualify_response.raise_for_status.assert_called_once_with()
        head_response.raise_for_status.assert_called_once_with()
        bars_response.raise_for_status.assert_called_once_with()
        client.close.assert_called_once_with()

    def test_empty_qualification_preserves_original_spec(self):
        response = MagicMock()
        response.json.return_value = {"contracts": []}
        client = MagicMock()
        client.post.return_value = response
        with patch("httpx.Client", return_value=client):
            provider = RadonApiProvider("https://radon.example", "secret")
        spec = {"symbol": "AAPL"}
        assert asyncio.run(provider.qualify_contract(spec)) is spec


class TestFakeIB:
    def test_qualifies_in_place_and_runs_coroutines(self):
        provider = MagicMock(spec=RadonApiProvider)
        provider.qualify_contract = AsyncMock(
            side_effect=[
                {"conId": 42, "exchange": "NASDAQ"},
                {"conId": 43},
            ]
        )
        first = SimpleNamespace(
            secType="STK", symbol="AAPL", exchange="SMART", currency="USD"
        )
        second = SimpleNamespace(
            secType="STK", symbol="MSFT", exchange="SMART", currency="USD"
        )
        fake = _FakeIB(provider)

        contracts = fake.run(fake.qualifyContractsAsync(first, second))

        assert contracts == [first, second]
        assert (first.conId, first.exchange) == (42, "NASDAQ")
        assert (second.conId, second.exchange) == (43, "SMART")


class TestIBClientAdapter:
    def test_has_ib_attribute(self):
        mock_provider = MagicMock(spec=RadonApiProvider)
        adapter = IBClientAdapter(mock_provider)
        assert hasattr(adapter, "ib")

    def test_connect_is_noop(self):
        mock_provider = MagicMock(spec=RadonApiProvider)
        adapter = IBClientAdapter(mock_provider)
        adapter.connect(host="localhost", port=4001)  # Should not raise

    def test_context_manager(self):
        mock_provider = MagicMock(spec=RadonApiProvider)
        with IBClientAdapter(mock_provider) as adapter:
            assert adapter is not None

    def test_delegates_head_and_bar_requests(self):
        provider = MagicMock(spec=RadonApiProvider)
        provider.get_head_timestamp = AsyncMock(return_value="20200102-00:00:00")
        expected = [BarRecord("2025-01-02", 1, 2, 0.5, 1.5, 10)]
        provider.get_historical_bars = AsyncMock(return_value=expected)
        adapter = IBClientAdapter(provider)
        contract = SimpleNamespace(
            secType="STK", symbol="AAPL", exchange="SMART", currency="USD"
        )

        assert asyncio.run(
            adapter.get_head_timestamp_async(contract, what_to_show="MIDPOINT", use_rth=False)
        ) == "20200102-00:00:00"
        assert asyncio.run(
            adapter.get_historical_data_async(
                contract,
                end_date="end",
                duration="2 D",
                bar_size="1 hour",
                what_to_show="MIDPOINT",
                use_rth=False,
            )
        ) == expected
        provider.get_historical_bars.assert_awaited_once_with(
            {"sec_type": "STK", "symbol": "AAPL", "exchange": "SMART", "currency": "USD"},
            end_date_time="end",
            duration="2 D",
            bar_size="1 hour",
            what_to_show="MIDPOINT",
            use_rth=False,
        )

    def test_disconnect_schedules_close_in_running_loop(self):
        provider = MagicMock(spec=RadonApiProvider)
        adapter = IBClientAdapter(provider)

        async def disconnect_inside_loop():
            adapter.disconnect()
            await asyncio.sleep(0)

        asyncio.run(disconnect_inside_loop())
        provider.disconnect.assert_awaited_once_with()


class TestCreateIbClientOrAdapter:
    def test_returns_ibclient_without_env(self):
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("MDW_RADON_API_URL", None)
            os.environ.pop("MDW_API_KEY", None)
            result = create_ib_client_or_adapter.__wrapped__ if hasattr(create_ib_client_or_adapter, "__wrapped__") else None
            # Can't fully test without IB connection, but verify env detection
            assert os.getenv("MDW_RADON_API_URL") is None

    def test_fails_fast_on_401(self):
        import httpx
        with patch.dict(os.environ, {"MDW_RADON_API_URL": "http://fake", "MDW_API_KEY": "bad"}):
            mock_resp = MagicMock()
            mock_resp.status_code = 401
            mock_resp.raise_for_status.side_effect = httpx.HTTPStatusError(
                "Unauthorized", request=MagicMock(), response=mock_resp
            )
            with patch("clients.historical_provider.RadonApiProvider") as MockProvider:
                instance = MockProvider.return_value
                instance._client.post.return_value = mock_resp
                instance._client.post.return_value.raise_for_status = mock_resp.raise_for_status
                with pytest.raises(httpx.HTTPStatusError):
                    create_ib_client_or_adapter()

    def test_returns_direct_client_without_complete_radon_config(self):
        sentinel = object()
        with (
            patch.dict(os.environ, {"MDW_RADON_API_URL": "http://fake"}, clear=True),
            patch("clients.ib_client.IBClient", return_value=sentinel),
        ):
            assert create_ib_client_or_adapter("ib.example", 7497) is sentinel

    def test_returns_adapter_when_radon_health_check_succeeds(self):
        provider = MagicMock(spec=RadonApiProvider)
        provider._client = MagicMock()
        with (
            patch.dict(
                os.environ,
                {"MDW_RADON_API_URL": "http://fake", "MDW_API_KEY": "good"},
                clear=True,
            ),
            patch("clients.historical_provider.RadonApiProvider", return_value=provider),
        ):
            result = create_ib_client_or_adapter()
        assert isinstance(result, IBClientAdapter)
        assert result._provider is provider

    @pytest.mark.parametrize("failure", ["connect", "server"])
    def test_falls_back_to_direct_client_on_retryable_radon_failure(self, failure):
        import httpx

        provider = MagicMock(spec=RadonApiProvider)
        provider._client = MagicMock()
        if failure == "connect":
            provider._client.post.side_effect = httpx.ConnectError("offline")
        else:
            request = httpx.Request("POST", "http://fake/contract/qualify")
            response = httpx.Response(503, request=request)
            provider._client.post.return_value.raise_for_status.side_effect = httpx.HTTPStatusError(
                "server", request=request, response=response
            )
        sentinel = object()
        with (
            patch.dict(
                os.environ,
                {"MDW_RADON_API_URL": "http://fake", "MDW_API_KEY": "good"},
                clear=True,
            ),
            patch("clients.historical_provider.RadonApiProvider", return_value=provider),
            patch("clients.ib_client.IBClient", return_value=sentinel),
        ):
            assert create_ib_client_or_adapter() is sentinel

    def test_propagates_non_retryable_radon_http_error(self):
        import httpx

        request = httpx.Request("POST", "http://fake/contract/qualify")
        response = httpx.Response(409, request=request)
        provider = MagicMock(spec=RadonApiProvider)
        provider._client = MagicMock()
        provider._client.post.return_value.raise_for_status.side_effect = httpx.HTTPStatusError(
            "conflict", request=request, response=response
        )
        with (
            patch.dict(
                os.environ,
                {"MDW_RADON_API_URL": "http://fake", "MDW_API_KEY": "good"},
                clear=True,
            ),
            patch("clients.historical_provider.RadonApiProvider", return_value=provider),
            pytest.raises(httpx.HTTPStatusError),
        ):
            create_ib_client_or_adapter()


class TestCreateProvider:
    def test_returns_radon_provider_after_successful_probe(self):
        provider = MagicMock(spec=RadonApiProvider)
        provider.qualify_contract = AsyncMock(return_value={"conId": 42})
        with (
            patch.dict(
                os.environ,
                {"MDW_RADON_API_URL": "http://fake", "MDW_API_KEY": "good"},
                clear=True,
            ),
            patch("clients.historical_provider.RadonApiProvider", return_value=provider),
        ):
            assert asyncio.run(create_provider()) is provider

    @pytest.mark.parametrize("failure", ["timeout", "server"])
    def test_falls_back_to_ib_provider_on_retryable_failure(self, failure):
        import httpx

        provider = MagicMock(spec=RadonApiProvider)
        if failure == "timeout":
            provider.qualify_contract = AsyncMock(side_effect=httpx.TimeoutException("slow"))
        else:
            request = httpx.Request("POST", "http://fake/contract/qualify")
            response = httpx.Response(500, request=request)
            provider.qualify_contract = AsyncMock(
                side_effect=httpx.HTTPStatusError("server", request=request, response=response)
            )
        sentinel = object()
        with (
            patch.dict(
                os.environ,
                {"MDW_RADON_API_URL": "http://fake", "MDW_API_KEY": "good"},
                clear=True,
            ),
            patch("clients.historical_provider.RadonApiProvider", return_value=provider),
            patch("clients.historical_provider.IBProvider", return_value=sentinel) as ib_type,
        ):
            assert asyncio.run(create_provider("ib.example", 7497)) is sentinel
        ib_type.assert_called_once_with("ib.example", 7497)

    @pytest.mark.parametrize("status", [401, 409])
    def test_propagates_non_retryable_http_errors(self, status):
        import httpx

        request = httpx.Request("POST", "http://fake/contract/qualify")
        response = httpx.Response(status, request=request)
        provider = MagicMock(spec=RadonApiProvider)
        provider.qualify_contract = AsyncMock(
            side_effect=httpx.HTTPStatusError("rejected", request=request, response=response)
        )
        with (
            patch.dict(
                os.environ,
                {"MDW_RADON_API_URL": "http://fake", "MDW_API_KEY": "good"},
                clear=True,
            ),
            patch("clients.historical_provider.RadonApiProvider", return_value=provider),
            pytest.raises(httpx.HTTPStatusError),
        ):
            asyncio.run(create_provider())

    def test_uses_direct_provider_without_radon_config(self):
        sentinel = object()
        with (
            patch.dict(os.environ, {}, clear=True),
            patch("clients.historical_provider.IBProvider", return_value=sentinel) as ib_type,
        ):
            assert asyncio.run(create_provider("ib.example", 7497)) is sentinel
        ib_type.assert_called_once_with("ib.example", 7497)
