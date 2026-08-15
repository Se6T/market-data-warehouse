"""Tests for IBClient.connect() — clientId 326 fallback behavior."""

from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, call, patch

import ib_insync
import pytest

from clients.ib_client import (
    IBClient,
    IBConnectionError,
    IBContractError,
    IBError,
    IBOrderError,
    IBTimeoutError,
)


# ── helpers ───────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _forbid_real_ib_connections(monkeypatch):
    """Fail closed if any test escapes the mocked IB boundary."""

    def refuse_connect(*args, **kwargs):
        raise AssertionError("tests must never connect to a real IB endpoint")

    monkeypatch.setattr(ib_insync.IB, "connect", refuse_connect)


def _make_client():
    """Return an IBClient wired to a MagicMock IB instance."""
    with patch("clients.ib_client.IB") as MockIB:
        mock_ib = MagicMock()
        MockIB.return_value = mock_ib
        client = IBClient()
    return client, mock_ib


# ══════════════════════════════════════════════════════════════════════
# connect — happy path
# ══════════════════════════════════════════════════════════════════════


class TestConnectSuccess:
    def test_connect_succeeds_on_first_try(self):
        client, mock_ib = _make_client()

        client.connect()

        mock_ib.connect.assert_called_once_with(
            "127.0.0.1", 4001, clientId=0, timeout=10
        )

    def test_connect_stores_last_client_id_on_success(self):
        client, mock_ib = _make_client()

        client.connect(client_id=5)

        assert client._last_client_id == 5


# ══════════════════════════════════════════════════════════════════════
# connect — clientId 326 fallback
# ══════════════════════════════════════════════════════════════════════


class TestConnectClientIdFallback:
    def test_retries_with_next_client_id_on_326(self):
        """Error 326 on clientId 0 → automatically retry with clientId 1."""
        client, mock_ib = _make_client()

        call_count = 0

        def connect_side_effect(host, port, clientId, timeout):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                client._last_error = (326, "client id already in use")
                raise TimeoutError()
            # Second call succeeds

        mock_ib.connect.side_effect = connect_side_effect

        client.connect()  # must not raise

        assert mock_ib.connect.call_count == 2
        calls = mock_ib.connect.call_args_list
        assert calls[0][1]["clientId"] == 0
        assert calls[1][1]["clientId"] == 1

    def test_updates_last_client_id_to_actual_connected_id(self):
        """_last_client_id reflects the clientId that actually connected."""
        client, mock_ib = _make_client()

        call_count = 0

        def connect_side_effect(host, port, clientId, timeout):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                client._last_error = (326, "client id already in use")
                raise TimeoutError()

        mock_ib.connect.side_effect = connect_side_effect

        client.connect()

        assert client._last_client_id == 1

    def test_raises_after_all_client_ids_exhausted(self):
        """If all 10 clientIds return 326, raise IBConnectionError."""
        client, mock_ib = _make_client()

        def connect_side_effect(host, port, clientId, timeout):
            client._last_error = (326, "client id already in use")
            raise TimeoutError()

        mock_ib.connect.side_effect = connect_side_effect

        with pytest.raises(IBConnectionError, match="all clientIds"):
            client.connect()

        assert mock_ib.connect.call_count == 10

    def test_logs_warning_on_326_retry(self, caplog):
        """A warning is emitted when falling back to the next clientId."""
        client, mock_ib = _make_client()

        call_count = 0

        def connect_side_effect(host, port, clientId, timeout):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                client._last_error = (326, "client id already in use")
                raise TimeoutError()

        mock_ib.connect.side_effect = connect_side_effect

        with caplog.at_level(logging.WARNING, logger="ib_client"):
            client.connect()

        messages = " ".join(r.message for r in caplog.records)
        assert "326" in messages or "already in use" in messages.lower()

    def test_non_326_error_does_not_retry_client_ids(self):
        """A plain TimeoutError (no 326) respects max_retries, not clientId retries."""
        client, mock_ib = _make_client()

        mock_ib.connect.side_effect = TimeoutError("gateway unreachable")

        with pytest.raises(IBConnectionError):
            client.connect()

        # max_retries=1 default: tried once, no clientId escalation
        assert mock_ib.connect.call_count == 1

    def test_stale_326_error_does_not_trigger_retry(self):
        """A _last_error=326 left over from a previous session must not cause retry."""
        client, mock_ib = _make_client()

        # Simulate stale state from a prior crashed session
        client._last_error = (326, "stale error from last run")

        # connect() itself succeeds immediately
        client.connect()

        # Should only have been called once — stale 326 was cleared before attempt
        mock_ib.connect.assert_called_once()

    def test_multiple_326s_before_success(self):
        """clientIds 0, 1, 2 all in use — succeeds on clientId 3."""
        client, mock_ib = _make_client()

        call_count = 0

        def connect_side_effect(host, port, clientId, timeout):
            nonlocal call_count
            call_count += 1
            if call_count < 4:
                client._last_error = (326, "client id already in use")
                raise TimeoutError()

        mock_ib.connect.side_effect = connect_side_effect

        client.connect()

        assert mock_ib.connect.call_count == 4
        assert mock_ib.connect.call_args_list[3][1]["clientId"] == 3
        assert client._last_client_id == 3


class TestConnectionLifecycle:
    def test_registry_lookup_unknown_name_and_retry_delay(self):
        client, mock_ib = _make_client()

        with pytest.raises(ValueError, match="Unknown client name 'missing'"):
            client.connect(client_name="missing")

        mock_ib.connect.side_effect = [TimeoutError("first"), None]
        with patch("clients.ib_client.time.sleep") as sleep:
            client.connect(
                host="gateway",
                port=7497,
                client_name="ib_orders",
                timeout=7,
                max_retries=2,
            )

        assert mock_ib.connect.call_args_list == [
            call("gateway", 7497, clientId=11, timeout=7),
            call("gateway", 7497, clientId=11, timeout=7),
        ]
        sleep.assert_called_once_with(1)
        assert client.ib is mock_ib

    def test_disconnect_reconnect_context_and_connection_guard(self):
        client, mock_ib = _make_client()
        mock_ib.isConnected.side_effect = [True, False, False, False]

        assert client.__enter__() is client
        client.disconnect()
        mock_ib.disconnect.assert_called_once_with()
        client.disconnect()
        client.__exit__(None, None, None)

        client._last_host = "gateway"
        client._last_port = 7497
        client._last_client_id = 8
        client._last_timeout = 4
        with patch.object(client, "disconnect") as disconnect, patch.object(
            client, "connect"
        ) as connect:
            client.reconnect()
        disconnect.assert_called_once_with()
        connect.assert_called_once_with(
            host="gateway", port=7497, client_id=8, timeout=4
        )

        mock_ib.isConnected.side_effect = None
        mock_ib.isConnected.return_value = False
        assert client.is_connected() is False
        with pytest.raises(IBConnectionError, match="Not connected"):
            client.get_positions()


class TestErrorHandling:
    @pytest.mark.parametrize(
        ("code", "level", "stored"),
        [(10358, logging.DEBUG, False), (2104, logging.INFO, False),
         (1100, logging.WARNING, False), (0, logging.ERROR, True)],
    )
    def test_error_codes_have_expected_severity_and_state(
        self, code, level, stored, caplog
    ):
        client, _ = _make_client()

        with caplog.at_level(level, logger="ib_client"):
            client._on_error(1, code, "message", object())

        assert caplog.records[-1].levelno == level
        assert (client._last_error is not None) is stored
        if stored:
            assert client._last_error == (0, "message")


def _connected_client():
    client, mock_ib = _make_client()
    mock_ib.isConnected.return_value = True
    return client, mock_ib


class TestPortfolioAndQueries:
    def test_portfolio_account_and_pnl_delegation(self):
        client, mock_ib = _connected_client()
        mock_ib.positions.return_value = ["position"]
        mock_ib.portfolio.return_value = ["portfolio"]
        mock_ib.accountSummary.return_value = ["summary"]
        pnl = object()
        mock_ib.reqPnL.return_value = pnl

        assert client.get_positions() == ["position"]
        assert client.get_portfolio("DU1") == ["portfolio"]
        assert client.get_account_summary("group", ["ignored"]) == ["summary"]
        assert client.get_pnl("DU1") is pnl
        client.cancel_pnl(None)
        client.cancel_pnl(pnl)

        mock_ib.portfolio.assert_called_once_with("DU1")
        mock_ib.accountSummary.assert_called_once_with(account="group")
        mock_ib.reqPnL.assert_called_once_with("DU1")
        mock_ib.sleep.assert_called_once_with(2)
        mock_ib.cancelPnL.assert_called_once_with(pnl)

    def test_open_order_trade_execution_and_fill_queries(self):
        client, mock_ib = _connected_client()
        mock_ib.openTrades.return_value = ["open"]
        mock_ib.trades.return_value = ["trade"]
        mock_ib.reqExecutions.side_effect = [["filtered"], ["all"]]
        mock_ib.fills.return_value = ["fill"]

        assert client.get_open_orders() == ["open"]
        assert client.get_open_trades() == ["open"]
        assert client.get_trades() == ["trade"]
        assert client.get_executions("filter") == ["filtered"]
        assert client.get_executions() == ["all"]
        assert client.get_fills() == ["fill"]

        mock_ib.reqAllOpenOrders.assert_called_once_with()
        mock_ib.sleep.assert_called_once_with(0.5)
        assert mock_ib.reqExecutions.call_args_list == [call("filter"), call()]


class TestOrderOperations:
    def test_place_cancel_bracket_and_modify_orders(self):
        client, mock_ib = _connected_client()
        contract = SimpleNamespace(symbol="AAPL")
        order = SimpleNamespace(
            action="BUY", totalQuantity=2, lmtPrice=100, orderId=4
        )
        trade = SimpleNamespace(order=SimpleNamespace(orderId=4))
        mock_ib.placeOrder.return_value = trade

        assert client.place_order(contract, order) is trade
        cancelled = object()
        mock_ib.cancelOrder.return_value = cancelled
        assert client.cancel_order(order) is cancelled

        bracket = [object(), object(), object()]
        mock_ib.bracketOrder.return_value = bracket
        mock_ib.placeOrder.side_effect = ["parent", "profit", "stop"]
        assert client.place_bracket_order(contract, "BUY", 2, 100, 110, 95) == [
            "parent", "profit", "stop"
        ]

        mock_ib.placeOrder.side_effect = None
        mock_ib.placeOrder.return_value = trade
        assert client.modify_order(
            contract,
            order,
            lmt_price=101,
            total_quantity=3,
            aux_price=99,
            tif="GTC",
        ) is trade
        assert (order.lmtPrice, order.totalQuantity, order.auxPrice, order.tif) == (
            101, 3, 99, "GTC"
        )

    @pytest.mark.parametrize(
        ("method", "message"),
        [("place", "Failed to place order"), ("cancel", "Failed to cancel order"),
         ("bracket", "Failed to place bracket order"),
         ("modify", "Failed to modify order")],
    )
    def test_order_failures_are_normalized(self, method, message):
        client, mock_ib = _connected_client()
        contract = "contract"
        order = SimpleNamespace(action="BUY", totalQuantity=1, orderId=1)
        if method == "bracket":
            mock_ib.bracketOrder.side_effect = RuntimeError("rejected")
            operation = lambda: client.place_bracket_order(
                contract, "BUY", 1, 1, 2, 0.5
            )
        elif method == "cancel":
            mock_ib.cancelOrder.side_effect = RuntimeError("rejected")
            operation = lambda: client.cancel_order(order)
        else:
            mock_ib.placeOrder.side_effect = RuntimeError("rejected")
            operation = (
                (lambda: client.place_order(contract, order))
                if method == "place"
                else (lambda: client.modify_order(contract, order))
            )

        with pytest.raises(IBOrderError, match=message):
            operation()

    def test_order_status_prefers_perm_id_then_order_id_and_handles_missing(self):
        client, mock_ib = _connected_client()
        first = SimpleNamespace(order=SimpleNamespace(permId=10, orderId=1))
        second = SimpleNamespace(order=SimpleNamespace(permId=20, orderId=2))
        mock_ib.trades.return_value = [first, second]

        assert client.get_order_status(order_id=1, perm_id=20) is second
        assert client.get_order_status(order_id=1) is first
        assert client.get_order_status(order_id=99, perm_id=99) is None


class TestMarketDataAndContracts:
    def test_market_data_and_contract_delegation(self):
        client, mock_ib = _connected_client()
        contract = object()
        ticker = object()
        mock_ib.reqMktData.return_value = ticker
        mock_ib.reqSecDefOptParams.return_value = ["chain"]
        mock_ib.qualifyContracts.return_value = ["qualified"]

        assert client.get_quote(contract) is ticker
        assert client.get_quote(contract, snapshot=True, generic_ticks="100") is ticker
        client.cancel_market_data(contract)
        client.set_market_data_type(3)
        assert client.get_option_chain("AAPL", "SMART") == ["chain"]
        assert client.qualify_contract(contract) == "qualified"
        assert client.qualify_contracts(contract, "other") == ["qualified"]

        assert mock_ib.reqMktData.call_args_list[:2] == [
            call(contract, "", False, False), call(contract, "100", True, False)
        ]
        mock_ib.sleep.assert_called_once_with(2)
        mock_ib.cancelMktData.assert_called_once_with(contract)
        mock_ib.reqMarketDataType.assert_called_once_with(3)
        mock_ib.reqSecDefOptParams.assert_called_once_with("AAPL", "SMART", "STK", 0)

    def test_option_price_qualifies_contract_before_requesting_quote(self):
        client, mock_ib = _connected_client()
        option = object()
        qualified = object()
        ticker = object()
        mock_ib.qualifyContracts.return_value = [qualified]
        mock_ib.reqMktData.return_value = ticker

        with patch("clients.ib_client.Option", return_value=option) as option_type:
            assert client.get_option_price("AAPL", "20261218", 200, "C") is ticker

        option_type.assert_called_once_with(
            symbol="AAPL", lastTradeDateOrContractMonth="20261218", strike=200,
            right="C", exchange="SMART", currency="USD"
        )
        mock_ib.qualifyContracts.assert_called_once_with(option)
        mock_ib.reqMktData.assert_called_once_with(qualified, "", False, False)
        mock_ib.sleep.assert_called_once_with(2)

    def test_unqualified_contracts_raise_descriptive_errors(self):
        client, mock_ib = _connected_client()
        mock_ib.qualifyContracts.return_value = []

        with patch("clients.ib_client.Option", return_value="option"):
            with pytest.raises(IBContractError, match="Could not qualify option"):
                client.get_option_price("AAPL", "20261218", 200, "C")
        with pytest.raises(IBContractError, match="Failed to qualify contract"):
            client.qualify_contract("contract")


class TestFillMonitoring:
    def _trade(self, statuses):
        status = SimpleNamespace(
            status=statuses[0], avgFillPrice=101.5, filled=2
        )
        trade = SimpleNamespace(order=SimpleNamespace(orderId=7), orderStatus=status)
        return trade, status

    def test_wait_for_fill_returns_filled_trade(self):
        client, mock_ib = _connected_client()
        trade, status = self._trade(["Submitted"])
        statuses = iter(["Submitted", "Filled"])
        mock_ib.sleep.side_effect = lambda _: setattr(status, "status", next(statuses))

        assert client.wait_for_fill(trade, timeout=3, poll_interval=1) is trade
        assert mock_ib.sleep.call_count == 2

    @pytest.mark.parametrize("cancelled", ["Cancelled", "ApiCancelled"])
    def test_wait_for_fill_raises_for_cancelled_trade(self, cancelled):
        client, mock_ib = _connected_client()
        trade, status = self._trade([cancelled])
        mock_ib.sleep.side_effect = lambda _: setattr(status, "status", cancelled)
        with pytest.raises(IBOrderError, match="Order cancelled"):
            client.wait_for_fill(trade)

    def test_wait_for_fill_logs_inactive_then_times_out(self, caplog):
        client, mock_ib = _connected_client()
        trade, status = self._trade(["Inactive"])
        mock_ib.sleep.side_effect = lambda _: setattr(status, "status", "Inactive")

        with caplog.at_level(logging.WARNING, logger="ib_client"):
            with pytest.raises(IBTimeoutError, match="not filled within 2s"):
                client.wait_for_fill(trade, timeout=2, poll_interval=1)
        assert "may be rejected" in caplog.text


class TestHistoricalFlexAndUtility:
    def test_historical_head_details_and_sleep_delegation(self):
        client, mock_ib = _connected_client()
        contract = object()
        mock_ib.reqHistoricalData.return_value = ["bar"]
        mock_ib.reqHeadTimeStamp.return_value = "timestamp"
        mock_ib.reqContractDetails.return_value = ["details"]

        assert client.get_historical_data(
            contract, "2 D", "1 day", "MIDPOINT", False, "end", True
        ) == ["bar"]
        assert client.get_head_timestamp(contract, "BID", False) == "timestamp"
        assert client.get_contract_details(contract) == ["details"]
        client.sleep(0.25)

        mock_ib.reqHistoricalData.assert_called_once_with(
            contract, endDateTime="end", durationStr="2 D",
            barSizeSetting="1 day", whatToShow="MIDPOINT", useRTH=False,
            formatDate=1, keepUpToDate=True
        )
        mock_ib.reqHeadTimeStamp.assert_called_once_with(
            contract, whatToShow="BID", useRTH=False, formatDate=2
        )
        mock_ib.reqContractDetails.assert_called_once_with(contract)
        mock_ib.sleep.assert_called_once_with(0.25)

    def test_async_historical_and_head_timestamp_delegation(self):
        client, mock_ib = _connected_client()
        contract = object()
        mock_ib.reqHistoricalDataAsync = AsyncMock(return_value=["bar"])
        mock_ib.reqHeadTimeStampAsync = AsyncMock(return_value="timestamp")

        async def exercise():
            bars = await client.get_historical_data_async(
                contract, "2 D", "1 day", "MIDPOINT", False, "end"
            )
            timestamp = await client.get_head_timestamp_async(contract, "ASK", False)
            return bars, timestamp

        assert asyncio.run(exercise()) == (["bar"], "timestamp")

        mock_ib.reqHistoricalDataAsync.assert_awaited_once_with(
            contract, endDateTime="end", durationStr="2 D",
            barSizeSetting="1 day", whatToShow="MIDPOINT", useRTH=False,
            formatDate=1
        )
        mock_ib.reqHeadTimeStampAsync.assert_awaited_once_with(
            contract, whatToShow="ASK", useRTH=False, formatDate=2
        )

    def test_flex_query_success_and_failure_are_normalized(self):
        client, _ = _make_client()
        report = object()
        with patch("clients.ib_client.FlexReport", return_value=report) as flex:
            assert client.run_flex_query(42, "token") is report
        flex.assert_called_once_with(token="token", queryId=42)

        with patch("clients.ib_client.FlexReport", side_effect=RuntimeError("bad")):
            with pytest.raises(IBError, match="Flex query 42 failed: bad"):
                client.run_flex_query(42, "token")
