"""Dynamic Mini VIX current-contract owner tests (all broker I/O is fake)."""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pyarrow.parquet as pq
import pytest

from clients.symbol_ids import stable_symbol_id
from scripts import fetch_vxm_current as vxm


def _contract(**overrides):
    values = {
        "symbol": "VXM",
        "localSymbol": "VXMU6",
        "secType": "FUT",
        "exchange": "CFE",
        "currency": "USD",
        "tradingClass": "VXM",
        "multiplier": "100",
        "conId": 12345,
        "lastTradeDateOrContractMonth": "20260916",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _bar(day: str = "2026-08-21"):
    return SimpleNamespace(
        date=day, open=18.0, high=19.0, low=17.5, close=18.5, volume=321,
    )


class _FakeIB:
    def __init__(self, contracts=None, bars=None):
        self.contracts = contracts or [_contract()]
        self.bars = bars or [_bar()]
        self.connect_calls = []
        self.details_requests = []
        self.history_requests = []
        self.disconnected = False

    def connect(self, host, port, *, clientId, timeout, readonly):
        self.connect_calls.append((host, port, clientId, timeout, readonly))

    def reqContractDetails(self, request):
        self.details_requests.append(request)
        return [SimpleNamespace(contract=contract) for contract in self.contracts]

    def reqHistoricalData(self, contract, **kwargs):
        self.history_requests.append((contract, kwargs))
        return self.bars

    def disconnect(self):
        self.disconnected = True


def test_select_current_contract_is_exact_and_deterministic() -> None:
    later_lower_id = _contract(conId=9, lastTradeDateOrContractMonth="20261021", localSymbol="VXMV6")
    nearest_high_id = _contract(conId=8)
    nearest_low_id = _contract(conId=7)

    selected = vxm.select_current_contract(
        [later_lower_id, nearest_high_id, nearest_low_id],
        as_of=date(2026, 8, 22),
        roll_days=5,
    )

    assert selected.con_id == 7
    assert selected.expiry_date == date(2026, 9, 16)
    assert selected.symbol == "VXM_20260916"


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"symbol": "VX"}, "root"),
        ({"localSymbol": ""}, "local symbol"),
        ({"secType": "STK"}, "security type"),
        ({"exchange": "CBOE"}, "exchange"),
        ({"currency": "EUR"}, "currency"),
        ({"tradingClass": "VX"}, "trading class"),
        ({"multiplier": "1000"}, "multiplier"),
        ({"conId": 0}, "conId"),
        ({"lastTradeDateOrContractMonth": "202609"}, "exact expiry"),
    ],
)
def test_select_current_contract_rejects_wrong_identity(overrides, message) -> None:
    with pytest.raises(vxm.VXMRefreshError, match=message):
        vxm.select_current_contract(
            [_contract(**overrides)], as_of=date(2026, 8, 22), roll_days=5,
        )


def test_select_current_contract_rejects_duplicate_conid_and_roll_window() -> None:
    duplicate = [_contract(), _contract(localSymbol="VXMU6X")]
    with pytest.raises(vxm.VXMRefreshError, match="duplicate conId"):
        vxm.select_current_contract(duplicate, as_of=date(2026, 8, 22), roll_days=5)
    with pytest.raises(vxm.VXMRefreshError, match="beyond roll window"):
        vxm.select_current_contract(
            [_contract(lastTradeDateOrContractMonth="20260827")],
            as_of=date(2026, 8, 22),
            roll_days=5,
        )


def test_refresh_publishes_exact_contract_data_and_mapping_via_read_only_paper(tmp_path: Path) -> None:
    broker = _FakeIB()
    result = tmp_path / "result.json"
    mapping = tmp_path / "mapping.json"

    contract = vxm.refresh_current_vxm(
        warehouse=tmp_path / "warehouse",
        as_of=date(2026, 8, 22),
        result_json=result,
        mapping_json=mapping,
        roll_days=5,
        host="127.0.0.1",
        port=4002,
        ib_factory=lambda: broker,
    )

    assert broker.connect_calls == [("127.0.0.1", 4002, vxm.PAPER_CLIENT_ID, 10, True)]
    assert broker.disconnected is True
    assert len(broker.details_requests) == 1
    request = broker.details_requests[0]
    assert (request.symbol, request.secType, request.exchange, request.currency) == (
        "VXM", "FUT", "CFE", "USD",
    )
    assert broker.history_requests[0][1] == {
        "endDateTime": "20260823 23:59:59 UTC",
        "durationStr": "10 D",
        "barSizeSetting": "1 day",
        "whatToShow": "TRADES",
        "useRTH": True,
        "formatDate": 1,
        "keepUpToDate": False,
    }
    assert contract.symbol == "VXM_20260916"
    path = tmp_path / "warehouse/data-lake/bronze/asset_class=futures/symbol=VXM_20260916/data.parquet"
    table = pq.ParquetFile(path).read()
    assert table.to_pylist() == [{
        "trade_date": date(2026, 8, 21),
        "contract_id": stable_symbol_id("VXM_20260916"),
        "root_symbol": "VXM",
        "expiry_date": date(2026, 9, 16),
        "open": 18.0,
        "high": 19.0,
        "low": 17.5,
        "close": 18.5,
        "settlement": 18.5,
        "volume": 321,
        "open_interest": 0,
    }]
    result_doc = json.loads(result.read_text())
    assert result_doc["requested_symbols"] == ["VXM_20260916"]
    assert result_doc["results"] == [{"status": "succeeded", "symbol": "VXM_20260916"}]
    mapping_doc = json.loads(mapping.read_text())
    assert mapping_doc == {
        "as_of": "2026-08-22",
        "con_id": 12345,
        "contract_id": stable_symbol_id("VXM_20260916"),
        "currency": "USD",
        "exchange": "CFE",
        "expiry_date": "2026-09-16",
        "latest_session": "2026-08-21",
        "local_symbol": "VXMU6",
        "multiplier": "100",
        "roll_days": 5,
        "root": "VXM",
        "schema_version": 1,
        "sec_type": "FUT",
        "symbol": "VXM_20260916",
        "trading_class": "VXM",
    }


def test_refresh_fails_closed_without_completed_bar_or_on_nonpaper_port(tmp_path: Path) -> None:
    with pytest.raises(vxm.VXMRefreshError, match="PAPER port 4002"):
        vxm.refresh_current_vxm(
            warehouse=tmp_path,
            as_of=date(2026, 8, 22),
            result_json=tmp_path / "result.json",
            mapping_json=tmp_path / "mapping.json",
            roll_days=5,
            host="127.0.0.1",
            port=4001,
            ib_factory=_FakeIB,
        )
    with pytest.raises(vxm.VXMRefreshError, match="completed session"):
        vxm.refresh_current_vxm(
            warehouse=tmp_path,
            as_of=date(2026, 8, 22),
            result_json=tmp_path / "result.json",
            mapping_json=tmp_path / "mapping.json",
            roll_days=5,
            host="127.0.0.1",
            port=4002,
            ib_factory=lambda: _FakeIB(bars=[_bar("2026-08-20")]),
        )


@pytest.mark.parametrize("roll_days", [-1, True])
def test_select_current_contract_rejects_invalid_roll_days(roll_days) -> None:
    with pytest.raises(vxm.VXMRefreshError, match="non-negative integer"):
        vxm.select_current_contract(
            [_contract()], as_of=date(2026, 8, 22), roll_days=roll_days,
        )


def test_select_current_contract_rejects_impossible_exact_expiry() -> None:
    with pytest.raises(vxm.VXMRefreshError, match="exact expiry"):
        vxm.select_current_contract(
            [_contract(lastTradeDateOrContractMonth="20260231")],
            as_of=date(2026, 1, 1),
            roll_days=5,
        )


@pytest.mark.parametrize(
    ("bar", "message"),
    [
        (SimpleNamespace(date="bad", open=1, high=1, low=1, close=1, volume=1), "malformed"),
        (_bar("2026-08-23"), "invalid"),
        (SimpleNamespace(date="2026-08-21", open=18, high=19, low=17, close=18, volume=-1), "invalid"),
    ],
)
def test_bar_rows_rejects_malformed_and_invalid_market_data(bar, message) -> None:
    selected = vxm.select_current_contract(
        [_contract()], as_of=date(2026, 8, 22), roll_days=5,
    )
    with pytest.raises(vxm.VXMRefreshError, match=message):
        vxm._bar_rows([bar], selected, date(2026, 8, 22))


def test_mapping_writer_rejects_unsafe_path(tmp_path: Path) -> None:
    with pytest.raises(vxm.VXMRefreshError, match="absolute non-symlink"):
        vxm._write_mapping(Path("relative.json"), {})


def test_refresh_rejects_empty_host_before_constructing_broker(tmp_path: Path) -> None:
    factory = pytest.fail
    with pytest.raises(vxm.VXMRefreshError, match="host must be non-empty"):
        vxm.refresh_current_vxm(
            warehouse=tmp_path,
            as_of=date(2026, 8, 22),
            result_json=tmp_path / "result.json",
            mapping_json=tmp_path / "mapping.json",
            roll_days=5,
            host="",
            port=4002,
            ib_factory=factory,
        )


def test_refresh_rejects_nonloopback_host_before_constructing_broker(tmp_path: Path) -> None:
    with pytest.raises(vxm.VXMRefreshError, match="127.0.0.1"):
        vxm.refresh_current_vxm(
            warehouse=tmp_path,
            as_of=date(2026, 8, 22),
            result_json=tmp_path / "result.json",
            mapping_json=tmp_path / "mapping.json",
            roll_days=5,
            host="192.0.2.1",
            port=4002,
            ib_factory=pytest.fail,
        )


def test_parser_and_main_dispatch_without_broker_io(tmp_path: Path, monkeypatch, capsys) -> None:
    argv = [
        "--warehouse", str(tmp_path), "--as-of", "2026-08-22",
        "--result-json", str(tmp_path / "result.json"),
        "--mapping-json", str(tmp_path / "mapping.json"),
    ]
    parsed = vxm.build_parser().parse_args(argv)
    assert parsed.port == 4002

    refresh = SimpleNamespace(calls=[])
    def succeed(**kwargs):
        refresh.calls.append(kwargs)
    monkeypatch.setattr(vxm, "refresh_current_vxm", succeed)
    assert vxm.main(argv) == 0
    assert refresh.calls[0]["as_of"] == date(2026, 8, 22)

    def fail(**_kwargs):
        raise vxm.VXMRefreshError("deterministic failure")
    monkeypatch.setattr(vxm, "refresh_current_vxm", fail)
    assert vxm.main(argv) == 1
    assert "deterministic failure" in capsys.readouterr().err
