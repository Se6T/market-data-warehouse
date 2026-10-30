"""Transport and CLI boundaries for the IB-first market-data owner."""
import asyncio
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
import pytest
from scripts import fetch_volatility_daily as owner


def fake_ib(monkeypatch, qualified=True, bars=True):
    ib = Mock()
    ib.connectAsync = AsyncMock()
    ib.qualifyContractsAsync = AsyncMock(return_value=[object()] if qualified else [])
    ib.reqHistoricalDataAsync = AsyncMock(return_value=[
        SimpleNamespace(date=date(2026,10,21),open=17.0,high=18.0,low=16.0,close=16.68),
        SimpleNamespace(date=date(2026,10,22),open=17.0,high=18.0,low=16.0,close=17.0),
    ] if bars else [])
    monkeypatch.setattr(owner, 'IB', Mock(return_value=ib))
    return ib


def test_ib_transport_readonly_and_bounded_target(monkeypatch):
    ib = fake_ib(monkeypatch)
    result=asyncio.run(owner.fetch_ib_daily(['VIX'],date(2026,10,21),'127.0.0.1',4002))
    ib.connectAsync.assert_awaited_once_with('127.0.0.1',4002,clientId=839280,readonly=True,timeout=12)
    assert len(result['VIX'])==1
    assert result['VIX'][0]['date']=='2026-10-21'
    assert result['VIX'][0]['close']==16.68
    ib.disconnect.assert_called_once()


def test_unqualified_ib_identity_skips_history(monkeypatch):
    ib=fake_ib(monkeypatch,qualified=False)
    assert asyncio.run(owner.fetch_ib_daily(['VXSMH'],date(2026,10,21),'localhost',4002))=={}
    ib.reqHistoricalDataAsync.assert_not_awaited()
    ib.disconnect.assert_called_once()


def test_ib_symbol_error_preserves_other_symbols(monkeypatch):
    ib=fake_ib(monkeypatch)
    ib.qualifyContractsAsync.side_effect=[ValueError('bad symbol'),[object()]]
    result=asyncio.run(owner.fetch_ib_daily(['VXSMH','VIX'],date(2026,10,21),'localhost',4002))
    assert set(result)=={'VIX'}
    ib.disconnect.assert_called_once()


def test_ib_connect_failure_cleanup(monkeypatch):
    ib=fake_ib(monkeypatch)
    ib.connectAsync.side_effect=TimeoutError('unavailable')
    assert asyncio.run(owner.fetch_ib_daily(['VIX'],date(2026,10,21),'localhost',4002))=={}
    ib.disconnect.assert_called_once()


@pytest.mark.parametrize('status,exit_code',[('succeeded',0),('failed',1)])
def test_cli_result_contract(monkeypatch,tmp_path,status,exit_code):
    result=tmp_path/'result.json'
    monkeypatch.setattr('sys.argv',['fetch_volatility_daily.py','--symbols','VIX','--end','2026-10-21','--warehouse',str(tmp_path),'--result-json',str(result)])
    refresh=Mock(return_value={'VIX':status});monkeypatch.setattr(owner,'refresh',refresh)
    assert owner.main()==exit_code
    import json
    assert json.loads(result.read_text())['results']==[{'symbol':'VIX','status':status}]


def test_weekend_uses_last_completed_session(monkeypatch,tmp_path):
    monkeypatch.setattr(owner,'fetch_ib_daily',AsyncMock(return_value={'VIX':[{'date':'2026-10-23','open':17,'high':18,'low':16,'close':17,'volume':0}]}))
    monkeypatch.setattr(owner,'write_bronze_parquet',Mock())
    assert owner.refresh(['VIX'],date(2026,10,25),tmp_path,'localhost',4002)=={'VIX':'succeeded'}


def test_no_canonical_table_is_failure(monkeypatch,tmp_path):
    monkeypatch.setattr(owner,'fetch_ib_daily',AsyncMock(return_value={'VIX':[{'date':'2026-10-21'}]}))
    monkeypatch.setattr(owner,'bars_to_table',Mock(return_value=None))
    writer=Mock();monkeypatch.setattr(owner,'write_bronze_parquet',writer)
    assert owner.refresh(['VIX'],date(2026,10,21),tmp_path,'localhost',4002)=={'VIX':'failed'}
    writer.assert_not_called()
