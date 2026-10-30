from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock, Mock
import pytest
from scripts import fetch_volatility_daily as owner


def bars(day='2026-10-21',close=16.68):
    return [{'date':day,'open':17.25,'high':17.58,'low':16.51,'close':close,'volume':0}]


def test_ib_primary_no_cboe_request(monkeypatch,tmp_path):
    monkeypatch.setattr(owner,'fetch_ib_daily',AsyncMock(return_value={'VIX':bars()}))
    fallback=Mock(side_effect=AssertionError('IB succeeded: no fallback allowed'))
    monkeypatch.setattr(owner,'fetch_cboe_historical',fallback)
    write=Mock();monkeypatch.setattr(owner,'write_bronze_parquet',write)
    assert owner.refresh(['VIX'],date(2026,10,21),tmp_path,'127.0.0.1',4002)=={'VIX':'succeeded'}
    fallback.assert_not_called();write.assert_called_once()


def test_cboe_only_for_missing_ib_symbol(monkeypatch,tmp_path):
    monkeypatch.setattr(owner,'fetch_ib_daily',AsyncMock(return_value={'VIX':bars()}))
    fallback=Mock(return_value=bars());monkeypatch.setattr(owner,'fetch_cboe_historical',fallback)
    monkeypatch.setattr(owner,'write_bronze_parquet',Mock())
    assert owner.refresh(['VIX','VXSMH'],date(2026,10,21),tmp_path,'127.0.0.1',4002)=={'VIX':'succeeded','VXSMH':'succeeded'}
    fallback.assert_called_once_with('VXSMH')


def test_stale_sources_never_write(monkeypatch,tmp_path):
    monkeypatch.setattr(owner,'fetch_ib_daily',AsyncMock(return_value={'VIX':bars('2026-10-20')}))
    monkeypatch.setattr(owner,'fetch_cboe_historical',Mock(return_value=bars('2026-10-20')))
    write=Mock();monkeypatch.setattr(owner,'write_bronze_parquet',write)
    assert owner.refresh(['VIX'],date(2026,10,21),tmp_path,'127.0.0.1',4002)=={'VIX':'failed'}
    write.assert_not_called()


def test_per_symbol_failure_isolated(monkeypatch,tmp_path):
    monkeypatch.setattr(owner,'fetch_ib_daily',AsyncMock(return_value={'VIX':bars()}))
    monkeypatch.setattr(owner,'fetch_cboe_historical',Mock(side_effect=ValueError('missing')))
    monkeypatch.setattr(owner,'write_bronze_parquet',Mock())
    assert owner.refresh(['VXSMH','VIX'],date(2026,10,21),tmp_path,'127.0.0.1',4002)=={'VXSMH':'failed','VIX':'succeeded'}
