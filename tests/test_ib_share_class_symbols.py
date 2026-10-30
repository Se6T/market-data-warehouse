"""Regression for proven IBKR class-share contract spelling."""
import pytest
from scripts.daily_update import _make_contract

@pytest.mark.parametrize('ticker,expected', [('BH.A','BH A'),('CRD.A','CRD A'),('GEF.B','GEF B'),('MOG.A','MOG A'),('DOMO','HUCK'),('PSKY','SKYD'),('SPY','SPY'),('SMH.MI','SMH.MI')])
def test_class_share_contract_keeps_warehouse_identity_outside_ib(ticker,expected):
    contract = _make_contract(ticker)
    assert contract.symbol == expected
    assert contract.exchange == 'SMART'
    assert contract.currency == 'USD'
