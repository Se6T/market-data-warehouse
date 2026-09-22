"""Permanent fail-closed regressions for completed-session bronze reuse."""
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts import fetch_vxm_current as vxm
from tests.test_fetch_vxm_current import _FakeIB, _bar, _contract


def _seed(tmp_path):
    contract = _contract(conId=54321, localSymbol="VXMV6", lastTradeDateOrContractMonth="20261021")
    kwargs = dict(warehouse=tmp_path / "warehouse", as_of=date(2026, 9, 21),
                  result_json=tmp_path / "result.json", mapping_json=tmp_path / "mapping.json",
                  roll_days=5, host="127.0.0.1", port=4002)
    broker = _FakeIB(contracts=[contract], bars=[_bar(d) for d in ("2026-09-17", "2026-09-18", "2026-09-21")])
    selected = vxm.refresh_current_vxm(**kwargs, ib_factory=lambda: broker)
    path = kwargs["warehouse"] / "data-lake/bronze/asset_class=futures/symbol=VXM_20261021/data.parquet"
    return contract, kwargs, selected, path


@pytest.mark.parametrize("damage", ["stale", "gap", "identity", "expiry", "root", "nan", "negative_volume", "negative_oi", "future", "duplicate", "empty", "malformed_date", "crossed_ohlc"])
def test_bad_stored_session_does_not_hide_unavailable_vendor(tmp_path: Path, damage):
    contract, kwargs, _, path = _seed(tmp_path)
    table = pq.ParquetFile(path).read()
    rows = table.to_pylist()
    if damage == "stale": rows.pop()
    elif damage == "gap": rows.pop(1)
    elif damage == "identity": rows[0]["contract_id"] = 1
    elif damage == "expiry": rows[0]["expiry_date"] = date(2026, 11, 18)
    elif damage == "root": rows[0]["root_symbol"] = "VX"
    elif damage == "nan": rows[0]["open"] = float("nan")
    elif damage == "negative_volume": rows[0]["volume"] = -1
    elif damage == "negative_oi": rows[0]["open_interest"] = -1
    elif damage == "future": rows[-1]["trade_date"] = date(2026, 9, 22)
    elif damage == "duplicate": rows.append(rows[0].copy())
    elif damage == "empty": rows.clear()
    elif damage == "malformed_date": rows[0]["trade_date"] = None
    elif damage == "crossed_ohlc": rows[0]["high"] = 0.01
    pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), path)

    class Unavailable(_FakeIB):
        def reqHistoricalData(self, contract, **kwargs):
            raise RuntimeError("vendor unavailable sentinel")

    with pytest.raises(RuntimeError, match="vendor unavailable sentinel"):
        vxm.refresh_current_vxm(**kwargs, ib_factory=lambda: Unavailable(contracts=[contract]))


def test_cache_hit_preserves_bronze_bytes_and_mtime(tmp_path: Path):
    contract, kwargs, _, path = _seed(tmp_path)
    before = path.read_bytes(), path.stat().st_mtime_ns
    broker = _FakeIB(contracts=[contract], bars=[])
    vxm.refresh_current_vxm(**kwargs, ib_factory=lambda: broker)
    assert broker.history_requests == []
    assert (path.read_bytes(), path.stat().st_mtime_ns) == before


def test_symlinked_partition_is_not_reused(tmp_path: Path):
    _, kwargs, selected, path = _seed(tmp_path)
    target = path.with_name("original.parquet")
    path.rename(target)
    path.symlink_to(target)
    assert vxm._complete_stored_rows(kwargs["warehouse"], selected, kwargs["as_of"]) is None


def test_unreadable_cache_is_not_reused(tmp_path: Path, monkeypatch):
    _, kwargs, selected, _ = _seed(tmp_path)
    def unreadable(*args):
        raise OSError("unreadable")
    monkeypatch.setattr(vxm.BronzeClient, "read_symbol_rows", unreadable)
    assert vxm._complete_stored_rows(kwargs["warehouse"], selected, kwargs["as_of"]) is None
