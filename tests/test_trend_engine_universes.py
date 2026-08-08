from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REQUIRED_OPERATIONAL_ETFS = {
    "DBC",
    "NOBL",
    "SDY",
    "VIG",
    "SCHD",
    "VYM",
    "DVY",
    "HDV",
    "SPYD",
    "STRC",
    "SVIX",
}


def _tickers(name: str) -> list[str]:
    payload = json.loads((ROOT / "presets" / name).read_text(encoding="utf-8"))
    return [str(symbol) for symbol in payload["tickers"]]


def test_operational_sleeve_etfs_are_in_both_daily_refresh_presets() -> None:
    focused = _tickers("trend-engine-universe.json")
    full = _tickers("trend-engine-all-equity-universe.json")
    assert len(focused) == len(set(focused))
    assert len(full) == len(set(full))
    assert REQUIRED_OPERATIONAL_ETFS <= set(focused)
    assert REQUIRED_OPERATIONAL_ETFS <= set(full)
