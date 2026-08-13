from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PRESETS = ROOT / "presets"


def _tickers(name: str) -> list[str]:
    payload = json.loads((PRESETS / name).read_text(encoding="utf-8"))
    return [str(symbol) for symbol in payload["tickers"]]


def test_current_russell_2000_preset_is_exact_sector_union() -> None:
    sector_files = sorted(
        path
        for path in PRESETS.glob("r2k-*.json")
        if "tier-" not in path.name
    )
    sector_tickers: list[str] = []
    for path in sector_files:
        sector_tickers.extend(_tickers(path.name))

    current = _tickers("russell-2000-current.json")

    assert len(sector_files) == 11
    assert len(sector_tickers) == len(set(sector_tickers)) == 1927
    assert len(current) == len(set(current)) == 1927
    assert set(current) == set(sector_tickers)
    assert "MDV" not in current


def test_trend_engine_equity_union_contains_russell_and_other_inputs_once() -> None:
    russell = _tickers("russell-2000-current.json")
    other = _tickers("trend-engine-universe.json")
    combined = _tickers("trend-engine-all-equity-universe.json")

    assert len(combined) == len(set(combined)) == len(set(russell) | set(other))
    assert set(combined) == set(russell) | set(other)
    assert "MDV" not in combined
