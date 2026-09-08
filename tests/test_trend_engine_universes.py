from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PRESETS = ROOT / "presets"


def _tickers(name: str) -> list[str]:
    payload = json.loads((PRESETS / name).read_text(encoding="utf-8"))
    return [str(symbol) for symbol in payload["tickers"]]


def test_cash_acquired_apge_is_absent_from_every_active_projection() -> None:
    names = (
        "r2k-health-care.json", "r2k-tier-top-500.json", "r2k-tier-mid-500.json",
        "r2k.json", "russell-2000-current.json", "trend-engine-all-equity-universe.json",
    )
    for name in names:
        payload = json.loads((PRESETS / name).read_text(encoding="utf-8"))
        projections = [payload, *payload.get("groups", {}).values()]
        for projection in projections:
            assert "APGE" not in projection["tickers"], name
            assert all("APGE" not in pair for pair in projection.get("pairs", [])), name


def test_current_russell_2000_preset_is_exact_sector_union() -> None:
    sector_files = sorted(
        path
        for path in PRESETS.glob("r2k-*.json")
        if "tier-" not in path.name
    )
    sector_tickers: list[str] = []
    for path in sector_files:
        tickers = _tickers(path.name)
        payload = json.loads(path.read_text(encoding="utf-8"))
        declared = re.search(r"\((\d+) companies\)", payload["description"])
        assert declared is not None
        assert int(declared.group(1)) == len(tickers)
        sector_tickers.extend(tickers)

    current = _tickers("russell-2000-current.json")

    assert len(sector_files) == 11
    assert len(sector_tickers) == len(set(sector_tickers)) == 1918
    assert len(current) == len(set(current)) == 1918
    assert set(current) == set(sector_tickers)
    assert {"MDV", "BBBY", "TALK", "LEG", "RMAX", "TWO", "CRNX", "HLX"}.isdisjoint(
        current
    )


def test_r2k_aggregate_declares_its_exact_ticker_and_pair_counts() -> None:
    payload = json.loads((PRESETS / "r2k.json").read_text(encoding="utf-8"))
    declared = re.search(
        r"— (\d+) companies, (\d+) sectors, (\d+) pairs$", payload["description"]
    )

    assert declared is not None
    assert tuple(map(int, declared.groups())) == (
        len(payload["tickers"]),
        len(tuple(name for name in payload["groups"] if not name.startswith("tier-"))),
        len(payload["pairs"]),
    )


def test_trend_engine_equity_union_contains_russell_and_other_inputs_once() -> None:
    russell = _tickers("russell-2000-current.json")
    other = _tickers("trend-engine-universe.json")
    combined = _tickers("trend-engine-all-equity-universe.json")

    assert len(combined) == len(set(combined)) == len(set(russell) | set(other))
    assert set(combined) == set(russell) | set(other)
    assert {"MDV", "BBBY", "TALK", "LEG", "RMAX", "TWO", "CRNX", "HLX"}.isdisjoint(
        combined
    )
