"""Tests for the single-process composite futures refresh owner."""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import refresh_futures_batch as batch
from scripts._refresh_result import write_result


def _preset(path: Path) -> None:
    path.write_text(json.dumps({
        "name": "m0-futures-batch",
        "contracts": [{"root": "ES", "expiry": "202506"}],
    }))


def test_composite_owner_writes_one_truthful_result_for_ordinary_and_dynamic_futures(
    tmp_path: Path,
) -> None:
    preset = tmp_path / "preset.json"
    result = tmp_path / "result.json"
    mapping = tmp_path / "mapping.json"
    _preset(preset)
    daily_calls: list[list[str]] = []

    def daily_main(argv: list[str]) -> int:
        daily_calls.append(argv)
        write_result(
            Path(argv[argv.index("--result-json") + 1]),
            "futures", ["ES_202506"], {"ES_202506": "succeeded"},
        )
        return 0

    def refresh_vxm(**kwargs) -> SimpleNamespace:
        mapping.write_text(json.dumps({"symbol": "VXM_20250219"}))
        write_result(
            kwargs["result_json"], "futures", ["VXM_20250219"],
            {"VXM_20250219": "succeeded"},
        )
        return SimpleNamespace(symbol="VXM_20250219")

    exit_code = batch.refresh_futures_batch(
        warehouse=tmp_path,
        as_of=date(2025, 1, 2),
        preset=preset,
        result_json=result,
        mapping_json=mapping,
        prior_vxm_symbols=[],
        roll_days=5,
        provider="direct-ib",
        host="127.0.0.1",
        port=4002,
        daily_main=daily_main,
        vxm_refresh=refresh_vxm,
    )

    assert exit_code == 0
    assert len(daily_calls) == 1
    assert [daily_calls[0][daily_calls[0].index(flag) + 1] for flag in (
        "--provider", "--host", "--port",
    )] == ["direct-ib", "127.0.0.1", "4002"]
    assert json.loads(result.read_text()) == {
        "schema_version": 1,
        "asset_class": "futures",
        "requested_symbols": ["ES_202506", "VXM_20250219"],
        "results": [
            {"symbol": "ES_202506", "status": "succeeded"},
            {"symbol": "VXM_20250219", "status": "succeeded"},
        ],
    }


def test_composite_owner_reports_preservable_vxm_failure_without_claiming_success(
    tmp_path: Path,
) -> None:
    preset = tmp_path / "preset.json"
    result = tmp_path / "result.json"
    mapping = tmp_path / "mapping.json"
    _preset(preset)

    def daily_main(argv: list[str]) -> int:
        write_result(
            Path(argv[argv.index("--result-json") + 1]),
            "futures", ["ES_202506"], {"ES_202506": "succeeded"},
        )
        return 0

    def fail_vxm(**_kwargs) -> None:
        raise RuntimeError("sensitive failure")

    exit_code = batch.refresh_futures_batch(
        warehouse=tmp_path,
        as_of=date(2025, 1, 2),
        preset=preset,
        result_json=result,
        mapping_json=mapping,
        prior_vxm_symbols=["VXM_20250122"],
        roll_days=5,
        provider="direct-ib",
        host="127.0.0.1",
        port=4002,
        daily_main=daily_main,
        vxm_refresh=fail_vxm,
    )

    assert exit_code == 1
    assert not mapping.exists()
    document = json.loads(result.read_text())
    assert document["requested_symbols"] == ["ES_202506", "VXM_20250122"]
    assert document["results"] == [
        {"symbol": "ES_202506", "status": "succeeded"},
        {"symbol": "VXM_20250122", "status": "failed"},
    ]
    assert "sensitive" not in result.read_text()


@pytest.mark.parametrize(
    ("document", "symbols"),
    [
        ({"schema_version": 2, "asset_class": "futures", "requested_symbols": ["ES"], "results": []}, ["ES"]),
        (
            {
                "schema_version": 1,
                "asset_class": "futures",
                "requested_symbols": ["ES"],
                "results": [{"symbol": "OTHER", "status": "succeeded"}],
            },
            ["ES"],
        ),
    ],
)
def test_subresult_rejects_wrong_contract(
    tmp_path: Path, document: dict[str, object], symbols: list[str]
) -> None:
    path = tmp_path / "result.json"
    path.write_text(json.dumps(document))
    with pytest.raises(ValueError, match="sub-result"):
        batch._statuses(path, symbols)


def test_composite_owner_rejects_nonpaper_authority(tmp_path: Path) -> None:
    preset = tmp_path / "preset.json"
    _preset(preset)
    with pytest.raises(ValueError, match="127.0.0.1:4002"):
        batch.refresh_futures_batch(
            warehouse=tmp_path,
            as_of=date(2025, 1, 2),
            preset=preset,
            result_json=tmp_path / "result.json",
            mapping_json=tmp_path / "mapping.json",
            prior_vxm_symbols=[],
            roll_days=5,
            provider="direct-ib",
            host="live.invalid",
            port=4001,
        )


@pytest.mark.parametrize("ordinary_failure", ["mismatch", "exception"])
def test_composite_owner_reports_ordinary_failure(
    tmp_path: Path, ordinary_failure: str
) -> None:
    preset = tmp_path / "preset.json"
    result = tmp_path / "result.json"
    _preset(preset)

    def daily_main(argv: list[str]) -> int:
        if ordinary_failure == "exception":
            raise OSError("transport")
        write_result(
            Path(argv[argv.index("--result-json") + 1]),
            "futures",
            ["ES_202506"],
            {"ES_202506": "succeeded"},
        )
        return 1

    exit_code = batch.refresh_futures_batch(
        warehouse=tmp_path,
        as_of=date(2025, 1, 2),
        preset=preset,
        result_json=result,
        mapping_json=tmp_path / "mapping.json",
        prior_vxm_symbols=["VXM_20250122"],
        roll_days=5,
        provider="direct-ib",
        host="127.0.0.1",
        port=4002,
        daily_main=daily_main,
        vxm_refresh=lambda **_kwargs: (_ for _ in ()).throw(OSError("transport")),
    )
    assert exit_code == 1
    assert json.loads(result.read_text())["results"] == [
        {"symbol": "ES_202506", "status": "failed"},
        {"symbol": "VXM_20250122", "status": "failed"},
    ]


def test_main_parses_and_delegates_exact_authority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    preset = tmp_path / "preset.json"
    _preset(preset)
    seen: list[dict[str, object]] = []
    monkeypatch.setattr(
        batch,
        "refresh_futures_batch",
        lambda **kwargs: seen.append(kwargs) or 0,
    )
    assert batch.main([
        "--warehouse", str(tmp_path),
        "--as-of", "2025-01-02",
        "--preset", str(preset),
        "--result-json", str(tmp_path / "result.json"),
        "--mapping-json", str(tmp_path / "mapping.json"),
        "--prior-vxm-symbols", "VXM_20250122",
        "--roll-days", "5",
        "--provider", "direct-ib",
        "--host", "127.0.0.1",
        "--port", "4002",
    ]) == 0
    assert seen[0]["host"] == "127.0.0.1"
    assert seen[0]["port"] == 4002
