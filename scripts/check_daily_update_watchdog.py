#!/usr/bin/env python3
"""Watchdog for the scheduled daily parquet-first sync."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Sequence

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.run_daily_update_job import (
    AlertRequest,
    RunnerConfig,
    append_log,
    build_config,
    log_has_completion_marker,
    send_failure_alert,
)
WATCHDOG_ALERT_SENT_EXIT_CODE = 1
WATCHDOG_ALERT_FAILED_EXIT_CODE = 2


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Alert if today's scheduled daily update did not complete."
    )
    parser.add_argument(
        "--run-date",
        help="Run date to inspect in YYYY-MM-DD format. Defaults to today in local time.",
    )
    return parser.parse_args(list(argv))


def build_daily_log_file(log_dir: Path, run_date: str) -> Path:
    return log_dir / f"daily_update_{run_date}.log"


def build_watchdog_log_file(log_dir: Path, run_date: str) -> Path:
    return log_dir / f"daily_update_watchdog_{run_date}.log"


def build_watchdog_marker_file(warehouse_dir: Path, run_date: str) -> Path:
    return warehouse_dir / "state" / "daily-update-watchdog" / f"{run_date}.alerted"


def determine_watchdog_error(log_file: Path, run_date: str) -> str:
    if not log_file.exists():
        return (
            "Watchdog detected that the scheduled daily update did not start on "
            f"{run_date}; expected log file {log_file} was not created."
        )

    return (
        "Watchdog detected that the scheduled daily update did not complete "
        f"successfully on {run_date}; no completion marker was found in {log_file}."
    )


def record_alert_marker(marker_file: Path, message: str) -> None:
    marker_file.parent.mkdir(parents=True, exist_ok=True)
    marker_file.write_text(f"{message}\n", encoding="utf-8")


def run_watchdog(
    config: RunnerConfig,
    *,
    run_date: str,
    env: dict[str, str] | None = None,
    runner: callable = subprocess.run,
) -> int:
    daily_log_file = build_daily_log_file(config.log_dir, run_date)
    watchdog_log_file = build_watchdog_log_file(config.log_dir, run_date)
    marker_file = build_watchdog_marker_file(config.warehouse_dir, run_date)

    if log_has_completion_marker(daily_log_file):
        return 0

    reason = determine_watchdog_error(daily_log_file, run_date)
    append_log(watchdog_log_file, f"=== Daily Update Watchdog {run_date} ===")
    append_log(watchdog_log_file, reason)

    if marker_file.exists():
        append_log(
            watchdog_log_file,
            f"Alert already sent for {run_date}; skipping duplicate failure email.",
        )
        return WATCHDOG_ALERT_SENT_EXIT_CODE

    request = AlertRequest(
        run_date=run_date,
        log_file=daily_log_file,
        attempts=None,
        exit_code=None,
        error_summary=reason,
        repo_root=REPO_ROOT,
    )
    alert_result = send_failure_alert(
        config,
        request,
        watchdog_log_file,
        env=env,
        runner=runner,
    )
    if alert_result is None:
        append_log(watchdog_log_file, "Watchdog could not send a failure alert.")
        return WATCHDOG_ALERT_FAILED_EXIT_CODE

    alert_output = (alert_result.stdout or "").strip()
    if alert_result.returncode != 0:
        append_log(
            watchdog_log_file,
            (
                "WARNING: watchdog failure alert returned non-zero exit code "
                f"{alert_result.returncode}. {alert_output}"
            ).strip(),
        )
        return WATCHDOG_ALERT_FAILED_EXIT_CODE

    append_log(
        watchdog_log_file,
        f"Watchdog failure alert sent successfully. {alert_output}".strip(),
    )
    record_alert_marker(marker_file, reason)
    return WATCHDOG_ALERT_SENT_EXIT_CODE


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    run_date = args.run_date or datetime.now().strftime("%Y-%m-%d")
    config = build_config()
    return run_watchdog(config, run_date=run_date, env=os.environ.copy())


if __name__ == "__main__":
    raise SystemExit(main())
