"""Tests for scripts/check_daily_update_watchdog.py."""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from scripts.check_daily_update_watchdog import (
    WATCHDOG_ALERT_FAILED_EXIT_CODE,
    WATCHDOG_ALERT_SENT_EXIT_CODE,
    build_daily_log_file,
    build_watchdog_log_file,
    build_watchdog_marker_file,
    determine_watchdog_error,
    main,
    parse_args,
    record_alert_marker,
    run_watchdog,
)
from scripts.run_daily_update_job import RunnerConfig


def _config(tmp_path: Path, *, node_bin: str = "/opt/homebrew/bin/node") -> RunnerConfig:
    repo_root = tmp_path / "repo"
    script_dir = repo_root / "scripts"
    return RunnerConfig(
        warehouse_dir=tmp_path / "warehouse",
        log_dir=tmp_path / "warehouse" / "logs",
        daily_update_script=script_dir / "daily_update.py",
        alert_script=script_dir / "send_daily_update_failure_email.mjs",
        python_bin="/usr/bin/python3",
        node_bin=node_bin,
        max_attempts=3,
        retry_delay_seconds=300,
    )


class TestHelpers:
    def test_direct_script_bootstrap_restores_repo_import_path(self):
        module_path = Path(__file__).parents[1] / "scripts" / "check_daily_update_watchdog.py"
        repo_root = str(module_path.parent.parent)
        original_path = sys.path[:]
        sys.path[:] = [entry for entry in sys.path if entry != repo_root]
        try:
            spec = importlib.util.spec_from_file_location("watchdog_bootstrap_test", module_path)
            assert spec is not None and spec.loader is not None
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            assert sys.path[0] == repo_root
        finally:
            sys.path[:] = original_path

    def test_parse_args_and_path_builders(self, tmp_path):
        args = parse_args(["--run-date", "2026-03-11"])
        assert args.run_date == "2026-03-11"
        assert parse_args([]).run_date is None

        assert (
            build_daily_log_file(tmp_path, "2026-03-11")
            == tmp_path / "daily_update_2026-03-11.log"
        )
        assert (
            build_watchdog_log_file(tmp_path, "2026-03-11")
            == tmp_path / "daily_update_watchdog_2026-03-11.log"
        )
        assert (
            build_watchdog_marker_file(tmp_path, "2026-03-11")
            == tmp_path / "state" / "daily-update-watchdog" / "2026-03-11.alerted"
        )

    def test_determine_error_and_marker_recording(self, tmp_path):
        missing_log = tmp_path / "missing.log"
        assert "did not start" in determine_watchdog_error(missing_log, "2026-03-11")

        incomplete_log = tmp_path / "daily.log"
        incomplete_log.write_text("=== Daily Update 2026-03-11T20:05:07Z ===\n", encoding="utf-8")
        assert "did not complete successfully" in determine_watchdog_error(
            incomplete_log, "2026-03-11"
        )

        marker_file = build_watchdog_marker_file(tmp_path, "2026-03-11")
        record_alert_marker(marker_file, "sent")
        assert marker_file.read_text(encoding="utf-8") == "sent\n"


class TestRunWatchdog:
    def test_returns_healthy_when_daily_log_completed(self, tmp_path):
        config = _config(tmp_path)
        log_file = build_daily_log_file(config.log_dir, "2026-03-11")
        log_file.parent.mkdir(parents=True, exist_ok=True)
        log_file.write_text(
            "=== Done 2026-03-11T20:05:09Z (attempt 1/3) ===\n",
            encoding="utf-8",
        )

        assert run_watchdog(config, run_date="2026-03-11", env={}) == 0

    def test_skips_duplicate_alert_when_marker_exists(self, tmp_path):
        config = _config(tmp_path)
        marker_file = build_watchdog_marker_file(config.warehouse_dir, "2026-03-11")
        marker_file.parent.mkdir(parents=True, exist_ok=True)
        marker_file.write_text("already sent\n", encoding="utf-8")

        rc = run_watchdog(config, run_date="2026-03-11", env={})

        assert rc == WATCHDOG_ALERT_SENT_EXIT_CODE
        watchdog_log = build_watchdog_log_file(config.log_dir, "2026-03-11")
        assert "skipping duplicate failure email" in watchdog_log.read_text(
            encoding="utf-8"
        )

    def test_sends_alert_for_incomplete_log(self, tmp_path):
        config = _config(tmp_path)
        daily_log = build_daily_log_file(config.log_dir, "2026-03-11")
        daily_log.parent.mkdir(parents=True, exist_ok=True)
        daily_log.write_text("partial run only\n", encoding="utf-8")
        captured = {}

        def _send_failure_alert(config_arg, request, log_file, env=None, runner=None):
            captured["config"] = config_arg
            captured["request"] = request
            captured["log_file"] = log_file
            return SimpleNamespace(returncode=0, stdout="sent")

        with patch(
            "scripts.check_daily_update_watchdog.send_failure_alert",
            side_effect=_send_failure_alert,
        ):
            rc = run_watchdog(config, run_date="2026-03-11", env={"A": "1"})

        assert rc == WATCHDOG_ALERT_SENT_EXIT_CODE
        assert captured["config"] == config
        assert captured["request"].attempts is None
        assert captured["request"].exit_code is None
        assert captured["request"].log_file == daily_log
        assert captured["log_file"] == build_watchdog_log_file(config.log_dir, "2026-03-11")
        marker_file = build_watchdog_marker_file(config.warehouse_dir, "2026-03-11")
        assert marker_file.exists() is True
        watchdog_log = build_watchdog_log_file(config.log_dir, "2026-03-11")
        assert "Watchdog failure alert sent successfully. sent" in watchdog_log.read_text(
            encoding="utf-8"
        )

    def test_returns_failed_exit_code_when_alert_cannot_be_sent(self, tmp_path):
        config = _config(tmp_path)

        with patch("scripts.check_daily_update_watchdog.send_failure_alert", return_value=None):
            rc = run_watchdog(config, run_date="2026-03-11", env={})

        assert rc == WATCHDOG_ALERT_FAILED_EXIT_CODE
        watchdog_log = build_watchdog_log_file(config.log_dir, "2026-03-11")
        assert "could not send a failure alert" in watchdog_log.read_text(
            encoding="utf-8"
        )

    def test_returns_failed_exit_code_when_alert_command_fails(self, tmp_path):
        config = _config(tmp_path)

        with patch(
            "scripts.check_daily_update_watchdog.send_failure_alert",
            return_value=SimpleNamespace(returncode=2, stdout="smtp down"),
        ):
            rc = run_watchdog(config, run_date="2026-03-11", env={})

        assert rc == WATCHDOG_ALERT_FAILED_EXIT_CODE
        watchdog_log = build_watchdog_log_file(config.log_dir, "2026-03-11")
        assert "WARNING: watchdog failure alert returned non-zero exit code 2. smtp down" in watchdog_log.read_text(
            encoding="utf-8"
        )


class TestMain:
    def test_main_uses_build_config_and_run_watchdog(self, tmp_path):
        config = _config(tmp_path)

        with patch("scripts.check_daily_update_watchdog.build_config", return_value=config):
            with patch("scripts.check_daily_update_watchdog.run_watchdog", return_value=1) as run_mock:
                assert main(["--run-date", "2026-03-11"]) == 1

        run_mock.assert_called_once_with(
            config,
            run_date="2026-03-11",
            env=os.environ.copy(),
        )
