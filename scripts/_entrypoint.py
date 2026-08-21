"""Shared import-safe script entrypoint dispatch."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any


def run_main(
    module_name: str,
    main: Callable[[], Any],
    *,
    exit_with_result: bool = False,
) -> None:
    """Invoke ``main`` only for direct execution, optionally as an exit code."""
    if module_name != "__main__":
        return
    result = main()
    if exit_with_result:
        raise SystemExit(result)
