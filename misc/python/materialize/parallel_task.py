# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Run shell commands in parallel with timing and status reporting."""

from __future__ import annotations

import subprocess
import sys
import threading
import time
from collections.abc import Callable
from datetime import datetime, timedelta

from materialize import buildkite
from materialize.terminal import (
    COLOR_ERROR,
    COLOR_OK,
    STYLE_BOLD,
    with_formatting,
    with_formattings,
)

TaskSpec = list[str] | Callable[[], tuple[bool, str]]

OK = with_formatting("✓", COLOR_OK)
FAIL = with_formattings("✗", [COLOR_ERROR, STYLE_BOLD])


def _prefix(ci: str = "---") -> str:
    return ci + " " if buildkite.is_in_buildkite() else ""


class TaskThread(threading.Thread):
    """Runs a shell command or callable in a thread, capturing output, duration, and exit status."""

    def __init__(
        self, name: str, spec: TaskSpec | None = None, command: list[str] | None = None
    ):
        super().__init__()
        self.name = name
        self.output: str = ""
        self.success = False
        self.duration: timedelta = timedelta()
        resolved = command if spec is None else spec
        assert resolved is not None, "must provide spec or command"
        if callable(resolved):
            self._fn: Callable[[], tuple[bool, str]] | None = resolved
            self._command: list[str] | None = None
        else:
            self._fn = None
            self._command = resolved

    def run(self) -> None:
        start = datetime.now()
        try:
            if self._fn is not None:
                self.success, self.output = self._fn()
            else:
                assert self._command is not None
                proc = subprocess.Popen(
                    self._command,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                )
                stdout, _ = proc.communicate()
                self.success = proc.returncode == 0
                self.output = stdout.decode("utf-8").strip()
        except Exception as e:
            self.output = str(e)
            self.success = False
        self.duration = datetime.now() - start


class _SpinnerThread(threading.Thread):
    def __init__(self, label: str) -> None:
        super().__init__(daemon=True)
        self.label = label
        self.active = not buildkite.is_in_buildkite() and sys.stdout.isatty()

    def run(self) -> None:
        symbols = ["⣾", "⣷", "⣯", "⣟", "⡿", "⢿", "⣻", "⣽"]
        i = 0
        while self.active:
            print(f"\r\033[K{symbols[i]} {self.label}", end="", flush=True)
            i = (i + 1) % len(symbols)
            time.sleep(0.1)

    def stop(self) -> None:
        if self.active:
            self.active = False
            print("\r\033[K", end="", flush=True)


def run_parallel(
    tasks: list[tuple[str, TaskSpec]],
    verbose: bool = False,
) -> int:
    """Run tasks in parallel and print results sorted by duration.

    Args:
        tasks: List of (name, command) pairs.
        verbose: If True, print output even for successful tasks.

    Returns:
        0 if all tasks succeeded, 1 otherwise.
    """
    threads = [TaskThread(name, spec) for name, spec in tasks]

    spinner = _SpinnerThread(f"{len(threads)} tasks")
    spinner.start()

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    spinner.stop()

    failed = []
    for t in sorted(threads, key=lambda t: t.duration):
        secs = t.duration.total_seconds()
        if t.success:
            print(f"{_prefix('---')}{OK} [{secs:5.2f}s] {t.name}")
        else:
            print(f"{_prefix('+++')}{FAIL} [{secs:5.2f}s] {t.name}")
            failed.append(t.name)
        if t.output and (not t.success or verbose):
            print(t.output)

    if failed:
        print(f"{_prefix('+++')}{FAIL} Failed: {failed}")
        return 1

    print(f"{_prefix('+++')}{OK} All tasks successful")
    return 0
