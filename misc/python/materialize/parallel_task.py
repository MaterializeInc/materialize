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

import queue
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
        self,
        name: str,
        spec: TaskSpec | None = None,
        command: list[str] | None = None,
        done_queue: queue.Queue[TaskThread] | None = None,
    ):
        super().__init__()
        self.name = name
        self.output: str = ""
        self.success = False
        self.duration: timedelta = timedelta()
        self._done_queue = done_queue
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
        if self._done_queue is not None:
            self._done_queue.put(self)


class _SpinnerThread(threading.Thread):
    def __init__(self, remaining: int, suffix: str = "tasks") -> None:
        super().__init__(daemon=True)
        self._remaining = remaining
        self._suffix = suffix
        # "checks in foo" -> "check in foo"
        parts = suffix.split(" ", 1)
        self._singular = parts[0].rstrip("s") + (
            " " + parts[1] if len(parts) > 1 else ""
        )
        self._lock = threading.Lock()
        self.active = not buildkite.is_in_buildkite() and sys.stdout.isatty()

    def run(self) -> None:
        symbols = ["⣾", "⣷", "⣯", "⣟", "⡿", "⢿", "⣻", "⣽"]
        i = 0
        while self.active:
            with self._lock:
                remaining = self._remaining
            suffix = self._suffix if remaining != 1 else self._singular
            print(
                f"\r\033[K{symbols[i]} {remaining} {suffix}",
                end="",
                flush=True,
            )
            i = (i + 1) % len(symbols)
            time.sleep(0.1)

    def task_done(self) -> None:
        with self._lock:
            self._remaining -= 1

    def clear_line(self) -> None:
        if self.active:
            print("\r\033[K", end="", flush=True)

    def stop(self) -> None:
        if self.active:
            self.active = False
            print("\r\033[K", end="", flush=True)


def run_parallel(
    tasks: list[tuple[str, TaskSpec]],
    verbose: bool = False,
    print_duration: bool = True,
    spinner_suffix: str = "tasks",
    print_summary: bool = True,
) -> list[str]:
    """Run tasks in parallel and print results as each task finishes.

    Args:
        tasks: List of (name, spec) pairs.
        verbose: If True, print output even for successful tasks.
        print_duration: If True, include duration in status lines.
        spinner_suffix: Suffix after the count in the spinner, e.g. "tasks".
        print_summary: If True, print a final success/failure summary.

    Returns:
        List of failed task names (empty on full success).
    """
    done_q: queue.Queue[TaskThread] = queue.Queue()
    threads = [TaskThread(name, spec, done_queue=done_q) for name, spec in tasks]

    spinner = _SpinnerThread(len(threads), spinner_suffix)
    spinner.start()

    for t in threads:
        t.start()

    failed = []
    for _ in threads:
        t = done_q.get()
        spinner.task_done()
        spinner.clear_line()
        formatted_duration = (
            f" [{t.duration.total_seconds():5.2f}s]" if print_duration else ""
        )
        if t.success:
            print(f"{_prefix('---')}{OK}{formatted_duration} {t.name}")
        else:
            print(f"{_prefix('+++')}{FAIL}{formatted_duration} {t.name}")
            failed.append(t.name)
        if t.output and (not t.success or verbose):
            print(t.output)

    spinner.stop()

    if print_summary:
        if failed:
            print(f"{_prefix('+++')}{FAIL} Failed: {failed}")
        else:
            print(f"{_prefix('+++')}{OK} All tasks successful")

    return failed
