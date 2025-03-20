# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
import argparse
import os
import subprocess
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path

from materialize import MZ_ROOT, buildkite
from materialize.terminal import (
    COLOR_ERROR,
    COLOR_OK,
    STYLE_BOLD,
    with_formatting,
    with_formattings,
)

MAIN_PATH = MZ_ROOT / "ci" / "test" / "lint-main"
MAIN_CHECKS_PATH = MAIN_PATH / "checks"
CHECK_BEFORE_PATH = MAIN_PATH / "before"
CHECK_AFTER_PATH = MAIN_PATH / "after"
OK = with_formatting("✓", COLOR_OK)
FAIL = with_formattings("✗", [COLOR_ERROR, STYLE_BOLD])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="lint",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Lint the code",
    )
    parser.add_argument(
        "--print-duration", action=argparse.BooleanOptionalAction, default=True
    )
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--offline", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    print_duration = args.print_duration
    verbose_output = args.verbose
    offline = args.offline

    manager = LintManager(print_duration, verbose_output, offline)
    return_code = manager.run()
    return return_code


def prefix(ci: str = "---") -> str:
    return ci + " " if buildkite.is_in_buildkite() else ""


class LintManager:
    def __init__(self, print_duration: bool, verbose_output: bool, offline: bool):
        self.print_duration = print_duration
        self.verbose_output = verbose_output
        self.offline = offline

    def run(self) -> int:
        failed_checks = self.run_and_validate_if_no_previous_failures(
            CHECK_BEFORE_PATH, previous_failures=[]
        )
        failed_checks = self.run_and_validate_if_no_previous_failures(
            MAIN_CHECKS_PATH, previous_failures=failed_checks
        )
        failed_checks = self.run_and_validate_if_no_previous_failures(
            CHECK_AFTER_PATH, previous_failures=failed_checks
        )

        success = len(failed_checks) == 0

        print(
            prefix("+++") + f"{OK} All checks successful"
            if success
            else f"{FAIL} Checks failed: {failed_checks}"
        )

        return 0 if success else 1

    def is_ignore_file(self, path: Path) -> bool:
        return os.path.isdir(path)

    def run_and_validate_if_no_previous_failures(
        self, checks_path: Path, previous_failures: list[str]
    ) -> list[str]:
        if len(previous_failures) > 0:
            print(
                f"{prefix()}Skipping checks in '{checks_path}' due to previous failures"
            )
            return previous_failures
        else:
            return self.run_and_validate(checks_path)

    def run_and_validate(self, checks_path: Path) -> list[str]:
        """
        Runs checks in the given directory and validates their outcome.
        :return: names of failed checks
        """

        lint_files = [
            lint_file
            for lint_file in os.listdir(checks_path)
            if lint_file.endswith(".sh")
            and not self.is_ignore_file(checks_path / lint_file)
        ]
        lint_files.sort()

        threads = []

        check = "check" if len(lint_files) == 1 else "checks"

        status_printer_thread = StatusPrinterThread(
            f"{len(lint_files)} {check} in {checks_path.relative_to(MZ_ROOT)}"
        )
        status_printer_thread.start()

        for lint_file in lint_files:
            thread = LintingThread(checks_path, lint_file, offline=self.offline)
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

        status_printer_thread.stop()

        failed_checks = []

        for thread in sorted(threads, key=lambda thread: thread.duration):
            formatted_duration = (
                f" [{thread.duration.total_seconds():5.2f}s]"
                if self.print_duration
                else ""
            )
            if thread.success:
                status = f"{OK}{formatted_duration}"
                print(f"{prefix('---')}{status} {thread.name}")
            else:
                status = f"{FAIL}{formatted_duration}"
                print(f"{prefix('+++')}{status} {thread.name}")
                failed_checks.append(thread.name)

            if thread.has_output() and (not thread.success or self.verbose_output):
                print(thread.output)

        return failed_checks


class LintingThread(threading.Thread):
    def __init__(self, checks_path: Path, lint_file: str, offline: bool):
        super().__init__(target=self.run_single_script, args=(checks_path, lint_file))
        self.name = lint_file
        self.offline = offline
        self.output: str = ""
        self.success = False
        self.duration: timedelta | None = None

    def run_single_script(self, directory_path: Path, file_name: str) -> None:
        start_time = datetime.now()
        command = [str(directory_path / file_name)]

        if self.offline:
            command.append("--offline")

        try:
            # Note that coloring gets lost (e.g., in git diff)
            proc = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            stdout, _ = proc.communicate()
            self.success = proc.returncode == 0
            self.capture_output(stdout)
        except Exception as e:
            print(f"Error: {e}")
            self.success = False

        end_time = datetime.now()
        self.duration = end_time - start_time

    def capture_output(self, stdout: bytes) -> None:
        # stdout contains both stdout and stderr because stderr is piped there
        self.output = stdout.decode("utf-8").strip()

    def has_output(self) -> bool:
        return len(self.output) > 0


class StatusPrinterThread(threading.Thread):
    def __init__(self, current_step: str) -> None:
        super().__init__(target=self.print_status, args=())
        self.active = not buildkite.is_in_buildkite()
        self.current_step = current_step

    def print_status(self) -> None:
        symbols = ["⣾", "⣷", "⣯", "⣟", "⡿", "⢿", "⣻", "⣽"]

        i = 0
        while self.active:
            print(f"\r\033[K{symbols[i]} {self.current_step}", end="", flush=True)
            i = (i + 1) % len(symbols)
            time.sleep(0.1)

    def stop(self) -> None:
        if self.active:
            self.active = False
            print(f"\r\033[K{prefix()}{self.current_step}", end="", flush=True)
            print()


if __name__ == "__main__":
    exit(main())
