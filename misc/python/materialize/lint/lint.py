# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os
import subprocess
import threading
from datetime import datetime, timedelta
from pathlib import Path

from materialize import MZ_ROOT

MAIN_PATH = MZ_ROOT / "ci" / "test" / "lint-main"
MAIN_CHECKS_PATH = MAIN_PATH / "checks"
CHECK_BEFORE_PATH = MAIN_PATH / "before"
CHECK_AFTER_PATH = MAIN_PATH / "after"

PRINT_DURATION = False


def main() -> int:
    failed_checks = run_and_validate_if_no_previous_failures(
        CHECK_BEFORE_PATH, previous_failures=[]
    )
    failed_checks = run_and_validate_if_no_previous_failures(
        MAIN_CHECKS_PATH, previous_failures=failed_checks
    )
    failed_checks = run_and_validate_if_no_previous_failures(
        CHECK_AFTER_PATH, previous_failures=failed_checks
    )

    success = len(failed_checks) == 0

    print("+++ Linting result")
    print("All checks successful." if success else f"Checks failed: {failed_checks}")

    return 0 if success else 1


def is_ignore_file(path: Path) -> bool:
    return os.path.isdir(path)


def run_and_validate_if_no_previous_failures(
    checks_path: Path, previous_failures: list[str]
) -> list[str]:
    if len(previous_failures) > 0:
        print(f"--- Skipping checks in '{checks_path}' due to previous failures")
        return previous_failures
    else:
        return run_and_validate(checks_path)


def run_and_validate(checks_path: Path) -> list[str]:
    """
    Runs checks in the given directory and validates their outcome.
    :return: names of failed checks
    """
    print(f"--- Running checks in {checks_path}")

    paths = os.listdir(checks_path)

    threads = []

    for lint_file in paths:
        if is_ignore_file(checks_path / lint_file):
            continue

        thread = LintingThread(checks_path, lint_file)
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    failed_checks = []

    for thread in threads:
        formatted_duration = (
            f" in {thread.duration.total_seconds():.2f}s" if PRINT_DURATION else ""
        )
        if thread.success:
            print(f"--- {thread.name} (SUCCEEDED{formatted_duration})")
        else:
            print(f"+++ {thread.name} (FAILED{formatted_duration})")
            failed_checks.append(thread.name)

        if thread.has_output():
            print(thread.output)

    return failed_checks


class LintingThread(threading.Thread):
    def __init__(self, checks_path: Path, lint_file: str):
        super().__init__(target=self.run_single_script, args=(checks_path, lint_file))
        self.name = lint_file
        self.output: str = ""
        self.success = False
        self.duration: timedelta | None = None

    def run_single_script(self, directory_path: Path, file_name: str) -> None:
        start_time = datetime.now()

        try:
            # Note that coloring gets lost (e.g., in git diff)
            result = subprocess.run(
                directory_path / file_name,
                # TODO syserr
                # stderr=subprocess.STDOUT,
                capture_output=True,
                check=True,
            )

            self.capture_output(result.stdout)
            self.success = True
        except subprocess.CalledProcessError as e:
            self.capture_output(e.stdout)
            self.success = False
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


if __name__ == "__main__":
    main()
