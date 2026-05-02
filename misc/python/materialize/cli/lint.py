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
from pathlib import Path

from materialize import MZ_ROOT
from materialize.parallel_task import FAIL, OK, _prefix, run_parallel

MAIN_PATH = MZ_ROOT / "ci" / "test" / "lint-main"
MAIN_CHECKS_PATH = MAIN_PATH / "checks"
CHECK_BEFORE_PATH = MAIN_PATH / "before"
CHECK_AFTER_PATH = MAIN_PATH / "after"


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
            _prefix("+++") + f"{OK} All checks successful"
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
                f"{_prefix()}Skipping checks in '{checks_path}' due to previous failures"
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

        check = "check" if len(lint_files) == 1 else "checks"
        rel_path = checks_path.relative_to(MZ_ROOT)

        tasks = []
        for lint_file in lint_files:
            command = [str(checks_path / lint_file)]
            if self.offline:
                command.append("--offline")
            tasks.append((lint_file, command))

        return run_parallel(
            tasks,
            verbose=self.verbose_output,
            print_duration=self.print_duration,
            spinner_suffix=f"{check} in {rel_path}",
            print_summary=False,
        )


if __name__ == "__main__":
    exit(main())
