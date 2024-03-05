# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import re
from dataclasses import dataclass

from materialize import MZ_ROOT
from materialize.ui import CommandFailureCausedUIError, UIError


@dataclass
class TestResult:
    duration: float
    errors: list[TestFailureDetails]

    def is_successful(self) -> bool:
        return len(self.errors) == 0


@dataclass
class TestFailureDetails:
    message: str
    details: str | None
    test_class_name_override: str | None = None
    test_case_name_override: str | None = None
    # depending on the check, this may either be a file name or a path
    location: str | None = None
    line_number: int | None = None

    def location_as_file_name(self) -> str | None:
        if self.location is None:
            return None

        if "/" in self.location:
            return self.location[self.location.rindex("/") + 1 :]

        return self.location


class FailedTestExecutionError(UIError):
    """
    An UIError that is caused by a failing test.
    """

    def __init__(
        self,
        message: str,
        errors: list[TestFailureDetails],
        hint: str | None = None,
    ):
        super().__init__(message, hint)
        self.errors = errors


def try_determine_errors_from_cmd_execution(
    e: CommandFailureCausedUIError,
) -> list[TestFailureDetails]:
    output = e.stderr or e.stdout

    if "running docker compose failed" in str(e):
        return [determine_error_from_docker_compose_failure(e, output)]

    if output is None:
        return []

    error_chunks = extract_error_chunks_from_output(output)

    collected_errors = []
    for chunk in error_chunks:
        match = re.search(r"([^.]+\.td):(\d+):\d+:", chunk)
        if match is not None:
            # for .td files like Postgres CDC, file_path will just contain the file name
            file_path = match.group(1)
            line_number = int(match.group(2))
        else:
            # for .py files like platform checks, file_path will be a path
            file_path = try_determine_error_location_from_cmd(e.cmd)
            if file_path is None or ":" not in file_path:
                line_number = None
            else:
                parts = file_path.split(":")
                file_path = parts[0]
                line_number = int(parts[1])

        message = (
            f"Executing {file_path if file_path is not None else 'command'} failed!"
        )

        failure_details = TestFailureDetails(
            message,
            details=chunk,
            location=file_path,
            line_number=line_number,
        )

        if failure_details in collected_errors:
            # do not add an identical error again
            pass
        else:
            collected_errors.append(failure_details)

    return collected_errors


def determine_error_from_docker_compose_failure(
    e: CommandFailureCausedUIError, output: str | None
) -> TestFailureDetails:
    command = " ".join([str(x) for x in e.cmd])
    return TestFailureDetails(
        f"Docker compose failed: {command}",
        details=output,
        location=None,
        line_number=None,
    )


def try_determine_error_location_from_cmd(cmd: list[str]) -> str | None:
    root_path_as_string = f"{MZ_ROOT}/"
    for cmd_part in cmd:
        if type(cmd_part) == str and cmd_part.startswith("--source="):
            return cmd_part.removeprefix("--source=").replace(root_path_as_string, "")

    return None


def extract_error_chunks_from_output(output: str) -> list[str]:
    if "+++ !!! Error Report" not in output:
        return []

    error_output = output[: output.index("+++ !!! Error Report") - 1]
    error_chunks = error_output.split("^^^ +++")

    return [chunk.strip() for chunk in error_chunks if len(chunk.strip()) > 0]
