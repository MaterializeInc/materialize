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
from materialize.util import filter_cmd

PEM_CONTENT_RE = r"-----BEGIN ([A-Z ]+)-----[^-]+-----END [A-Z ]+-----"
PEM_CONTENT_REPLACEMENT = r"<\1>"


@dataclass
class TestResult:
    __test__ = False

    duration: float
    errors: list[TestFailureDetails]

    def is_successful(self) -> bool:
        return len(self.errors) == 0


@dataclass
class TestFailureDetails:
    __test__ = False

    message: str
    details: str | None
    additional_details_header: str | None = None
    additional_details: str | None = None
    test_class_name_override: str | None = None
    """The test class usually describes the framework."""
    test_case_name_override: str | None = None
    """The test case usually describes the workflow, unless more fine-grained information is available."""
    location: str | None = None
    """depending on the check, this may either be a file name or a path"""
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
        errors: list[TestFailureDetails],
        error_summary: str = "At least one test failed",
    ):
        super().__init__(error_summary)
        self.errors = errors


def try_determine_errors_from_cmd_execution(
    e: CommandFailureCausedUIError, test_context: str | None
) -> list[TestFailureDetails]:
    output = e.stderr or e.stdout

    if "running docker compose failed" in str(e):
        return [determine_error_from_docker_compose_failure(e, output, test_context)]

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
            test_case_name_override=test_context,
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
    e: CommandFailureCausedUIError, output: str | None, test_context: str | None
) -> TestFailureDetails:
    command = to_sanitized_command_str(e.cmd)
    context_prefix = f"{test_context}: " if test_context is not None else ""
    return TestFailureDetails(
        f"{context_prefix}Docker compose failed: {command}",
        details=output,
        test_case_name_override=test_context,
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


def to_sanitized_command_str(cmd: list[str]) -> str:
    command_str = " ".join([str(x) for x in filter_cmd(cmd)])
    return re.sub(PEM_CONTENT_RE, PEM_CONTENT_REPLACEMENT, command_str)
