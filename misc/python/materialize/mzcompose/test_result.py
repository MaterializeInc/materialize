# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

from dataclasses import dataclass

from materialize.ui import UIError


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
    # depending on the check, this may either be a file name or a path
    location: str | None
    line_number: int | None

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
