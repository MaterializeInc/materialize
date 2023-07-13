# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from enum import Enum
from typing import List, Optional, Sequence

from materialize.output_consistency.ignore_filter.inconsistency_ignore_filter import (
    InconsistencyIgnoreFilter,
    YesIgnore,
)
from materialize.output_consistency.output.format_constants import LI_PREFIX
from materialize.output_consistency.validation.validation_message import (
    ValidationError,
    ValidationMessage,
    ValidationRemark,
    ValidationWarning,
)


class ValidationVerdict(Enum):
    SUCCESS = 1
    SUCCESS_WITH_WARNINGS = 2
    IGNORED_FAILURE = 3
    FAILURE = 4

    def succeeded(self) -> bool:
        return self in {
            ValidationVerdict.SUCCESS,
            ValidationVerdict.SUCCESS_WITH_WARNINGS,
        }

    def accepted(self) -> bool:
        return self in {
            ValidationVerdict.SUCCESS,
            ValidationVerdict.SUCCESS_WITH_WARNINGS,
            ValidationVerdict.IGNORED_FAILURE,
        }


class ValidationOutcome:
    """Outcome of a result comparison"""

    def __init__(self) -> None:
        self.success_reason: Optional[str] = None
        self.count_ignored_errors = 0
        self.errors: List[ValidationError] = []
        self.warnings: List[ValidationWarning] = []
        self.remarks: List[ValidationRemark] = []

    def add_error(
        self, ignore_filter: InconsistencyIgnoreFilter, error: ValidationError
    ) -> None:
        ignore_verdict = ignore_filter.shall_ignore_error(error)
        if isinstance(ignore_verdict, YesIgnore):
            self.count_ignored_errors += 1
            self.add_warning(
                ValidationWarning(
                    f"Ignoring {error.error_type.name} ({error.message}) because of {ignore_verdict.reason}",
                    f"SQL is {error.query_execution.generic_sql}",
                )
            )
        else:
            self.errors.append(error)

    def add_warning(self, warning: ValidationWarning) -> None:
        self.warnings.append(warning)

    def add_remark(self, remark: ValidationRemark) -> None:
        self.remarks.append(remark)

    def verdict(self) -> ValidationVerdict:
        if self.has_errors():
            return ValidationVerdict.FAILURE
        if self.has_ignored_errors():
            return ValidationVerdict.IGNORED_FAILURE
        if self.has_warnings():
            return ValidationVerdict.SUCCESS_WITH_WARNINGS
        else:
            return ValidationVerdict.SUCCESS

    def has_ignored_errors(self) -> bool:
        return self.count_ignored_errors > 0

    def has_errors(self) -> bool:
        return len(self.errors) > 0

    def has_warnings(self) -> bool:
        return len(self.warnings) > 0

    def has_remarks(self) -> bool:
        return len(self.remarks) > 0

    def error_output(self) -> str:
        return self._problem_marker_output(self.errors)

    def warning_output(self) -> str:
        return self._problem_marker_output(self.warnings)

    def remark_output(self) -> str:
        return self._problem_marker_output(self.remarks)

    def _problem_marker_output(self, entries: Sequence[ValidationMessage]) -> str:
        if len(entries) == 0:
            return ""

        return "\n".join(f"{LI_PREFIX}{str(entry)}" for entry in entries)
