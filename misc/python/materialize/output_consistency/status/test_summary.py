# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from dataclasses import dataclass, field

from materialize.mzcompose.test_result import TestFailureDetails
from materialize.output_consistency.operation.operation import DbOperationOrFunction
from materialize.output_consistency.status.consistency_test_logger import (
    ConsistencyTestLogger,
)


@dataclass
class DbOperationOrFunctionStats:
    count_top_level: int = 0
    count_nested: int = 0
    count_generation_failed: int = 0


@dataclass
class ConsistencyTestSummary(ConsistencyTestLogger):
    """Summary of the test execution"""
    dry_run: bool = False
    mode: str = "UNKNOWN"
    count_executed_query_templates: int = 0
    count_successful_query_templates: int = 0
    count_ignored_error_query_templates: int = 0
    count_with_warning_query_templates: int = 0
    failures: list[TestFailureDetails] = field(default_factory=list)
    stats_by_operation_and_function: dict[
        DbOperationOrFunction, DbOperationOrFunctionStats
    ] = field(default_factory=dict)

    def __post_init__(self):
        self.mode = "LIVE_DATABASE" if not self.dry_run else "DRY_RUN"

    def add_failures(self, failures: list[TestFailureDetails]):
        self.failures.extend(failures)

    def all_passed(self) -> bool:
        all_passed = (
            self.count_executed_query_templates
            == self.count_successful_query_templates
            + self.count_ignored_error_query_templates
        )

        assert all_passed == (len(self.failures) == 0)
        return all_passed

    def get(self) -> str:
        count_accepted_queries = (
            self.count_successful_query_templates
            + self.count_ignored_error_query_templates
        )

        count_ok = count_accepted_queries
        count_all = self.count_executed_query_templates
        percentage = 100 * count_ok / count_all if count_all > 0 else 0

        output_rows = [
            f"{count_ok}/{count_all} ({round(percentage, 2)}%) queries passed"
            f" in mode '{self.mode}'.",
            f"{self.count_ignored_error_query_templates} queries were ignored after execution.",
            f"{self.count_with_warning_query_templates} queries had warnings.",
        ]

        output_rows.extend(self._get_global_warning_rows())

        return "\n".join(output_rows)

    def _get_global_warning_rows(self) -> list[str]:
        if len(self.global_warnings) == 0:
            return []

        unique_warnings_with_count = dict()
        for warning in self.global_warnings:
            unique_warnings_with_count[warning] = 1 + (
                unique_warnings_with_count.get(warning) or 0
            )

        unique_global_warnings = [
            f"{warning} ({count} occurrences)"
            for warning, count in unique_warnings_with_count.items()
        ]
        unique_global_warnings.sort()

        warning_rows = [
            f"{len(unique_global_warnings)} unique, non-query specific warnings occurred:"
        ]

        for warning in unique_global_warnings:
            warning_rows.append(f"* {warning}")

        return warning_rows

    def get_function_and_operation_stats(self) -> str:
        output = []

        for (
            operation_or_function,
            stats,
        ) in self.stats_by_operation_and_function.items():
            output.append(
                f"* {operation_or_function.to_description()}: {stats.count_top_level} top level, {stats.count_nested} nested, {stats.count_generation_failed} generation failed"
            )

        output.sort()

        return "\n".join(output)
