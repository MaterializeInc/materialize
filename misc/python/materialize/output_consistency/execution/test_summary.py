# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from typing import List


class ConsistencyTestLogger:
    def __init__(
        self,
    ) -> None:
        self.global_warnings: List[str] = []

    def add_global_warning(self, message: str) -> None:
        self.global_warnings.append(message)


class ConsistencyTestSummary(ConsistencyTestLogger):
    """Summary of the test execution"""

    def __init__(
        self,
        dry_run: bool = False,
        count_executed_query_templates: int = 0,
        count_successful_query_templates: int = 0,
        count_ignored_error_query_templates: int = 0,
        count_with_warning_query_templates: int = 0,
    ):
        super().__init__()
        self.mode = "LIVE_DATABASE" if not dry_run else "DRY_RUN"
        self.count_executed_query_templates = count_executed_query_templates
        self.count_successful_query_templates = count_successful_query_templates
        self.count_ignored_error_query_templates = count_ignored_error_query_templates
        self.count_with_warning_query_templates = count_with_warning_query_templates

    def all_passed(self) -> bool:
        return (
            self.count_successful_query_templates
            == self.count_executed_query_templates
            + self.count_ignored_error_query_templates
        )

    def __str__(self) -> str:
        count_accepted_queries = (
            self.count_successful_query_templates
            + self.count_ignored_error_query_templates
        )
        output_rows = [
            f"{count_accepted_queries}/{self.count_executed_query_templates} queries passed"
            f" in mode '{self.mode}'.",
            f"{self.count_ignored_error_query_templates} queries were ignored after execution.",
            f"{self.count_with_warning_query_templates} queries had warnings.",
        ]

        output_rows.extend(self._get_global_warning_rows())

        return "\n".join(output_rows)

    def _get_global_warning_rows(self) -> List[str]:
        if len(self.global_warnings) == 0:
            return []

        unique_global_warnings = set(self.global_warnings)

        warning_rows = [
            f"{len(unique_global_warnings)} unique, non-query specific warnings occurred:"
        ]

        for warning in unique_global_warnings:
            warning_rows.append(f"* {warning}")

        return warning_rows
