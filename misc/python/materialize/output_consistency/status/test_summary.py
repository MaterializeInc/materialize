# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from __future__ import annotations

from dataclasses import dataclass, field

from materialize.mzcompose.test_result import TestFailureDetails
from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.expression.expression_with_args import (
    ExpressionWithArgs,
)
from materialize.output_consistency.operation.operation import DbOperationOrFunction
from materialize.output_consistency.output.reproduction_code_printer import (
    ReproductionCodePrinter,
)
from materialize.output_consistency.query.query_template import QueryTemplate
from materialize.output_consistency.status.consistency_test_logger import (
    ConsistencyTestLogger,
)
from materialize.output_consistency.validation.validation_outcome import (
    ValidationOutcome,
    ValidationVerdict,
)


@dataclass
class DbOperationOrFunctionStats:
    count_top_level_expression_generated: int = 0
    count_nested_expression_generated: int = 0
    count_expression_generation_failed: int = 0
    count_included_in_executed_queries: int = 0
    count_included_in_successfully_executed_queries: int = 0

    def to_description(self) -> str:
        if self.count_included_in_successfully_executed_queries:
            success_experienced_info = "successfully executed at least once"
        else:
            count_generated = (
                self.count_top_level_expression_generated
                + self.count_nested_expression_generated
            )
            if count_generated == 0:
                success_experienced_info = "expression never generated"
            elif self.count_included_in_executed_queries == 0:
                success_experienced_info = "query with this expression never generated"
            elif self.count_included_in_executed_queries < 15:
                success_experienced_info = "not included in any query that was successfully executed in all strategies!"
            else:
                success_experienced_info = "not included in any query that was successfully executed in all strategies (possibly invalid operation specification)!"

        return (
            f"{self.count_top_level_expression_generated} top level, "
            f"{self.count_nested_expression_generated} nested, "
            f"{self.count_expression_generation_failed} generation failed, "
            f"{success_experienced_info}"
        )

    def merge(self, other: DbOperationOrFunctionStats) -> None:
        self.count_top_level_expression_generated = (
            self.count_top_level_expression_generated
            + other.count_top_level_expression_generated
        )
        self.count_nested_expression_generated = (
            self.count_nested_expression_generated
            + other.count_nested_expression_generated
        )
        self.count_expression_generation_failed = (
            self.count_expression_generation_failed
            + other.count_expression_generation_failed
        )
        self.count_included_in_executed_queries = (
            self.count_included_in_executed_queries
            + other.count_included_in_executed_queries
        )
        self.count_included_in_successfully_executed_queries = (
            self.count_included_in_successfully_executed_queries
            + other.count_included_in_successfully_executed_queries
        )


@dataclass
class DbOperationVariant:
    operation: DbOperationOrFunction
    param_count: int

    def to_description(self) -> str:
        return self.operation.to_description(self.param_count)

    def __hash__(self):
        return hash(self.to_description())


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
    stats_by_operation_variant: dict[DbOperationVariant, DbOperationOrFunctionStats] = (
        field(default_factory=dict)
    )
    count_available_data_types: int = 0
    count_available_op_variants: int = 0
    count_predefined_queries: int = 0
    count_generated_select_expressions: int = 0
    count_ignored_select_expressions: int = 0
    used_ignore_reasons: set[str] = field(default_factory=set)

    def __post_init__(self):
        self.mode = "LIVE_DATABASE" if not self.dry_run else "DRY_RUN"

    def count_failures(self) -> int:
        return len(self.failures)

    def merge(self, other: ConsistencyTestSummary) -> None:
        assert self.dry_run == other.dry_run
        assert self.mode == other.mode

        self.count_executed_query_templates = (
            self.count_executed_query_templates + other.count_executed_query_templates
        )
        self.count_successful_query_templates = (
            self.count_successful_query_templates
            + other.count_successful_query_templates
        )
        self.count_ignored_error_query_templates = (
            self.count_ignored_error_query_templates
            + other.count_ignored_error_query_templates
        )
        self.count_with_warning_query_templates = (
            self.count_with_warning_query_templates
            + other.count_with_warning_query_templates
        )
        self.failures.extend(other.failures)

        for operation_variant, other_stats in other.stats_by_operation_variant.items():
            stats = self.stats_by_operation_variant.get(operation_variant)
            if stats is None:
                self.stats_by_operation_variant[operation_variant] = other_stats
            else:
                stats.merge(other_stats)

        self.count_available_data_types = max(
            self.count_available_data_types, other.count_available_data_types
        )
        self.count_available_op_variants = max(
            self.count_available_op_variants, other.count_available_op_variants
        )
        self.count_predefined_queries = max(
            self.count_predefined_queries, other.count_predefined_queries
        )

        self.count_generated_select_expressions = (
            self.count_generated_select_expressions
            + other.count_generated_select_expressions
        )
        self.count_ignored_select_expressions = (
            self.count_ignored_select_expressions
            + other.count_ignored_select_expressions
        )

    def add_failures(self, failures: list[TestFailureDetails]) -> None:
        self.failures.extend(failures)

    def record_ignore_reason_usage(self, reason: str) -> None:
        self.used_ignore_reasons.add(reason)

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
            operation_variant,
            stats,
        ) in self.stats_by_operation_variant.items():
            output.append(
                f"* {operation_variant.to_description()}: {stats.to_description()}"
            )

        output.sort()

        return "\n".join(output)

    def format_used_ignore_entries(self) -> str:
        output = []

        for ignore_reason in self.used_ignore_reasons:
            output.append(f"* {ignore_reason}")

        output.sort()
        return "\n".join(output)

    def count_used_ops(self) -> int:
        return len(self.stats_by_operation_variant)

    def accept_expression_generation_statistics(
        self,
        operation: DbOperationOrFunction,
        expression: ExpressionWithArgs | None,
        number_of_args: int,
        is_top_level: bool = True,
    ) -> None:
        operation_variant = DbOperationVariant(operation, number_of_args)
        stats = self.stats_by_operation_variant.get(operation_variant)

        if stats is None:
            stats = DbOperationOrFunctionStats()
            self.stats_by_operation_variant[operation_variant] = stats

        if expression is None:
            assert is_top_level, "expressions at nested levels must not be None"
            stats.count_expression_generation_failed = (
                stats.count_expression_generation_failed + 1
            )
            return

        if is_top_level:
            stats.count_top_level_expression_generated = (
                stats.count_top_level_expression_generated + 1
            )
        else:
            stats.count_nested_expression_generated = (
                stats.count_nested_expression_generated + 1
            )

        for arg in expression.args:
            if isinstance(arg, ExpressionWithArgs):
                self.accept_expression_generation_statistics(
                    operation=arg.operation,
                    expression=arg,
                    number_of_args=arg.count_args(),
                    is_top_level=False,
                )

    def accept_execution_result(
        self,
        query: QueryTemplate,
        test_outcome: ValidationOutcome,
        reproduction_code_printer: ReproductionCodePrinter,
    ) -> None:
        self.count_executed_query_templates += 1
        verdict = test_outcome.verdict()

        if verdict in {
            ValidationVerdict.SUCCESS,
            ValidationVerdict.SUCCESS_WITH_WARNINGS,
        }:
            self.count_successful_query_templates += 1
        elif verdict == ValidationVerdict.IGNORED_FAILURE:
            self.count_ignored_error_query_templates += 1
        elif verdict == ValidationVerdict.FAILURE:
            self.add_failures(
                test_outcome.to_failure_details(reproduction_code_printer)
            )
        else:
            raise RuntimeError(f"Unexpected verdict: {verdict}")

        if test_outcome.has_warnings():
            self.count_with_warning_query_templates += 1

        self._accept_executed_query(
            query, test_outcome.query_execution_succeeded_in_all_strategies
        )

    def _accept_executed_query(
        self, query: QueryTemplate, successfully_executed_in_all_strategies: bool
    ) -> None:
        # only consider expressions in the SELECT part for now

        for expression in query.select_expressions:
            self._accept_expression_in_executed_query(
                expression,
                successfully_executed_in_all_strategies,
            )

    def _accept_expression_in_executed_query(
        self, expression: Expression, successfully_executed_in_all_strategies: bool
    ) -> None:
        if not isinstance(expression, ExpressionWithArgs):
            return

        operation_variant = DbOperationVariant(
            expression.operation, expression.count_args()
        )
        stats = self.stats_by_operation_variant.get(operation_variant)
        assert (
            stats is not None
        ), f"no stats for {operation_variant.to_description()} found"

        stats.count_included_in_executed_queries = (
            stats.count_included_in_executed_queries + 1
        )

        if successfully_executed_in_all_strategies:
            stats.count_included_in_successfully_executed_queries = (
                stats.count_included_in_successfully_executed_queries + 1
            )

        for arg in expression.args:
            self._accept_expression_in_executed_query(
                arg, successfully_executed_in_all_strategies
            )
