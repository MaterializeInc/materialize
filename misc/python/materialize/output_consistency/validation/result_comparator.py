# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from typing import cast

from materialize.output_consistency.query.query_result import (
    QueryExecution,
    QueryFailure,
    QueryOutcome,
    QueryResult,
)
from materialize.output_consistency.validation.validation_outcome import (
    ValidationOutcome,
)


class ResultComparator:
    def compare_results(self, query_execution: QueryExecution) -> ValidationOutcome:
        comparison_outcome = ValidationOutcome(query_execution)

        if len(query_execution.outcomes) == 0:
            raise RuntimeError("Contains no outcomes!")

        if len(query_execution.outcomes) == 1:
            raise RuntimeError("Contains only one outcome, nothing to compare against!")

        self.validate_outcomes_metadata(query_execution.outcomes, comparison_outcome)

        if not comparison_outcome.success():
            # do not continue with value comparison if metadata differs
            return comparison_outcome

        queries_succeeded = query_execution.outcomes[0].successful

        if queries_succeeded:
            self.validate_outcomes_data(query_execution.outcomes, comparison_outcome)

        return comparison_outcome

    def validate_outcomes_metadata(
        self, outcomes: list[QueryOutcome], comparison_outcome: ValidationOutcome
    ) -> None:
        outcome1 = outcomes[0]

        for index in range(1, len(outcomes)):
            self.validate_metadata_of_two_outcomes(
                outcome1, outcomes[index], comparison_outcome
            )

    def validate_metadata_of_two_outcomes(
        self,
        outcome1: QueryOutcome,
        outcome2: QueryOutcome,
        comparison_outcome: ValidationOutcome,
    ) -> None:
        if outcome1.successful != outcome2.successful:
            comparison_outcome.add_error(
                "Outcome differs",
                value1=outcome1.__class__.__name__,
                value2=outcome2.__class__.__name__,
                strategy1=outcome1.strategy,
                strategy2=outcome2.strategy,
                sql1=outcome1.sql,
                sql2=outcome2.sql,
            )
            return

        both_successful = outcome1.successful and outcome2.successful
        both_failed = not outcome1.successful and not outcome2.successful

        if both_successful:
            self.validate_row_count(
                cast(QueryResult, outcome1),
                cast(QueryResult, outcome2),
                comparison_outcome,
            )

        if both_failed:
            self.validate_error_messages(
                cast(QueryFailure, outcome1),
                cast(QueryFailure, outcome2),
                comparison_outcome,
            )

        if not both_successful:
            any_failure = outcome1 if not outcome1.successful else outcome2
            self.warn_on_failure_with_multiple_columns(
                cast(QueryFailure, any_failure), comparison_outcome
            )

    def validate_row_count(
        self,
        result1: QueryResult,
        result2: QueryResult,
        comparison_outcome: ValidationOutcome,
    ) -> None:
        # It is ok if both results don't have any rows.

        num_rows1 = len(result1.result_rows)
        num_rows2 = len(result2.result_rows)

        if num_rows1 != num_rows2:
            comparison_outcome.add_error(
                "Row count differs",
                value1=str(num_rows1),
                value2=str(num_rows2),
                strategy1=result1.strategy,
                strategy2=result2.strategy,
                sql1=result1.sql,
                sql2=result2.sql,
            )

    def validate_error_messages(
        self,
        failure1: QueryFailure,
        failure2: QueryFailure,
        comparison_outcome: ValidationOutcome,
    ) -> None:
        if failure1.error_message != failure2.error_message:
            comparison_outcome.add_error(
                "Error differs",
                value1=failure1.error_message,
                value2=failure2.error_message,
                strategy1=failure1.strategy,
                strategy2=failure2.strategy,
                sql1=failure1.sql,
                sql2=failure2.sql,
            )

    def warn_on_failure_with_multiple_columns(
        self,
        failure: QueryFailure,
        comparison_outcome: ValidationOutcome,
    ) -> None:
        if failure.query_column_count > 1:
            comparison_outcome.add_warning(
                "Query error with multiple columns",
                "Queries expected to return an error should contain only one colum.",
                sql=failure.sql,
            )

    def validate_outcomes_data(
        self, outcomes: list[QueryOutcome], comparison_outcome: ValidationOutcome
    ) -> None:
        # each outcome is known to contain at least one row
        # each row is supposed to have the same number of columns

        result_outcome1 = cast(QueryResult, outcomes[0])

        for index in range(1, len(outcomes)):
            other_outcome = cast(QueryResult, outcomes[index])
            self.validate_data_of_two_outcomes(
                result_outcome1, other_outcome, comparison_outcome
            )

    def validate_data_of_two_outcomes(
        self,
        outcome1: QueryResult,
        outcome2: QueryResult,
        comparison_outcome: ValidationOutcome,
    ) -> None:
        num_columns1 = len(outcome1.result_rows[0])
        num_columns2 = len(outcome2.result_rows[0])

        if num_columns1 == 0:
            raise RuntimeError("Result contains no columns!")

        if num_columns1 != num_columns2:
            raise RuntimeError("Results count different number of columns!")

        for col_index in range(0, num_columns1):
            self.validate_column(outcome1, outcome2, col_index, comparison_outcome)

    def validate_column(
        self,
        result1: QueryResult,
        result2: QueryResult,
        col_index: int,
        comparison_outcome: ValidationOutcome,
    ) -> None:
        # both results are known to be not empty and have the same number of rows
        row_length = len(result1.result_rows)

        for row_index in range(0, row_length):
            result_value1 = result1.result_rows[row_index][col_index]
            result_value2 = result2.result_rows[row_index][col_index]

            if result_value1 != result_value2:
                comparison_outcome.add_error(
                    "Value differs",
                    value1=result_value1,
                    value2=result_value2,
                    strategy1=result1.strategy,
                    strategy2=result2.strategy,
                    sql1=result1.sql,
                    sql2=result2.sql,
                    location=f"row index {row_index}, column index {col_index}",
                )
