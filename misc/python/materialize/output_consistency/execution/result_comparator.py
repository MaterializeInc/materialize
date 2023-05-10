# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from typing import cast

from materialize.output_consistency.execution.comparison_outcome import (
    ComparisonOutcome,
)
from materialize.output_consistency.query.query_result import (
    QueryExecution,
    QueryFailure,
    QueryOutcome,
    QueryResult,
)


class ResultComparator:
    def compare_results(self, query_execution: QueryExecution) -> ComparisonOutcome:
        print(f"Query {query_execution.index}: {query_execution.generic_sql}")
        comparison_outcome = ComparisonOutcome(query_execution)

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
        self, outcomes: list[QueryOutcome], comparison_outcome: ComparisonOutcome
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
        comparison_outcome: ComparisonOutcome,
    ) -> None:
        if outcome1.successful != outcome2.successful:
            comparison_outcome.add_error(
                "Outcome differs",
                value1=outcome1.__class__.__name__,
                value2=outcome2.__class__.__name__,
                strategy1=outcome1.strategy,
                strategy2=outcome2.strategy,
            )
            return

        both_successful = outcome1.successful
        both_failed = not outcome1.successful

        # both failed, error messages must (somewhat) match
        if both_failed:
            error1 = cast(QueryFailure, outcome1).error_message
            error2 = cast(QueryFailure, outcome2).error_message

            if error1 != error2:
                comparison_outcome.add_error(
                    "Error differs",
                    value1=error1,
                    value2=error2,
                    strategy1=outcome1.strategy,
                    strategy2=outcome2.strategy,
                )
                return

        # both succeeded, only check that number of entries match at this point
        if both_successful:
            num_rows1 = len(cast(QueryResult, outcome1).result_rows)
            num_rows2 = len(cast(QueryResult, outcome2).result_rows)

            if num_rows1 == 0 and num_rows2 == 0:
                # no rows in both results, this is ok
                return

            if num_rows1 != num_rows2:
                comparison_outcome.add_error(
                    "Row count differs",
                    value1=str(num_rows1),
                    value2=str(num_rows2),
                    strategy1=outcome1.strategy,
                    strategy2=outcome2.strategy,
                )

    def validate_outcomes_data(
        self, outcomes: list[QueryOutcome], comparison_outcome: ComparisonOutcome
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
        comparison_outcome: ComparisonOutcome,
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
        comparison_outcome: ComparisonOutcome,
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
                    location=f"row index {row_index}, column index {col_index}",
                )
