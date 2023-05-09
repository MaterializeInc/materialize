# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from typing import cast

from materialize.output_consistency.execution.comparison_outcome import ComparisonOutcome
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

        self.validate_outcome_metadata(query_execution.outcomes, comparison_outcome)

        if not comparison_outcome.success():
            # do not continue with value comparison if metadata differs
            return comparison_outcome

        queries_succeeded = query_execution.outcomes[0].successful

        if queries_succeeded:
            self.validate_outcome_data(query_execution.outcomes, comparison_outcome)

        return comparison_outcome

    def validate_outcome_metadata(self, outcomes: list[QueryOutcome], comparison_outcome: ComparisonOutcome) -> None:
        outcome1 = outcomes[0]

        for index in range(1, len(outcomes)):
            self.validate_two_outcome_metadata(outcome1, outcomes[index], comparison_outcome)

    def validate_two_outcome_metadata(
        self, outcome1: QueryOutcome, outcome2: QueryOutcome,comparison_outcome: ComparisonOutcome
    ) -> None:
        if outcome1.successful != outcome2.successful:
            comparison_outcome.add_error("Outcome differs", value1=outcome1.__class__.__name__, value2=outcome2.__class__.__name__, strategy1= outcome1.strategy, strategy2= outcome2.strategy)
            return

        both_successful = outcome1.successful
        both_failed = not outcome1.successful

        # both failed, error messages must (somewhat) match
        if both_failed:
            error1 = cast(QueryFailure, outcome1).error_message
            error2 = cast(QueryFailure, outcome2).error_message

            if error1 != error2:
                comparison_outcome.add_error("Error differs", value1=error1, value2=error2, strategy1= outcome1.strategy, strategy2= outcome2.strategy)
                return

        # both succeeded, only check that number of entries match at this point
        if both_successful:
            result_value1 = cast(QueryResult, outcome1).result_data
            result_value2 = cast(QueryResult, outcome2).result_data

            if len(result_value1) == 0:
                raise RuntimeError("Contains no columns!")

            if len(result_value1) != len(result_value2):
                raise RuntimeError("Result count mismatch!")


    def validate_outcome_data(self, outcomes: list[QueryOutcome],comparison_outcome: ComparisonOutcome) -> None:
        num_columns = len(cast(QueryResult, outcomes[0]).result_data)

        for col_index in range(0, num_columns):
            self.validate_column(outcomes, col_index,comparison_outcome)

    def validate_column(self, outcomes: list[QueryOutcome], col_index: int, comparison_outcome: ComparisonOutcome) -> None:
        # all query outcomes are known to be successful

        first_result = cast(QueryResult, outcomes[0])

        for strategy_index in range(1, len(outcomes)):
            other_result = cast(QueryResult, outcomes[strategy_index])
            self.validate_two_values(
                first_result, other_result, col_index,comparison_outcome
            )

    def validate_two_values(
        self, result1: QueryResult, result2: QueryResult, col_index: int, comparison_outcome: ComparisonOutcome
    ) -> None:
        result_value1 = result1.result_data[col_index]
        result_value2 = result2.result_data[col_index]

        if result_value1 != result_value2:
            comparison_outcome.add_error("Error differs", value1=result_value1, value2=result_value2, strategy1= result1.strategy, strategy2= result2.strategy,  col_index=col_index)
