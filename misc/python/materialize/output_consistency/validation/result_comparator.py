# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
import math
from decimal import Decimal
from typing import Any, cast

from materialize.output_consistency.ignore_filter.inconsistency_ignore_filter import (
    InconsistencyIgnoreFilter,
)
from materialize.output_consistency.query.query_result import (
    QueryExecution,
    QueryFailure,
    QueryOutcome,
    QueryResult,
)
from materialize.output_consistency.validation.error_message_normalizer import (
    ErrorMessageNormalizer,
)
from materialize.output_consistency.validation.validation_message import (
    ValidationError,
    ValidationErrorType,
    ValidationRemark,
    ValidationWarning,
)
from materialize.output_consistency.validation.validation_outcome import (
    ValidationOutcome,
)


class ResultComparator:
    """Compares the outcome (result or failure) of multiple query executions"""

    def __init__(self, ignore_filter: InconsistencyIgnoreFilter):
        self.ignore_filter = ignore_filter
        self.error_message_normalizer = ErrorMessageNormalizer()

    def compare_results(self, query_execution: QueryExecution) -> ValidationOutcome:
        validation_outcome = ValidationOutcome()

        if len(query_execution.outcomes) == 0:
            raise RuntimeError("Contains no outcomes!")

        if len(query_execution.outcomes) == 1:
            raise RuntimeError("Contains only one outcome, nothing to compare against!")

        if query_execution.query_template.expect_error:
            validation_outcome.add_remark(ValidationRemark("DB error is possible"))

        self.validate_outcomes_metadata(query_execution, validation_outcome)

        if not validation_outcome.success():
            # do not continue with value comparison if metadata differs
            return validation_outcome

        queries_succeeded = query_execution.outcomes[0].successful

        if queries_succeeded:
            self.validate_outcomes_data(query_execution, validation_outcome)
            validation_outcome.success_reason = "result data matches"
        else:
            # error messages were already validated at metadata validation
            validation_outcome.success_reason = "error message matches"

        return validation_outcome

    def validate_outcomes_metadata(
        self, query_execution: QueryExecution, validation_outcome: ValidationOutcome
    ) -> None:
        outcomes = query_execution.outcomes
        outcome1 = outcomes[0]

        for index in range(1, len(outcomes)):
            self.validate_metadata_of_two_outcomes(
                query_execution, outcome1, outcomes[index], validation_outcome
            )

    def validate_metadata_of_two_outcomes(
        self,
        query_execution: QueryExecution,
        outcome1: QueryOutcome,
        outcome2: QueryOutcome,
        validation_outcome: ValidationOutcome,
    ) -> None:
        if outcome1.successful != outcome2.successful:
            validation_outcome.add_error(
                self.ignore_filter,
                ValidationError(
                    query_execution,
                    ValidationErrorType.SUCCESS_MISMATCH,
                    "Outcome differs",
                    value1=outcome1.__class__.__name__,
                    value2=outcome2.__class__.__name__,
                    strategy1=outcome1.strategy,
                    strategy2=outcome2.strategy,
                    sql1=outcome1.sql,
                    sql2=outcome2.sql,
                ),
            )
            return

        both_successful = outcome1.successful and outcome2.successful
        both_failed = not outcome1.successful and not outcome2.successful

        if both_successful:
            self.validate_row_count(
                query_execution,
                cast(QueryResult, outcome1),
                cast(QueryResult, outcome2),
                validation_outcome,
            )

            # this needs will no longer be sensible when more than two evaluation strategies are used
            self.remark_on_success_with_single_column(outcome1, validation_outcome)

        if (
            both_failed
            and not query_execution.query_template.disable_error_message_validation
        ):
            failure1 = cast(QueryFailure, outcome1)
            self.validate_error_messages(
                query_execution,
                failure1,
                cast(QueryFailure, outcome2),
                validation_outcome,
            )

        if not both_successful:
            any_failure = cast(
                QueryFailure, outcome1 if not outcome1.successful else outcome2
            )
            validation_outcome.add_remark(
                ValidationRemark(
                    f"DB error in '{any_failure.strategy.name}' was: {any_failure.error_message}"
                )
            )
            self.warn_on_failure_with_multiple_columns(any_failure, validation_outcome)

    def validate_row_count(
        self,
        query_execution: QueryExecution,
        result1: QueryResult,
        result2: QueryResult,
        validation_outcome: ValidationOutcome,
    ) -> None:
        # It is ok if both results don't have any rows.

        num_rows1 = len(result1.result_rows)
        num_rows2 = len(result2.result_rows)

        if num_rows1 != num_rows2:
            validation_outcome.add_error(
                self.ignore_filter,
                ValidationError(
                    query_execution,
                    ValidationErrorType.ROW_COUNT_MISMATCH,
                    "Row count differs",
                    value1=str(num_rows1),
                    value2=str(num_rows2),
                    strategy1=result1.strategy,
                    strategy2=result2.strategy,
                    sql1=result1.sql,
                    sql2=result2.sql,
                ),
            )

    def validate_error_messages(
        self,
        query_execution: QueryExecution,
        failure1: QueryFailure,
        failure2: QueryFailure,
        validation_outcome: ValidationOutcome,
    ) -> None:
        norm_error_message_1 = self.error_message_normalizer.normalize(
            failure1.error_message
        )
        norm_error_message_2 = self.error_message_normalizer.normalize(
            failure2.error_message
        )

        if norm_error_message_1 != norm_error_message_2:
            validation_outcome.add_error(
                self.ignore_filter,
                ValidationError(
                    query_execution,
                    ValidationErrorType.ERROR_MISMATCH,
                    "Error message differs",
                    value1=norm_error_message_1,
                    value2=norm_error_message_2,
                    strategy1=failure1.strategy,
                    strategy2=failure2.strategy,
                    sql1=failure1.sql,
                    sql2=failure2.sql,
                ),
            )

    def warn_on_failure_with_multiple_columns(
        self,
        failure: QueryOutcome,
        validation_outcome: ValidationOutcome,
    ) -> None:
        if failure.query_column_count > 1:
            # this should not occur if the config property 'split_and_retry_on_db_error' is enabled
            validation_outcome.add_warning(
                ValidationWarning(
                    "Query error with multiple columns",
                    "Query expected to return an error should contain only one colum.",
                    sql=failure.sql,
                )
            )

    def remark_on_success_with_single_column(
        self,
        result: QueryOutcome,
        validation_outcome: ValidationOutcome,
    ) -> None:
        if result.query_column_count == 1:
            validation_outcome.add_remark(
                ValidationRemark(
                    "Query success with single column",
                    "Query successfully returning a value could be run with other queries.",
                    sql=result.sql,
                )
            )

    def validate_outcomes_data(
        self,
        query_execution: QueryExecution,
        validation_outcome: ValidationOutcome,
    ) -> None:
        # each outcome is known to have the same number of rows
        # each row is supposed to have the same number of columns

        outcomes = query_execution.outcomes
        result1 = cast(QueryResult, outcomes[0])

        if len(result1.result_rows) == 0:
            # this is a valid case; all outcomes have the same number of rows
            return

        for index in range(1, len(outcomes)):
            other_result = cast(QueryResult, outcomes[index])
            self.validate_data_of_two_outcomes(
                query_execution, result1, other_result, validation_outcome
            )

    def validate_data_of_two_outcomes(
        self,
        query_execution: QueryExecution,
        outcome1: QueryResult,
        outcome2: QueryResult,
        validation_outcome: ValidationOutcome,
    ) -> None:
        num_columns1 = len(outcome1.result_rows[0])
        num_columns2 = len(outcome2.result_rows[0])

        if num_columns1 == 0:
            raise RuntimeError("Result contains no columns!")

        if num_columns1 != num_columns2:
            raise RuntimeError("Results count different number of columns!")

        for col_index in range(0, num_columns1):
            self.validate_column(
                query_execution, outcome1, outcome2, col_index, validation_outcome
            )

    def validate_column(
        self,
        query_execution: QueryExecution,
        result1: QueryResult,
        result2: QueryResult,
        col_index: int,
        validation_outcome: ValidationOutcome,
    ) -> None:
        # both results are known to be not empty and have the same number of rows
        row_length = len(result1.result_rows)

        for row_index in range(0, row_length):
            result_value1 = result1.result_rows[row_index][col_index]
            result_value2 = result2.result_rows[row_index][col_index]
            expression = query_execution.query_template.select_expressions[
                col_index
            ].to_sql(True)

            if not self.is_value_equal(result_value1, result_value2):
                validation_outcome.add_error(
                    self.ignore_filter,
                    ValidationError(
                        query_execution,
                        ValidationErrorType.CONTENT_MISMATCH,
                        "Value differs",
                        value1=result_value1,
                        value2=result_value2,
                        strategy1=result1.strategy,
                        strategy2=result2.strategy,
                        sql1=result1.sql,
                        sql2=result2.sql,
                        col_index=col_index,
                        location=f"row index {row_index}, column index {col_index} ('{expression}')",
                    ),
                )

    def is_value_equal(self, value1: Any, value2: Any) -> bool:
        if value1 == value2:
            return True

        if isinstance(value1, Decimal) and isinstance(value2, Decimal):
            return value1.is_nan() and value2.is_nan()

        if isinstance(value1, float) and isinstance(value2, float):
            return math.isnan(value1) and math.isnan(value2)

        return False
