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

from materialize.output_consistency.execution.query_output_mode import QueryOutputMode
from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.ignore_filter.inconsistency_ignore_filter import (
    GenericInconsistencyIgnoreFilter,
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
    ValidationErrorDetails,
    ValidationErrorType,
    ValidationRemark,
    ValidationWarning,
)
from materialize.output_consistency.validation.validation_outcome import (
    ValidationOutcome,
)


class ResultComparator:
    """Compares the outcome (result or failure) of multiple query executions"""

    def __init__(
        self,
        ignore_filter: GenericInconsistencyIgnoreFilter,
        error_message_normalizer: ErrorMessageNormalizer,
    ):
        self.ignore_filter = ignore_filter
        self.error_message_normalizer = error_message_normalizer

    def compare_results(self, query_execution: QueryExecution) -> ValidationOutcome:
        validation_outcome = ValidationOutcome()

        if len(query_execution.outcomes) == 0:
            raise RuntimeError("Contains no outcomes!")

        if len(query_execution.outcomes) == 1:
            raise RuntimeError("Contains only one outcome, nothing to compare against!")

        if query_execution.query_template.expect_error:
            validation_outcome.add_remark(ValidationRemark("DB error is possible"))

        self.validate_outcomes_metadata(query_execution, validation_outcome)

        if not validation_outcome.verdict().succeeded():
            # do not continue with value comparison if metadata differs
            return validation_outcome

        # this statement must not be before the metadata validation (otherwise successful of the outcomes may differ)
        validation_outcome.query_execution_succeeded_in_all_strategies = (
            query_execution.outcomes[0].successful
        )

        if validation_outcome.query_execution_succeeded_in_all_strategies:
            self.validate_outcomes_data(query_execution, validation_outcome)
            if query_execution.query_output_mode in {
                QueryOutputMode.EXPLAIN,
                QueryOutputMode.EXPLAIN_PHYSICAL,
            }:
                validation_outcome.success_reason = "explain plan matches"
            else:
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
            expression = self._expression_if_only_one_in_query(query_execution)

            validation_outcome.add_error(
                self.ignore_filter,
                ValidationError(
                    query_execution,
                    ValidationErrorType.SUCCESS_MISMATCH,
                    "Outcome differs",
                    details1=ValidationErrorDetails(
                        strategy=outcome1.strategy,
                        value=outcome1.__class__.__name__,
                        sql=outcome1.sql,
                        sql_error=(
                            outcome1.error_message
                            if isinstance(outcome1, QueryFailure)
                            else None
                        ),
                    ),
                    details2=ValidationErrorDetails(
                        strategy=outcome2.strategy,
                        value=outcome2.__class__.__name__,
                        sql=outcome2.sql,
                        sql_error=(
                            outcome2.error_message
                            if isinstance(outcome2, QueryFailure)
                            else None
                        ),
                    ),
                    concerned_expression=expression,
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

        if both_failed and self.shall_validate_error_message(query_execution):
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

    def shall_validate_error_message(self, query_execution: QueryExecution) -> bool:
        return not query_execution.query_template.disable_error_message_validation

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
            expression = self._expression_if_only_one_in_query(query_execution)

            validation_outcome.add_error(
                self.ignore_filter,
                ValidationError(
                    query_execution,
                    ValidationErrorType.ROW_COUNT_MISMATCH,
                    "Row count differs",
                    details1=ValidationErrorDetails(
                        strategy=result1.strategy, value=str(num_rows1), sql=result1.sql
                    ),
                    details2=ValidationErrorDetails(
                        strategy=result2.strategy, value=str(num_rows2), sql=result2.sql
                    ),
                    concerned_expression=expression,
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
            expression = self._expression_if_only_one_in_query(query_execution)

            validation_outcome.add_error(
                self.ignore_filter,
                ValidationError(
                    query_execution,
                    ValidationErrorType.ERROR_MISMATCH,
                    "Error message differs",
                    details1=ValidationErrorDetails(
                        strategy=failure1.strategy,
                        value=norm_error_message_1,
                        sql=failure1.sql,
                        sql_error=failure1.error_message,
                    ),
                    details2=ValidationErrorDetails(
                        strategy=failure2.strategy,
                        value=norm_error_message_2,
                        sql=failure2.sql,
                        sql_error=failure2.error_message,
                    ),
                    concerned_expression=expression,
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

            if query_execution.query_output_mode in {
                QueryOutputMode.EXPLAIN,
                QueryOutputMode.EXPLAIN_PHYSICAL,
            }:
                self.validate_explain_plan(
                    query_execution, result1, other_result, validation_outcome
                )
            else:
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
            raise RuntimeError("Results contain a different number of columns!")

        if num_columns1 != len(query_execution.query_template.select_expressions):
            # This would happen with the disabled .* operator on a row() function
            raise RuntimeError(
                "Number of columns in the result does not match the number of select expressions!"
            )

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

        column_values1 = []
        column_values2 = []
        expression = query_execution.query_template.select_expressions[col_index]

        for row_index in range(0, row_length):
            column_values1.append(result1.result_rows[row_index][col_index])
            column_values2.append(result2.result_rows[row_index][col_index])

        if self.ignore_row_order(expression):
            column_values1 = self._sort_column_values(column_values1)
            column_values2 = self._sort_column_values(column_values2)

        for row_index in range(0, row_length):
            result_value1 = column_values1[row_index]
            result_value2 = column_values2[row_index]

            if not self.is_value_equal(result_value1, result_value2, expression):
                error_type = ValidationErrorType.CONTENT_MISMATCH
                error_message = "Value differs"
            elif not self.is_type_equal(result_value1, result_value2):
                # check the type after the value because it has a lower relevance
                error_type = ValidationErrorType.CONTENT_TYPE_MISMATCH
                result_value1 = type(result_value1)
                result_value2 = type(result_value2)
                error_message = "Value type differs"
            else:
                continue

            validation_outcome.add_error(
                self.ignore_filter,
                ValidationError(
                    query_execution,
                    error_type,
                    error_message,
                    details1=ValidationErrorDetails(
                        strategy=result1.strategy, value=result_value1, sql=result1.sql
                    ),
                    details2=ValidationErrorDetails(
                        strategy=result2.strategy, value=result_value2, sql=result2.sql
                    ),
                    col_index=col_index,
                    concerned_expression=expression,
                    location=f"row index {row_index}, column index {col_index}",
                ),
            )

    def validate_explain_plan(
        self,
        query_execution: QueryExecution,
        outcome1: QueryResult,
        outcome2: QueryResult,
        validation_outcome: ValidationOutcome,
    ) -> None:
        num_columns1 = len(outcome1.result_rows[0])
        num_columns2 = len(outcome2.result_rows[0])

        assert num_columns1 == 1
        assert num_columns2 == 1

        explain_plan1 = outcome1.result_rows[0][0]
        explain_plan2 = outcome2.result_rows[0][0]

        for data_source in query_execution.query_template.get_all_data_sources():
            new_source_name = f"<db_object-{data_source.table_index or 1}>"
            explain_plan1 = explain_plan1.replace(
                data_source.get_db_object_name(
                    outcome1.strategy.get_db_object_name(
                        query_execution.query_template.storage_layout,
                        data_source=data_source,
                    ),
                ),
                new_source_name,
            )
            explain_plan2 = explain_plan2.replace(
                data_source.get_db_object_name(
                    outcome2.strategy.get_db_object_name(
                        query_execution.query_template.storage_layout,
                        data_source=data_source,
                    ),
                ),
                new_source_name,
            )

        if explain_plan1 == explain_plan2:
            return

        expression = self._expression_if_only_one_in_query(query_execution)

        validation_outcome.add_error(
            self.ignore_filter,
            ValidationError(
                query_execution,
                ValidationErrorType.EXPLAIN_PLAN_MISMATCH,
                "Explain plan differs",
                details1=ValidationErrorDetails(
                    strategy=outcome1.strategy, value=explain_plan1, sql=outcome1.sql
                ),
                details2=ValidationErrorDetails(
                    strategy=outcome2.strategy, value=explain_plan2, sql=outcome2.sql
                ),
                concerned_expression=expression,
            ),
        )

    def is_type_equal(self, value1: Any, value2: Any) -> bool:
        if value1 is None or value2 is None:
            # ignore None values
            return True

        return type(value1) == type(value2)

    def is_value_equal(
        self,
        value1: Any,
        value2: Any,
        expression: Expression,
        is_tolerant: bool = False,
    ) -> bool:
        if value1 == value2:
            return True

        if (isinstance(value1, list) and isinstance(value2, list)) or (
            isinstance(value1, tuple) and isinstance(value2, tuple)
        ):
            return self.is_list_or_tuple_equal(value1, value2, expression)

        if isinstance(value1, dict) and isinstance(value2, dict):
            return self.is_dict_equal(value1, value2, expression)

        if isinstance(value1, Decimal) and isinstance(value2, Decimal):
            if value1.is_nan() and value2.is_nan():
                return True
            else:
                return value1 == value2

        if isinstance(value1, float) and isinstance(value2, float):
            if math.isnan(value1) and math.isnan(value2):
                return True
            else:
                return value1 == value2

        return False

    def is_list_or_tuple_equal(
        self,
        collection1: list[Any] | tuple[Any],
        collection2: list[Any] | tuple[Any],
        expression: Expression,
    ) -> bool:
        if len(collection1) != len(collection2):
            return False

        if (
            self.ignore_order_when_comparing_collection(expression)
            and self._can_be_sorted(collection1)
            and self._can_be_sorted(collection2)
        ):
            collection1 = sorted(collection1)
            collection2 = sorted(collection2)

        for value1, value2 in zip(collection1, collection2):
            # use is_tolerant because tuples may contain all values as strings
            if not self.is_value_equal(value1, value2, expression, is_tolerant=True):
                return False

        return True

    def is_dict_equal(
        self,
        dict1: dict[Any, Any],
        dict2: dict[Any, Any],
        expression: Expression,
    ) -> bool:
        if len(dict1) != len(dict2):
            return False

        if not self.is_value_equal(dict1.keys(), dict2.keys(), expression):
            return False

        for key in dict1.keys():
            if not self.is_value_equal(dict1[key], dict2[key], expression):
                return False

        return True

    def ignore_row_order(self, expression: Expression) -> bool:
        return False

    def ignore_order_when_comparing_collection(self, expression: Expression) -> bool:
        return False

    def _can_be_sorted(self, collection: list[Any] | tuple[Any]) -> bool:
        for element in collection:
            if isinstance(element, dict):
                return False

        return True

    def _expression_if_only_one_in_query(
        self, query_execution: QueryExecution
    ) -> Expression | None:
        if len(query_execution.query_template.select_expressions) == 1:
            return query_execution.query_template.select_expressions[0]

        return None

    def _sort_column_values(self, column_values: list[Any]) -> list[Any]:
        # needed because, for example, None values have no order
        def sort_key(value: Any) -> Any:
            return str(value)

        return sorted(column_values, key=sort_key)
