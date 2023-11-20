# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from __future__ import annotations

from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.expression.expression_with_args import (
    ExpressionWithArgs,
)
from materialize.output_consistency.ignore_filter.ignore_verdict import (
    IgnoreVerdict,
    NoIgnore,
)
from materialize.output_consistency.operation.operation import (
    DbFunction,
    DbOperation,
    DbOperationOrFunction,
)
from materialize.output_consistency.query.query_template import QueryTemplate
from materialize.output_consistency.selection.selection import DataRowSelection
from materialize.output_consistency.validation.validation_message import (
    ValidationError,
    ValidationErrorType,
)


class GenericInconsistencyIgnoreFilter:
    """Allows specifying and excluding expressions with known output inconsistencies"""

    def __init__(
        self,
        pre_execution_filter: PreExecutionInconsistencyIgnoreFilterBase,
        post_execution_filter: PostExecutionInconsistencyIgnoreFilterBase,
    ):
        self.pre_execution_filter = pre_execution_filter
        self.post_execution_filter = post_execution_filter

    def shall_ignore_expression(
        self, expression: Expression, row_selection: DataRowSelection
    ) -> IgnoreVerdict:
        """This filter is applied before the query execution."""
        return self.pre_execution_filter.shall_ignore_expression(
            expression, row_selection
        )

    def shall_ignore_error(self, error: ValidationError) -> IgnoreVerdict:
        """This filter is applied on an error after the query execution."""
        return self.post_execution_filter.shall_ignore_error(error)


class PreExecutionInconsistencyIgnoreFilterBase:
    def shall_ignore_expression(
        self, expression: Expression, row_selection: DataRowSelection
    ) -> IgnoreVerdict:
        if expression.is_leaf():
            return NoIgnore()
        elif isinstance(expression, ExpressionWithArgs):
            return self._shall_ignore_expression_with_args(expression, row_selection)
        else:
            raise RuntimeError(f"Unsupported expression type: {type(expression)}")

    def _shall_ignore_expression_with_args(
        self,
        expression: ExpressionWithArgs,
        row_selection: DataRowSelection,
    ) -> IgnoreVerdict:
        # check expression itself
        expression_verdict = self._visit_expression_with_args(expression, row_selection)
        if expression_verdict.ignore:
            return expression_verdict

        # recursively check arguments
        for arg in expression.args:
            arg_expression_verdict = self.shall_ignore_expression(arg, row_selection)
            if arg_expression_verdict.ignore:
                return arg_expression_verdict

        return NoIgnore()

    def _visit_expression_with_args(
        self,
        expression: ExpressionWithArgs,
        row_selection: DataRowSelection,
    ) -> IgnoreVerdict:
        expression_characteristics = (
            expression.recursively_collect_involved_characteristics(row_selection)
        )

        invocation_verdict = self._matches_problematic_operation_or_function_invocation(
            expression, expression.operation, expression_characteristics
        )
        if invocation_verdict.ignore:
            return invocation_verdict

        if isinstance(expression.operation, DbFunction):
            db_function = expression.operation

            invocation_verdict = self._matches_problematic_function_invocation(
                db_function, expression, expression_characteristics
            )
            if invocation_verdict.ignore:
                return invocation_verdict

        if isinstance(expression.operation, DbOperation):
            db_operation = expression.operation

            invocation_verdict = self._matches_problematic_operation_invocation(
                db_operation, expression, expression_characteristics
            )
            if invocation_verdict.ignore:
                return invocation_verdict

        return NoIgnore()

    def _matches_problematic_operation_or_function_invocation(
        self,
        expression: ExpressionWithArgs,
        operation: DbOperationOrFunction,
        _all_involved_characteristics: set[ExpressionCharacteristics],
    ) -> IgnoreVerdict:
        return NoIgnore()

    def _matches_problematic_function_invocation(
        self,
        db_function: DbFunction,
        expression: ExpressionWithArgs,
        all_involved_characteristics: set[ExpressionCharacteristics],
    ) -> IgnoreVerdict:
        return NoIgnore()

    def _matches_problematic_operation_invocation(
        self,
        db_operation: DbOperation,
        expression: ExpressionWithArgs,
        all_involved_characteristics: set[ExpressionCharacteristics],
    ) -> IgnoreVerdict:
        return NoIgnore()


class PostExecutionInconsistencyIgnoreFilterBase:
    def shall_ignore_error(self, error: ValidationError) -> IgnoreVerdict:
        query_template = error.query_execution.query_template
        contains_aggregation = query_template.contains_aggregations

        if error.error_type == ValidationErrorType.SUCCESS_MISMATCH:
            return self._shall_ignore_success_mismatch(
                error, query_template, contains_aggregation
            )

        if error.error_type == ValidationErrorType.ERROR_MISMATCH:
            return self._shall_ignore_error_mismatch(
                error, query_template, contains_aggregation
            )

        if error.error_type == ValidationErrorType.CONTENT_MISMATCH:
            return self._shall_ignore_content_mismatch(
                error, query_template, contains_aggregation
            )

        if error.error_type == ValidationErrorType.ROW_COUNT_MISMATCH:
            return self._shall_ignore_error_mismatch(
                error, query_template, contains_aggregation
            )

        raise RuntimeError(f"Unexpected validation error type: {error.error_type}")

    def _shall_ignore_success_mismatch(
        self,
        error: ValidationError,
        query_template: QueryTemplate,
        contains_aggregation: bool,
    ) -> IgnoreVerdict:
        return NoIgnore()

    def _shall_ignore_error_mismatch(
        self,
        error: ValidationError,
        query_template: QueryTemplate,
        contains_aggregation: bool,
    ) -> IgnoreVerdict:
        return NoIgnore()

    def _shall_ignore_content_mismatch(
        self,
        error: ValidationError,
        query_template: QueryTemplate,
        contains_aggregation: bool,
    ) -> IgnoreVerdict:
        return NoIgnore()

    def _shall_ignore_row_count_mismatch(
        self,
        error: ValidationError,
        query_template: QueryTemplate,
        contains_aggregation: bool,
    ) -> IgnoreVerdict:
        return NoIgnore()
