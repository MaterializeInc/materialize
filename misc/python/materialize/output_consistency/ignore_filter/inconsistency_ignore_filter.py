# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from typing import Set

from attr import dataclass

from materialize.output_consistency.execution.evaluation_strategy import (
    EvaluationStrategyKey,
)
from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.expression.expression_with_args import (
    ExpressionWithArgs,
)
from materialize.output_consistency.input_data.return_specs.number_return_spec import (
    NumericReturnTypeSpec,
)
from materialize.output_consistency.operation.operation import (
    DbFunction,
    DbOperationOrFunction,
)
from materialize.output_consistency.selection.selection import DataRowSelection
from materialize.output_consistency.validation.validation_message import (
    ValidationError,
    ValidationErrorType,
)


@dataclass
class IgnoreVerdict:
    ignore: bool


@dataclass(frozen=True)
class YesIgnore(IgnoreVerdict):
    reason: str
    ignore: bool = True


@dataclass(frozen=True)
class NoIgnore(IgnoreVerdict):
    ignore: bool = False


class InconsistencyIgnoreFilter:
    """Allows specifying and excluding expressions with known output inconsistencies"""

    def __init__(self) -> None:
        self.pre_execution_filter = PreExecutionInconsistencyIgnoreFilter()
        self.post_execution_filter = PostExecutionInconsistencyIgnoreFilter()

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


class PreExecutionInconsistencyIgnoreFilter:
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
        """True if the expression shall be ignored."""
        if not expression.operation.is_aggregation:
            # currently no issues without aggregation are known
            return NoIgnore()

        expression_characteristics = (
            expression.recursively_collect_involved_characteristics(row_selection)
        )

        invocation_verdict = self._matches_problematic_operation_or_function_invocation(
            expression, expression.operation, expression_characteristics
        )
        if invocation_verdict.ignore:
            return invocation_verdict

        if isinstance(expression.operation, DbFunction):
            # currently only issues with functions are known, therefore ignore operations
            db_function = expression.operation

            invocation_verdict = self._matches_problematic_function_invocation(
                db_function, expression_characteristics
            )
            if invocation_verdict.ignore:
                return invocation_verdict

        return NoIgnore()

    def _matches_problematic_operation_or_function_invocation(
        self,
        expression: ExpressionWithArgs,
        operation: DbOperationOrFunction,
        _all_involved_characteristics: Set[ExpressionCharacteristics],
    ) -> IgnoreVerdict:
        if operation.is_aggregation:
            for arg in expression.args:
                if arg.is_leaf():
                    continue

                arg_type_spec = arg.resolve_return_type_spec()
                if (
                    isinstance(arg_type_spec, NumericReturnTypeSpec)
                    and not arg_type_spec.only_integer
                ):
                    # tracked with https://github.com/MaterializeInc/materialize/issues/19592
                    return YesIgnore("#19592")

        return NoIgnore()

    def _matches_problematic_function_invocation(
        self,
        db_function: DbFunction,
        all_involved_characteristics: Set[ExpressionCharacteristics],
    ) -> IgnoreVerdict:
        # Note that function names are always provided in lower case.
        if db_function.function_name in {
            "sum",
            "avg",
            "stddev_samp",
            "stddev_pop",
            "var_samp",
            "var_pop",
        }:
            if ExpressionCharacteristics.MAX_VALUE in all_involved_characteristics:
                # tracked with https://github.com/MaterializeInc/materialize/issues/19511
                return YesIgnore("#19511")

            if (
                ExpressionCharacteristics.DECIMAL in all_involved_characteristics
                and ExpressionCharacteristics.TINY_VALUE in all_involved_characteristics
            ):
                # tracked with https://github.com/MaterializeInc/materialize/issues/19511
                return YesIgnore("#19511")

        return NoIgnore()


class PostExecutionInconsistencyIgnoreFilter:
    def shall_ignore_error(self, error: ValidationError) -> IgnoreVerdict:
        if error.error_type == ValidationErrorType.SUCCESS_MISMATCH:
            outcome_by_strategy_id = error.query_execution.get_outcome_by_strategy_key()

            dfr_successful = outcome_by_strategy_id[
                EvaluationStrategyKey.DATAFLOW_RENDERING
            ].successful
            ctf_successful = outcome_by_strategy_id[
                EvaluationStrategyKey.CONSTANT_FOLDING
            ].successful

            if (
                error.query_execution.query_template.contains_aggregations
                and not dfr_successful
                and ctf_successful
            ):
                # see https://github.com/MaterializeInc/materialize/issues/19662
                return YesIgnore("#19662")

        return NoIgnore()
