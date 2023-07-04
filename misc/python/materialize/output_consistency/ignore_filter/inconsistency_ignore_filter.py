# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from typing import List, Set

from attr import dataclass

from materialize.output_consistency.data_value.data_value import DataValue
from materialize.output_consistency.enum.enum_constant import EnumConstant
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
    DbFunctionWithCustomPattern,
    DbOperation,
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
        expression: ExpressionWithArgs,
        all_involved_characteristics: Set[ExpressionCharacteristics],
    ) -> IgnoreVerdict:
        # Note that function names are always provided in lower case.
        if db_function.function_name_in_lower_case in {
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

        if db_function.function_name_in_lower_case in {"regexp_match"}:
            if len(expression.args) == 3 and isinstance(
                expression.args[2], EnumConstant
            ):
                # This is a regexp_match function call with case-insensitive configuration.
                # https://github.com/MaterializeInc/materialize/issues/18494
                return YesIgnore("#18494")

        if db_function.function_name_in_lower_case in {
            "array_agg",
            "string_agg",
        } and not isinstance(db_function, DbFunctionWithCustomPattern):
            # The unordered variants are to be ignored.
            # https://github.com/MaterializeInc/materialize/issues/19832
            return YesIgnore("#19832")

        return NoIgnore()

    def _matches_problematic_operation_invocation(
        self,
        db_operation: DbOperation,
        expression: ExpressionWithArgs,
        all_involved_characteristics: Set[ExpressionCharacteristics],
    ) -> IgnoreVerdict:
        # https://github.com/MaterializeInc/materialize/issues/18494
        if db_operation.pattern in {"$ ~* $", "$ !~* $"}:
            return YesIgnore("#18494")

        return NoIgnore()


class PostExecutionInconsistencyIgnoreFilter:
    def shall_ignore_error(self, error: ValidationError) -> IgnoreVerdict:
        query_template = error.query_execution.query_template
        contains_aggregation = query_template.contains_aggregations

        if error.error_type == ValidationErrorType.SUCCESS_MISMATCH:
            outcome_by_strategy_id = error.query_execution.get_outcome_by_strategy_key()

            dfr_successful = outcome_by_strategy_id[
                EvaluationStrategyKey.DATAFLOW_RENDERING
            ].successful
            ctf_successful = outcome_by_strategy_id[
                EvaluationStrategyKey.CONSTANT_FOLDING
            ].successful

            dfr_fails_but_ctf_succeeds = not dfr_successful and ctf_successful

            if dfr_fails_but_ctf_succeeds and self._uses_shortcut_optimization(
                query_template.select_expressions, contains_aggregation
            ):
                # see https://github.com/MaterializeInc/materialize/issues/19662
                return YesIgnore("#19662")

            if (
                dfr_fails_but_ctf_succeeds
                and query_template.where_expression is not None
                and self._uses_shortcut_optimization(
                    [query_template.where_expression], contains_aggregation
                )
            ):
                # see https://github.com/MaterializeInc/materialize/issues/17189
                return YesIgnore("#17189")

        if error.error_type == ValidationErrorType.ERROR_MISMATCH:
            if self._uses_shortcut_optimization(
                query_template.select_expressions, contains_aggregation
            ):
                # see https://github.com/MaterializeInc/materialize/issues/17189
                return YesIgnore("#17189")

            if query_template.where_expression is not None:
                # The error message may depend on the evaluation order of the where expression.
                # see https://github.com/MaterializeInc/materialize/issues/17189
                return YesIgnore("#17189")

        return NoIgnore()

    def _uses_shortcut_optimization(
        self, expressions: List[Expression], contains_aggregation: bool
    ) -> bool:
        if self._uses_aggregation_shortcut_optimization(
            expressions, contains_aggregation
        ):
            return True
        if self._uses_null_shortcut_optimization(expressions):
            return True

        return False

    def _uses_aggregation_shortcut_optimization(
        self, expressions: List[Expression], contains_aggregation: bool
    ) -> bool:
        if not contains_aggregation:
            # all current known optimizations causing issues involve aggregations
            return False

        def is_function_taking_shortcut(expression: Expression) -> bool:
            functions_taking_shortcuts = {"count", "string_agg"}

            if isinstance(expression, ExpressionWithArgs):
                operation = expression.operation
                return (
                    isinstance(operation, DbFunction)
                    and operation.function_name_in_lower_case
                    in functions_taking_shortcuts
                )
            return False

        for expression in expressions:
            if expression.contains(is_function_taking_shortcut):
                # see https://github.com/MaterializeInc/materialize/issues/17189
                return True

        return False

    def _uses_null_shortcut_optimization(self, expressions: List[Expression]) -> bool:
        def is_eq_null_comparison(expression: Expression) -> bool:
            if isinstance(expression, ExpressionWithArgs):
                operation = expression.operation
                is_eq_comparison = (
                    isinstance(operation, DbOperation) and operation.pattern == "$ = $"
                )

                if not is_eq_comparison:
                    return False

                is_arg0_null = isinstance(
                    expression.args[0], DataValue
                ) and expression.args[0].has_any_characteristic(
                    {ExpressionCharacteristics.NULL}
                )
                is_arg1_null = isinstance(
                    expression.args[1], DataValue
                ) and expression.args[1].has_any_characteristic(
                    {ExpressionCharacteristics.NULL}
                )
                return is_arg0_null or is_arg1_null

            return False

        for expression in expressions:
            if expression.contains(is_eq_null_comparison):
                # see https://github.com/MaterializeInc/materialize/issues/17189
                return True

        return False
