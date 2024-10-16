# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from functools import partial

from materialize.output_consistency.data_value.data_value import DataValue
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
from materialize.output_consistency.ignore_filter.expression_matchers import (
    is_table_function,
    matches_fun_by_any_name,
    matches_fun_by_name,
)
from materialize.output_consistency.ignore_filter.ignore_verdict import (
    IgnoreVerdict,
    NoIgnore,
    YesIgnore,
)
from materialize.output_consistency.ignore_filter.inconsistency_ignore_filter import (
    GenericInconsistencyIgnoreFilter,
    PostExecutionInconsistencyIgnoreFilterBase,
    PreExecutionInconsistencyIgnoreFilterBase,
)
from materialize.output_consistency.input_data.return_specs.number_return_spec import (
    NumericReturnTypeSpec,
)
from materialize.output_consistency.operation.operation import (
    DbFunction,
    DbOperation,
    DbOperationOrFunction,
)
from materialize.output_consistency.query.query_result import QueryFailure
from materialize.output_consistency.query.query_template import QueryTemplate
from materialize.output_consistency.selection.column_selection import (
    ALL_QUERY_COLUMNS_BY_INDEX_SELECTION,
)
from materialize.output_consistency.validation.validation_message import (
    ValidationError,
)

AGGREGATION_SHORTCUT_FUNCTION_NAMES = {"count", "string_agg"}
DIFFERENT_EVALUATION_ORDER_FUNCTION_NAMES = {"map_agg"}


class InternalOutputInconsistencyIgnoreFilter(GenericInconsistencyIgnoreFilter):
    """Allows specifying and excluding expressions with known output inconsistencies"""

    def __init__(self):
        super().__init__(
            PreExecutionInternalOutputInconsistencyIgnoreFilter(),
            PostExecutionInternalOutputInconsistencyIgnoreFilter(),
        )


class PreExecutionInternalOutputInconsistencyIgnoreFilter(
    PreExecutionInconsistencyIgnoreFilterBase
):
    def _matches_problematic_operation_or_function_invocation(
        self,
        expression: ExpressionWithArgs,
        operation: DbOperationOrFunction,
        _all_involved_characteristics: set[ExpressionCharacteristics],
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
                    return YesIgnore("database-issues#4341")

        return NoIgnore()

    def _matches_problematic_function_invocation(
        self,
        db_function: DbFunction,
        expression: ExpressionWithArgs,
        all_involved_characteristics: set[ExpressionCharacteristics],
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
                return YesIgnore("database-issues#4341")

            if (
                ExpressionCharacteristics.DECIMAL in all_involved_characteristics
                and ExpressionCharacteristics.TINY_VALUE in all_involved_characteristics
            ):
                return YesIgnore("database-issues#4341")

        return NoIgnore()

    def _matches_problematic_operation_invocation(
        self,
        db_operation: DbOperation,
        expression: ExpressionWithArgs,
        all_involved_characteristics: set[ExpressionCharacteristics],
    ) -> IgnoreVerdict:
        return NoIgnore()


class PostExecutionInternalOutputInconsistencyIgnoreFilter(
    PostExecutionInconsistencyIgnoreFilterBase
):
    def _shall_ignore_success_mismatch(
        self,
        error: ValidationError,
        query_template: QueryTemplate,
        contains_aggregation: bool,
    ) -> IgnoreVerdict:
        outcome_by_strategy_id = error.query_execution.get_outcome_by_strategy_key()

        dfr_outcome = outcome_by_strategy_id[
            EvaluationStrategyKey.MZ_DATAFLOW_RENDERING
        ]
        ctf_outcome = outcome_by_strategy_id[EvaluationStrategyKey.MZ_CONSTANT_FOLDING]
        dfr_successful = dfr_outcome.successful
        ctf_successful = ctf_outcome.successful

        dfr_fails_but_ctf_succeeds = not dfr_successful and ctf_successful
        dfr_succeeds_but_ctf_fails = dfr_successful and not ctf_successful

        if dfr_fails_but_ctf_succeeds and self._uses_shortcut_optimization(
            query_template.select_expressions, contains_aggregation
        ):
            return YesIgnore("database-issues#5850")

        if (
            dfr_fails_but_ctf_succeeds
            and query_template.where_expression is not None
            and self._uses_shortcut_optimization(
                [query_template.where_expression], contains_aggregation
            )
        ):
            return YesIgnore("database-issues#4972")

        if dfr_succeeds_but_ctf_fails or dfr_fails_but_ctf_succeeds:
            if query_template.has_where_condition():
                # An evaluation strategy may touch further rows than the selected subset and thereby run into evaluation
                # errors (while the other uses another order).
                return YesIgnore("database-issues#4972")

            if (
                query_template.has_where_condition()
                or query_template.has_row_selection()
            ) or query_template.uses_join():
                # Where expression, or row filter, or join constraint are set. They might be evaluated in a different
                # order. Furthermore, constant folding may detect that the join constraint cannot be satisfied without
                # evaluating it (which will fail).
                return YesIgnore("database-issues#4972: evaluation order")

        if self._uses_eager_evaluation(query_template):
            return YesIgnore("database-issues#4972")

        if dfr_succeeds_but_ctf_fails:
            assert isinstance(ctf_outcome, QueryFailure)

            if (
                ctf_outcome.error_message == "key cannot be null"
                and query_template.matches_any_expression(
                    partial(
                        matches_fun_by_name,
                        function_name_in_lower_case="jsonb_object_agg",
                    ),
                    True,
                )
            ):
                return YesIgnore(
                    "database-issues#8246: jsonb_object_agg with NULL as key"
                )

        return NoIgnore()

    def _shall_ignore_error_mismatch(
        self,
        error: ValidationError,
        query_template: QueryTemplate,
        contains_aggregation: bool,
    ) -> IgnoreVerdict:
        dfr_outcome = error.query_execution.get_outcome_by_strategy_key()[
            EvaluationStrategyKey.MZ_DATAFLOW_RENDERING
        ]
        ctf_outcome = error.query_execution.get_outcome_by_strategy_key()[
            EvaluationStrategyKey.MZ_CONSTANT_FOLDING
        ]

        assert isinstance(dfr_outcome, QueryFailure)
        assert isinstance(ctf_outcome, QueryFailure)

        all_characteristics = query_template.get_involved_characteristics(
            ALL_QUERY_COLUMNS_BY_INDEX_SELECTION
        )

        if self._uses_shortcut_optimization(
            query_template.select_expressions, contains_aggregation
        ):
            return YesIgnore("database-issues#4972: evaluation order")

        if self._uses_eager_evaluation(query_template):
            return YesIgnore("database-issues#4972: evaluation order")

        if query_template.has_where_condition() or query_template.uses_join():
            # The error message may depend on the evaluation order of the where expression or join constraint.
            return YesIgnore("database-issues#4972: evaluation order")

        if (
            ExpressionCharacteristics.INFINITY in all_characteristics
            and ExpressionCharacteristics.MAX_VALUE in all_characteristics
        ):
            return YesIgnore("database-issues#4972: evaluation order")

        if query_template.matches_any_expression(
            is_table_function,
            True,
        ):
            return YesIgnore(
                "Table function rows executed in different order, resulting in different error messages"
            )

        if query_template.matches_any_expression(
            partial(
                matches_fun_by_any_name,
                function_names_in_lower_case=DIFFERENT_EVALUATION_ORDER_FUNCTION_NAMES,
            ),
            True,
        ):
            return YesIgnore("database-issues#4972: evaluation order")

        return NoIgnore()

    def _uses_shortcut_optimization(
        self, expressions: list[Expression], contains_aggregation: bool
    ) -> bool:
        for expression in expressions:
            if expression.matches(
                partial(
                    uses_aggregation_shortcut_optimization,
                    contains_aggregation=contains_aggregation,
                ),
                True,
            ):
                return True

        if self._might_use_null_shortcut_optimization(expressions):
            return True

        return False

    def _might_use_null_shortcut_optimization(
        self, expressions: list[Expression]
    ) -> bool:
        def is_null_expression(expression: Expression) -> bool:
            return isinstance(
                expression, DataValue
            ) and expression.has_any_characteristic({ExpressionCharacteristics.NULL})

        for expression in expressions:
            if expression.contains(is_null_expression, True):
                # Constant folding takes shortcuts when it can infer that an expression will be NULL or not
                # (e.g., `chr(huge_value) = NULL` won't be fully evaluated)
                return True

        return False

    def _uses_eager_evaluation(self, query_template: QueryTemplate) -> bool:
        # note that these functions do not necessarily require an aggregation

        functions_with_eager_evaluation = {"coalesce"}

        return query_template.matches_any_expression(
            partial(
                matches_fun_by_any_name,
                function_names_in_lower_case=functions_with_eager_evaluation,
            ),
            True,
        )


def uses_aggregation_shortcut_optimization(
    expression: Expression, contains_aggregation: bool
) -> bool:
    if not contains_aggregation:
        # all current known optimizations causing issues involve aggregations
        return False

    return expression.matches(
        partial(
            matches_fun_by_any_name,
            function_names_in_lower_case=AGGREGATION_SHORTCUT_FUNCTION_NAMES,
        ),
        True,
    )
