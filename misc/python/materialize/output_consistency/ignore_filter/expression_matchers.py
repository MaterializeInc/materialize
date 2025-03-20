# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from collections.abc import Callable
from functools import partial

from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.enum.enum_constant import EnumConstant
from materialize.output_consistency.expression.expression import (
    Expression,
    LeafExpression,
)
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.expression.expression_with_args import (
    ExpressionWithArgs,
)
from materialize.output_consistency.input_data.operations.date_time_operations_provider import (
    DATE_TIME_OPERATION_TYPES,
)
from materialize.output_consistency.input_data.params.enum_constant_operation_params import (
    TAG_DATA_TYPE_ENUM,
)
from materialize.output_consistency.input_data.return_specs.number_return_spec import (
    NumericReturnTypeSpec,
)
from materialize.output_consistency.input_data.types.all_types_provider import (
    internal_type_identifiers_to_data_type_names,
)
from materialize.output_consistency.input_data.types.number_types_provider import (
    NON_INTEGER_TYPE_IDENTIFIERS,
)
from materialize.output_consistency.operation.operation import (
    DbFunction,
    DbOperation,
    match_function_by_name,
)
from materialize.output_consistency.query.query_template import QueryTemplate
from materialize.output_consistency.selection.row_selection import (
    ALL_ROWS_SELECTION,
    DataRowSelection,
)


def matches_x_or_y(
    expression: Expression,
    x: Callable[[Expression], bool],
    y: Callable[[Expression], bool],
) -> bool:
    return x(expression) or y(expression)


def matches_x_and_y(
    expression: Expression,
    x: Callable[[Expression], bool],
    y: Callable[[Expression], bool],
) -> bool:
    return x(expression) and y(expression)


def matches_fun_by_name(
    expression: Expression, function_name_in_lower_case: str
) -> bool:
    if isinstance(expression, ExpressionWithArgs):
        return match_function_by_name(expression.operation, function_name_in_lower_case)
    return False


def matches_fun_by_any_name(
    expression: Expression, function_names_in_lower_case: set[str]
) -> bool:
    for function_name in function_names_in_lower_case:
        if matches_fun_by_name(expression, function_name):
            return True

    return False


def matches_op_by_pattern(expression: Expression, pattern: str) -> bool:
    if isinstance(expression, ExpressionWithArgs) and isinstance(
        expression.operation, DbOperation
    ):
        return expression.operation.pattern == pattern
    return False


def matches_op_by_any_pattern(expression: Expression, patterns: set[str]) -> bool:
    for pattern in patterns:
        if matches_op_by_pattern(expression, pattern):
            return True

    return False


def matches_expression_with_only_plain_arguments(expression: Expression) -> bool:
    if isinstance(expression, ExpressionWithArgs):
        for arg in expression.args:
            if not arg.is_leaf():
                return False

    return True


def matches_any_expression_arg(
    expression: Expression, arg_matcher: Callable[[Expression], bool]
) -> bool:
    if isinstance(expression, ExpressionWithArgs):
        for arg_expression in expression.args:
            if arg_matcher(arg_expression):
                return True
    return False


def matches_recursively(
    expression: Expression, matcher: Callable[[Expression], bool]
) -> bool:
    if isinstance(expression, ExpressionWithArgs):
        for arg_expression in expression.args:
            is_match = matches_recursively(arg_expression, matcher)
            if is_match:
                return True

    return matcher(expression)


def matches_nested_expression(expression: Expression) -> bool:
    return not matches_expression_with_only_plain_arguments(expression)


def is_function_invoked_only_with_non_nested_parameters(
    query_template: QueryTemplate, function_name_in_lowercase: str
) -> bool:
    at_least_one_invocation_with_nested_args = query_template.matches_any_expression(
        partial(
            matches_x_and_y,
            x=partial(
                matches_fun_by_name,
                function_name_in_lower_case=function_name_in_lowercase,
            ),
            y=matches_nested_expression,
        ),
        True,
    )
    return not at_least_one_invocation_with_nested_args


def involves_data_type_category(
    expression: Expression, data_type_category: DataTypeCategory
) -> bool:
    return involves_data_type_categories(expression, {data_type_category})


def involves_data_type_categories(
    expression: Expression, data_type_categories: set[DataTypeCategory]
) -> bool:
    return expression.resolve_resulting_return_type_category() in data_type_categories


def is_known_to_involve_exact_data_types(
    expression: Expression, internal_data_type_identifiers: set[str]
):
    if (
        len(internal_data_type_identifiers.intersection(NON_INTEGER_TYPE_IDENTIFIERS))
        > 0
    ):
        if is_known_to_return_non_integer_number(expression):
            return True

    if isinstance(expression, EnumConstant) and expression.is_tagged(
        TAG_DATA_TYPE_ENUM
    ):
        # this matches castings to a certain type
        type_names = internal_type_identifiers_to_data_type_names(
            internal_data_type_identifiers
        )
        if expression.value in type_names:
            return True

    exact_data_type = expression.try_resolve_exact_data_type()

    if exact_data_type is None:
        return False

    return exact_data_type.internal_identifier in internal_data_type_identifiers


def is_operation_tagged(expression: Expression, tag: str) -> bool:
    if isinstance(expression, ExpressionWithArgs):
        return expression.operation.is_tagged(tag)

    return False


def argument_has_any_characteristic(
    expression: Expression,
    arg_index: int,
    characteristics: set[ExpressionCharacteristics],
    row_selection: DataRowSelection = ALL_ROWS_SELECTION,
) -> bool:
    if isinstance(expression, ExpressionWithArgs):
        assert arg_index < len(
            expression.operation.params
        ), f"Invalid argument index for {expression.operation_to_pattern()}"
        param = expression.operation.params[arg_index]
        if param.optional and len(expression.args) <= arg_index:
            return False

        argument = expression.args[arg_index]
        return argument.has_any_characteristic(
            characteristics, row_selection=row_selection
        )

    return False


def is_any_date_time_expression(expression: Expression) -> bool:
    all_date_time_function_names = {
        function.function_name_in_lower_case
        for function in DATE_TIME_OPERATION_TYPES
        if isinstance(function, DbFunction)
    }
    all_date_time_operation_patterns = {
        operation.pattern
        for operation in DATE_TIME_OPERATION_TYPES
        if isinstance(operation, DbOperation)
    }

    def is_date_time_leaf_expression(expression: Expression) -> bool:
        return (
            isinstance(expression, LeafExpression)
            and expression.data_type.category == DataTypeCategory.DATE_TIME
        )

    return (
        expression.matches(is_date_time_leaf_expression, True)
        or expression.matches(
            partial(
                matches_fun_by_any_name,
                function_names_in_lower_case=all_date_time_function_names,
            ),
            True,
        )
        or expression.matches(
            partial(
                matches_op_by_any_pattern,
                patterns=all_date_time_operation_patterns,
            ),
            True,
        )
    )


def is_timezone_conversion_expression(expression: Expression) -> bool:
    return expression.matches(
        partial(
            matches_fun_by_any_name,
            function_names_in_lower_case={"timezone"},
        ),
        True,
    ) or expression.matches(
        partial(
            matches_op_by_any_pattern,
            patterns={"$ AT TIME ZONE $::TEXT"},
        ),
        True,
    )


def is_known_to_return_non_integer_number(expression: Expression):
    return_type_spec = expression.resolve_return_type_spec()

    if isinstance(return_type_spec, NumericReturnTypeSpec):
        return return_type_spec.always_floating_type

    return False


def is_table_function(expression: Expression) -> bool:
    if isinstance(expression, ExpressionWithArgs):
        return expression.operation.is_table_function
    return False
