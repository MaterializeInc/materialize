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
from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.expression.expression_with_args import (
    ExpressionWithArgs,
)
from materialize.output_consistency.operation.operation import (
    DbOperation,
    match_function_by_name,
)
from materialize.output_consistency.query.query_template import QueryTemplate


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
    return expression.resolve_return_type_category() == data_type_category
