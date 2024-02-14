# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import re
from functools import partial

from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
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
from materialize.output_consistency.ignore_filter.expression_matchers import (
    involves_data_type_category,
    matches_fun_by_any_name,
    matches_fun_by_name,
    matches_op_by_pattern,
    matches_x_or_y,
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
from materialize.output_consistency.ignore_filter.param_matchers import (
    index_of_param_by_type,
)
from materialize.output_consistency.input_data.operations.generic_operations_provider import (
    TAG_CASTING,
)
from materialize.output_consistency.input_data.operations.jsonb_operations_provider import (
    TAG_JSONB_TO_TEXT,
)
from materialize.output_consistency.input_data.operations.text_operations_provider import (
    TAG_REGEX,
)
from materialize.output_consistency.input_data.params.text_operation_param import (
    TextOperationParam,
)
from materialize.output_consistency.input_data.return_specs.number_return_spec import (
    NumericReturnTypeSpec,
)
from materialize.output_consistency.input_data.return_specs.text_return_spec import (
    TextReturnTypeSpec,
)
from materialize.output_consistency.operation.operation import (
    DbFunction,
    DbOperation,
    DbOperationOrFunction,
)
from materialize.output_consistency.query.query_result import QueryFailure
from materialize.output_consistency.query.query_template import QueryTemplate
from materialize.output_consistency.validation.validation_message import ValidationError

NAME_OF_NON_EXISTING_FUNCTION_PATTERN = re.compile(
    r"function (\w)\(.*?\) does not exist"
)


class PgInconsistencyIgnoreFilter(GenericInconsistencyIgnoreFilter):
    """Allows specifying and excluding expressions with known output inconsistencies"""

    def __init__(self):
        super().__init__(
            PgPreExecutionInconsistencyIgnoreFilter(),
            PgPostExecutionInconsistencyIgnoreFilter(),
        )


class PgPreExecutionInconsistencyIgnoreFilter(
    PreExecutionInconsistencyIgnoreFilterBase
):
    def _matches_problematic_operation_or_function_invocation(
        self,
        expression: ExpressionWithArgs,
        operation: DbOperationOrFunction,
        _all_involved_characteristics: set[ExpressionCharacteristics],
    ) -> IgnoreVerdict:
        if matches_float_comparison(expression):
            return YesIgnore("#22022: real with decimal comparison")

        if operation.is_tagged(TAG_REGEX):
            regex_param_index = index_of_param_by_type(
                operation.params, TextOperationParam
            )
            assert regex_param_index is not None

            if expression.args[regex_param_index].has_any_characteristic(
                {ExpressionCharacteristics.TEXT_WITH_SPECIAL_SPACE_CHARS}
            ):
                return YesIgnore("#22000: regexp with linebreak")

        if operation.is_tagged(TAG_JSONB_TO_TEXT) and expression.matches(
            partial(
                involves_data_type_category, data_type_category=DataTypeCategory.JSONB
            ),
            True,
        ):
            return YesIgnore("Consequence of #23571")

        if operation.is_tagged(TAG_CASTING) and expression.matches(
            partial(matches_fun_by_name, function_name_in_lower_case="to_char"),
            True,
        ):
            return YesIgnore("#25228: date format that cannot be parsed")

        return super()._matches_problematic_operation_or_function_invocation(
            expression, operation, _all_involved_characteristics
        )

    def _matches_problematic_function_invocation(
        self,
        db_function: DbFunction,
        expression: ExpressionWithArgs,
        all_involved_characteristics: set[ExpressionCharacteristics],
    ) -> IgnoreVerdict:
        inconsistent_value_ordering_functions = ["array_agg", "string_agg"]
        if (
            db_function.function_name_in_lower_case
            in inconsistent_value_ordering_functions
            # only exclude unordered variant
            and "ORDER BY" not in db_function.to_pattern(db_function.max_param_count)
        ):
            return YesIgnore("inconsistent ordering, not an error")

        if db_function.function_name_in_lower_case == "jsonb_pretty":
            return YesIgnore("Accepted")

        if db_function.function_name_in_lower_case == "date_trunc":
            precision = expression.args[0]
            if isinstance(precision, EnumConstant) and precision.value == "second":
                return YesIgnore("#22017: date_trunc with seconds")
            if isinstance(precision, EnumConstant) and precision.value == "quarter":
                return YesIgnore("#21996: date_trunc does not support quarter")

        if db_function.function_name_in_lower_case == "lpad" and expression.args[
            1
        ].has_any_characteristic({ExpressionCharacteristics.NEGATIVE}):
            return YesIgnore("#21997: lpad with negative")

        if db_function.function_name_in_lower_case in {"min", "max"}:
            return_type_spec = expression.args[0].resolve_return_type_spec()
            if isinstance(return_type_spec, TextReturnTypeSpec):
                return YesIgnore("#22002: min/max on text different")

        if db_function.function_name_in_lower_case == "replace":
            # replace is not working properly with empty text; however, it is not possible to reliably determine if an
            # expression is an empty text, we therefore need to exclude the function completely
            return YesIgnore("#22001: replace")

        if db_function.function_name_in_lower_case == "regexp_replace":
            if expression.args[2].has_any_characteristic(
                {ExpressionCharacteristics.TEXT_WITH_BACKSLASH_CHAR}
            ):
                return YesIgnore("#23605: regexp with backslash")

        if db_function.function_name_in_lower_case == "nullif":
            type_arg0 = expression.args[0].try_resolve_exact_data_type()
            type_arg1 = expression.args[1].try_resolve_exact_data_type()

            if (
                type_arg0 is not None
                and type_arg1 is not None
                and type_arg0.type_name != type_arg1.type_name
            ):
                # Postgres returns a double for nullif(int, double), which does not seem better
                return YesIgnore("not considered worse")

        return NoIgnore()

    def _matches_problematic_operation_invocation(
        self,
        db_operation: DbOperation,
        expression: ExpressionWithArgs,
        all_involved_characteristics: set[ExpressionCharacteristics],
    ) -> IgnoreVerdict:
        if "$ AT TIME ZONE $" in db_operation.pattern:
            return YesIgnore("other time-zone handling")

        if (
            "$::TIMESTAMPTZ($)" in db_operation.pattern
            or "$::TIMESTAMP($)" in db_operation.pattern
        ) and expression.args[0].has_any_characteristic(
            {ExpressionCharacteristics.NULL}
        ):
            return YesIgnore("#22014: timestamp precision with null")

        if (
            db_operation.pattern in {"$ + $", "$ - $"}
            and db_operation.params[0].get_declared_type_category()
            == DataTypeCategory.DATE_TIME
            and db_operation.params[1].get_declared_type_category()
            == DataTypeCategory.DATE_TIME
        ):
            return YesIgnore("#24578: different representation")

        if db_operation.pattern in {"$ ~ $", "$ ~* $", "$ !~ $", "$ !~* $"}:
            return YesIgnore(
                "Materialize regular expressions are similar to, but not identical to, PostgreSQL regular expressions."
            )

        if (
            db_operation.pattern in {"$ || $"}
            and db_operation.params[0].get_declared_type_category()
            == DataTypeCategory.JSONB
        ):
            return YesIgnore("#23578: empty result in || with simple values")

        if db_operation.pattern in [
            "position($ IN $)",
            "trim($ $ FROM $)",
        ] and expression.matches(
            partial(
                involves_data_type_category, data_type_category=DataTypeCategory.JSONB
            ),
            True,
        ):
            return YesIgnore("Consequence of #23571")

        if db_operation.pattern == "CAST ($ AS $)":
            casting_target = expression.args[1]
            assert isinstance(casting_target, EnumConstant)

            if casting_target.value == "DECIMAL(39)":
                return YesIgnore(
                    "#24678: different specification of default DECIMAL type"
                )

        return NoIgnore()


class PgPostExecutionInconsistencyIgnoreFilter(
    PostExecutionInconsistencyIgnoreFilterBase
):
    def _shall_ignore_success_mismatch(
        self,
        error: ValidationError,
        query_template: QueryTemplate,
        contains_aggregation: bool,
    ) -> IgnoreVerdict:
        outcome_by_strategy_id = error.query_execution.get_outcome_by_strategy_key()

        mz_outcome = outcome_by_strategy_id[EvaluationStrategyKey.MZ_DATAFLOW_RENDERING]
        pg_outcome = outcome_by_strategy_id[EvaluationStrategyKey.POSTGRES]

        if isinstance(pg_outcome, QueryFailure):
            return self._shall_ignore_pg_failure_where_mz_succeeds(pg_outcome)

        if isinstance(mz_outcome, QueryFailure):
            return self._shall_ignore_mz_failure_where_pg_succeeds(
                query_template, mz_outcome
            )

        return super()._shall_ignore_success_mismatch(
            error, query_template, contains_aggregation
        )

    def _shall_ignore_pg_failure_where_mz_succeeds(
        self, pg_outcome: QueryFailure
    ) -> IgnoreVerdict:
        pg_error_msg = pg_outcome.error_message

        if is_unknown_function_or_operation_invocation(pg_error_msg):
            # this does not necessarily mean that the function exists in one database but not the other; it could also
            # be a subsequent error when an expression (an argument) is evaluated to another type
            return YesIgnore(
                "Function or operation does not exist for the evaluated input"
            )

        if 'syntax error at or near "="' in pg_error_msg:
            # Postgres does not take: bool_null = bool_false = bool_true
            return YesIgnore("accepted")

        if "smallint out of range" in pg_error_msg:
            # mz handles this better
            return YesIgnore("accepted")

        if " out of range" in pg_error_msg:
            return YesIgnore("#22265")

        if "value overflows numeric format" in pg_error_msg:
            return YesIgnore("#21994")

        if _error_message_is_about_zero_or_value_ranges(pg_error_msg):
            return YesIgnore("Caused by a different precision")

        if (
            "cannot get array length of a scalar" in pg_error_msg
            or "cannot get array length of a non-array" in pg_error_msg
            or "cannot delete from scalar" in pg_error_msg
        ):
            return YesIgnore("Not supported by pg")

        if (
            "cannot cast type boolean to bigint" in pg_error_msg
            or "cannot cast type bigint to boolean" in pg_error_msg
        ):
            return YesIgnore("Not supported by pg")

        if 'invalid input syntax for type time: ""' in pg_error_msg:
            return YesIgnore("#24736: different handling of empty time string")

        return NoIgnore()

    def _shall_ignore_mz_failure_where_pg_succeeds(
        self, query_template: QueryTemplate, mz_outcome: QueryFailure
    ) -> IgnoreVerdict:
        mz_error_msg = mz_outcome.error_message

        if is_unknown_function_or_operation_invocation(mz_error_msg):
            function_name = extract_unknown_function_from_error_msg(mz_error_msg)
            # this does not necessarily mean that the function exists in one database but not the other; it could
            # also be a subsequent error when an expression (an argument) is evaluated to another type
            return YesIgnore(
                f"Function or operation does not exist for the evaluated input: {function_name}"
            )

        def matches_round_function(expression: Expression) -> bool:
            return (
                isinstance(expression, ExpressionWithArgs)
                and isinstance(expression.operation, DbFunction)
                and expression.operation.function_name_in_lower_case == "round"
            )

        if (
            "value out of range: overflow" in mz_error_msg
            and query_template.matches_any_expression(matches_round_function, True)
        ):
            return YesIgnore("#22028: round overflow")

        if (
            "value out of range: overflow" in mz_error_msg
            or "cannot take square root of a negative number" in mz_error_msg
            # both "inf" and "-inf"
            or 'inf" real out of range' in mz_error_msg
            or 'inf" double precision out of range' in mz_error_msg
        ):
            return YesIgnore("#21994: overflow")

        if (
            "value out of range: underflow" in mz_error_msg
            or '"-inf" real out of range' in mz_error_msg
        ):
            return YesIgnore("#21995: underflow")

        if (
            "precision for type timestamp or timestamptz must be between 0 and 6"
            in mz_error_msg
        ):
            return YesIgnore("#22020: unsupported timestamp precision")

        if "array_agg on arrays not yet supported" in mz_error_msg:
            return YesIgnore("(no ticket)")

        if "field position must be greater than zero" in mz_error_msg:
            return YesIgnore("#22023: split_part")

        if "timestamp out of range" in mz_error_msg:
            return YesIgnore("#22264")

        if "invalid regular expression: regex parse error" in mz_error_msg:
            return YesIgnore("#22956")

        if "invalid regular expression flag" in mz_error_msg:
            return YesIgnore("#22958")

        if "unit 'invalid_value_123' not recognized" in mz_error_msg:
            return YesIgnore("#22957")

        if "invalid time zone" in mz_error_msg:
            return YesIgnore("#22984")

        if _error_message_is_about_zero_or_value_ranges(mz_error_msg):
            return YesIgnore("Caused by a different precision")

        return NoIgnore()

    def _shall_ignore_content_mismatch(
        self,
        error: ValidationError,
        query_template: QueryTemplate,
        contains_aggregation: bool,
    ) -> IgnoreVerdict:
        def matches_math_aggregation_fun(expression: Expression) -> bool:
            return matches_fun_by_any_name(
                expression,
                {
                    "sum",
                    "avg",
                    "var_pop",
                    "var_samp",
                    "stddev_pop",
                    "stddev_samp",
                },
            )

        def matches_math_op_with_large_or_tiny_val(expression: Expression) -> bool:
            if isinstance(expression, ExpressionWithArgs):
                if isinstance(expression.operation, DbOperation):
                    return expression.operation.pattern in {
                        "$ + $",
                        "$ - $",
                        "$ / $",
                        "$ % $",
                    } and expression.has_any_characteristic(
                        {
                            ExpressionCharacteristics.MAX_VALUE,
                            ExpressionCharacteristics.TINY_VALUE,
                        }
                    )
            return False

        def matches_fun_with_problematic_floating_behavior(
            expression: Expression,
        ) -> bool:
            return matches_fun_by_any_name(
                expression,
                {
                    "sin",
                    "cos",
                    "tan",
                    "asin",
                    "acos",
                    "atan",
                    "asinh",
                    "acosh",
                    "atanh",
                    "cot",
                    "cos",
                    "cosh",
                    "log",
                    "log10",
                    "ln",
                    "pow",
                    "radians",
                },
            )

        def matches_mod_with_decimal(expression: Expression) -> bool:
            if isinstance(expression, ExpressionWithArgs) and isinstance(
                expression.operation, DbOperation
            ):
                if expression.operation.pattern == "$ % $":
                    arg1_ret_type_spec = expression.args[1].resolve_return_type_spec()

                    if isinstance(arg1_ret_type_spec, NumericReturnTypeSpec):
                        return arg1_ret_type_spec.always_floating_type

            return False

        def matches_nullif(expression: Expression) -> bool:
            if isinstance(expression, ExpressionWithArgs) and isinstance(
                expression.operation, DbFunction
            ):
                return expression.operation.function_name_in_lower_case == "nullif"

            return False

        if query_template.matches_any_expression(matches_math_aggregation_fun, True):
            return YesIgnore("#22003: aggregation function")

        if query_template.matches_any_expression(
            matches_fun_with_problematic_floating_behavior, True
        ):
            return YesIgnore("Different precision causes issues")

        if query_template.matches_any_expression(matches_float_comparison, True):
            return YesIgnore("Caused by a different precision")

        if query_template.matches_any_expression(matches_mod_with_decimal, True):
            return YesIgnore("Caused by a different precision")

        if query_template.matches_any_expression(
            partial(matches_fun_by_name, function_name_in_lower_case="mod"), True
        ):
            return YesIgnore("#22005: mod")

        if query_template.matches_any_expression(
            partial(
                matches_x_or_y,
                x=partial(matches_fun_by_name, function_name_in_lower_case="date_part"),
                y=partial(matches_op_by_pattern, pattern="EXTRACT($ FROM $)"),
            ),
            True,
        ):
            return YesIgnore("#23586")

        if query_template.matches_any_expression(
            matches_math_op_with_large_or_tiny_val, True
        ):
            return YesIgnore("#22266: arithmetic funs with large value")

        if query_template.matches_any_expression(matches_nullif, True):
            return YesIgnore("#22267: nullif")

        if query_template.matches_any_expression(
            partial(matches_op_by_pattern, pattern="CAST ($ AS $)"),
            True,
        ):
            # cut ".000" endings
            value1_str = re.sub(r"\.0+$", "", str(error.value1))
            value2_str = re.sub(r"\.0+$", "", str(error.value2))

            if value1_str == value2_str:
                return YesIgnore("#24687: different representation of DECIMAL type")

        return NoIgnore()

    def _shall_ignore_row_count_mismatch(
        self,
        error: ValidationError,
        query_template: QueryTemplate,
        contains_aggregation: bool,
    ) -> IgnoreVerdict:
        if query_template.matches_any_expression(matches_float_comparison, True):
            return YesIgnore("Caused by a different precision")

        return super()._shall_ignore_row_count_mismatch(
            error, query_template, contains_aggregation
        )


def matches_float_comparison(expression: Expression) -> bool:
    if isinstance(expression, ExpressionWithArgs) and isinstance(
        expression.operation, DbOperation
    ):
        if expression.operation.pattern == "$ = $":
            type_spec_arg0 = expression.args[0].resolve_return_type_spec()
            type_spec_arg1 = expression.args[1].resolve_return_type_spec()

            return (
                isinstance(type_spec_arg0, NumericReturnTypeSpec)
                and type_spec_arg0.always_floating_type
            ) or (
                isinstance(type_spec_arg1, NumericReturnTypeSpec)
                and type_spec_arg1.always_floating_type
            )

    return False


def _error_message_is_about_zero_or_value_ranges(message: str) -> bool:
    return (
        "is not defined for zero" in message
        or "is not defined for negative numbers" in message
        or "zero raised to a negative power is undefined" in message
        or "cannot take logarithm of zero" in message
        or "cannot take logarithm of a negative number" in message
        or "division by zero" in message
        or "is defined for numbers between -1 and 1 inclusive" in message
        or "is defined for numbers greater than or equal to 1" in message
        or "cannot take square root of a negative number" in message
        or "negative substring length not allowed" in message
        or "input is out of range" in message
        or "pow cannot return complex numbers" in message
        or "timestamp cannot be NaN" in message
        or "a negative number raised to a non-integer power yields a complex result"
        in message
    )


def is_unknown_function_or_operation_invocation(error_msg: str) -> bool:
    return (
        "No function matches the given name and argument types" in error_msg
        or "No operator matches the given name and argument types" in error_msg
        or ("WHERE clause error: " in error_msg and "does not exist" in error_msg)
    )


def extract_unknown_function_from_error_msg(error_msg: str) -> str | None:
    match = NAME_OF_NON_EXISTING_FUNCTION_PATTERN.search(error_msg)

    if match is not None:
        function_name = match.group(1)
        return function_name

    # do not parse not existing operators
    return None
