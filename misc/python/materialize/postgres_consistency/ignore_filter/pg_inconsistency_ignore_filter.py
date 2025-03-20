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
    involves_data_type_categories,
    involves_data_type_category,
    is_any_date_time_expression,
    is_known_to_involve_exact_data_types,
    is_operation_tagged,
    is_table_function,
    matches_any_expression_arg,
    matches_fun_by_any_name,
    matches_fun_by_name,
    matches_op_by_any_pattern,
    matches_op_by_pattern,
    matches_x_and_y,
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
from materialize.output_consistency.input_data.operations.array_operations_provider import (
    TAG_ARRAY_INDEX_OPERATION,
)
from materialize.output_consistency.input_data.operations.equality_operations_provider import (
    TAG_EQUALITY,
    TAG_EQUALITY_ORDERING,
)
from materialize.output_consistency.input_data.operations.generic_operations_provider import (
    TAG_CASTING,
)
from materialize.output_consistency.input_data.operations.jsonb_operations_provider import (
    TAG_JSONB_AGGREGATION,
    TAG_JSONB_OBJECT_GENERATION,
    TAG_JSONB_TO_TEXT,
    TAG_JSONB_VALUE_ACCESS,
)
from materialize.output_consistency.input_data.operations.number_operations_provider import (
    TAG_BASIC_ARITHMETIC_OP,
)
from materialize.output_consistency.input_data.operations.record_operations_provider import (
    TAG_RECORD_CREATION,
)
from materialize.output_consistency.input_data.operations.string_operations_provider import (
    TAG_REGEX,
)
from materialize.output_consistency.input_data.operations.table_operations_provider import (
    TAG_TABLE_FUNCTION_WITH_NON_NUMERIC_SORT_ORDER,
)
from materialize.output_consistency.input_data.return_specs.number_return_spec import (
    NumericReturnTypeSpec,
)
from materialize.output_consistency.input_data.types.date_time_types_provider import (
    TIME_TYPE_IDENTIFIER,
)
from materialize.output_consistency.input_data.types.number_types_provider import (
    DECIMAL_TYPE_IDENTIFIERS,
    DOUBLE_TYPE_IDENTIFIER,
    REAL_TYPE_IDENTIFIER,
)
from materialize.output_consistency.input_data.types.string_type_provider import (
    BPCHAR_8_TYPE_IDENTIFIER,
    CHAR_6_TYPE_IDENTIFIER,
)
from materialize.output_consistency.operation.operation import (
    DbFunction,
    DbOperation,
    DbOperationOrFunction,
)
from materialize.output_consistency.query.query_result import QueryFailure, QueryResult
from materialize.output_consistency.query.query_template import QueryTemplate
from materialize.output_consistency.selection.column_selection import (
    ALL_QUERY_COLUMNS_BY_INDEX_SELECTION,
)
from materialize.output_consistency.validation.validation_message import ValidationError

NAME_OF_NON_EXISTING_FUNCTION_PATTERN = re.compile(
    r"function (\w)\(.*?\) does not exist"
)

MATH_FUNCTIONS_WITH_PROBLEMATIC_FLOATING_BEHAVIOR = {
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
    "trunc",
}

MATH_AGGREGATION_FUNCTIONS = {
    "sum",
    "avg",
    "var_pop",
    "var_samp",
    "stddev_pop",
    "stddev_samp",
}

MATH_ARITHMETIC_OP_PATTERNS = {"$ + $", "$ - $", "$ / $", "$ % $"}


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
        all_involved_characteristics: set[ExpressionCharacteristics],
    ) -> IgnoreVerdict:
        if matches_float_comparison(expression):
            return YesIgnore("database-issues#6630: real with decimal comparison")

        if (
            operation.is_tagged(TAG_JSONB_TO_TEXT)
            or operation.is_tagged(TAG_CASTING)
            or operation.is_tagged(TAG_RECORD_CREATION)
        ) and expression.matches(
            partial(
                involves_data_type_category, data_type_category=DataTypeCategory.JSONB
            ),
            True,
        ):
            return YesIgnore("Consequence of database-issues#7085")

        if operation.is_tagged(TAG_CASTING) and expression.matches(
            partial(matches_fun_by_name, function_name_in_lower_case="to_char"),
            True,
        ):
            return YesIgnore("database-issues#7529: date format that cannot be parsed")

        if (
            expression.matches(
                partial(
                    is_known_to_involve_exact_data_types,
                    internal_data_type_identifiers={
                        BPCHAR_8_TYPE_IDENTIFIER,
                        CHAR_6_TYPE_IDENTIFIER,
                    },
                ),
                True,
            )
            and ExpressionCharacteristics.STRING_WITH_SPECIAL_SPACE_CHARS
            in all_involved_characteristics
        ):
            return YesIgnore("database-issues#8061: bpchar and char trim newline")

        if operation.is_tagged(TAG_JSONB_AGGREGATION) and expression.matches(
            partial(
                is_known_to_involve_exact_data_types,
                internal_data_type_identifiers={
                    BPCHAR_8_TYPE_IDENTIFIER,
                    CHAR_6_TYPE_IDENTIFIER,
                },
            ),
            True,
        ):
            return YesIgnore(
                "database-issues#8267: bpchar in jsonb aggregation without spaces"
            )

        if expression.matches(
            partial(matches_any_expression_arg, arg_matcher=is_table_function),
            True,
        ) and expression.has_any_characteristic({ExpressionCharacteristics.NULL}):
            return YesIgnore(
                "database-issues#8410: table functions: combination with operations with NULL"
            )

        return super()._matches_problematic_operation_or_function_invocation(
            expression, operation, all_involved_characteristics
        )

    def _matches_problematic_function_invocation(
        self,
        db_function: DbFunction,
        expression: ExpressionWithArgs,
        all_involved_characteristics: set[ExpressionCharacteristics],
    ) -> IgnoreVerdict:
        if db_function.function_name_in_lower_case == "generate_subscripts":
            return YesIgnore(
                "database-issues#8410: table functions: combination with operations with NULL"
            )

        if db_function.function_name_in_lower_case == "jsonb_pretty":
            return YesIgnore("Accepted")

        if db_function.function_name_in_lower_case == "date_trunc":
            precision = expression.args[0]
            if isinstance(precision, EnumConstant) and precision.value == "second":
                return YesIgnore("database-issues#6628: date_trunc with seconds")
            if isinstance(precision, EnumConstant) and precision.value == "quarter":
                return YesIgnore(
                    "database-issues#6614: date_trunc does not support quarter"
                )

        if db_function.function_name_in_lower_case == "lpad" and expression.args[
            1
        ].has_any_characteristic({ExpressionCharacteristics.NEGATIVE}):
            return YesIgnore("database-issues#6615: lpad with negative")

        if db_function.function_name_in_lower_case in {"min", "max"}:
            return_type_category = expression.args[0].resolve_return_type_category()
            if return_type_category == DataTypeCategory.STRING:
                return YesIgnore(
                    "database-issues#6620: ordering on text different (min/max)"
                )
            if return_type_category == DataTypeCategory.JSONB:
                return YesIgnore(
                    "database-issues#7812: ordering on JSON different (min/max)"
                )
            if return_type_category == DataTypeCategory.ARRAY:
                return YesIgnore(
                    "database-issues#8108: ordering on array different (min/max)"
                )

        if db_function.function_name_in_lower_case == "replace":
            # replace is not working properly with empty text; however, it is not possible to reliably determine if an
            # expression is an empty text, we therefore need to exclude the function completely
            return YesIgnore("database-issues#6619: replace")

        if db_function.function_name_in_lower_case == "regexp_replace":
            if expression.args[2].has_any_characteristic(
                {ExpressionCharacteristics.STRING_WITH_BACKSLASH_CHAR}
            ):
                return YesIgnore("database-issues#7095: regexp with backslash")
            if expression.count_args() == 4:
                regex_flag = expression.args[3]
                assert isinstance(regex_flag, EnumConstant)
                if regex_flag.value == "g":
                    return YesIgnore("Strange Postgres behavior (see PR 26526)")

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

        if db_function.function_name_in_lower_case == "decode" and expression.args[
            0
        ].has_any_characteristic(
            {ExpressionCharacteristics.STRING_WITH_SPECIAL_SPACE_CHARS}
        ):
            return YesIgnore(
                "database-issues#7733 (base64 decode with new line and tab)"
            )

        if (
            db_function.function_name_in_lower_case == "coalesce"
            and expression.matches(
                partial(
                    is_known_to_involve_exact_data_types,
                    internal_data_type_identifiers={
                        BPCHAR_8_TYPE_IDENTIFIER,
                        CHAR_6_TYPE_IDENTIFIER,
                    },
                ),
                True,
            )
        ):
            # do not explicitly require the TEXT type to be included because it can appear by applying || to two char values
            return YesIgnore("database-issues#8067: bpchar and char with coalesce")

        if db_function.function_name_in_lower_case == "row":
            if expression.matches(
                partial(
                    involves_data_type_categories,
                    data_type_categories={
                        DataTypeCategory.RANGE,
                        DataTypeCategory.ARRAY,
                    },
                ),
                True,
            ):
                return YesIgnore(
                    "materialize#28130 / database-issues#8245: record type with array or ranges"
                )

            if expression.matches(
                partial(
                    involves_data_type_category,
                    data_type_category=DataTypeCategory.BYTEA,
                ),
                True,
            ):
                return YesIgnore("database-issues#8314: record type with bytea")

            if expression.matches(
                partial(
                    is_known_to_involve_exact_data_types,
                    internal_data_type_identifiers=DECIMAL_TYPE_IDENTIFIERS,
                ),
                True,
            ):
                return YesIgnore(
                    "Consequence of database-issues#7675: decimal 0s are not shown"
                )

        if expression.matches(
            partial(
                matches_fun_by_name,
                function_name_in_lower_case="pg_size_pretty",
            ),
            True,
        ):
            if expression.matches(
                partial(
                    is_known_to_involve_exact_data_types,
                    internal_data_type_identifiers=DECIMAL_TYPE_IDENTIFIERS,
                ),
                True,
            ):
                return YesIgnore(
                    "Consequence of database-issues#7675: decimal 0s are not shown"
                )

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
            return YesIgnore("database-issues#6625: timestamp precision with null")

        if (
            db_operation.pattern in {"$ + $", "$ - $"}
            and db_operation.params[0].get_declared_type_category()
            == DataTypeCategory.DATE_TIME
            and db_operation.params[1].get_declared_type_category()
            == DataTypeCategory.DATE_TIME
        ):
            return YesIgnore("database-issues#7325: different representation")

        if db_operation.pattern == "$ % $" and (
            ExpressionCharacteristics.MAX_VALUE in all_involved_characteristics
            or ExpressionCharacteristics.TINY_VALUE in all_involved_characteristics
        ):
            return YesIgnore("database-issues#8134: modulo with large / tiny values")

        if db_operation.is_tagged(TAG_REGEX):
            return YesIgnore(
                "Materialize regular expressions are similar to, but not identical to, PostgreSQL regular expressions."
            )

        if (
            (
                db_operation.is_tagged(TAG_EQUALITY_ORDERING)
                or db_operation.is_tagged(TAG_EQUALITY)
                or db_operation.pattern == "$ IN ($)"
            )
            and expression.args[0].resolve_return_type_category()
            == DataTypeCategory.NUMERIC
            and expression.args[1].resolve_return_type_category()
            == DataTypeCategory.NUMERIC
        ):
            return YesIgnore(
                "database-issues#7815: imprecise comparison between REAL and DECIMAL"
            )

        if (
            db_operation.pattern in {"$ || $"}
            and db_operation.params[0].get_declared_type_category()
            == DataTypeCategory.JSONB
        ):
            return YesIgnore(
                "database-issues#7087: empty result in || with simple values"
            )

        if db_operation.pattern in [
            "position($ IN $)",
            "trim($ $ FROM $)",
        ] and expression.matches(
            partial(
                involves_data_type_category, data_type_category=DataTypeCategory.JSONB
            ),
            True,
        ):
            return YesIgnore("Consequence of database-issues#7085")

        if db_operation.is_tagged(TAG_CASTING):
            casting_target = expression.args[1]
            assert isinstance(casting_target, EnumConstant)

            if casting_target.value == "DECIMAL(39)":
                return YesIgnore(
                    "database-issues#7344: different specification of default DECIMAL type"
                )

        if db_operation.is_tagged(TAG_EQUALITY_ORDERING):
            return_type_category_1 = expression.args[0].resolve_return_type_category()
            return_type_category_2 = expression.args[1].resolve_return_type_category()
            if DataTypeCategory.STRING in {
                return_type_category_1,
                return_type_category_2,
            }:
                return YesIgnore(
                    "database-issues#6620: ordering on text different (<, <=, ...)"
                )
            if DataTypeCategory.JSONB in {
                return_type_category_1,
                return_type_category_2,
            }:
                return YesIgnore(
                    "database-issues#7812: ordering on JSON different (<, <=, ...)"
                )
            if DataTypeCategory.ARRAY in {
                return_type_category_1,
                return_type_category_2,
            }:
                return YesIgnore(
                    "database-issues#8108: ordering on array different (<, <=, ...)"
                )

        if db_operation.is_tagged(TAG_CASTING) and expression.matches(
            partial(
                is_known_to_involve_exact_data_types,
                internal_data_type_identifiers={
                    BPCHAR_8_TYPE_IDENTIFIER,
                    CHAR_6_TYPE_IDENTIFIER,
                },
            ),
            True,
        ):
            return YesIgnore("database-issues#8068: casting bpchar or char")

        if (
            db_operation.pattern == "$ IS NULL"
            and ExpressionCharacteristics.NULL in all_involved_characteristics
            and expression.matches(
                partial(
                    involves_data_type_category,
                    data_type_category=DataTypeCategory.RECORD,
                ),
                True,
            )
        ):
            return YesIgnore("database-issues#8632: IS NULL on record with NULL value")

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
            assert isinstance(mz_outcome, QueryResult)
            return self._shall_ignore_pg_failure_where_mz_succeeds(
                pg_outcome, mz_outcome, query_template
            )

        if isinstance(mz_outcome, QueryFailure):
            assert isinstance(pg_outcome, QueryResult)
            return self._shall_ignore_mz_failure_where_pg_succeeds(
                query_template, mz_outcome
            )

        if query_template.matches_any_expression(
            partial(
                is_operation_tagged,
                tag=TAG_JSONB_OBJECT_GENERATION,
            ),
            True,
        ) and (
            len(
                set.intersection(
                    {
                        ExpressionCharacteristics.MAX_VALUE,
                        ExpressionCharacteristics.INFINITY,
                        ExpressionCharacteristics.NAN,
                    },
                    query_template.get_involved_characteristics(
                        ALL_QUERY_COLUMNS_BY_INDEX_SELECTION
                    ),
                )
            )
            > 0
            or query_template.matches_any_expression(
                partial(
                    matches_any_expression_arg,
                    arg_matcher=partial(
                        matches_fun_by_any_name,
                        function_names_in_lower_case=MATH_FUNCTIONS_WITH_PROBLEMATIC_FLOATING_BEHAVIOR,
                    ),
                ),
                True,
            )
        ):
            return YesIgnore(
                "Deviations in floating point functions may cause an invalid JSONB (e.g., database-issues#8505 producing NaN in mz)"
            )

        return super()._shall_ignore_success_mismatch(
            error, query_template, contains_aggregation
        )

    def _shall_ignore_pg_failure_where_mz_succeeds(
        self,
        pg_outcome: QueryFailure,
        mz_outcome: QueryResult,
        query_template: QueryTemplate,
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
            return YesIgnore("database-issues#6717")

        if "value overflows numeric format" in pg_error_msg:
            return YesIgnore("database-issues#6612")

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

        if "invalid regular expression: parentheses () not balanced" in pg_error_msg:
            return YesIgnore(
                "Materialize regular expressions are similar to, but not identical to, PostgreSQL regular expressions."
            )

        if 'invalid input syntax for type time: ""' in pg_error_msg:
            return YesIgnore(
                "database-issues#7367: different handling of empty time string"
            )

        if (
            'syntax error at or near "NULL"' in pg_error_msg
            and query_template.matches_any_expression(
                partial(matches_op_by_pattern, pattern="EXTRACT($ FROM $)"), True
            )
        ):
            return YesIgnore(
                "database-issues#8019: different error handling when extracting from timestamp"
            )

        if (
            "invalid input syntax for type uuid" in pg_error_msg
            and mz_outcome.row_count() == 0
        ):
            return YesIgnore(
                "mz does not evaluate a failing, constant expression when the result contains zero rows"
            )

        if query_template.matches_any_expression(
            partial(matches_fun_by_name, function_name_in_lower_case="coalesce"), True
        ):
            return YesIgnore(
                "Postgres resolves all arguments, possibly resulting in an evaluation error"
            )

        if query_template.matches_any_expression(
            partial(matches_fun_by_name, function_name_in_lower_case="pg_typeof"),
            True,
        ):
            return YesIgnore("mz shortcuts the evaluation, avoiding evaluation errors")

        if (
            "field name must not be null" == pg_error_msg
            and query_template.matches_any_expression(
                partial(
                    matches_fun_by_name,
                    function_name_in_lower_case="jsonb_object_agg",
                ),
                True,
            )
        ):
            return YesIgnore("database-issues#8246: jsonb_object_agg with NULL as key")

        if (
            re.search("function pg_size_pretty(.*?) is not unique", pg_error_msg)
            is not None
        ):
            return YesIgnore("mz does not implement all overloadings")

        if (
            "key value must be scalar, not array, composite, or json" == pg_error_msg
            and query_template.matches_any_expression(
                partial(
                    matches_fun_by_name,
                    function_name_in_lower_case="jsonb_object_agg",
                ),
                True,
            )
        ):
            return YesIgnore(
                "database-issues#8249: jsonb_object_agg with non-scalar key"
            )

        if query_template.matches_any_expression(
            partial(
                is_operation_tagged,
                tag=TAG_JSONB_VALUE_ACCESS,
            ),
            True,
        ):
            return YesIgnore("Different evaluation order")

        if "argument of IN must not return a set" in pg_error_msg:
            return YesIgnore("Not supported by Postgres")

        if query_template.matches_any_expression(
            partial(
                matches_op_by_pattern,
                pattern="$ IS NULL",
            ),
            True,
        ):
            return YesIgnore("Evaluation shortcut for IS NULL")

        if (
            "FULL JOIN is only supported with merge-joinable or hash-joinable join conditions"
            in pg_error_msg
        ):
            return YesIgnore("Not supported by Postgres")

        if query_template.matches_any_expression(
            is_table_function,
            True,
        ) and ExpressionCharacteristics.NULL in query_template.get_involved_characteristics(
            ALL_QUERY_COLUMNS_BY_INDEX_SELECTION
        ):
            # where condition will not be evaluated if mz knows that the number of resulting
            # rows is zero
            return YesIgnore("Evaluation shortcut for table functions")

        if (
            query_template.matches_any_expression(
                partial(
                    matches_fun_by_name, function_name_in_lower_case="jsonb_object_agg"
                ),
                True,
            )
            and mz_outcome.row_count() == 0
        ):
            return YesIgnore("Evaluation shortcut")

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
            return YesIgnore("database-issues#6634: round overflow")

        if (
            "value out of range: overflow" in mz_error_msg
            or "cannot take square root of a negative number" in mz_error_msg
            # both "inf" and "-inf"
            or 'inf" real out of range' in mz_error_msg
            or 'inf" double precision out of range' in mz_error_msg
        ):
            return YesIgnore("database-issues#6612: overflow")

        if (
            "value out of range: underflow" in mz_error_msg
            or '"-inf" real out of range' in mz_error_msg
        ):
            return YesIgnore("database-issues#6613: underflow")

        if (
            "precision for type timestamp or timestamptz must be between 0 and 6"
            in mz_error_msg
        ):
            return YesIgnore("database-issues#6629: unsupported timestamp precision")

        if "array_agg on arrays not yet supported" in mz_error_msg:
            return YesIgnore("database-issues#8310: array_agg on arrays")

        if "field position must be greater than zero" in mz_error_msg:
            return YesIgnore("database-issues#6631: split_part")

        if "timestamp out of range" in mz_error_msg:
            return YesIgnore("database-issues#6716")

        if "bigint out of range" in mz_error_msg:
            # when a large decimal number or NaN is used as an array index
            return YesIgnore("database-issues#8252")

        if "invalid regular expression: regex parse error" in mz_error_msg:
            return YesIgnore("database-issues#6921")

        if "invalid regular expression flag" in mz_error_msg:
            return YesIgnore("database-issues#6923")

        if "unit 'invalid_value_123' not recognized" in mz_error_msg:
            return YesIgnore("database-issues#6922")

        if "invalid time zone" in mz_error_msg:
            return YesIgnore("database-issues#6927")

        if _error_message_is_about_zero_or_value_ranges(mz_error_msg):
            return YesIgnore("Caused by a different precision")

        if query_template.limit == 0:
            return YesIgnore("database-issues#4972: LIMIT 0 does not swallow errors")

        if (
            query_template.matches_any_expression(
                partial(matches_fun_by_name, function_name_in_lower_case="pg_typeof"),
                True,
            )
            and "invalid input syntax for type" in mz_error_msg
        ):
            # Postgres returns regtype which can be cast to numbers while mz returns a string
            return YesIgnore("regtype of postgres can be cast")

        if "array_agg on character not yet supported" in mz_error_msg:
            return YesIgnore("database-issues#8060: array_agg on character")

        if "array subscript does not support slices" in mz_error_msg:
            return YesIgnore("array subscript does not support slices")

        if "|| does not support implicitly casting" in mz_error_msg:
            return YesIgnore(
                "database-issues#8219: no implicit casting from ...[] to ...[]"
            )

        if "cannot reference pseudo type pg_catalog.record" in mz_error_msg:
            return YesIgnore("database-issues#5211: cannot reference pg_catalog.record")

        if query_template.matches_any_expression(
            partial(is_operation_tagged, tag=TAG_ARRAY_INDEX_OPERATION),
            True,
        ):
            return YesIgnore("Different evaluation order")

        if (
            "numeric field overflow" in mz_error_msg
            and query_template.matches_any_expression(
                partial(
                    involves_data_type_category,
                    data_type_category=DataTypeCategory.JSONB,
                ),
                True,
            )
        ):
            return YesIgnore("database-issues#8258: JSONB with large number")

        if (
            "function casting double precision to numeric is only defined for finite arguments"
            in mz_error_msg
        ):
            return YesIgnore("database-issues#8281: infinity to decimal")

        if "invalid regular expression flag: n" in mz_error_msg:
            return YesIgnore("database-issues#8409: regex n flag")

        if "aggregate functions are not allowed in table function" in mz_error_msg:
            return YesIgnore(
                "database-issues#8313: aggregate functions are not allowed in table function arguments"
            )

        if "table functions are not allowed in other table functions" in mz_error_msg:
            return YesIgnore(
                "database-issues#8315: table functions are not allowed in other table functions"
            )

        if (
            "table functions are not allowed in aggregate function calls"
            in mz_error_msg
        ):
            return YesIgnore(
                "database-issues#8422: table functions are not allowed in aggregate function calls"
            )

        if (
            query_template.has_where_condition() or query_template.has_row_selection()
        ) and query_template.uses_join():
            return YesIgnore(
                "Different evaluation order of join clause and where filter"
            )

        if query_template.uses_join():
            # more generic catch
            return YesIgnore(
                "database-issues#8504: eager evaluation of select expression in mz"
            )

        if (
            "invalid input syntax for type jsonb" in mz_error_msg
            and "is out of range" in mz_error_msg
        ):
            return YesIgnore("value out of range")

        if "coalesce could not convert type" in mz_error_msg:
            return YesIgnore("database-issues#8648: coalesce could not convert type")

        return NoIgnore()

    def _shall_ignore_content_mismatch(
        self,
        error: ValidationError,
        query_template: QueryTemplate,
        contains_aggregation: bool,
        col_index: int,
        all_involved_characteristics: set[ExpressionCharacteristics],
    ) -> IgnoreVerdict:
        # Content mismatch ignore entries should only operate on the expression of the column with the mismatch (and on
        # expressions in other parts of the query like, for example, the WHERE part)!

        def matches_math_op_with_large_or_tiny_val(expression: Expression) -> bool:
            if isinstance(expression, ExpressionWithArgs):
                if isinstance(expression.operation, DbOperation):
                    return (
                        expression.operation.pattern in MATH_ARITHMETIC_OP_PATTERNS
                        and expression.has_any_characteristic(
                            {
                                ExpressionCharacteristics.MAX_VALUE,
                                ExpressionCharacteristics.TINY_VALUE,
                            }
                        )
                    )
            return False

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

        if query_template.matches_specific_select_or_filter_expression(
            col_index,
            partial(
                matches_fun_by_any_name,
                function_names_in_lower_case=MATH_AGGREGATION_FUNCTIONS,
            ),
            True,
        ):
            return YesIgnore("database-issues#6621: aggregation function")

        if query_template.matches_specific_select_or_filter_expression(
            col_index,
            partial(
                matches_fun_by_any_name,
                function_names_in_lower_case=MATH_FUNCTIONS_WITH_PROBLEMATIC_FLOATING_BEHAVIOR,
            ),
            True,
        ):
            return YesIgnore("Different precision causes issues")

        if query_template.matches_specific_select_or_filter_expression(
            col_index, matches_float_comparison, True
        ):
            return YesIgnore("Caused by a different precision")

        if query_template.matches_specific_select_or_filter_expression(
            col_index, matches_mod_with_decimal, True
        ):
            return YesIgnore("Caused by a different precision")

        if query_template.matches_specific_select_or_filter_expression(
            col_index,
            partial(matches_fun_by_name, function_name_in_lower_case="mod"),
            True,
        ):
            return YesIgnore("database-issues#6623: mod")

        if query_template.matches_specific_select_or_filter_expression(
            col_index,
            partial(
                matches_x_or_y,
                x=partial(matches_fun_by_name, function_name_in_lower_case="date_part"),
                y=partial(matches_op_by_pattern, pattern="EXTRACT($ FROM $)"),
            ),
            True,
        ):
            return YesIgnore("database-issues#7089")

        if query_template.matches_specific_select_or_filter_expression(
            col_index, matches_math_op_with_large_or_tiny_val, True
        ):
            return YesIgnore("database-issues#6718: arithmetic funs with large value")

        if query_template.matches_specific_select_or_filter_expression(
            col_index, matches_nullif, True
        ):
            return YesIgnore("database-issues#6719: nullif")

        if query_template.matches_specific_select_or_filter_expression(
            col_index,
            partial(is_operation_tagged, tag=TAG_CASTING),
            True,
        ) and query_template.matches_specific_select_or_filter_expression(
            col_index,
            partial(
                involves_data_type_category, data_type_category=DataTypeCategory.NUMERIC
            ),
            True,
        ):
            value1_str = str(error.details1.value)
            value2_str = str(error.details2.value)

            # cut ".000" endings
            value1_str = re.sub(r"\.0+$", "", value1_str)
            value2_str = re.sub(r"\.0+$", "", value2_str)

            # align exponent representation (e.g., '4.5e-07' => '4.5e-7')
            value1_str = re.sub(r"e-0+(\d+$)$", r"e-\1", value1_str)
            value2_str = re.sub(r"e-0+(\d+$)$", r"e-\1", value2_str)

            if value1_str == value2_str:
                return YesIgnore(
                    "database-issues#7348: different representation of floating-point type"
                )

        if (
            query_template.matches_specific_select_or_filter_expression(
                col_index,
                partial(
                    matches_fun_by_any_name,
                    function_names_in_lower_case={"upper", "initcap"},
                ),
                True,
            )
            and ExpressionCharacteristics.STRING_WITH_ESZETT
            in all_involved_characteristics
        ):
            return YesIgnore("database-issues#7938: eszett in upper")

        if query_template.matches_specific_select_or_filter_expression(
            col_index,
            partial(matches_fun_by_name, function_name_in_lower_case="pg_typeof"),
            True,
        ):
            verdict = self._shall_ignore_pg_typeof_content_mismatch(
                query_template, col_index
            )

            if verdict.ignore:
                return verdict

        if query_template.matches_specific_select_or_filter_expression(
            col_index,
            partial(
                is_operation_tagged,
                tag=TAG_JSONB_AGGREGATION,
            ),
            True,
        ):
            if query_template.matches_specific_select_or_filter_expression(
                col_index,
                partial(
                    involves_data_type_category,
                    data_type_category=DataTypeCategory.DATE_TIME,
                ),
                True,
            ):
                return YesIgnore("database-issues#8247: different date string in JSONB")

            if query_template.matches_specific_select_or_filter_expression(
                col_index,
                partial(
                    involves_data_type_category,
                    data_type_category=DataTypeCategory.NUMERIC,
                ),
                True,
            ):
                return YesIgnore("database-issues#8251: non-quoted numbers")

        if (
            ExpressionCharacteristics.DATE_WITH_SHORT_YEAR
            in all_involved_characteristics
        ):
            return YesIgnore("database-issues#8289: short date format")

        if query_template.matches_specific_select_or_filter_expression(
            col_index,
            partial(
                matches_op_by_any_pattern,
                patterns={"$ = ANY ($)", "$ = ALL ($)"},
            ),
            True,
        ) and (
            ExpressionCharacteristics.NULL in all_involved_characteristics
            or ExpressionCharacteristics.COLLECTION_EMPTY
            in all_involved_characteristics
        ):
            return YesIgnore(
                "database-issues#8293: ALL and ANY with NULL or empty array"
            )

        # use here matches_any_expression instead of matches_specific_select_or_filter_expression on purpose because
        # the concerned expression may affect other columns as well
        if query_template.matches_any_expression(
            partial(
                is_operation_tagged, tag=TAG_TABLE_FUNCTION_WITH_NON_NUMERIC_SORT_ORDER
            ),
            True,
        ) and (query_template.has_offset() or query_template.has_limit()):
            # When table functions are used, a row-order insensitive comparison will be conducted in the result
            # comparator. However, this is not sufficient when a LIMIT or OFFSET clause is present.
            return YesIgnore(
                "Different sort order (partially a consequence of database-issues#6620 and database-issues#7812)"
            )

        if query_template.matches_specific_select_or_filter_expression(
            col_index,
            partial(matches_fun_by_name, function_name_in_lower_case="unnest"),
            True,
        ) and query_template.matches_specific_select_or_filter_expression(
            col_index,
            partial(
                involves_data_type_categories,
                data_type_categories={
                    DataTypeCategory.ARRAY,
                },
            ),
            True,
        ):
            return YesIgnore("database-issues#8466: unnest on array uses wrong order")

        return NoIgnore()

    def _shall_ignore_pg_typeof_content_mismatch(
        self,
        query_template: QueryTemplate,
        col_index: int,
    ) -> IgnoreVerdict:
        if query_template.matches_specific_select_or_filter_expression(
            col_index, is_any_date_time_expression, True
        ):
            # "time without time zone" (mz) vs. "time" (pg)
            # The condition is rather generic because it must also match when a text operation (e.g., upper)
            # is applied to the string.
            return YesIgnore("Different type name for time")

        if query_template.matches_specific_select_or_filter_expression(
            col_index,
            partial(
                matches_fun_by_any_name, function_names_in_lower_case={"floor", "ceil"}
            ),
            True,
        ):
            return YesIgnore("database-issues#8407: return type of floor and ceil")

        if query_template.matches_specific_select_or_filter_expression(
            col_index,
            partial(matches_fun_by_name, function_name_in_lower_case="array_agg"),
            True,
        ):
            return YesIgnore(
                "database-issues#8028: array_agg(pg_typeof(...)) in pg flattens result"
            )

        if query_template.matches_specific_select_or_filter_expression(
            col_index,
            partial(
                is_known_to_involve_exact_data_types,
                internal_data_type_identifiers={REAL_TYPE_IDENTIFIER},
            ),
            True,
        ) and not query_template.matches_specific_select_or_filter_expression(
            col_index,
            partial(
                is_known_to_involve_exact_data_types,
                internal_data_type_identifiers={DOUBLE_TYPE_IDENTIFIER},
            ),
            True,
        ):
            # e.g., round(1::REAL) returns REAL in mz but DOUBLE PRECISION in pg
            return YesIgnore("mz does not use double when operating on real value")

        if query_template.matches_specific_select_or_filter_expression(
            col_index,
            partial(
                matches_x_and_y,
                x=partial(matches_fun_by_name, function_name_in_lower_case="pg_typeof"),
                y=partial(
                    matches_any_expression_arg,
                    arg_matcher=partial(
                        matches_fun_by_name, function_name_in_lower_case="pg_typeof"
                    ),
                ),
            ),
            True,
        ):
            # nested invocation of pg_typeof
            return YesIgnore(
                "pg_typeof(pg_typeof(...)) returns regtype in pg but text in mz"
            )

        if query_template.matches_specific_select_or_filter_expression(
            col_index, partial(is_operation_tagged, tag=TAG_BASIC_ARITHMETIC_OP), True
        ):
            return YesIgnore("database-issues#8417: numeric return type inconsistency")

        if query_template.matches_specific_select_or_filter_expression(
            col_index,
            partial(
                is_known_to_involve_exact_data_types,
                internal_data_type_identifiers={TIME_TYPE_IDENTIFIER},
            ),
            True,
        ):
            return YesIgnore("database-issues#8588: different type name for time")

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

    def _shall_ignore_content_type_mismatch(
        self,
        error: ValidationError,
        query_template: QueryTemplate,
        contains_aggregation: bool,
        col_index: int,
        all_involved_characteristics: set[ExpressionCharacteristics],
    ) -> IgnoreVerdict:
        details_by_strategy_key = error.get_details_by_strategy_key()

        mz_error = details_by_strategy_key[EvaluationStrategyKey.MZ_DATAFLOW_RENDERING]
        pg_error = details_by_strategy_key[EvaluationStrategyKey.POSTGRES]

        if mz_error.value == float and pg_error.value == int:
            return YesIgnore("database-issues#7811: float instead of int returned")

        return self._shall_ignore_content_mismatch(
            error,
            query_template,
            contains_aggregation,
            col_index,
            all_involved_characteristics,
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
        or re.search("function (.*?) does not exist", error_msg) is not None
        or "operator does not exist:" in error_msg
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
