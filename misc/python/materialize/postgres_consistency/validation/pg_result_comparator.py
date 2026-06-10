# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
import math
import re
from decimal import Decimal
from functools import partial
from typing import Any

from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.ignore_filter.expression_matchers import (
    is_operation_tagged,
    is_table_function,
)
from materialize.output_consistency.ignore_filter.inconsistency_ignore_filter import (
    GenericInconsistencyIgnoreFilter,
)
from materialize.output_consistency.input_data.operations.jsonb_operations_provider import (
    TAG_JSONB_OBJECT_GENERATION,
)
from materialize.output_consistency.query.query_result import QueryExecution
from materialize.output_consistency.validation.error_message_normalizer import (
    ErrorMessageNormalizer,
)
from materialize.output_consistency.validation.result_comparator import ResultComparator

# Examples:
# * 2038-01-19 03:14:18
# * 2038-01-19 03:14:18.123
# * 2038-01-19 03:14:18.123+00
# * 2038-01-19 03:14:18.123+00 BC
# * 2038-01-19 03:14:18+00
# * 2038-01-19 03:14:18-03:00
# * 2038-01-19T03:14:18+00 (when used in JSONB)
TIMESTAMP_PATTERN = re.compile(
    r"^\d{4,}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?([+-]\d+(:\d+)?)?( (BC|AC))?$"
)

# Examples:
# * NaN
# * 1
# * -1.23
# * 1.23e-3
# * 1.23e+3
DECIMAL_PATTERN = re.compile(r"^NaN|[+-]?\d+(\.\d+)?(e[+-]?\d+)?$")

# Examples:
# * ["1","2"]
# * [1,2]
# * [1, 2]
# * [1, [1, 2]]
SIMPLIFIED_ARRAY_PATTERN = re.compile(r"\[(.*),(.*)\]")

# Examples:
# * {"a": 1, "c": 3}
JSON_OBJECT_PATTERN = re.compile(r"\{(.*)[:,](.*)\}")


class PostgresResultComparator(ResultComparator):
    """Compares the outcome (result or failure) of multiple query executions"""

    def __init__(
        self,
        ignore_filter: GenericInconsistencyIgnoreFilter,
        error_message_normalizer: ErrorMessageNormalizer,
    ):
        super().__init__(ignore_filter, error_message_normalizer)
        self.floating_precision = 1e-03

    def shall_validate_error_message(self, query_execution: QueryExecution) -> bool:
        # do not compare error messages at all
        return False

    def is_value_equal(
        self,
        value1: Any,
        value2: Any,
        expression: Expression,
        is_tolerant: bool = False,
    ) -> bool:
        if super().is_value_equal(value1, value2, expression, is_tolerant=is_tolerant):
            return True

        if isinstance(value1, Decimal):
            if isinstance(value2, Decimal):
                return self.is_decimal_equal(value1, value2)
            if isinstance(value2, float):
                return self.is_decimal_equal(value1, Decimal(value2))
        if isinstance(value1, float):
            if isinstance(value2, float):
                return self.is_float_equal(value1, value2)
            if isinstance(value2, Decimal):
                return self.is_decimal_equal(Decimal(value1), value2)
        if isinstance(value1, str) and isinstance(value2, str):
            return self.is_str_equal(value1, value2, is_tolerant)

        if is_tolerant:
            type1 = type(value1)
            type2 = type(value2)

            if type1 in {int, float, Decimal} and type2 in {int, float, Decimal}:
                # This is needed for imprecise results of floating type operations that are returned as int or float
                # values in dicts of JSONB data.
                return self.is_decimal_equal(Decimal(value1), Decimal(value2))

        return False

    def is_decimal_equal(self, value1: Decimal, value2: Decimal) -> bool:
        if value1.is_nan():
            return value2.is_nan()

        return math.isclose(value1, value2, rel_tol=self.floating_precision)

    def is_float_equal(self, value1: float, value2: float) -> bool:
        if math.isnan(value1):
            return math.isnan(value2)

        return math.isclose(value1, value2, rel_tol=self.floating_precision)

    def is_str_equal(self, value1: str, value2: str, is_tolerant: bool) -> bool:
        if self.is_timestamp(value1) and self.is_timestamp(value2):
            return self.is_timestamp_equal(value1, value2)

        if (
            SIMPLIFIED_ARRAY_PATTERN.search(value1)
            or JSON_OBJECT_PATTERN.search(value1)
        ) and (
            SIMPLIFIED_ARRAY_PATTERN.search(value2)
            or JSON_OBJECT_PATTERN.search(value2)
        ):
            # This is a rather eager pattern to also match concatenated strings.
            # tracked with database-issues#7085
            value1 = value1.replace(", ", ",").replace(": ", ":")
            value2 = value2.replace(", ", ",").replace(": ", ":")

        # Postgres uses 'mon' / 'mons' instead of 'month' / 'months'
        value1 = value1.replace(" month", " mon")
        value2 = value2.replace(" month", " mon")

        if is_tolerant and self.is_decimal(value1) and self.is_decimal(value2):
            try:
                return self.is_decimal_equal(Decimal(value1), Decimal(value2))
            except Exception:
                return True

        return value1 == value2

    def is_decimal(self, value: str):
        return DECIMAL_PATTERN.match(value) is not None

    def is_timestamp(self, value: str) -> bool:
        return TIMESTAMP_PATTERN.match(value) is not None

    def is_timestamp_equal(self, value1: str, value2: str) -> bool:
        # try to match any of these
        last_second_and_milliseconds_regex = r"(\d\.\d+)"
        last_second_before_timezone_regex = r"(?<=:\d)(\d)(?=\+)"
        last_second_at_the_end_regex = r"(?<=:\d)(\d$)"

        last_second_and_milliseconds_pattern = re.compile(
            f"{last_second_and_milliseconds_regex}|{last_second_before_timezone_regex}|{last_second_at_the_end_regex}"
        )

        if last_second_and_milliseconds_pattern.search(
            value1
        ) and last_second_and_milliseconds_pattern.search(value2):
            # drop milliseconds and, if present, trunc last digit of second
            value1 = last_second_and_milliseconds_pattern.sub("0", value1)
            value2 = last_second_and_milliseconds_pattern.sub("0", value2)

        value1 = self._normalize_jsonb_timestamp(value1)
        value2 = self._normalize_jsonb_timestamp(value2)

        assert self.is_timestamp(value1)
        assert self.is_timestamp(value2)
        return value1 == value2

    def _normalize_jsonb_timestamp(self, value: str) -> str:
        # this is due to database-issues#8247

        pattern_for_date = r"\d+-\d+-\d+"
        pattern_for_time = r"\d+:\d+:\d+[+-]\d+"
        pattern_for_value_without_t_sep_and_timezone_mins = (
            rf"({pattern_for_date})T({pattern_for_time}):\d+"
        )
        match = re.match(pattern_for_value_without_t_sep_and_timezone_mins, value)

        if match is None:
            return value

        return match.group(1) + " " + match.group(2)

    def ignore_row_order(self, expression: Expression) -> bool:
        if expression.matches(
            is_table_function,
            True,
        ):
            # inconsistent sort order
            return True

        return False

    def ignore_order_when_comparing_collection(self, expression: Expression) -> bool:
        if expression.matches(
            partial(is_operation_tagged, tag=TAG_JSONB_OBJECT_GENERATION),
            True,
        ):
            # this is because of database-issues#8266
            return True

        return False
