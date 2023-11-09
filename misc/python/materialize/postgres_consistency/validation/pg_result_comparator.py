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
from typing import Any

from materialize.output_consistency.ignore_filter.inconsistency_ignore_filter import (
    InconsistencyIgnoreFilter,
)
from materialize.output_consistency.query.query_result import QueryExecution
from materialize.output_consistency.validation.result_comparator import ResultComparator

TIMESTAMP_PATTERN = re.compile(r"\d{4,}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+")


class PostgresResultComparator(ResultComparator):
    """Compares the outcome (result or failure) of multiple query executions"""

    def __init__(self, ignore_filter: InconsistencyIgnoreFilter):
        super().__init__(ignore_filter)
        self.floating_precision = 1e-03

    def shall_validate_error_message(self, query_execution: QueryExecution) -> bool:
        # do not compare error messages at all
        return False

    def is_value_equal(self, value1: Any, value2: Any) -> bool:
        if super().is_value_equal(value1, value2):
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
            return self.is_str_equal(value1, value2)

        return False

    def is_decimal_equal(self, value1: Decimal, value2: Decimal) -> bool:
        if value1.is_nan():
            return value2.is_nan()

        return math.isclose(value1, value2, rel_tol=self.floating_precision)

    def is_float_equal(self, value1: float, value2: float) -> bool:
        if math.isnan(value1):
            return math.isnan(value2)

        return math.isclose(value1, value2, rel_tol=self.floating_precision)

    def is_str_equal(self, value1: str, value2: str) -> bool:
        if self.is_timestamp(value1):
            return self.is_timestamp_equal(value1, value2)

        # Postgres uses 'mons' instead of 'months'
        return value1.replace(" mons", " months") == value2.replace(" mons", " months")

    def is_timestamp(self, value1: str) -> bool:
        return TIMESTAMP_PATTERN.match(value1) is not None

    def is_timestamp_equal(self, value1: str, value2: str) -> bool:
        # a timezone might be at the end, do not discard that
        milliseconds_pattern = re.compile(r"\d\.\d+")

        if milliseconds_pattern.search(value1) and milliseconds_pattern.search(value2):
            # drop milliseconds and trunc last digit of second
            value1 = last_second_and_milliseconds_pattern.sub("0", value1)
            value2 = last_second_and_milliseconds_pattern.sub("0", value2)

        return value1 == value2
