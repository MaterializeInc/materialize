# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
import math
from decimal import Decimal
from typing import Any

from materialize.output_consistency.ignore_filter.inconsistency_ignore_filter import (
    InconsistencyIgnoreFilter,
)
from materialize.output_consistency.query.query_result import QueryExecution
from materialize.output_consistency.validation.result_comparator import ResultComparator


class PostgresResultComparator(ResultComparator):
    """Compares the outcome (result or failure) of multiple query executions"""

    def __init__(self, ignore_filter: InconsistencyIgnoreFilter):
        super().__init__(ignore_filter)

    def shall_validate_error_message(self, query_execution: QueryExecution) -> bool:
        # do not compare error messages at all
        return False

    def is_value_equal(self, value1: Any, value2: Any) -> bool:
        if value1 == value2:
            return True

        value1 = self.round_value_if_decimal(value1)
        value2 = self.round_value_if_decimal(value2)
        return super().is_value_equal(value1, value2)

    def round_value_if_decimal(self, value: Any, digits: int = 3) -> Any:
        if isinstance(value, Decimal) and not value.is_nan():
            return round(value, digits)

        if isinstance(value, float) and not math.isnan(value):
            return round(value, digits)

        return value
