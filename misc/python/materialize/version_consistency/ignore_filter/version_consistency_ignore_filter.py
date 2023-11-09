# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from functools import partial

from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.expression.expression import (
    Expression,
    LeafExpression,
)
from materialize.output_consistency.expression.expression_with_args import (
    ExpressionWithArgs,
)
from materialize.output_consistency.ignore_filter.expression_matchers import (
    matches_fun_by_any_name,
    matches_op_by_any_pattern,
)
from materialize.output_consistency.ignore_filter.ignore_verdict import YesIgnore
from materialize.output_consistency.ignore_filter.inconsistency_ignore_filter import (
    IgnoreVerdict,
    InconsistencyIgnoreFilter,
    NoIgnore,
)
from materialize.output_consistency.input_data.operations.date_time_operations_provider import (
    DATE_TIME_OPERATION_TYPES,
)
from materialize.output_consistency.operation.operation import (
    DbFunction,
    DbOperation,
)
from materialize.output_consistency.selection.selection import DataRowSelection
from materialize.output_consistency.validation.validation_message import (
    ValidationError,
)
from materialize.util import MzVersion

MZ_VERSION_0_77_0 = MzVersion.parse_mz("v0.77.0")


class VersionConsistencyIgnoreFilter(InconsistencyIgnoreFilter):
    def __init__(self, mz1_version: str, mz2_version: str):
        super().__init__()
        self.mz1_version = MzVersion.parse_mz(mz1_version, drop_dev_suffix=True)
        self.mz2_version = MzVersion.parse_mz(mz2_version, drop_dev_suffix=True)
        self.lower_version, self.higher_version = (
            (self.mz1_version, self.mz2_version)
            if self.mz1_version < self.mz2_version
            else (self.mz2_version, self.mz1_version)
        )

    def shall_ignore_expression(
        self, expression: Expression, row_selection: DataRowSelection
    ) -> IgnoreVerdict:
        if not self._contains_only_available_operations(expression, self.mz1_version):
            return YesIgnore(f"Feature is not available in {self.mz1_version}")

        if not self._contains_only_available_operations(expression, self.mz2_version):
            return YesIgnore(f"Feature is not available in {self.mz2_version}")

        if (
            self.lower_version < MZ_VERSION_0_77_0 <= self.higher_version
            and self._is_any_date_time_expression(expression)
        ):
            return YesIgnore("Fixed issue regarding time zone handling")

        return NoIgnore()

    def shall_ignore_error(self, error: ValidationError) -> IgnoreVerdict:
        return NoIgnore()

    def _contains_only_available_operations(
        self, expression: Expression, mz_version: MzVersion
    ) -> bool:
        def is_newer_operation(expression: Expression) -> bool:
            if not isinstance(expression, ExpressionWithArgs):
                return False

            if expression.operation.since_mz_version is None:
                return False

            feature_version = expression.operation.since_mz_version

            return feature_version > mz_version

        return not expression.matches(is_newer_operation, True)

    def _is_any_date_time_expression(self, expression: Expression) -> bool:
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
