# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from functools import partial

from materialize.mz_version import MzVersion
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
    matches_fun_by_name,
    matches_op_by_any_pattern,
)
from materialize.output_consistency.ignore_filter.ignore_verdict import YesIgnore
from materialize.output_consistency.ignore_filter.inconsistency_ignore_filter import (
    GenericInconsistencyIgnoreFilter,
    PostExecutionInconsistencyIgnoreFilterBase,
    PreExecutionInconsistencyIgnoreFilterBase,
)
from materialize.output_consistency.ignore_filter.internal_output_inconsistency_ignore_filter import (
    IgnoreVerdict,
)
from materialize.output_consistency.input_data.operations.date_time_operations_provider import (
    DATE_TIME_OPERATION_TYPES,
)
from materialize.output_consistency.operation.operation import (
    DbFunction,
    DbOperation,
)
from materialize.output_consistency.selection.selection import DataRowSelection

# Do not specify "-dev" versions. The suffix will be cropped; it is not necessary.
MZ_VERSION_0_77_0 = MzVersion.parse_mz("v0.77.0")
MZ_VERSION_0_78_0 = MzVersion.parse_mz("v0.78.0")
MZ_VERSION_0_81_0 = MzVersion.parse_mz("v0.81.0")
MZ_VERSION_0_88_0 = MzVersion.parse_mz("v0.88.0")


class VersionConsistencyIgnoreFilter(GenericInconsistencyIgnoreFilter):
    def __init__(self, mz1_version_string: str, mz2_version_string: str):
        mz1_version = MzVersion.parse_mz(mz1_version_string, drop_dev_suffix=True)
        mz2_version = MzVersion.parse_mz(mz2_version_string, drop_dev_suffix=True)
        lower_version, higher_version = (
            (mz1_version, mz2_version)
            if mz1_version < mz2_version
            else (mz2_version, mz1_version)
        )
        super().__init__(
            VersionPreExecutionInconsistencyIgnoreFilter(lower_version, higher_version),
            VersionPostExecutionInconsistencyIgnoreFilter(
                lower_version, higher_version
            ),
        )


class VersionPreExecutionInconsistencyIgnoreFilter(
    PreExecutionInconsistencyIgnoreFilterBase
):
    def __init__(self, lower_version: MzVersion, higher_version: MzVersion):
        self.lower_version = lower_version
        self.higher_version = higher_version

    def shall_ignore_expression(
        self, expression: Expression, row_selection: DataRowSelection
    ) -> IgnoreVerdict:
        if not self._contains_only_available_operations(expression, self.lower_version):
            return YesIgnore(f"Feature is not available in {self.lower_version}")

        if not self._contains_only_available_operations(
            expression, self.higher_version
        ):
            return YesIgnore(f"Feature is not available in {self.higher_version}")

        if (
            self.lower_version < MZ_VERSION_0_77_0 <= self.higher_version
            and self._is_any_date_time_expression(expression)
        ):
            return YesIgnore("Fixed issue regarding time zone handling")

        if (
            self.lower_version < MZ_VERSION_0_78_0 <= self.higher_version
            and expression.matches(
                partial(
                    matches_fun_by_any_name,
                    function_names_in_lower_case={"array_agg", "string_agg"},
                ),
                True,
            )
        ):
            return YesIgnore("Accepted: no order explicitly specified")

        if (
            self.lower_version < MZ_VERSION_0_81_0 <= self.higher_version
            and expression.matches(
                partial(
                    matches_fun_by_any_name,
                    function_names_in_lower_case={"min", "max"},
                ),
                True,
            )
        ):
            return YesIgnore("Implemented min/max for interval and time types in 24007")

        if (
            self.lower_version < MZ_VERSION_0_88_0 <= self.higher_version
            and expression.matches(
                partial(
                    matches_fun_by_name,
                    function_name_in_lower_case="date_trunc",
                ),
                True,
            )
        ):
            return YesIgnore("date_trunc fixed in 25202")

        return super().shall_ignore_expression(expression, row_selection)

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


class VersionPostExecutionInconsistencyIgnoreFilter(
    PostExecutionInconsistencyIgnoreFilterBase
):
    def __init__(self, lower_version: MzVersion, higher_version: MzVersion):
        self.lower_version = lower_version
        self.higher_version = higher_version
