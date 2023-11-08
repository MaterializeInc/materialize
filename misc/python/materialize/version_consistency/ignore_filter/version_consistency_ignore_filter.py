# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.expression.expression_with_args import (
    ExpressionWithArgs,
)
from materialize.output_consistency.ignore_filter.ignore_verdict import YesIgnore
from materialize.output_consistency.ignore_filter.inconsistency_ignore_filter import (
    IgnoreVerdict,
    InconsistencyIgnoreFilter,
    NoIgnore,
)
from materialize.output_consistency.selection.selection import DataRowSelection
from materialize.output_consistency.validation.validation_message import (
    ValidationError,
)
from materialize.util import MzVersion


class VersionConsistencyIgnoreFilter(InconsistencyIgnoreFilter):
    def __init__(self, mz1_version: str, mz2_version: str):
        super().__init__()
        self.mz1_version = MzVersion.parse_mz(mz1_version, drop_dev_suffix=True)
        self.mz2_version = MzVersion.parse_mz(mz2_version, drop_dev_suffix=True)

    def shall_ignore_expression(
        self, expression: Expression, row_selection: DataRowSelection
    ) -> IgnoreVerdict:
        if not self._contains_only_available_operations(expression, self.mz1_version):
            return YesIgnore(f"Feature is not available in {self.mz1_version}")

        if not self._contains_only_available_operations(expression, self.mz2_version):
            return YesIgnore(f"Feature is not available in {self.mz2_version}")

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
