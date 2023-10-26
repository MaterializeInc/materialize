# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.ignore_filter.inconsistency_ignore_filter import (
    IgnoreVerdict,
    InconsistencyIgnoreFilter,
    NoIgnore,
)
from materialize.output_consistency.selection.selection import DataRowSelection
from materialize.output_consistency.validation.validation_message import (
    ValidationError,
)


class NoneIgnoreFilter(InconsistencyIgnoreFilter):
    def shall_ignore_expression(
        self, expression: Expression, row_selection: DataRowSelection
    ) -> IgnoreVerdict:
        return NoIgnore()

    def shall_ignore_error(self, error: ValidationError) -> IgnoreVerdict:
        return NoIgnore()
