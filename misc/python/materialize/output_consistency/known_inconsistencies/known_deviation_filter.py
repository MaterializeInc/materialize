# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.output_consistency.execution.value_storage_layout import (
    ValueStorageLayout,
)
from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.selection.selection import DataRowSelection


class KnownOutputInconsistenciesFilter:
    """Allows specifying and excluding expressions with known output inconsistencies"""

    def matches(self, expression: Expression, row_selection: DataRowSelection) -> bool:
        if expression.storage_layout == ValueStorageLayout.HORIZONTAL:
            # Optimization because currently no issues are known
            return False

        return False
