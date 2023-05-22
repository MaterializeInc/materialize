# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import List, Optional, Set

from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.operation.operation_param import OperationParam


class AnyOperationParam(OperationParam):
    def __init__(
        self,
        optional: bool = False,
        incompatibilities: Optional[Set[ExpressionCharacteristics]] = None,
        incompatibility_combinations: Optional[
            List[Set[ExpressionCharacteristics]]
        ] = None,
    ):
        super().__init__(
            DataTypeCategory.ANY,
            optional,
            incompatibilities,
            incompatibility_combinations,
        )
