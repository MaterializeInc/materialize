# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Optional

from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)


class OperationParam:
    def __init__(
        self,
        type_category: DataTypeCategory,
        optional: bool = False,
        incompatibilities: Optional[set[ExpressionCharacteristics]] = None,
        incompatibility_combinations: Optional[
            list[set[ExpressionCharacteristics]]
        ] = None,
    ):
        self.type_category = type_category
        self.optional = optional

        if incompatibility_combinations is None:
            incompatibility_combinations = list()

        self.incompatibility_combinations = incompatibility_combinations

        if incompatibilities is not None:
            for incompatibility in incompatibilities:
                self.incompatibility_combinations.append({incompatibility})

    def supports_arg(self, arg: Expression) -> bool:
        for incompatibility_combination in self.incompatibility_combinations:
            overlapping_incompatibility_combination = (
                incompatibility_combination & arg.characteristics
            )

            if len(overlapping_incompatibility_combination) == len(
                incompatibility_combination
            ):
                return False

        return True
