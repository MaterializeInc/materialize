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
from materialize.output_consistency.data_type.value_characteristics import (
    ValueCharacteristics,
)
from materialize.output_consistency.expressions.expression import Expression


class OperationParam:
    def __init__(
        self,
        type_category: DataTypeCategory,
        optional: bool = False,
        incompatibilities: Optional[set[ValueCharacteristics]] = None,
        incompatibility_combinations: Optional[list[set[ValueCharacteristics]]] = None,
    ):
        if incompatibilities is None:
            incompatibilities = set()

        if incompatibility_combinations is None:
            incompatibility_combinations = list()

        self.type_category = type_category
        self.optional = optional
        self.incompatibilities: set[ValueCharacteristics] = incompatibilities
        self.incompatibility_combinations = incompatibility_combinations

    def supports_arg(self, arg: Expression) -> bool:
        overlapping_incompatibilities = self.incompatibilities & arg.characteristics

        if len(overlapping_incompatibilities) > 0:
            return False

        for incompatibility_combination in self.incompatibility_combinations:
            overlapping_incompatibility_combination = (
                incompatibility_combination & arg.characteristics
            )

            if len(overlapping_incompatibility_combination) == len(
                incompatibility_combination
            ):
                return False

        return True


class NumericOperationParam(OperationParam):
    def __init__(
        self,
        optional: bool = False,
        incompatibilities: Optional[set[ValueCharacteristics]] = None,
        incompatibility_combinations: Optional[list[set[ValueCharacteristics]]] = None,
    ):
        if incompatibilities is None:
            incompatibilities = set()

        incompatibilities.add(ValueCharacteristics.OVERSIZE)

        super().__init__(
            DataTypeCategory.NUMERIC,
            optional,
            incompatibilities,
            incompatibility_combinations,
        )
