# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import List, Optional, Set

from materialize.output_consistency.data_type.data_type import DataType
from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)


class OperationParam:
    """Parameter of a database operation or database function"""

    def __init__(
        self,
        type_category: DataTypeCategory,
        optional: bool = False,
        incompatibilities: Optional[Set[ExpressionCharacteristics]] = None,
        incompatibility_combinations: Optional[
            List[Set[ExpressionCharacteristics]]
        ] = None,
    ):
        """
        Create a parameter for an operation.
        :param optional: this parameter can be omitted
        :param incompatibilities: a value annotated with any of these characteristics is considered invalid
        :param incompatibility_combinations: a value annotated with all characteristics of any entry is considered invalid
        """
        self.type_category = type_category
        self.optional = optional

        if incompatibility_combinations is None:
            incompatibility_combinations = list()

        self.incompatibility_combinations = incompatibility_combinations

        if incompatibilities is not None:
            for incompatibility in incompatibilities:
                self.incompatibility_combinations.append({incompatibility})

    def supports_type(self, data_type: DataType) -> bool:
        raise RuntimeError("Not implemented")

    def supports_expression(self, arg: Expression) -> bool:
        for incompatibility_combination in self.incompatibility_combinations:
            if arg.has_all_characteristics(incompatibility_combination):
                return False

        return True

    def __str__(self) -> str:
        return f"{type(self).__name__} (optional={self.optional})"
