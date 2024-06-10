# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.output_consistency.data_type.data_type import DataType
from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.operation.return_type_spec import ReturnTypeSpec


class OperationParam:
    """Parameter of a database operation or database function"""

    def __init__(
        self,
        type_category: DataTypeCategory,
        optional: bool = False,
        incompatibilities: set[ExpressionCharacteristics] | None = None,
        incompatibility_combinations: (
            list[set[ExpressionCharacteristics]] | None
        ) = None,
    ):
        """
        Create a parameter for an operation.
        :param optional: this parameter can be omitted
        :param incompatibilities: a value annotated with any of these characteristics is considered invalid
        :param incompatibility_combinations: a value annotated with all characteristics of any entry is considered invalid
        """
        self._type_category = type_category
        self.optional = optional

        if incompatibility_combinations is None:
            incompatibility_combinations = list()

        self.incompatibility_combinations = incompatibility_combinations

        if incompatibilities is not None:
            for incompatibility in incompatibilities:
                self.incompatibility_combinations.append({incompatibility})

    def supports_type(
        self, data_type: DataType, previous_args: list[Expression]
    ) -> bool:
        raise NotImplementedError

    def might_support_type_as_input_assuming_category_matches(
        self, return_type_spec: ReturnTypeSpec
    ) -> bool:
        return True

    def supports_expression(self, arg: Expression) -> bool:
        for incompatibility_combination in self.incompatibility_combinations:
            if arg.has_all_characteristics(incompatibility_combination):
                return False

        return True

    def get_declared_type_category(self) -> DataTypeCategory:
        return self._type_category

    def resolve_type_category(
        self, previous_args: list[Expression]
    ) -> DataTypeCategory:
        return self._type_category

    def __str__(self) -> str:
        return f"{type(self).__name__} (optional={self.optional})"
