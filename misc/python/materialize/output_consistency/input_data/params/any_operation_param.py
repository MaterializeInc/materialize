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
from materialize.output_consistency.operation.operation_param import OperationParam


class AnyOperationParam(OperationParam):
    def __init__(
        self,
        include_record_type: bool = True,
        optional: bool = False,
        incompatibilities: set[ExpressionCharacteristics] | None = None,
        incompatibility_combinations: (
            list[set[ExpressionCharacteristics]] | None
        ) = None,
    ):
        super().__init__(
            DataTypeCategory.ANY,
            optional,
            incompatibilities,
            incompatibility_combinations,
        )

        self.include_record_type = include_record_type

    def supports_type(
        self, data_type: DataType, previous_args: list[Expression]
    ) -> bool:
        if (
            not self.include_record_type
            and data_type.category == DataTypeCategory.RECORD
        ):
            return False

        return True


class AnyLikeOtherOperationParam(OperationParam):
    def __init__(
        self,
        index_of_previous_param: int,
        optional: bool = False,
        incompatibilities: set[ExpressionCharacteristics] | None = None,
        incompatibility_combinations: (
            list[set[ExpressionCharacteristics]] | None
        ) = None,
    ):
        super().__init__(
            DataTypeCategory.ANY,
            optional,
            incompatibilities,
            incompatibility_combinations,
        )
        self.index_of_previous_param = index_of_previous_param

    def resolve_type_category(
        self, previous_args: list[Expression]
    ) -> DataTypeCategory:
        previous_arg = self._get_previous_arg(previous_args)
        return previous_arg.resolve_return_type_category()

    def supports_type(
        self, data_type: DataType, previous_args: list[Expression]
    ) -> bool:
        return self.resolve_type_category(previous_args) == data_type.category

    def _get_previous_arg(self, previous_args: list[Expression]) -> Expression:
        if self.index_of_previous_param >= len(previous_args):
            raise RuntimeError(
                f"Requested previous arg at index {self.index_of_previous_param}"
                f" but list only contains {len(previous_args)} args"
            )
        return previous_args[self.index_of_previous_param]
