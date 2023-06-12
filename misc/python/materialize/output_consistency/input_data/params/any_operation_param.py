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

    def supports_type(
        self, data_type: DataType, previous_args: List[Expression]
    ) -> bool:
        return True


class AnyLikeOtherOperationParam(OperationParam):
    def __init__(
        self,
        index_of_previous_param: int,
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
        self.index_of_previous_param = index_of_previous_param

    def resolve_type_category(
        self, previous_args: List[Expression]
    ) -> DataTypeCategory:
        previous_arg = self._get_previous_arg(previous_args)
        return previous_arg.resolve_return_type_category()

    def supports_type(
        self, data_type: DataType, previous_args: List[Expression]
    ) -> bool:
        return self.resolve_type_category(previous_args) == data_type.category

    def _get_previous_arg(self, previous_args: List[Expression]) -> Expression:
        if self.index_of_previous_param >= len(previous_args):
            raise RuntimeError(
                f"Missing previous arg at index {self.index_of_previous_param}"
            )
        return previous_args[self.index_of_previous_param]
