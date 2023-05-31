# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import List

from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.operation.return_type_spec import ReturnTypeSpec


class DynamicReturnTypeSpec(ReturnTypeSpec):
    def __init__(
        self,
    ) -> None:
        super().__init__(DataTypeCategory.DYNAMIC)

    def resolve_type_category(self, args: List[Expression]) -> DataTypeCategory:
        if len(args) == 0:
            raise RuntimeError(
                f"Return type category {DataTypeCategory.DYNAMIC} must not be used without arguments"
            )
        else:
            return args[0].resolve_return_type_category()
