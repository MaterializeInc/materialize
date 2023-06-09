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
from materialize.output_consistency.operation.return_type_spec import ReturnTypeSpec


class DynamicReturnTypeSpec(ReturnTypeSpec):
    def __init__(self, param_index_to_take_type: int = 0):
        super().__init__(DataTypeCategory.DYNAMIC, [param_index_to_take_type])

    def resolve_type_category(
        self, input_arg_type_hints: List[DataTypeCategory]
    ) -> DataTypeCategory:
        if input_arg_type_hints is None:
            raise RuntimeError(
                f"Return type category {DataTypeCategory.DYNAMIC} requires arg hints"
            )

        assert len(input_arg_type_hints) == 1, "Missing input type hint"
        return input_arg_type_hints[0]
