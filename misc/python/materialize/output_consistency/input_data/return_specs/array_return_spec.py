# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.input_data.return_specs.collection_return_spec import (
    CollectionReturnTypeSpec,
)


class ArrayReturnTypeSpec(CollectionReturnTypeSpec):
    def __init__(
        self,
        param_index_of_array_value_type: int = 0,
        array_value_type_category: DataTypeCategory = DataTypeCategory.DYNAMIC,
    ):
        super().__init__(
            DataTypeCategory.ARRAY,
            param_index_of_array_value_type,
            array_value_type_category,
        )
