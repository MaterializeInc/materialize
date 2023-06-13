# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from typing import List, Optional

from materialize.output_consistency.data_type.data_type_category import DataTypeCategory


class ReturnTypeSpec:
    """Return type specification of a database operation or database function"""

    def __init__(
        self,
        type_category: DataTypeCategory,
        indices_of_required_input_type_hints: Optional[List[int]] = None,
    ):
        self.type_category = type_category
        self.indices_of_required_input_type_hints = indices_of_required_input_type_hints

        assert (
            type_category != DataTypeCategory.ANY
        ), f"{DataTypeCategory.ANY} is not allowed as return type category"

    def resolve_type_category(
        self, input_arg_type_hints: List[DataTypeCategory]
    ) -> DataTypeCategory:
        return self.type_category
