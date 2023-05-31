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


class ReturnTypeSpec:
    """Return type specification of a database operation or database function"""

    def __init__(
        self,
        type_category: DataTypeCategory,
    ):
        self.type_category = type_category

        if type_category == DataTypeCategory.ANY:
            raise RuntimeError(
                f"{DataTypeCategory.ANY} is not allowed as return type category"
            )

    def resolve_type_category(
        self, first_arg_type_category: Optional[DataTypeCategory]
    ) -> DataTypeCategory:
        return self.type_category
