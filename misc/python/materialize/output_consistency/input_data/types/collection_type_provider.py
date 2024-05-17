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
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.input_data.return_specs.collection_return_spec import (
    CollectionReturnTypeSpec,
)


class CollectionDataType(DataType):
    def __init__(
        self,
        internal_identifier: str,
        type_name: str,
        data_type_category: DataTypeCategory,
        entry_value_1: str,
        entry_value_2: str,
        value_type_category: DataTypeCategory,
        is_pg_compatible: bool,
    ):
        super().__init__(
            internal_identifier,
            type_name,
            data_type_category,
            is_pg_compatible=is_pg_compatible,
        )
        self.entry_value_1 = entry_value_1
        self.entry_value_2 = entry_value_2
        self.value_type_category = value_type_category

    def resolve_return_type_spec(
        self, characteristics: set[ExpressionCharacteristics]
    ) -> CollectionReturnTypeSpec:
        return self._create_collection_return_type_spec()

    def _create_collection_return_type_spec(self) -> CollectionReturnTypeSpec:
        raise NotImplementedError
