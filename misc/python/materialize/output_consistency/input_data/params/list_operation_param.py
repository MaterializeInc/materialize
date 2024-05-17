# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.input_data.params.collection_operation_param import (
    CollectionLikeOtherCollectionOperationParam,
    CollectionOfOtherElementOperationParam,
    CollectionOperationParam,
)


class ListOperationParam(CollectionOperationParam):
    def __init__(
        self,
        optional: bool = False,
        incompatibilities: set[ExpressionCharacteristics] | None = None,
        value_type_category: DataTypeCategory | None = None,
    ):
        super().__init__(
            DataTypeCategory.LIST,
            optional,
            incompatibilities,
            value_type_category=value_type_category,
        )


class ListLikeOtherListOperationParam(CollectionLikeOtherCollectionOperationParam):
    def get_collection_type_category(self) -> DataTypeCategory:
        return DataTypeCategory.LIST


class ListOfOtherElementOperationParam(CollectionOfOtherElementOperationParam):
    def get_collection_type_category(self) -> DataTypeCategory:
        return DataTypeCategory.LIST
