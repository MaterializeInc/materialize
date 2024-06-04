# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.enum.enum_constant import EnumConstant
from materialize.output_consistency.enum.enum_operation_param import (
    EnumConstantOperationParam,
)
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.input_data.params.enum_constant_operation_params import (
    TAG_DATA_TYPE_ENUM,
)
from materialize.output_consistency.input_data.types.all_types_provider import (
    DATA_TYPES,
)


class TypeEnumConstant(EnumConstant):
    def __init__(
        self,
        value: str,
        resulting_type_category: DataTypeCategory,
        characteristics: set[ExpressionCharacteristics],
        tags: set[str] | None = None,
    ):
        super().__init__(
            value=value, add_quotes=False, characteristics=characteristics, tags=tags
        )
        self.resulting_type_category = resulting_type_category

    def resolve_resulting_return_type_category(self) -> DataTypeCategory:
        return self.resulting_type_category


class TypeEnumConstantOperationParam(EnumConstantOperationParam):
    def __init__(
        self,
        must_be_pg_compatible: bool,
    ):
        type_names_with_category = [
            (data_type.type_name, data_type.category)
            for data_type in DATA_TYPES
            if data_type.is_pg_compatible or not must_be_pg_compatible
        ]
        all_type_names = [x[0] for x in type_names_with_category]
        corresponding_type_categories = [x[1] for x in type_names_with_category]
        super().__init__(
            values=all_type_names,
            add_quotes=False,
            add_invalid_value=False,
            add_null_value=True,
            tags={TAG_DATA_TYPE_ENUM},
        )
        self.corresponding_type_categories = corresponding_type_categories

        # this is due to add_null_value
        corresponding_type_categories.insert(0, DataTypeCategory.STRING)

        assert len(all_type_names) == len(
            corresponding_type_categories
        ), "length of type names and categories do not match"

    def get_enum_constant(self, index: int) -> EnumConstant:
        original_constant = super().get_enum_constant(index)
        type_category = self.corresponding_type_categories[index]
        # convert it to TypeEnumConstant
        return TypeEnumConstant(
            value=original_constant.value,
            resulting_type_category=type_category,
            characteristics=original_constant.own_characteristics,
            tags=original_constant.tags,
        )
