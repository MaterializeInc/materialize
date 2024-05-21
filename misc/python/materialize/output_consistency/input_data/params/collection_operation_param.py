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
from materialize.output_consistency.input_data.params.any_operation_param import (
    AnyLikeOtherOperationParam,
)
from materialize.output_consistency.input_data.return_specs.collection_return_spec import (
    CollectionReturnTypeSpec,
)
from materialize.output_consistency.input_data.types.collection_type_provider import (
    CollectionDataType,
)
from materialize.output_consistency.operation.operation_param import OperationParam


class CollectionOperationParam(OperationParam):
    """Base for ListOperationParam, MapOperationParam"""

    def __init__(
        self,
        type_category: DataTypeCategory,
        optional: bool = False,
        incompatibilities: set[ExpressionCharacteristics] | None = None,
        value_type_category: DataTypeCategory | None = None,
    ):
        super().__init__(
            type_category=type_category,
            optional=optional,
            incompatibilities=incompatibilities,
        )
        self.declared_type_category = type_category
        self.value_type_category = value_type_category

    def supports_type(
        self, data_type: DataType, previous_args: list[Expression]
    ) -> bool:
        if not self.matches_collection_type(data_type):
            return False

        if self.value_type_category is None:
            return True
        else:
            assert isinstance(data_type, CollectionDataType)
            return data_type.value_type_category == self.value_type_category

    def matches_collection_type(self, data_type: DataType) -> bool:
        return data_type.category == self.declared_type_category


class CollectionLikeOtherCollectionOperationParam(AnyLikeOtherOperationParam):
    def supports_type(
        self, data_type: DataType, previous_args: list[Expression]
    ) -> bool:
        if not self.matches_collection_type(data_type):
            return False

        if isinstance(data_type, CollectionDataType):
            previous_arg = self._get_previous_arg(previous_args)

            previous_arg_ret_type_spec = previous_arg.resolve_return_type_spec()
            assert isinstance(previous_arg_ret_type_spec, CollectionReturnTypeSpec)
            return (
                data_type.value_type_category
                == previous_arg_ret_type_spec.get_entry_value_type()
            )

        return False

    def get_collection_type_category(self) -> DataTypeCategory:
        raise NotImplementedError

    def matches_collection_type(self, data_type: DataType) -> bool:
        return data_type.category == self.get_collection_type_category()


class CollectionOfOtherElementOperationParam(AnyLikeOtherOperationParam):
    def supports_type(
        self, data_type: DataType, previous_args: list[Expression]
    ) -> bool:
        if not self.matches_collection_type(data_type):
            return False

        previous_arg = self._get_previous_arg(previous_args)
        previous_arg_ret_type_category = previous_arg.resolve_return_type_category()

        assert isinstance(data_type, CollectionDataType)
        return data_type.value_type_category == previous_arg_ret_type_category

    def get_collection_type_category(self) -> DataTypeCategory:
        raise NotImplementedError

    def matches_collection_type(self, data_type: DataType) -> bool:
        return data_type.category == self.get_collection_type_category()


class ElementOfOtherCollectionOperationParam(AnyLikeOtherOperationParam):
    def supports_type(
        self, data_type: DataType, previous_args: list[Expression]
    ) -> bool:
        previous_arg = self._get_previous_arg(previous_args)
        previous_arg_ret_type_spec = previous_arg.resolve_return_type_spec()
        assert isinstance(previous_arg_ret_type_spec, CollectionReturnTypeSpec)
        return data_type.category == previous_arg_ret_type_spec.get_entry_value_type()
