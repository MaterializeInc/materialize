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
from materialize.output_consistency.input_data.return_specs.string_return_spec import (
    StringReturnTypeSpec,
)
from materialize.output_consistency.input_data.types.string_type_provider import (
    TEXT_TYPE_IDENTIFIER,
    StringDataType,
)
from materialize.output_consistency.operation.operation_param import OperationParam
from materialize.output_consistency.operation.return_type_spec import ReturnTypeSpec


class StringOperationParam(OperationParam):
    def __init__(
        self,
        optional: bool = False,
        incompatibilities: set[ExpressionCharacteristics] | None = None,
        only_type_text: bool = False,
    ):
        super().__init__(
            DataTypeCategory.STRING,
            optional,
            incompatibilities,
            incompatibility_combinations=None,
        )
        self.only_type_text = only_type_text

    def supports_type(
        self, data_type: DataType, previous_args: list[Expression]
    ) -> bool:
        if not isinstance(data_type, StringDataType):
            return False

        if self.only_type_text:
            return data_type.internal_identifier == TEXT_TYPE_IDENTIFIER

        return True

    def might_support_type_as_input_assuming_category_matches(
        self, return_type_spec: ReturnTypeSpec
    ) -> bool:
        # In doubt return True

        if isinstance(return_type_spec, StringReturnTypeSpec):
            if self.only_type_text and not return_type_spec.is_text_type:
                return False

        return super().might_support_type_as_input_assuming_category_matches(
            return_type_spec
        )
