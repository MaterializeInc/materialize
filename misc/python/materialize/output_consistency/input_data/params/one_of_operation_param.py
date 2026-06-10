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
from materialize.output_consistency.operation.operation_param import OperationParam
from materialize.output_consistency.operation.return_type_spec import ReturnTypeSpec
from materialize.output_consistency.selection.randomized_picker import RandomizedPicker


class OneOf(OperationParam):
    def __init__(
        self,
        choices: list[OperationParam],
        optional: bool = False,
    ):
        super().__init__(
            DataTypeCategory.STRING,
            optional,
        )
        self.choices = choices
        assert len(choices) > 0, "No choices provided"

    def supports_type(
        self, data_type: DataType, previous_args: list[Expression]
    ) -> bool:
        raise RuntimeError("Unsuppported")

    def might_support_type_as_input_assuming_category_matches(
        self, return_type_spec: ReturnTypeSpec
    ) -> bool:
        raise RuntimeError("Unsuppported")

    def pick(self, randomized_picker: RandomizedPicker) -> OperationParam:
        param_choice_index = randomized_picker.random_number(0, len(self.choices) - 1)
        return self.choices[param_choice_index]
