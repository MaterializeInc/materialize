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
from materialize.output_consistency.input_data.return_specs.dynamic_return_spec import (
    DynamicReturnTypeSpec,
)
from materialize.output_consistency.operation.return_type_spec import InputArgTypeHints


class CollectionEntryReturnTypeSpec(DynamicReturnTypeSpec):
    def __init__(self, param_index_to_take_type: int):
        super().__init__(param_index_to_take_type)
        self.requires_return_type_spec_hints = True

    def resolve_type_category(
        self, input_arg_type_hints: InputArgTypeHints
    ) -> DataTypeCategory:
        assert (
            input_arg_type_hints is not None
            and self.indices_of_required_input_type_hints is not None
            and not input_arg_type_hints.is_empty()
        ), "Invalid state"
        input_arg_return_type_spec = (
            input_arg_type_hints.return_type_spec_of_requested_args[
                self.indices_of_required_input_type_hints[0]
            ]
        )

        assert isinstance(input_arg_return_type_spec, CollectionReturnTypeSpec)
        return input_arg_return_type_spec.get_entry_value_type()
