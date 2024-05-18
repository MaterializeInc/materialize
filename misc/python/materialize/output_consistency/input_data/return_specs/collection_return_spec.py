# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.operation.return_type_spec import (
    InputArgTypeHints,
    ReturnTypeSpec,
)


class CollectionReturnTypeSpec(ReturnTypeSpec):
    def __init__(
        self,
        data_type_category: DataTypeCategory,
        param_index_of_map_value_type: int = 0,
        entry_value_type_category: DataTypeCategory = DataTypeCategory.DYNAMIC,
    ):
        super().__init__(data_type_category, [param_index_of_map_value_type])
        self._entry_value_type_category = entry_value_type_category

    def resolve_type_category(
        self, input_arg_type_hints: InputArgTypeHints
    ) -> DataTypeCategory:
        # update the value type of the collection entries
        if self._entry_value_type_category == DataTypeCategory.DYNAMIC:
            self._update_collection_value_type(input_arg_type_hints)

        # provide the actual return value
        return super().resolve_type_category(input_arg_type_hints)

    def _update_collection_value_type(
        self, input_arg_type_hints: InputArgTypeHints
    ) -> None:
        assert (
            input_arg_type_hints is not None
            and self.indices_of_required_input_type_hints is not None
            and not input_arg_type_hints.is_empty()
        ), "Invalid state"
        self._entry_value_type_category = (
            input_arg_type_hints.type_category_of_requested_args[
                self.indices_of_required_input_type_hints[0]
            ]
        )

    def get_entry_value_type(self) -> DataTypeCategory:
        assert (
            self._entry_value_type_category != DataTypeCategory.DYNAMIC
        ), "entry value type not resolved"
        return self._entry_value_type_category
