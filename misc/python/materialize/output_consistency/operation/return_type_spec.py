# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

from materialize.output_consistency.data_type.data_type_category import DataTypeCategory


class InputArgTypeHints:
    def __init__(self):
        self.type_category_of_requested_args: dict[int, DataTypeCategory] = dict()
        """Only filled for specified indices_of_required_input_type_hints"""
        self.return_type_spec_of_requested_args: dict[int, ReturnTypeSpec] = dict()
        """Only filled for specified indices_of_required_input_type_hints and if requires_return_type_spec_hints is True"""

    def is_empty(self) -> bool:
        return len(self.type_category_of_requested_args) == 0


class ReturnTypeSpec:
    """Return type specification of a database operation or database function"""

    def __init__(
        self,
        type_category: DataTypeCategory,
        indices_of_required_input_type_hints: list[int] | None = None,
    ):
        self.type_category = type_category
        self.indices_of_required_input_type_hints = indices_of_required_input_type_hints
        self.requires_return_type_spec_hints = False

        assert (
            type_category != DataTypeCategory.ANY
        ), f"{DataTypeCategory.ANY} is not allowed as return type category"

    def resolve_type_category(
        self, input_arg_type_hints: InputArgTypeHints
    ) -> DataTypeCategory:
        return self.type_category
