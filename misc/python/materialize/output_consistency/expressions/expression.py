# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from materialize.output_consistency.data_type.data_type_group import DataTypeGroup
from materialize.output_consistency.data_type.value_characteristics import (
    ValueCharacteristics,
)


class Expression:
    def __init__(
        self, characteristics: set[ValueCharacteristics], is_expect_error: bool = False
    ):
        self.is_expect_error = is_expect_error
        self.characteristics = characteristics

    def to_sql(self) -> str:
        raise RuntimeError("Not implemented")

    def resolve_data_type_group(self) -> DataTypeGroup:
        raise RuntimeError("Not implemented")
