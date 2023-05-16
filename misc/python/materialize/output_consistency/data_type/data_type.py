# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Optional

from materialize.output_consistency.data_type.data_type_category import DataTypeCategory


class DataType:
    def __init__(self, identifier: str, type_name: str, category: DataTypeCategory):
        self.identifier = identifier
        self.type_name = type_name
        self.category = category


class NumberDataType(DataType):
    def __init__(
        self,
        identifier: str,
        type_name: str,
        is_signed: bool,
        is_decimal: bool,
        tiny_value: str,
        max_value: str,
        max_negative_value: Optional[str],
    ):
        super().__init__(identifier, type_name, DataTypeCategory.NUMERIC)
        self.is_signed = is_signed
        self.is_decimal = is_decimal
        self.tiny_value = tiny_value
        self.max_value = max_value
        self.max_negative_value = max_negative_value
