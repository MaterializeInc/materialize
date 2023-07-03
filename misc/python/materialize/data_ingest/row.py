# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from enum import Enum
from typing import Any, Optional, Type

from materialize.data_ingest.data_type import DataType

class Operation(Enum):
    INSERT = 1
    UPSERT = 2
    DELETE = 3


class Field:
    name: str
    typ: Type[DataType]
    is_key: bool
    value: Optional[Any]

    def __init__(self, name: str, typ: Type[DataType], is_key: bool, value: Optional[Any] = None):
        self.name = name
        self.typ = typ
        self.is_key = is_key
        self.value = value

    def set_random_value(record_size: RecordSize) -> None:
        self.value = self.typ.random_value(record_size)

    def __repr__(self) -> str:
        return f"Field({'key' if self.is_key else 'value'}, {self.name}: {self.typ} = {self.value})"


class Row:
    fields: List[Field]
    operation: Operation

    def __init__(self, fields: List[Field], operation: Operation):
        self.fields = fields
        self.operation = operation

    def __repr__(self) -> str:
        return f"Row({self.fields}, {self.operation})"
