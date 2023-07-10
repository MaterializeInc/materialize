# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from enum import Enum
from typing import Any, List

from materialize.data_ingest.field import Field


class Operation(Enum):
    INSERT = 1
    UPSERT = 2
    DELETE = 3


class Row:
    fields: List[Field]
    values: List[Any]
    operation: Operation

    def __init__(self, fields: List[Field], values: List[Any], operation: Operation):
        self.fields = fields
        self.values = values
        self.operation = operation

    def __repr__(self) -> str:
        return f"Row({self.fields}, {self.values}, {self.operation})"
