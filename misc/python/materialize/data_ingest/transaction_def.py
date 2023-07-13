# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from enum import Enum
from typing import Iterator, List

from materialize.data_ingest.definition import Definition
from materialize.data_ingest.field import Field
from materialize.data_ingest.rowlist import RowList
from materialize.data_ingest.transaction import Transaction


class TransactionSize(Enum):
    SINGLE_OPERATION = 1
    HUGE = 1_000_000_000


class TransactionDef:
    operations: List[Definition]
    size: TransactionSize

    def __init__(
        self,
        operations: List[Definition],
        size: TransactionSize = TransactionSize.SINGLE_OPERATION,
    ):
        self.operations = operations
        self.size = size

    def generate(self, fields: List[Field]) -> Iterator[Transaction]:
        full_rowlist: List[RowList] = []
        for definition in self.operations:
            for i, rowlist in enumerate(definition.generate(fields)):
                full_rowlist.append(rowlist)
                if i + 1 == self.size.value:
                    yield Transaction(full_rowlist)
                    full_rowlist = []
        if full_rowlist:
            yield Transaction(full_rowlist)
