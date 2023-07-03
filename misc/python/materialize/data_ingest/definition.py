# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from copy import deepcopy
from enum import Enum
from typing import List, Optional

from materialize.data_ingest.data_type import RecordSize
from materialize.data_ingest.field import Field
from materialize.data_ingest.row import Operation, Row
from materialize.data_ingest.rowlist import RowList
from materialize.data_ingest.transaction import Transaction


class Records(Enum):
    ONE = 1
    MANY = 2
    ALL = 3


class Keyspace(Enum):
    SINGLE_VALUE = 1
    LARGE = 2
    EXISTING = 3


class Target(Enum):
    KAFKA = 1
    POSTGRES = 2
    PRINT = 3


class Definition:
    def generate(self, fields: List[Field]) -> List[Transaction]:
        raise NotImplementedError


class Insert(Definition):
    def __init__(self, count: Records, record_size: RecordSize):
        self.count = count
        self.record_size = record_size
        self.current_key = 0

    def max_key(self) -> int:
        if self.count == Records.ONE:
            return 1
        elif self.count == Records.MANY:
            return 1000
        else:
            raise ValueError(f"Unexpected count {self.count}")

    def generate(self, fields: List[Field]) -> List[Transaction]:
        if self.count == Records.ONE:
            count = 1
        elif self.count == Records.MANY:
            count = 1000
        else:
            raise ValueError(f"Unexpected count {self.count}")

        fields_with_values = deepcopy(fields)

        transactions = []
        for i in range(count):
            for field in fields_with_values:
                if field.is_key:
                    field.value = field.typ.num_value(self.current_key)
                else:
                    field.value = field.typ.random_value(self.record_size)
            self.current_key += 1
            transactions.append(
                Transaction(
                    [
                        RowList(
                            [
                                Row(
                                    fields=deepcopy(fields_with_values),
                                    operation=Operation.INSERT,
                                )
                            ]
                        )
                    ]
                )
            )

        return transactions


class Upsert(Definition):
    def __init__(self, keyspace: Keyspace, count: Records, record_size: RecordSize):
        self.keyspace = keyspace
        self.count = count
        self.record_size = record_size

    def generate(self, fields: List[Field]) -> List[Transaction]:
        if self.count == Records.ONE:
            count = 1
        elif self.count == Records.MANY:
            count = 1000
        else:
            raise ValueError(f"Unexpected count {self.count}")

        fields_with_values = deepcopy(fields)

        transactions = []
        for i in range(count):
            for field in fields_with_values:
                if field.is_key:
                    if self.keyspace == Keyspace.SINGLE_VALUE:
                        field.value = field.typ.num_value(0)
                    else:
                        raise NotImplementedError
                else:
                    field.value = field.typ.random_value(self.record_size)

            transactions.append(
                Transaction(
                    [
                        RowList(
                            [
                                Row(
                                    fields=deepcopy(fields_with_values),
                                    operation=Operation.UPSERT,
                                )
                            ]
                        )
                    ]
                )
            )

        return transactions


class Delete(Definition):
    def __init__(
        self,
        number_of_records: Records,
        record_size: RecordSize,
        num: Optional[int] = None,
    ):
        self.number_of_records = number_of_records
        self.record_size = record_size
        self.num = num

    def generate(self, fields: List[Field]) -> List[Transaction]:
        transactions = []

        fields_with_values = [field for field in fields if field.is_key]

        if self.number_of_records == Records.ONE:
            for field in fields_with_values:
                field.value = field.typ.random_value(self.record_size)
            transactions.append(
                Transaction([RowList([Row(fields_with_values, Operation.DELETE)])])
            )
        elif self.number_of_records == Records.MANY:
            for i in range(1000):
                for field in fields_with_values:
                    field.value = field.typ.random_value(self.record_size)
                transactions.append(
                    Transaction(
                        [RowList([Row(deepcopy(fields_with_values), Operation.DELETE)])]
                    )
                )
        elif self.number_of_records == Records.ALL:
            assert self.num is not None
            for i in range(self.num):
                for field in fields_with_values:
                    field.value = field.typ.num_value(i)
                transactions.append(
                    Transaction(
                        [RowList([Row(deepcopy(fields_with_values), Operation.DELETE)])]
                    )
                )
        else:
            raise ValueError(f"Unexpected number of records {self.number_of_records}")

        assert transactions

        return transactions
