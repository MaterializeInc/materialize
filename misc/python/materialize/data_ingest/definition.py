# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
from enum import Enum
from typing import List

from materialize.data_ingest.data_type import RecordSize, DataType
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
    def generate(self) -> List[Transaction]:
        raise NotImplementedError


class Insert(Definition):
    def __init__(self, count: Records, record_size: RecordSize):
        self.count = count
        self.record_size = record_size
        self.current_key = 1

    def generate(self, fields: List[Field]) -> List[Transaction]:
        key = self.current_key
        self.current_key += 1

        if self.count == Records.ONE:
            count = 1
        elif self.count == Records.MANY:
            count = 1000
        else:
            raise ValueError(f"Unexpected count {self.count}")

        if self.record_size == RecordSize.TINY:
            value = random.randint(-127, 128)
        elif self.record_size == RecordSize.SMALL:
            value = random.randint(-32768, 32767)
        elif self.record_size == RecordSize.MEDIUM:
            value = random.randint(-2147483648, 2147483647)
        elif self.record_size == RecordSize.LARGE:
            value = random.randint(-9223372036854775808, 9223372036854775807)
        else:
            raise ValueError(f"Unexpected count {self.count}")

        #fields = [Field("key1", Type.INT, True), Field("value1", Type.STRING, False), Field("value2", Type.FLOAT, False)]
        fields_with_values = fields.copy()

        transactions = []
        for i in range(count):
            for field in fields_with_values:
                field.value = field.type
            transactions.append(
                Transaction(
                    [RowList([Row(fields=fields_with_values, operation=Operation.INSERT)])]
                )
            )

        return transactions


class Upsert(Definition):
    def __init__(self, keyspace: Keyspace, count: Records, record_size: RecordSize):
        self.keyspace = keyspace
        self.count = count
        self.record_size = record_size

    def generate(self) -> List[Transaction]:
        if self.keyspace == Keyspace.SINGLE_VALUE:
            key = 1
        elif self.keyspace == Keyspace.LARGE:
            key = random.randint(0, 1_000_000)
        else:
            raise ValueError(f"Unexpected keyspace {self.keyspace}")

        if self.count == Records.ONE:
            count = 1
        elif self.count == Records.MANY:
            count = 1000
        else:
            raise ValueError(f"Unexpected count {self.count}")

        if self.record_size == RecordSize.TINY:
            value = random.randint(-127, 128)
        elif self.record_size == RecordSize.SMALL:
            value = random.randint(-32768, 32767)
        elif self.record_size == RecordSize.MEDIUM:
            value = random.randint(-2147483648, 2147483647)
        elif self.record_size == RecordSize.LARGE:
            value = random.randint(-9223372036854775808, 9223372036854775807)
        else:
            raise ValueError(f"Unexpected count {self.count}")

        transactions = []
        for i in range(count):
            transactions.append(
                Transaction(
                    [RowList([Row(fields=fields, operation=Operation.UPSERT)])]
                )
            )

        return transactions


class Delete(Definition):
    def __init__(self, number_of_records: Records):
        self.number_of_records = number_of_records

    def generate(self) -> List[Transaction]:
        transactions = []

        if self.number_of_records == Records.ONE:
            raise NotImplementedError
        elif self.number_of_records == Records.MANY:
            raise NotImplementedError
        elif self.number_of_records == Records.ALL:
            for key in range(1000):
                transactions.append(
                    Transaction([RowList([Row(key=key, operation=Operation.DELETE)])])
                )
        else:
            raise ValueError(f"Unexpected number of records {self.number_of_records}")

        return transactions
