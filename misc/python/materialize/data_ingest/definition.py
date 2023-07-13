# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from enum import Enum
from typing import Iterator, List, Optional

from materialize.data_ingest.data_type import RecordSize
from materialize.data_ingest.field import Field
from materialize.data_ingest.row import Operation, Row
from materialize.data_ingest.rowlist import RowList


class Records(Enum):
    ALL = 0  #  Only applies to DELETE operations
    ONE = 1
    SOME = 1_000
    MANY = 1_000_000


class Keyspace(Enum):
    SINGLE_VALUE = 1
    LARGE = 2
    EXISTING = 3


class Target(Enum):
    KAFKA = 1
    POSTGRES = 2
    PRINT = 3


class Definition:
    def generate(self, fields: List[Field]) -> Iterator[RowList]:
        raise NotImplementedError


class Insert(Definition):
    def __init__(self, count: Records, record_size: RecordSize):
        self.count = count.value
        self.record_size = record_size
        self.current_key = 0

    def max_key(self) -> int:
        if self.count < 1:
            raise ValueError(
                f'Unexpected count {self.count}, doesn\'t make sense to generate "ALL" values'
            )
        return self.count

    def generate(self, fields: List[Field]) -> Iterator[RowList]:
        if self.count < 1:
            raise ValueError(
                f'Unexpected count {self.count}, doesn\'t make sense to generate "ALL" values'
            )

        for i in range(self.count):
            if self.current_key >= self.count:
                break

            values = [
                field.data_type.numeric_value(self.current_key)
                if field.is_key
                else field.data_type.random_value(self.record_size)
                for field in fields
            ]
            self.current_key += 1

            yield RowList(
                [
                    Row(
                        fields=fields,
                        values=values,
                        operation=Operation.INSERT,
                    )
                ]
            )


class Upsert(Definition):
    def __init__(self, keyspace: Keyspace, count: Records, record_size: RecordSize):
        self.keyspace = keyspace
        self.count = count.value
        self.record_size = record_size

    def generate(self, fields: List[Field]) -> Iterator[RowList]:
        if self.count < 1:
            raise ValueError(
                f'Unexpected count {self.count}, doesn\'t make sense to generate "ALL" values'
            )

        for i in range(self.count):
            values = [
                field.data_type.numeric_value(0)
                if field.is_key
                else field.data_type.random_value(self.record_size)
                for field in fields
            ]

            yield RowList(
                [
                    Row(
                        fields=fields,
                        values=values,
                        operation=Operation.UPSERT,
                    )
                ]
            )


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

    def generate(self, fields: List[Field]) -> Iterator[RowList]:

        if self.number_of_records == Records.ONE:
            values = [
                field.data_type.random_value(self.record_size)
                for field in fields
                if field.is_key
            ]
            yield RowList([Row(fields, values, Operation.DELETE)])
        elif self.number_of_records in (Records.SOME, Records.MANY):
            for i in range(self.number_of_records.value):
                values = [
                    field.data_type.random_value(self.record_size)
                    for field in fields
                    if field.is_key
                ]
                yield RowList([Row(fields, values, Operation.DELETE)])
        elif self.number_of_records == Records.ALL:
            assert self.num is not None
            for i in range(self.num):
                values = [
                    field.data_type.numeric_value(i) for field in fields if field.is_key
                ]
                yield RowList([Row(fields, values, Operation.DELETE)])
        else:
            raise ValueError(f"Unexpected number of records {self.number_of_records}")
