# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
import string
from enum import Enum
from typing import Any


class RecordSize(Enum):
    TINY = 1
    SMALL = 2
    MEDIUM = 3
    LARGE = 4


class Backend(Enum):
    AVRO = 1
    POSTGRES = 2


class DataType:
    """As supported by Avro: https://avro.apache.org/docs/1.11.1/specification/_print/"""

    @staticmethod
    def random_value(record_size: RecordSize) -> Any:
        """Generate a random value, should be possible for all types."""
        raise NotImplementedError

    @staticmethod
    def numeric_value(num: int) -> Any:
        """Generate a value that corresponds to `num`, so that it will always be the same value for the same input `num`, but fits into the type. This doesn't make sense for a type like boolean."""
        raise NotImplementedError

    @staticmethod
    def name(backend: Backend) -> str:
        raise NotImplementedError


class NullType(DataType):
    @staticmethod
    def random_value(record_size: RecordSize) -> Any:
        return None


class BooleanType(DataType):
    @staticmethod
    def random_value(record_size: RecordSize) -> Any:
        return random.choice((True, False))

    @staticmethod
    def name(backend: Backend) -> str:
        return "boolean"


class IntType(DataType):
    @staticmethod
    def random_value(record_size: RecordSize) -> Any:
        if record_size == RecordSize.TINY:
            return random.randint(-127, 128)
        elif record_size == RecordSize.SMALL:
            return random.randint(-32768, 32767)
        elif record_size in (RecordSize.MEDIUM, RecordSize.LARGE):
            return random.randint(-2147483648, 2147483647)
        else:
            raise ValueError(f"Unexpected record size {record_size}")

    @staticmethod
    def numeric_value(num: int) -> Any:
        return num

    @staticmethod
    def name(backend: Backend) -> str:
        return "int"


class LongType(DataType):
    @staticmethod
    def random_value(record_size: RecordSize) -> Any:
        if record_size == RecordSize.TINY:
            return random.randint(-127, 128)
        elif record_size == RecordSize.SMALL:
            return random.randint(-32768, 32767)
        elif record_size == RecordSize.MEDIUM:
            return random.randint(-2147483648, 2147483647)
        elif record_size == RecordSize.LARGE:
            return random.randint(-9223372036854775808, 9223372036854775807)
        else:
            raise ValueError(f"Unexpected record size {record_size}")

    @staticmethod
    def numeric_value(num: int) -> Any:
        return num

    @staticmethod
    def name(backend: Backend) -> str:
        if backend == Backend.AVRO:
            return "long"
        else:
            return "bigint"


class FloatType(DataType):
    @staticmethod
    def random_value(record_size: RecordSize) -> Any:
        if record_size == RecordSize.TINY:
            return random.random()
        elif record_size == RecordSize.SMALL:
            return random.uniform(-100, 100)
        elif record_size == RecordSize.MEDIUM:
            return random.uniform(-1_000_000, 1_000_000)
        elif record_size == RecordSize.LARGE:
            return random.uniform(-1_000_000_000, 1_000_000_000_00)
        else:
            raise ValueError(f"Unexpected record size {record_size}")

    @staticmethod
    def numeric_value(num: int) -> Any:
        return num

    @staticmethod
    def name(backend: Backend) -> str:
        if backend == Backend.AVRO:
            return "float"
        else:
            return "float4"


class DoubleType(FloatType):
    @staticmethod
    def name(backend: Backend) -> str:
        if backend == Backend.AVRO:
            return "double"
        else:
            return "float8"


class StringType(DataType):
    @staticmethod
    def random_value(record_size: RecordSize) -> Any:
        if record_size == RecordSize.TINY:
            return random.choice(("foo", "bar", "baz"))
        elif record_size == RecordSize.SMALL:
            return "".join(random.choice(string.printable) for _ in range(3))
        elif record_size == RecordSize.MEDIUM:
            return "".join(random.choice(string.printable) for _ in range(10))
        elif record_size == RecordSize.LARGE:
            return "".join(random.choice(string.printable) for _ in range(100))
        else:
            raise ValueError(f"Unexpected record size {record_size}")

    @staticmethod
    def numeric_value(num: int) -> Any:
        return f"key{num}"

    @staticmethod
    def name(backend: Backend) -> str:
        if backend == Backend.AVRO:
            return "string"
        else:
            return "text"
