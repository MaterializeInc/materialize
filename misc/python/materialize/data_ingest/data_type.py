# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json
import random
import string
from enum import Enum
from typing import Any

from pg8000.native import literal

from materialize.util import all_subclasses


class RecordSize(Enum):
    TINY = 1
    SMALL = 2
    MEDIUM = 3
    LARGE = 4


class Backend(Enum):
    AVRO = 1
    JSON = 2
    POSTGRES = 3


class DataType:
    """As supported by Avro: https://avro.apache.org/docs/1.11.1/specification/_print/"""

    @staticmethod
    def random_value(
        rng: random.Random,
        record_size: RecordSize = RecordSize.LARGE,
        in_query: bool = False,
    ) -> Any:
        """Generate a random value, should be possible for all types."""
        raise NotImplementedError

    @staticmethod
    def numeric_value(num: int, in_query: bool = False) -> Any:
        """Generate a value that corresponds to `num`, so that it will always be the same value for the same input `num`, but fits into the type. This doesn't make sense for a type like boolean."""
        raise NotImplementedError

    @staticmethod
    def name(backend: Backend = Backend.POSTGRES) -> str:
        raise NotImplementedError


class Boolean(DataType):
    @staticmethod
    def random_value(
        rng: random.Random,
        record_size: RecordSize = RecordSize.LARGE,
        in_query: bool = False,
    ) -> Any:
        return rng.choice((True, False))

    @staticmethod
    def name(backend: Backend = Backend.POSTGRES) -> str:
        return "boolean"


class SmallInt(DataType):
    @staticmethod
    def random_value(
        rng: random.Random,
        record_size: RecordSize = RecordSize.LARGE,
        in_query: bool = False,
    ) -> Any:
        if record_size == RecordSize.TINY:
            min, max = -127, 128
        elif record_size in (RecordSize.SMALL, RecordSize.MEDIUM, RecordSize.LARGE):
            min, max = -32768, 32767
        else:
            raise ValueError(f"Unexpected record size {record_size}")

        if rng.randrange(10) == 0:
            return min
        if rng.randrange(10) == 0:
            return max
        return rng.randint(min, max)

    @staticmethod
    def numeric_value(num: int, in_query: bool = False) -> Any:
        return num

    @staticmethod
    def name(backend: Backend = Backend.POSTGRES) -> str:
        if backend == Backend.AVRO:
            return "int"  # no explicit support in AVRO
        elif backend == Backend.JSON:
            return "integer"  # no explicit support in JSON
        else:
            return "smallint"


class Int(DataType):
    @staticmethod
    def random_value(
        rng: random.Random,
        record_size: RecordSize = RecordSize.LARGE,
        in_query: bool = False,
    ) -> Any:
        if record_size == RecordSize.TINY:
            min, max = -127, 128
        elif record_size == RecordSize.SMALL:
            min, max = -32768, 32767
        elif record_size in (RecordSize.MEDIUM, RecordSize.LARGE):
            min, max = -2147483648, 2147483647
        else:
            raise ValueError(f"Unexpected record size {record_size}")

        if rng.randrange(10) == 0:
            return min
        if rng.randrange(10) == 0:
            return max
        return rng.randint(min, max)

    @staticmethod
    def numeric_value(num: int, in_query: bool = False) -> Any:
        return num

    @staticmethod
    def name(backend: Backend = Backend.POSTGRES) -> str:
        if backend == Backend.JSON:
            return "integer"
        else:
            return "int"


class Long(DataType):
    @staticmethod
    def random_value(
        rng: random.Random,
        record_size: RecordSize = RecordSize.LARGE,
        in_query: bool = False,
    ) -> Any:
        if record_size == RecordSize.TINY:
            min, max = -127, 128
        elif record_size == RecordSize.SMALL:
            min, max = -32768, 32767
        elif record_size == RecordSize.MEDIUM:
            min, max = -2147483648, 2147483647
        elif record_size == RecordSize.LARGE:
            min, max = -9223372036854775808, 9223372036854775807
        else:
            raise ValueError(f"Unexpected record size {record_size}")

        if rng.randrange(10) == 0:
            return min
        if rng.randrange(10) == 0:
            return max
        return rng.randint(min, max)

    @staticmethod
    def numeric_value(num: int, in_query: bool = False) -> Any:
        return num

    @staticmethod
    def name(backend: Backend = Backend.POSTGRES) -> str:
        if backend == Backend.AVRO:
            return "long"
        elif backend == Backend.JSON:
            return "integer"
        else:
            return "bigint"


class Float(DataType):
    @staticmethod
    def random_value(
        rng: random.Random,
        record_size: RecordSize = RecordSize.LARGE,
        in_query: bool = False,
    ) -> Any:
        if rng.randrange(10) == 0:
            return 1.0
        if rng.randrange(10) == 0:
            return 0.0

        if record_size == RecordSize.TINY:
            return rng.random()
        elif record_size == RecordSize.SMALL:
            return rng.uniform(-100, 100)
        elif record_size == RecordSize.MEDIUM:
            return rng.uniform(-1_000_000, 1_000_000)
        elif record_size == RecordSize.LARGE:
            return rng.uniform(-1_000_000_000, 1_000_000_000_00)
        else:
            raise ValueError(f"Unexpected record size {record_size}")

    @staticmethod
    def numeric_value(num: int, in_query: bool = False) -> Any:
        return num

    @staticmethod
    def name(backend: Backend = Backend.POSTGRES) -> str:
        if backend == Backend.AVRO:
            return "float"
        elif backend == Backend.JSON:
            return "number"
        else:
            return "float4"


class Double(Float):
    @staticmethod
    def name(backend: Backend = Backend.POSTGRES) -> str:
        if backend == Backend.AVRO:
            return "double"
        elif backend == Backend.JSON:
            return "number"
        else:
            return "float8"


class Text(DataType):
    @staticmethod
    def random_value(
        rng: random.Random,
        record_size: RecordSize = RecordSize.LARGE,
        in_query: bool = False,
    ) -> Any:
        if rng.randrange(10) == 0:
            result = rng.choice(
                [
                    "NULL",
                    "0.0",
                    "True",
                    # "",
                    "表ポあA鷗ŒéＢ逍Üßªąñ丂㐀𠀀",
                    rng.randint(-100, 100),
                ]
            )
        # Fails: unterminated dollar-quoted string
        # chars = string.printable
        chars = string.ascii_letters + string.digits
        if record_size == RecordSize.TINY:
            result = rng.choice(("foo", "bar", "baz"))
        elif record_size == RecordSize.SMALL:
            result = "".join(rng.choice(chars) for _ in range(3))
        elif record_size == RecordSize.MEDIUM:
            result = "".join(rng.choice(chars) for _ in range(10))
        elif record_size == RecordSize.LARGE:
            result = "".join(rng.choice(chars) for _ in range(100))
        else:
            raise ValueError(f"Unexpected record size {record_size}")

        return literal(str(result)) if in_query else str(result)

    @staticmethod
    def numeric_value(num: int, in_query: bool = False) -> Any:
        result = f"key{num}"
        return f"'{result}'" if in_query else str(result)

    @staticmethod
    def name(backend: Backend = Backend.POSTGRES) -> str:
        if backend == Backend.POSTGRES:
            return "text"
        else:
            return "string"


class Bytea(Text):
    @staticmethod
    def name(backend: Backend = Backend.POSTGRES) -> str:
        if backend == Backend.AVRO:
            return "bytes"
        elif backend == Backend.JSON:
            return "string"
        else:
            return "bytea"


class Jsonb(DataType):
    @staticmethod
    def name(backend: Backend = Backend.POSTGRES) -> str:
        if backend == Backend.AVRO:
            return "record"
        elif backend == Backend.JSON:
            return "object"
        else:
            return "jsonb"

    @staticmethod
    def random_value(
        rng: random.Random,
        record_size: RecordSize = RecordSize.LARGE,
        in_query: bool = False,
    ) -> Any:
        if record_size == RecordSize.TINY:
            key_range = 1
        elif record_size == RecordSize.SMALL:
            key_range = 5
        elif record_size == RecordSize.MEDIUM:
            key_range = 10
        elif record_size == RecordSize.LARGE:
            key_range = 20
        else:
            raise ValueError(f"Unexpected record size {record_size}")
        result = {f"key{key}": str(rng.randint(-100, 100)) for key in range(key_range)}
        return f"'{json.dumps(result)}'::jsonb" if in_query else json.dumps(result)

    @staticmethod
    def numeric_value(num: int, in_query: bool = False) -> Any:
        result = {f"key{num}": str(num)}
        return f"'{json.dumps(result)}'::jsonb" if in_query else json.dumps(result)


class TextTextMap(DataType):
    @staticmethod
    def name(backend: Backend = Backend.POSTGRES) -> str:
        if backend == Backend.AVRO:
            return "record"
        elif backend == Backend.JSON:
            return "object"
        else:
            return "map[text=>text]"

    @staticmethod
    def random_value(
        rng: random.Random,
        record_size: RecordSize = RecordSize.LARGE,
        in_query: bool = False,
    ) -> Any:
        if record_size == RecordSize.TINY:
            key_range = 1
        elif record_size == RecordSize.SMALL:
            key_range = 5
        elif record_size == RecordSize.MEDIUM:
            key_range = 10
        elif record_size == RecordSize.LARGE:
            key_range = 20
        else:
            raise ValueError(f"Unexpected record size {record_size}")
        values = [
            f"{Text.numeric_value(i)} => {str(rng.randint(-100, 100))}"
            for i in range(0, key_range)
        ]
        values_str = f"{{{', '.join(values)}}}"
        return f"'{values_str}'::map[text=>text]" if in_query else values_str

    @staticmethod
    def numeric_value(num: int, in_query: bool = False) -> Any:
        values = [
            f"{Text.numeric_value(num)} => {Text.numeric_value(num)}"
            for i in range(0, num)
        ]
        values_str = f"{{{', '.join(values)}}}"
        return f"'{values_str}'::map[text=>text]" if in_query else values_str


# Sort to keep determinism for reproducible runs with specific seed
DATA_TYPES = sorted(list(all_subclasses(DataType)), key=repr)

# fastavro._schema_common.UnknownType: record
# bytea requires Python bytes type instead of str
DATA_TYPES_FOR_AVRO = sorted(
    list(set(DATA_TYPES) - {TextTextMap, Jsonb, Bytea, Boolean}), key=repr
)

# MySQL doesn't support keys of unlimited size
DATA_TYPES_FOR_KEY = sorted(list(set(DATA_TYPES_FOR_AVRO) - {Text, Bytea}), key=repr)

NUMBER_TYPES = [SmallInt, Int, Long, Float, Double]
