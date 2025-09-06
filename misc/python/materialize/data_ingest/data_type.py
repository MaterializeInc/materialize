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
import uuid
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
    MYSQL = 4
    SQL_SERVER = 5
    MATERIALIZE = 6


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
    def name(backend: Backend = Backend.MATERIALIZE) -> str:
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
    def name(backend: Backend = Backend.MATERIALIZE) -> str:
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
    def name(backend: Backend = Backend.MATERIALIZE) -> str:
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
    def name(backend: Backend = Backend.MATERIALIZE) -> str:
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
    def name(backend: Backend = Backend.MATERIALIZE) -> str:
        if backend == Backend.AVRO:
            return "long"
        elif backend == Backend.JSON:
            return "integer"
        else:
            return "bigint"


class UInt2(DataType):
    @staticmethod
    def random_value(
        rng: random.Random,
        record_size: RecordSize = RecordSize.LARGE,
        in_query: bool = False,
    ) -> Any:
        if record_size == RecordSize.TINY:
            min, max = 0, 256
        elif record_size in (RecordSize.SMALL, RecordSize.MEDIUM, RecordSize.LARGE):
            min, max = 0, 65535
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
    def name(backend: Backend = Backend.MATERIALIZE) -> str:
        if backend == Backend.AVRO:
            return "int"  # no explicit support in AVRO
        elif backend == Backend.JSON:
            return "integer"  # no explicit support in JSON
        elif backend == Backend.POSTGRES:
            return "numeric"  # no support in Postgres
        elif backend == Backend.MYSQL:
            return "smallint unsigned"
        else:
            return "uint2"


class UInt4(DataType):
    @staticmethod
    def random_value(
        rng: random.Random,
        record_size: RecordSize = RecordSize.LARGE,
        in_query: bool = False,
    ) -> Any:
        if record_size == RecordSize.TINY:
            min, max = 0, 256
        elif record_size == RecordSize.SMALL:
            min, max = 0, 65535
        elif record_size in (RecordSize.MEDIUM, RecordSize.LARGE):
            min, max = 0, 4294967295
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
    def name(backend: Backend = Backend.MATERIALIZE) -> str:
        if backend == Backend.AVRO:
            return "int"  # no explicit support in AVRO
        elif backend == Backend.JSON:
            return "integer"  # no explicit support in JSON
        elif backend == Backend.POSTGRES:
            return "numeric"  # no support in Postgres
        elif backend == Backend.MYSQL:
            return "int unsigned"
        else:
            return "uint4"


class UInt8(DataType):
    @staticmethod
    def random_value(
        rng: random.Random,
        record_size: RecordSize = RecordSize.LARGE,
        in_query: bool = False,
    ) -> Any:
        if record_size == RecordSize.TINY:
            min, max = 0, 256
        elif record_size == RecordSize.SMALL:
            min, max = 0, 65535
        elif record_size == RecordSize.MEDIUM:
            min, max = 0, 4294967295
        elif record_size == RecordSize.LARGE:
            min, max = 0, 18446744073709551615
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
    def name(backend: Backend = Backend.MATERIALIZE) -> str:
        if backend == Backend.AVRO:
            return "long"
        elif backend == Backend.JSON:
            return "integer"
        elif backend == Backend.POSTGRES:
            return "numeric"  # no support in Postgres
        elif backend == Backend.MYSQL:
            return "bigint unsigned"
        else:
            return "uint8"


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
    def name(backend: Backend = Backend.MATERIALIZE) -> str:
        if backend == Backend.AVRO:
            return "float"
        elif backend == Backend.JSON:
            return "number"
        elif backend == Backend.SQL_SERVER:
            return "real"
        else:
            return "float4"


class Double(Float):
    @staticmethod
    def name(backend: Backend = Backend.MATERIALIZE) -> str:
        if backend == Backend.AVRO:
            return "double"
        elif backend == Backend.JSON:
            return "number"
        elif backend == Backend.SQL_SERVER:
            return "float"
        else:
            return "float8"


class Numeric(Float):
    @staticmethod
    def name(backend: Backend = Backend.MATERIALIZE) -> str:
        if backend == Backend.AVRO:
            return "double"
        elif backend == Backend.JSON:
            return "number"
        else:
            return "numeric"


class Numeric383(Float):
    @staticmethod
    def name(backend: Backend = Backend.MATERIALIZE) -> str:
        if backend == Backend.AVRO:
            return "double"
        elif backend == Backend.JSON:
            return "number"
        else:
            return "numeric(38,3)"


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
    def name(backend: Backend = Backend.MATERIALIZE) -> str:
        if backend in (
            Backend.MATERIALIZE,
            Backend.POSTGRES,
            Backend.MYSQL,
        ):
            return "text"
        elif backend == Backend.SQL_SERVER:
            return "varchar(1024)"
        else:
            return "string"


class Bytea(Text):
    @staticmethod
    def name(backend: Backend = Backend.MATERIALIZE) -> str:
        if backend == Backend.AVRO:
            return "bytes"
        elif backend == Backend.JSON:
            return "string"
        else:
            return "bytea"


class UUID(DataType):
    @staticmethod
    def random_value(
        rng: random.Random,
        record_size: RecordSize = RecordSize.LARGE,
        in_query: bool = False,
    ) -> Any:
        result = rng.choice(
            [
                "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
                "6f5eec33-a3c9-40b2-ae06-58f53aca6e7d",
                "00000000-0000-0000-0000-000000000000",
                "ffffffff-ffff-ffff-ffff-ffffffffffff",
                uuid.UUID(int=rng.getrandbits(128), version=4),
            ]
        )
        return f"'{result}'::uuid" if in_query else str(result)

    @staticmethod
    def numeric_value(num: int, in_query: bool = False) -> Any:
        result = uuid.uuid1(clock_seq=num)
        return f"'{result}'::uuid" if in_query else str(result)

    @staticmethod
    def name(backend: Backend = Backend.MATERIALIZE) -> str:
        return "uuid"


class Jsonb(DataType):
    @staticmethod
    def name(backend: Backend = Backend.MATERIALIZE) -> str:
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
    def name(backend: Backend = Backend.MATERIALIZE) -> str:
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


class IntArray(DataType):
    @staticmethod
    def name(backend: Backend = Backend.MATERIALIZE) -> str:
        if backend == Backend.AVRO:
            raise ValueError("Unsupported")
        elif backend == Backend.JSON:
            raise ValueError("Unsupported")
        else:
            return "int[]"

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
        values = [str(rng.randint(-100, 100)) for i in range(0, key_range)]
        values_str = f"{{{', '.join(values)}}}"
        return f"'{values_str}'::int[]" if in_query else values_str

    @staticmethod
    def numeric_value(num: int, in_query: bool = False) -> Any:
        values = [str(num) for i in range(0, num)]
        values_str = f"{{{', '.join(values)}}}"
        return f"'{values_str}'::int[]" if in_query else values_str


class IntList(DataType):
    @staticmethod
    def name(backend: Backend = Backend.MATERIALIZE) -> str:
        if backend == Backend.AVRO:
            raise ValueError("Unsupported")
        elif backend == Backend.JSON:
            raise ValueError("Unsupported")
        else:
            return "int list"

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
        values = [str(rng.randint(-100, 100)) for i in range(0, key_range)]
        values_str = f"{{{', '.join(values)}}}"
        return f"'{values_str}'::int list" if in_query else values_str

    @staticmethod
    def numeric_value(num: int, in_query: bool = False) -> Any:
        values = [str(num) for i in range(0, num)]
        values_str = f"{{{', '.join(values)}}}"
        return f"'{values_str}'::int list" if in_query else values_str


class Timestamp(DataType):
    @staticmethod
    def random_value(
        rng: random.Random,
        record_size: RecordSize = RecordSize.LARGE,
        in_query: bool = False,
    ) -> Any:
        if rng.randrange(100) == 0:
            result = "1-01-01"
        elif rng.randrange(100) == 0:
            result = "99999-12-31"
        else:
            result = f"{rng.randrange(1, 100000)}-{rng.randrange(1, 13)}-{rng.randrange(1, 29)}"
        return f"TIMESTAMP '{result}'" if in_query else str(result)

    @staticmethod
    def numeric_value(num: int, in_query: bool = False) -> Any:
        day = num % 28
        month = num // 28 % 12
        year = num // 336
        result = f"{year}-{month}-{day}"
        return f"TIMESTAMP '{result}'" if in_query else str(result)

    @staticmethod
    def name(backend: Backend = Backend.MATERIALIZE) -> str:
        if backend == Backend.AVRO:
            raise ValueError("Unsupported")
        elif backend == Backend.JSON:
            raise ValueError("Unsupported")
        elif backend == Backend.SQL_SERVER:
            return "datetime2"
        else:
            return "timestamp"


class MzTimestamp(DataType):
    @staticmethod
    def random_value(
        rng: random.Random,
        record_size: RecordSize = RecordSize.LARGE,
        in_query: bool = False,
    ) -> Any:
        if rng.randrange(100) == 0:
            result = "1970-01-01"
        elif rng.randrange(100) == 0:
            result = "99999-12-31"
        else:
            result = f"{rng.randrange(1970, 100000)}-{rng.randrange(1, 13)}-{rng.randrange(1, 29)}"
        return f"MZ_TIMESTAMP '{result}'" if in_query else result

    @staticmethod
    def numeric_value(num: int, in_query: bool = False) -> Any:
        day = num % 28
        month = num // 28 % 12
        year = 1970 + (num // 336)
        result = f"{year}-{month}-{day}"
        return f"MZ_TIMESTAMP '{result}'" if in_query else result

    @staticmethod
    def name(backend: Backend = Backend.MATERIALIZE) -> str:
        if backend != Backend.MATERIALIZE:
            raise ValueError("Unsupported")
        return "mz_timestamp"


class Date(DataType):
    @staticmethod
    def random_value(
        rng: random.Random,
        record_size: RecordSize = RecordSize.LARGE,
        in_query: bool = False,
    ) -> Any:
        if rng.randrange(100) == 0:
            result = "1-01-01"
        elif rng.randrange(100) == 0:
            result = "99999-12-31"
        else:
            result = f"{rng.randrange(1, 100000)}-{rng.randrange(1, 13)}-{rng.randrange(1, 29)}"
        return f"DATE '{result}'" if in_query else result

    @staticmethod
    def numeric_value(num: int, in_query: bool = False) -> Any:
        day = num % 28
        month = num // 28 % 12
        year = num // 336
        result = f"{year}-{month}-{day}"
        return f"DATE '{result}'" if in_query else result

    @staticmethod
    def name(backend: Backend = Backend.MATERIALIZE) -> str:
        if backend == Backend.AVRO:
            raise ValueError("Unsupported")
        elif backend == Backend.JSON:
            raise ValueError("Unsupported")
        else:
            return "date"


class Time(DataType):
    @staticmethod
    def random_value(
        rng: random.Random,
        record_size: RecordSize = RecordSize.LARGE,
        in_query: bool = False,
    ) -> Any:
        if rng.randrange(100) == 0:
            result = "00:00:00"
        elif rng.randrange(100) == 0:
            result = "23:59:59.999999"
        else:
            result = f"{rng.randrange(0, 24)}:{rng.randrange(0, 60)}:{rng.randrange(0, 60)}.{rng.randrange(0, 1000000)}"
        return f"TIME '{result}'" if in_query else result

    @staticmethod
    def numeric_value(num: int, in_query: bool = False) -> Any:
        seconds = num % 60
        minutes = num // 60 % 60
        hours = num // 3600
        result = f"{hours}:{minutes}:{seconds}"
        return f"TIME '{result}'" if in_query else result

    @staticmethod
    def name(backend: Backend = Backend.MATERIALIZE) -> str:
        if backend == Backend.AVRO:
            raise ValueError("Unsupported")
        elif backend == Backend.JSON:
            raise ValueError("Unsupported")
        else:
            return "time"


class Interval(DataType):
    @staticmethod
    def random_value(
        rng: random.Random,
        record_size: RecordSize = RecordSize.LARGE,
        in_query: bool = False,
    ) -> Any:
        if rng.randrange(100) == 0:
            result = (
                "-178956970 years -8 months -2147483648 days -2562047788:00:54.775808"
            )
        elif rng.randrange(100) == 0:
            result = "178956970 years 7 months 2147483647 days 2562047788:00:54.775807"
        elif record_size == RecordSize.TINY:
            result = f"{rng.random():.0f} MINUTE"
        elif record_size == RecordSize.SMALL:
            result = f"{rng.uniform(-100, 100):.0f} days {rng.uniform(-100, 100):.0f} seconds"
        elif record_size == RecordSize.MEDIUM:
            result = f"{rng.uniform(-100, 100):.0f} years {rng.uniform(-100, 100):.0f} days {rng.uniform(-100, 100):.0f} seconds"
        elif record_size == RecordSize.LARGE:
            result = f"{rng.uniform(-178956970, 178956970):.0f} years {rng.uniform(-365, 365):.0f} days {rng.uniform(-1000000000, 1000000000):.0f} seconds"
        else:
            raise ValueError(f"Unexpected record size {record_size}")
        return f"INTERVAL '{result}'" if in_query else result

    @staticmethod
    def numeric_value(num: int, in_query: bool = False) -> Any:
        return f"INTERVAL '{num}' MINUTE" if in_query else str(num)

    @staticmethod
    def name(backend: Backend = Backend.MATERIALIZE) -> str:
        if backend == Backend.AVRO:
            raise ValueError("Unsupported")
        elif backend == Backend.JSON:
            raise ValueError("Unsupported")
        else:
            return "interval"


class Oid(Int):
    @staticmethod
    def name(backend: Backend = Backend.MATERIALIZE) -> str:
        if backend == Backend.AVRO:
            raise ValueError("Unsupported")
        elif backend == Backend.JSON:
            raise ValueError("Unsupported")
        else:
            return "oid"


# Sort to keep determinism for reproducible runs with specific seed
DATA_TYPES = sorted(list(all_subclasses(DataType)), key=repr)

# fastavro._schema_common.UnknownType: record
# bytea requires Python bytes type instead of str
DATA_TYPES_FOR_AVRO = sorted(
    list(
        set(DATA_TYPES)
        - {
            TextTextMap,
            Jsonb,
            Bytea,
            Boolean,
            UUID,
            Interval,
            IntList,
            IntArray,
            Time,
            Date,
            Timestamp,
            MzTimestamp,
            Oid,
            Numeric,
            Numeric383,
            UInt2,
            UInt4,
            UInt8,
            Float,
            Double,
        }
    ),
    key=repr,
)

DATA_TYPES_FOR_MYSQL = sorted(
    list(
        set(DATA_TYPES)
        - {
            IntList,
            IntArray,
            UUID,
            TextTextMap,
            Interval,
            Oid,
            Jsonb,
            Bytea,
            Boolean,
            Numeric,
            Numeric383,
            UInt2,
            UInt4,
            UInt8,
        }
    ),
    key=repr,
)

DATA_TYPES_FOR_SQL_SERVER = sorted(
    list(
        set(DATA_TYPES)
        - {
            IntList,
            IntArray,
            UUID,
            TextTextMap,
            Interval,
            Oid,
            Jsonb,
            Bytea,
            Boolean,
            Numeric,
            Numeric383,
            UInt2,
            UInt4,
            UInt8,
            Date,
            Time,
            Timestamp,
            MzTimestamp,
            Float,
            Double,
        }
    ),
    key=repr,
)

# MySQL doesn't support keys of unlimited size
DATA_TYPES_FOR_KEY = sorted(
    list(set(DATA_TYPES_FOR_AVRO) - {Text, Bytea, IntList, IntArray, Float, Double}),
    key=repr,
)

NUMBER_TYPES = [
    SmallInt,
    Int,
    Long,
    UInt2,
    UInt4,
    UInt8,
    Float,
    Double,
    Numeric,
    Numeric383,
]
