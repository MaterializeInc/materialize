# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Column class for data generation in workload replay.
"""

from __future__ import annotations

import json
import random
import string
import uuid
from typing import Any

from pg8000.native import literal

from materialize.workload_replay.util import (
    long_tail_choice,
    long_tail_float,
    long_tail_int,
    long_tail_text,
)


class Column:
    """Represents a column with type information and data generation capabilities."""

    def __init__(
        self, name: str, typ: str, nullable: bool, default: Any, data_shape: str | None
    ):
        self.name = name
        self.typ = typ
        self.nullable = nullable
        self.default = default
        self.chars = string.ascii_letters + string.digits
        self.data_shape = data_shape
        if data_shape:
            assert typ in ("text", "bytea"), f"Can't create text shape for type {typ}"

        self._hot_strings = [
            f"{name}_a",
            f"{name}_b",
            f"{name}_c",
            "foo",
            "bar",
            "baz",
            "0",
            "1",
            "NULL",
        ]

    def avro_type(self) -> str | list[str]:
        """Return the Avro type for this column."""
        result = self.typ
        if self.typ in ("text", "bytea", "character", "character varying"):
            result = "string"
        elif self.typ in ("smallint", "integer", "uint2", "uint4"):
            result = "int"
        elif self.typ in ("bigint", "uint8"):
            result = "long"
        elif self.typ in ("double precision", "numeric"):
            result = "double"
        elif self.typ in ("timestamp with time zone", "timestamp without time zone"):
            result = "long"
        return ["null", result] if self.nullable else result

    def kafka_value(self, rng: random.Random) -> Any:
        """Generate a value suitable for Kafka serialization."""
        if self.default and rng.randrange(10) == 0 and self.default != "NULL":
            return str(self.default)
        if self.nullable and rng.randrange(10) == 0:
            return None

        if self.typ == "boolean":
            return rng.random() < 0.2

        elif self.typ == "smallint":
            return long_tail_int(-32768, 32767, rng=rng)
        elif self.typ == "integer":
            return long_tail_int(-2147483648, 2147483647, rng=rng)
        elif self.typ == "bigint":
            return long_tail_int(-9223372036854775808, 9223372036854775807, rng=rng)

        elif self.typ == "uint2":
            return long_tail_int(0, 65535, rng=rng)
        elif self.typ == "uint4":
            return long_tail_int(0, 4294967295, rng=rng)
        elif self.typ == "uint8":
            return long_tail_int(0, 18446744073709551615, rng=rng)

        elif self.typ in ("float", "double precision", "numeric"):
            return long_tail_float(-1_000_000_000.0, 1_000_000_000.0, rng=rng)

        elif self.typ in ("text", "bytea"):
            if self.data_shape == "datetime":
                year = long_tail_choice(
                    [2023, 2024, 2025, 2022, 2021, 2020, 2019], hot_prob=0.9, rng=rng
                )
                return literal(
                    f"{year}-{rng.randrange(1, 13):02}-{rng.randrange(1, 29):02}T{rng.randrange(0, 23):02}:{rng.randrange(0, 59):02}:{rng.randrange(0, 59):02}Z"
                )
            elif self.data_shape:
                raise ValueError(f"Unhandled text shape {self.data_shape}")
            return literal(long_tail_text(self.chars, 100, self._hot_strings, rng=rng))

        elif self.typ in ("character", "character varying"):
            return literal(long_tail_text(self.chars, 10, self._hot_strings, rng=rng))

        elif self.typ == "uuid":
            return str(uuid.UUID(int=rng.getrandbits(128), version=4))

        elif self.typ == "jsonb":
            result = {
                f"key{key}": str(long_tail_int(-100, 100, rng=rng)) for key in range(20)
            }
            return json.dumps(result)

        elif self.typ in ("timestamp with time zone", "timestamp without time zone"):
            now = 1700000000000  # doesn't need to be exact
            if rng.random() < 0.9:
                return now + long_tail_int(-86_400_000, 86_400_000, rng=rng)
            else:
                return rng.randrange(0, 9223372036854775807)

        elif self.typ == "mz_timestamp":
            year = long_tail_choice(
                [2023, 2024, 2025, 2022, 2021, 2020, 2019], hot_prob=0.9, rng=rng
            )
            return literal(f"{year}-{rng.randrange(1, 13)}-{rng.randrange(1, 29)}")

        elif self.typ == "date":
            year = long_tail_choice(
                [2023, 2024, 2025, 2022, 2021, 2020, 2019], hot_prob=0.9, rng=rng
            )
            return literal(f"{year}-{rng.randrange(1, 13)}-{rng.randrange(1, 29)}")

        elif self.typ == "time":
            if rng.random() < 0.8:
                common = ["00:00:00.000000", "12:00:00.000000", "23:59:59.000000"]
                return literal(rng.choice(common))
            return literal(
                f"{rng.randrange(0, 24)}:{rng.randrange(0, 60)}:{rng.randrange(0, 60)}.{rng.randrange(0, 1000000)}"
            )

        elif self.typ == "int2range":
            a = str(long_tail_int(-32768, 32767, rng=rng))
            b = str(long_tail_int(-32768, 32767, rng=rng))
            return literal(f"[{a},{b})")

        elif self.typ == "int4range":
            a = str(long_tail_int(-2147483648, 2147483647, rng=rng))
            b = str(long_tail_int(-2147483648, 2147483647, rng=rng))
            return literal(f"[{a},{b})")

        elif self.typ == "int8range":
            a = str(long_tail_int(-9223372036854775808, 9223372036854775807, rng=rng))
            b = str(long_tail_int(-9223372036854775808, 9223372036854775807, rng=rng))
            return literal(f"[{a},{b})")

        elif self.typ == "map":
            return {
                str(i): str(long_tail_int(-100, 100, rng=rng)) for i in range(0, 20)
            }

        elif self.typ == "text[]":
            values = [
                literal(long_tail_text(self.chars, 100, self._hot_strings, rng=rng))
                for _ in range(5)
            ]
            return literal(f"{{{', '.join(values)}}}")

        else:
            raise ValueError(f"Unhandled data type {self.typ}")

    def value(self, rng: random.Random, in_query: bool = True) -> Any:
        """Generate a value suitable for SQL queries or COPY operations."""
        if self.default and rng.randrange(10) == 0 and self.default != "NULL":
            return str(self.default) if in_query else self.default

        if self.nullable and rng.randrange(10) == 0:
            return "NULL" if in_query else None

        if self.typ == "boolean":
            val = rng.random() < 0.2
            return ("true" if val else "false") if in_query else val

        elif self.typ == "smallint":
            val = long_tail_int(-32768, 32767, rng=rng)
            return str(val) if in_query else val

        elif self.typ == "integer":
            val = long_tail_int(-2147483648, 2147483647, rng=rng)
            return str(val) if in_query else val

        elif self.typ == "bigint":
            val = long_tail_int(-9223372036854775808, 9223372036854775807, rng=rng)
            return str(val) if in_query else val

        elif self.typ == "uint2":
            val = long_tail_int(0, 65535, rng=rng)
            return str(val) if in_query else val

        elif self.typ == "uint4":
            val = long_tail_int(0, 4294967295, rng=rng)
            return str(val) if in_query else val

        elif self.typ == "uint8":
            val = long_tail_int(0, 18446744073709551615, rng=rng)
            return str(val) if in_query else val

        elif self.typ in ("float", "double precision", "numeric"):
            val = long_tail_float(-1_000_000_000.0, 1_000_000_000.0, rng=rng)
            return str(val) if in_query else val

        elif self.typ in ("text", "bytea"):
            if self.data_shape == "datetime":
                year = long_tail_choice(
                    [2023, 2024, 2025, 2022, 2021, 2020, 2019], hot_prob=0.9, rng=rng
                )
                s = (
                    f"{year}-{rng.randrange(1, 13):02}-{rng.randrange(1, 29):02}"
                    f"T{rng.randrange(0, 23):02}:{rng.randrange(0, 59):02}:{rng.randrange(0, 59):02}Z"
                )
                return literal(s) if in_query else s

            elif self.data_shape:
                raise ValueError(f"Unhandled text shape {self.data_shape}")

            s = long_tail_text(self.chars, 100, self._hot_strings, rng=rng)
            return literal(s) if in_query else s

        elif self.typ in ("character", "character varying"):
            s = long_tail_text(self.chars, 10, self._hot_strings, rng=rng)
            return literal(s) if in_query else s

        elif self.typ == "uuid":
            u = uuid.UUID(int=rng.getrandbits(128), version=4)
            return str(u) if in_query else u

        elif self.typ == "jsonb":
            obj = {
                f"key{key}": str(long_tail_int(-100, 100, rng=rng)) for key in range(20)
            }
            if in_query:
                return f"'{json.dumps(obj)}'::jsonb"
            else:
                return json.dumps(obj)

        elif self.typ in ("timestamp with time zone", "timestamp without time zone"):
            year = long_tail_choice(
                [2023, 2024, 2025, 2022, 2021, 2020, 2019], hot_prob=0.9, rng=rng
            )
            s = f"{year}-{rng.randrange(1, 13)}-{rng.randrange(1, 29)}"
            return literal(s) if in_query else s

        elif self.typ == "mz_timestamp":
            year = long_tail_choice(
                [2023, 2024, 2025, 2022, 2021, 2020, 2019], hot_prob=0.9, rng=rng
            )
            s = f"{year}-{rng.randrange(1, 13)}-{rng.randrange(1, 29)}"
            return literal(s) if in_query else s

        elif self.typ == "date":
            year = long_tail_choice(
                [2023, 2024, 2025, 2022, 2021, 2020, 2019], hot_prob=0.9, rng=rng
            )
            s = f"{year}-{rng.randrange(1, 13)}-{rng.randrange(1, 29)}"
            return literal(s) if in_query else s

        elif self.typ == "time":
            if rng.random() < 0.8:
                s = rng.choice(
                    ["00:00:00.000000", "12:00:00.000000", "23:59:59.000000"]
                )
                return literal(s) if in_query else s

            s = (
                f"{rng.randrange(0, 24)}:{rng.randrange(0, 60)}:{rng.randrange(0, 60)}"
                f".{rng.randrange(0, 1000000)}"
            )
            return literal(s) if in_query else s

        elif self.typ == "int2range":
            a = str(long_tail_int(-32768, 32767, rng=rng))
            b = str(long_tail_int(-32768, 32767, rng=rng))
            s = f"[{a},{b})"
            return literal(s) if in_query else s

        elif self.typ == "int4range":
            a = str(long_tail_int(-2147483648, 2147483647, rng=rng))
            b = str(long_tail_int(-2147483648, 2147483647, rng=rng))
            s = f"[{a},{b})"
            return literal(s) if in_query else s

        elif self.typ == "int8range":
            a = str(long_tail_int(-9223372036854775808, 9223372036854775807, rng=rng))
            b = str(long_tail_int(-9223372036854775808, 9223372036854775807, rng=rng))
            s = f"[{a},{b})"
            return literal(s) if in_query else s

        elif self.typ == "map":
            if in_query:
                values = [
                    f"'{i}' => {str(long_tail_int(-100, 100, rng=rng))}"
                    for i in range(0, 20)
                ]
                return literal(f"{{{', '.join(values)}}}")
            else:
                # COPY text input for map expects the literal form too
                values = [
                    f'"{i}"=>"{str(long_tail_int(-100, 100, rng=rng))}"'
                    for i in range(0, 20)
                ]
                return "{" + ",".join(values) + "}"

        elif self.typ == "text[]":
            if in_query:
                values = [
                    literal(long_tail_text(self.chars, 100, self._hot_strings, rng=rng))
                    for _ in range(5)
                ]
                return literal(f"{{{', '.join(values)}}}")
            else:
                return [
                    long_tail_text(self.chars, 100, self._hot_strings, rng=rng)
                    for _ in range(5)
                ]

        else:
            raise ValueError(f"Unhandled data type {self.typ}")
