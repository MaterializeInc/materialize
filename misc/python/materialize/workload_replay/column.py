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
    long_tail_float,
    long_tail_int,
    long_tail_rank,
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

        self._years = list(range(2019, 2026))
        self._seq_counter = 0

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

    def _shaped_text(self, rng: random.Random) -> str | None:
        """Generate text according to data_shape, or None if not applicable."""
        if self.data_shape == "datetime":
            return self._random_datetime(rng)
        elif self.data_shape == "random":
            length = rng.randrange(5, 40)
            return "".join(rng.choice(self.chars) for _ in range(length))
        elif self.data_shape == "uuid":
            return str(uuid.UUID(int=rng.getrandbits(128), version=4))
        elif self.data_shape == "sequential":
            self._seq_counter += 1
            return f"{self.name}_{self._seq_counter}"
        elif self.data_shape == "zipfian":
            rank = long_tail_rank(n=10000, a=1.3, rng=rng)
            return f"{self.name}_{rank}"
        elif self.data_shape is not None and self.data_shape != "duration":
            raise ValueError(f"Unhandled data_shape {self.data_shape!r}")
        return None

    def _shaped_float(self, rng: random.Random) -> float | None:
        """Generate a float according to data_shape, or None if not applicable."""
        if self.data_shape == "duration":
            return round(rng.uniform(10.0, 1800.0), 2)
        return None

    def _random_date(self, rng: random.Random) -> str:
        """Generate a uniformly random date string."""
        year = rng.choice(self._years)
        return f"{year}-{rng.randrange(1, 13):02}-{rng.randrange(1, 29):02}"

    def _random_datetime(self, rng: random.Random) -> str:
        """Generate a uniformly random datetime string."""
        return (
            f"{self._random_date(rng)}"
            f"T{rng.randrange(0, 24):02}:{rng.randrange(0, 60):02}:{rng.randrange(0, 60):02}Z"
        )

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
            shaped = self._shaped_float(rng)
            if shaped is not None:
                return shaped
            return long_tail_float(-1_000_000_000.0, 1_000_000_000.0, rng=rng)

        elif self.typ in ("text", "bytea"):
            shaped = self._shaped_text(rng)
            if shaped is not None:
                return literal(shaped)
            return literal(long_tail_text(self.chars, 100, self._hot_strings, rng=rng))

        elif self.typ in ("character", "character varying"):
            shaped = self._shaped_text(rng)
            if shaped is not None:
                return literal(shaped)
            return literal(long_tail_text(self.chars, 10, self._hot_strings, rng=rng))

        elif self.typ == "uuid":
            return str(uuid.UUID(int=rng.getrandbits(128), version=4))

        elif self.typ == "jsonb":
            result = {
                f"key{key}": str(long_tail_int(-100, 100, rng=rng)) for key in range(20)
            }
            return json.dumps(result)

        elif self.typ in ("timestamp with time zone", "timestamp without time zone"):
            # Epoch millis spread uniformly across 2019–2025
            # 2019-01-01 = 1546300800000, 2026-01-01 = 1767225600000
            return rng.randrange(1546300800000, 1767225600000)

        elif self.typ == "mz_timestamp":
            return literal(self._random_date(rng))

        elif self.typ == "date":
            return literal(self._random_date(rng))

        elif self.typ == "time":
            if rng.random() < 0.8:
                common = ["00:00:00.000000", "12:00:00.000000", "23:59:59.000000"]
                return literal(rng.choice(common))
            return literal(
                f"{rng.randrange(0, 24)}:{rng.randrange(0, 60)}:{rng.randrange(0, 60)}.{rng.randrange(0, 1000000)}"
            )

        elif self.typ == "int2range":
            a = long_tail_int(-32768, 32767, rng=rng)
            b = long_tail_int(-32768, 32767, rng=rng)
            lo, hi = min(a, b), max(a, b)
            return literal(f"[{lo},{hi})")

        elif self.typ == "int4range":
            a = long_tail_int(-2147483648, 2147483647, rng=rng)
            b = long_tail_int(-2147483648, 2147483647, rng=rng)
            lo, hi = min(a, b), max(a, b)
            return literal(f"[{lo},{hi})")

        elif self.typ == "int8range":
            a = long_tail_int(-9223372036854775808, 9223372036854775807, rng=rng)
            b = long_tail_int(-9223372036854775808, 9223372036854775807, rng=rng)
            lo, hi = min(a, b), max(a, b)
            return literal(f"[{lo},{hi})")

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
            shaped = self._shaped_float(rng)
            if shaped is not None:
                return str(shaped) if in_query else shaped
            val = long_tail_float(-1_000_000_000.0, 1_000_000_000.0, rng=rng)
            return str(val) if in_query else val

        elif self.typ in ("text", "bytea"):
            shaped = self._shaped_text(rng)
            if shaped is not None:
                return literal(shaped) if in_query else shaped
            s = long_tail_text(self.chars, 100, self._hot_strings, rng=rng)
            return literal(s) if in_query else s

        elif self.typ in ("character", "character varying"):
            shaped = self._shaped_text(rng)
            if shaped is not None:
                return literal(shaped) if in_query else shaped
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
            s = self._random_date(rng)
            return literal(s) if in_query else s

        elif self.typ == "mz_timestamp":
            s = self._random_date(rng)
            return literal(s) if in_query else s

        elif self.typ == "date":
            s = self._random_date(rng)
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
            a = long_tail_int(-32768, 32767, rng=rng)
            b = long_tail_int(-32768, 32767, rng=rng)
            lo, hi = min(a, b), max(a, b)
            s = f"[{lo},{hi})"
            return literal(s) if in_query else s

        elif self.typ == "int4range":
            a = long_tail_int(-2147483648, 2147483647, rng=rng)
            b = long_tail_int(-2147483648, 2147483647, rng=rng)
            lo, hi = min(a, b), max(a, b)
            s = f"[{lo},{hi})"
            return literal(s) if in_query else s

        elif self.typ == "int8range":
            a = long_tail_int(-9223372036854775808, 9223372036854775807, rng=rng)
            b = long_tail_int(-9223372036854775808, 9223372036854775807, rng=rng)
            lo, hi = min(a, b), max(a, b)
            s = f"[{lo},{hi})"
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
            # Custom data type, or not supported yet
            return "NULL" if in_query else None
