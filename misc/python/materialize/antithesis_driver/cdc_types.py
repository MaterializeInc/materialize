# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""A table with one column per upstream type, whose rows say what they hold.

Each upstream of a CDC source has a table `types (k, seed, tseed, ...)`. The
value of a column is a function of the column and of the row's `seed`, or of
`tseed` for the large columns, so a row read from the export in Materialize
carries what every one of its columns must hold. A check needs no model of
which upstream writes applied or how far the source has read.

The large columns follow a seed of their own so that an update can leave them
untouched. Postgres then sends them as unchanged TOAST values, and SQL Server
captures the row without them.

`Column.samples` pairs each upstream literal with Materialize's rendering of
it. The renderings were read from v26.51.0 and are the same in every release
the driver runs against, since a rollout upgrades the version mid-timeline.
"""

from __future__ import annotations

import hashlib
import zlib
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

SWEEP = 32
"""Seeds below this pick sample `seed % len(samples)` of every column, so rows
with seeds `0..SWEEP-1` hold every sample of every column."""
KEYSPACE = 48
"""Keys from `SWEEP` up are the workload's. Rows below it are written before
the export exists and never change, so they arrive by snapshot."""
UNTOUCHED = 2**31
"""Seeds from here up are given only by an update that leaves the large
columns untouched, so a row with such a seed went through one last."""
BIG_CHARS = 12_000

Sample = tuple[str | None, str | None]


@dataclass(frozen=True)
class Column:
    name: str
    ddl: str
    """The upstream column type."""
    samples: tuple[Sample, ...] = ()
    """(upstream literal, Materialize text) pairs. The literal is sent as a
    string parameter and parsed by `cast`."""
    cast: str = "%s"
    """The upstream expression that makes the column's value of the literal."""
    read: str = "{}::text"
    """The Materialize expression whose result must equal the expected text."""
    text: bool = False
    """Whether the export needs the column in `TEXT COLUMNS`."""
    big: Callable[[int], Sample] | None = None
    """For a large column: its value for a `tseed`."""


def same(*literals: str | None) -> tuple[Sample, ...]:
    """Samples that Materialize renders exactly as they are written."""
    return tuple((literal, literal) for literal in literals)


def _mix(seed: int, name: str) -> int:
    # Not `hash`, which differs between processes.
    return zlib.crc32(f"{seed}:{name}".encode())


def value(column: Column, seed: int, tseed: int) -> Sample:
    if column.big is not None:
        return column.big(tseed)
    n = len(column.samples)
    return column.samples[seed % n if seed < SWEEP else _mix(seed, column.name) % n]


def big_text(tseed: int) -> str:
    """Text that no compression shrinks below the size at which Postgres moves
    a value out of line."""
    out = []
    block = str(tseed).encode()
    while sum(map(len, out)) < BIG_CHARS:
        block = hashlib.sha256(block).digest()
        out.append(block.hex())
    return "".join(out)[:BIG_CHARS]


def md5(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()


def create_table(table: str, columns: tuple[Column, ...]) -> str:
    body = ", ".join(f"{c.name} {c.ddl}" for c in columns)
    return (
        f"CREATE TABLE {table} (k int PRIMARY KEY, seed bigint NOT NULL,"
        f" tseed bigint NOT NULL, {body})"
    )


def insert(
    table: str, columns: tuple[Column, ...], k: int, seed: int, tseed: int
) -> tuple[str, tuple[Any, ...]]:
    names = ", ".join(c.name for c in columns)
    casts = ", ".join(c.cast for c in columns)
    return (
        f"INSERT INTO {table} (k, seed, tseed, {names}) VALUES (%s, %s, %s, {casts})",
        (k, seed, tseed, *(value(c, seed, tseed)[0] for c in columns)),
    )


def update(
    table: str,
    columns: tuple[Column, ...],
    lo: int,
    hi: int,
    seed: int,
    tseed: int | None,
) -> tuple[str, tuple[Any, ...]]:
    """Give the rows with keys in `[lo, hi]` a new `seed`, and a new `tseed`
    unless it is None, which leaves the large columns untouched."""
    changed = [c for c in columns if tseed is not None or c.big is None]
    sets = ["seed = %s", *(f"{c.name} = {c.cast}" for c in changed)]
    params: list[Any] = [seed, *(value(c, seed, tseed or 0)[0] for c in changed)]
    if tseed is not None:
        sets.append("tseed = %s")
        params.append(tseed)
    return (
        f"UPDATE {table} SET {', '.join(sets)} WHERE k BETWEEN %s AND %s",
        (*params, lo, hi),
    )


def delete(table: str, lo: int, hi: int) -> tuple[str, tuple[Any, ...]]:
    return f"DELETE FROM {table} WHERE k BETWEEN %s AND %s", (lo, hi)


def sweep(table: str, columns: tuple[Column, ...]) -> list[tuple[str, tuple[Any, ...]]]:
    """The rows that exist before the export does: every sample of every column."""
    return [insert(table, columns, k, k, k) for k in range(SWEEP)]


def text_columns(columns: tuple[Column, ...]) -> str:
    """The export's `WITH` clause, empty without text columns."""
    names = ", ".join(c.name for c in columns if c.text)
    return f" WITH (TEXT COLUMNS ({names}))" if names else ""


def select(export: str, columns: tuple[Column, ...]) -> str:
    reads = ", ".join(c.read.format(c.name) for c in columns)
    return f"SELECT k, seed, tseed, {reads} FROM {export}"


@dataclass
class Verdict:
    rows: int = 0
    duplicates: list[int] = field(default_factory=list)
    mismatches: list[dict[str, Any]] = field(default_factory=list)
    """The first differing columns, each with its row's key and seeds."""
    nulls: int = 0
    """How many NULLs were expected and found."""
    snapshot_rows: int = 0
    workload_rows: int = 0
    untouched_rows: int = 0
    """Rows whose last change left their large columns untouched."""
    keys: dict[int, tuple[int, int]] = field(default_factory=dict)
    """Each row's seeds, by key."""


def judge(columns: tuple[Column, ...], rows: list[tuple[Any, ...]]) -> Verdict:
    """Compare every row an export returned for `select` with its seeds."""
    v = Verdict(rows=len(rows))
    keys = Counter(int(r[0]) for r in rows)
    v.duplicates = sorted(k for k, n in keys.items() if n > 1)[:10]
    for k, seed, tseed, *observed in rows:
        k, seed, tseed = int(k), int(seed), int(tseed)
        v.keys[k] = (seed, tseed)
        if k < SWEEP:
            v.snapshot_rows += 1
        else:
            v.workload_rows += 1
        if seed >= UNTOUCHED:
            v.untouched_rows += 1
        for column, got in zip(columns, observed):
            want = value(column, seed, tseed)[1]
            if got != want:
                if len(v.mismatches) < 10:
                    v.mismatches.append(
                        {
                            "k": k,
                            "seed": seed,
                            "tseed": tseed,
                            "column": column.name,
                            "expected": want if want is None else want[:200],
                            "observed": got if got is None else str(got)[:200],
                        }
                    )
            elif want is None:
                v.nulls += 1
    return v


def _big(read: str, expected: Callable[[str], str]) -> dict[str, Any]:
    """A large column. `read` reduces it to a length and a digest in
    Materialize, and `expected` does the same to the literal."""

    def sample(tseed: int) -> Sample:
        text = big_text(tseed)
        return text, expected(text)

    return {"big": sample, "read": read}


POSTGRES_ENUM = "drv.types_enum"
ENUM_VALUES = ("small", "medium", "has space")

_TEXTS = same(
    None,
    "",
    "a",
    "NULL",
    "'",
    '"',
    "\\",
    "a\tb\nc",
    " padded ",
    "κλειδί",
    "日本語 🎉",
    "{1,2}",
    "x" * 300,
)

POSTGRES: tuple[Column, ...] = (
    Column("c_bool", "bool", ((None, None), ("t", "true"), ("f", "false"))),
    Column("c_int2", "int2", same(None, "0", "-32768", "32767", "1")),
    Column("c_int4", "int4", same(None, "0", "-2147483648", "2147483647", "-1")),
    Column(
        "c_int8",
        "int8",
        same(None, "0", "-9223372036854775808", "9223372036854775807", "42"),
    ),
    Column("c_oid", "oid", same(None, "0", "4294967295", "16384")),
    Column(
        "c_float4",
        "float4",
        (
            (None, None),
            ("0", "0"),
            ("-0", "-0"),
            ("1.5", "1.5"),
            ("3.4028235e38", "3.4028235e+38"),
            ("1e-45", "1e-45"),
            ("NaN", "NaN"),
            ("Infinity", "Infinity"),
            ("-Infinity", "-Infinity"),
            ("0.1", "0.1"),
        ),
    ),
    Column(
        "c_float8",
        "float8",
        (
            (None, None),
            ("0", "0"),
            ("-0", "-0"),
            ("0.1", "0.1"),
            ("-123456.789", "-123456.789"),
            ("NaN", "NaN"),
            ("Infinity", "Infinity"),
            ("-Infinity", "-Infinity"),
            ("1.7976931348623157e308", "1.7976931348623157e+308"),
            ("5e-324", "5e-324"),
        ),
    ),
    Column(
        "c_numeric",
        "numeric",
        (
            (None, None),
            ("0", "0"),
            ("-1.5", "-1.5"),
            ("12345678901234567890.123456789", "12345678901234567890.123456789"),
            ("0.000000000000000000000000000001", "0.000000000000000000000000000001"),
            ("999999999999999999999999999999999999999", "9" * 39),
            ("1.10", "1.1"),
            ("NaN", "NaN"),
        ),
    ),
    Column(
        "c_numeric_scale",
        "numeric(20, 5)",
        (
            (None, None),
            ("0", "0"),
            ("123456789012345.12345", "123456789012345.12345"),
            ("-0.00001", "-0.00001"),
            ("1.5", "1.5"),
        ),
    ),
    Column("c_text", "text", _TEXTS),
    Column("c_varchar", "varchar(40)", same(None, "", "a", "κλειδί", "x" * 40)),
    Column(
        "c_char",
        "char(5)",
        ((None, None), ("", ""), ("ab", "ab"), ("abcde", "abcde"), ("κ", "κ")),
    ),
    Column(
        "c_bytea",
        "bytea",
        same(None, "\\x", "\\x00", "\\xdeadbeef", "\\x" + bytes(range(256)).hex()),
    ),
    Column(
        "c_date",
        "date",
        same(
            None,
            "1970-01-01",
            "0001-01-01",
            "9999-12-31",
            "2024-02-29",
            "0044-03-15 BC",
            "262142-12-31",
        ),
    ),
    Column(
        "c_time",
        "time",
        same(None, "00:00:00", "23:59:59.999999", "12:34:56.789", "01:02:03"),
    ),
    Column(
        "c_timestamp",
        "timestamp",
        same(
            None,
            "1970-01-01 00:00:00",
            "0001-01-01 00:00:00",
            "9999-12-31 23:59:59.999999",
            "2024-02-29 12:34:56.123456",
            "1969-12-31 23:59:59.5",
            "0044-03-15 12:00:00 BC",
        ),
    ),
    Column(
        "c_timestamptz",
        "timestamptz",
        (
            (None, None),
            ("1970-01-01 00:00:00+00", "1970-01-01 00:00:00+00"),
            ("2024-02-29 12:34:56.123456+05:30", "2024-02-29 07:04:56.123456+00"),
            ("0001-01-01 00:00:00+00", "0001-01-01 00:00:00+00"),
            ("9999-12-31 23:59:59.999999+00", "9999-12-31 23:59:59.999999+00"),
            ("2000-06-15 23:30:00-08", "2000-06-16 07:30:00+00"),
        ),
    ),
    Column(
        "c_interval",
        "interval",
        (
            (None, None),
            ("0", "00:00:00"),
            (
                "1 year 2 mons 3 days 04:05:06.789",
                "1 year 2 mons 3 days 04:05:06.789",
            ),
            ("-1 day", "-1 days"),
            ("-1 year -2 mons +3 days -04:05:06", "-1 years -2 mons +3 days -04:05:06"),
            ("1000000 hours", "1000000:00:00"),
            ("0.000001 seconds", "00:00:00.000001"),
        ),
    ),
    Column(
        "c_uuid",
        "uuid",
        same(
            None,
            "00000000-0000-0000-0000-000000000000",
            "ffffffff-ffff-ffff-ffff-ffffffffffff",
            "123e4567-e89b-12d3-a456-426614174000",
        ),
    ),
    Column(
        "c_json",
        "json",
        (
            (None, None),
            ("null", "null"),
            (
                '{"b": 1,  "a": [1, 2.5, "x", null, true]}',
                '{"a":[1,2.5,"x",null,true],"b":1}',
            ),
            ('"plain \\"quoted\\" \\u00e9"', '"plain \\"quoted\\" é"'),
            ("[]", "[]"),
            ("12345678901234567890", "12345678901234567890"),
            ('{"k": "v", "k": "w"}', '{"k":"w"}'),
        ),
    ),
    Column(
        "c_jsonb",
        "jsonb",
        (
            (None, None),
            ("null", "null"),
            (
                '{"b": {"c": [1, {"d": null}]}, "a": -1.5e3}',
                '{"a":-1500,"b":{"c":[1,{"d":null}]}}',
            ),
            ('"κλειδί"', '"κλειδί"'),
            ("{}", "{}"),
            ("false", "false"),
        ),
    ),
    Column(
        "c_int4_array",
        "int4[]",
        same(None, "{}", "{1,2,3}", "{1,NULL,-2147483648}", "{{1,2},{3,4}}"),
    ),
    Column(
        "c_text_array",
        "text[]",
        same(None, "{}", "{a,b}", '{"a,b","c\\"d",NULL,"NULL",""}', "{κλειδί}"),
    ),
    Column("c_numeric_array", "numeric[]", same(None, "{}", "{1.5,-2,NULL,0.001}")),
    Column(
        "c_timestamptz_array",
        "timestamptz[]",
        (
            (None, None),
            ("{}", "{}"),
            (
                '{"2024-02-29 12:34:56+00",NULL}',
                '{"2024-02-29 12:34:56+00",NULL}',
            ),
        ),
    ),
    Column(
        "c_int4range",
        "int4range",
        (
            (None, None),
            ("empty", "empty"),
            ("[1,5)", "[1,5)"),
            ("(,)", "(,)"),
            ("[1,5]", "[1,6)"),
        ),
    ),
    Column(
        "c_numrange",
        "numrange",
        same(None, "empty", "[1.5,2.5)", "(,3]", "[0,)"),
    ),
    Column(
        "c_daterange",
        "daterange",
        same(None, "empty", "[2024-01-01,2024-02-29)", "(,)"),
    ),
    Column(
        "c_tsrange",
        "tsrange",
        (
            (None, None),
            ("empty", "empty"),
            (
                '["2024-01-01 00:00:00","2024-01-02 12:00:00.5")',
                '["2024-01-01 00:00:00","2024-01-02 12:00:00.5")',
            ),
        ),
    ),
    Column("c_int2vector", "int2vector", same(None, "1 2 3", "", "-32768 32767")),
    Column(
        "c_int8range",
        "int8range",
        same(None, "empty", "[-9223372036854775808,9223372036854775807)"),
    ),
    Column(
        "c_tstzrange",
        "tstzrange",
        (
            (None, None),
            ("empty", "empty"),
            (
                '["2024-01-01 00:00:00+00","2024-01-02 00:00:00+00")',
                '["2024-01-01 00:00:00+00","2024-01-02 00:00:00+00")',
            ),
        ),
    ),
    Column("c_char1", '"char"', same(None, "a", "Z")),
    Column("c_bool_array", "bool[]", same(None, "{}", "{t,f,NULL}")),
    Column(
        "c_uuid_array",
        "uuid[]",
        same(None, "{123e4567-e89b-12d3-a456-426614174000,NULL}"),
    ),
    Column("c_bytea_array", "bytea[]", same(None, '{"\\\\xdead","\\\\x"}')),
    Column("c_date_array", "date[]", same(None, "{2024-02-29,NULL,0001-01-01}")),
    Column(
        "c_jsonb_array",
        "jsonb[]",
        ((None, None), ('{"{\\"a\\": 1}","null",NULL}', '{"{\\"a\\":1}","null",NULL}')),
    ),
    Column(
        "c_float8_array", "float8[]", same(None, "{1.5,NaN,Infinity,-Infinity,NULL}")
    ),
    Column(
        "c_bit",
        "bit(8)",
        same(None, "10101010", "00000000"),
        cast="%s::bit(8)",
        text=True,
    ),
    Column("c_varbit", "varbit", same(None, "101", ""), cast="%s::varbit", text=True),
    Column("c_xml", "xml", same(None, '<a b="c">d</a>'), cast="%s::xml", text=True),
    Column("c_point", "point", same(None, "(1.5,-2)"), cast="%s::point", text=True),
    Column(
        "c_macaddr",
        "macaddr",
        same(None, "08:00:2b:01:02:03"),
        cast="%s::macaddr",
        text=True,
    ),
    Column(
        "c_cidr",
        "cidr",
        same(None, "10.0.0.0/8", "2001:db8::/32"),
        cast="%s::cidr",
        text=True,
    ),
    Column(
        "c_enum",
        POSTGRES_ENUM,
        same(None, *ENUM_VALUES),
        cast=f"%s::{POSTGRES_ENUM}",
        text=True,
    ),
    Column(
        "c_money",
        "money",
        ((None, None), ("0", "$0.00"), ("1234.56", "$1,234.56"), ("-0.01", "-$0.01")),
        cast="%s::money",
        text=True,
    ),
    Column(
        "c_inet",
        "inet",
        same(None, "192.168.1.1", "10.0.0.0/8", "::1", "2001:db8::/32"),
        cast="%s::inet",
        text=True,
    ),
    Column(
        "c_timetz",
        "timetz",
        same(None, "12:34:56+05:30", "00:00:00+00"),
        cast="%s::timetz",
        text=True,
    ),
    Column(
        "c_big_text",
        "text",
        **_big("length({0}) || ':' || md5({0})", lambda t: f"{len(t)}:{md5(t)}"),
    ),
    Column(
        "c_big_bytea",
        "bytea",
        cast="decode(%s, 'hex')",
        **_big(
            "length({0}) || ':' || md5(encode({0}, 'hex'))",
            lambda t: f"{len(t) // 2}:{md5(t)}",
        ),
    ),
)


def _hex(*values: str | None) -> tuple[Sample, ...]:
    """Bytes given in hex, which Materialize renders as `bytea`."""
    return tuple((v, v if v is None else "\\x" + v) for v in values)


_MYSQL_BIG = "length({0}) || ':' || md5({0})"

MYSQL: tuple[Column, ...] = (
    Column("c_tinyint", "tinyint", same(None, "0", "-128", "127")),
    Column("c_tinyint_u", "tinyint unsigned", same(None, "0", "255")),
    Column("c_smallint", "smallint", same(None, "0", "-32768", "32767")),
    Column("c_smallint_u", "smallint unsigned", same(None, "0", "65535")),
    Column("c_mediumint", "mediumint", same(None, "0", "8388607", "1")),
    Column("c_mediumint_u", "mediumint unsigned", same(None, "0", "16777215")),
    Column("c_int", "int", same(None, "0", "-2147483648", "2147483647")),
    Column("c_int_u", "int unsigned", same(None, "0", "4294967295")),
    Column(
        "c_bigint",
        "bigint",
        same(None, "0", "-9223372036854775808", "9223372036854775807"),
    ),
    Column("c_bigint_u", "bigint unsigned", same(None, "0", "18446744073709551615")),
    Column("c_boolean", "boolean", same(None, "1", "0")),
    Column(
        "c_bit",
        "bit(64)",
        same(None, "0", "1", "18446744073709551615", "170"),
        cast="CAST(%s AS UNSIGNED)",
    ),
    Column(
        "c_decimal",
        "decimal(38, 10)",
        (
            (None, None),
            ("0", "0"),
            ("-1.5", "-1.5"),
            (
                "1234567890123456789012345678.0123456789",
                "1234567890123456789012345678.0123456789",
            ),
            ("0.0000000001", "0.0000000001"),
            ("1.10", "1.1"),
        ),
    ),
    Column("c_numeric", "numeric(10, 0)", same(None, "0", "9999999999", "-9999999999")),
    Column(
        "c_float",
        "float",
        (
            (None, None),
            ("0", "0"),
            ("1.5", "1.5"),
            ("-3.40282e38", "-3.40282e+38"),
            ("1.17549e-38", "1.17549e-38"),
            ("0.1", "0.1"),
        ),
    ),
    Column(
        "c_double",
        "double",
        (
            (None, None),
            ("0", "0"),
            ("0.1", "0.1"),
            ("-123456.789", "-123456.789"),
            ("1.7976931348623157e308", "1.7976931348623157e+308"),
            ("2.2250738585072014e-308", "2.2250738585072014e-308"),
        ),
    ),
    Column("c_real", "real", same(None, "0", "2.5")),
    Column(
        "c_char",
        "char(5)",
        ((None, None), ("", ""), ("ab", "ab"), ("abcde", "abcde"), ("κ", "κ")),
    ),
    Column("c_varchar", "varchar(40)", same(None, "", "a", "κλειδί", "x" * 40)),
    Column("c_tinytext", "tinytext", same(None, "", "a", "κλειδί")),
    Column("c_text", "text", _TEXTS),
    Column("c_mediumtext", "mediumtext", same(None, "", "日本語 🎉")),
    Column(
        "c_binary", "binary(4)", _hex(None, "deadbeef", "61626364"), cast="UNHEX(%s)"
    ),
    Column(
        "c_varbinary",
        "varbinary(300)",
        _hex(None, "", "00", "deadbeef", bytes(range(256)).hex()),
        cast="UNHEX(%s)",
    ),
    Column("c_tinyblob", "tinyblob", _hex(None, "", "00ff"), cast="UNHEX(%s)"),
    Column(
        "c_blob",
        "blob",
        _hex(None, "", "deadbeef", bytes(range(256)).hex()),
        cast="UNHEX(%s)",
    ),
    Column("c_mediumblob", "mediumblob", _hex(None, "", "0102"), cast="UNHEX(%s)"),
    Column(
        "c_date",
        "date",
        same(None, "1970-01-01", "1000-01-01", "9999-12-31", "2024-02-29"),
    ),
    Column(
        "c_time",
        "time(6)",
        same(None, "00:00:00", "23:59:59.999999", "12:34:56.789", "01:02:03"),
    ),
    Column(
        "c_datetime",
        "datetime(6)",
        same(
            None,
            "1970-01-01 00:00:00",
            "1000-01-01 00:00:00",
            "9999-12-31 23:59:59.999999",
            "2024-02-29 12:34:56.123456",
        ),
    ),
    Column("c_datetime0", "datetime", same(None, "2024-02-29 12:34:56")),
    Column(
        "c_timestamp",
        "timestamp(6) NULL",
        (
            (None, None),
            ("1970-01-01 00:00:01", "1970-01-01 00:00:01"),
            ("2038-01-19 03:14:07.999999", "2038-01-19 03:14:07.999999"),
            ("2024-02-29 12:34:56.123456", "2024-02-29 12:34:56.123456"),
            ("2024-02-29 12:34:56+05:30", "2024-02-29 07:04:56"),
        ),
    ),
    Column(
        "c_json",
        "json",
        (
            (None, None),
            ("null", "null"),
            (
                '{"b": 1,  "a": [1, 2.5, "x", null, true]}',
                '{"a":[1,2.5,"x",null,true],"b":1}',
            ),
            ('"plain \\"quoted\\" \\u00e9"', '"plain \\"quoted\\" é"'),
            ("[]", "[]"),
            ('{"k": {"n": -1.5e3}}', '{"k":{"n":-1500}}'),
            ("12345678901234567890", "12345678901234567890"),
            ("1.0", "1"),
        ),
    ),
    Column("c_char_spaces", "char(5)", ((None, None), ("ab  ", "ab"), (" a", " a"))),
    Column("c_varchar_spaces", "varchar(10)", same(None, "a  ", "  ")),
    Column(
        "c_bit8", "bit(8)", same(None, "0", "170", "255"), cast="CAST(%s AS UNSIGNED)"
    ),
    Column("c_year0", "year", same(None, "0000"), text=True),
    Column(
        "c_enum",
        "enum('small', 'medium', 'has space')",
        same(None, *ENUM_VALUES),
        text=True,
    ),
    Column("c_year", "year", same(None, "1901", "2155", "2024"), text=True),
    Column(
        "c_big_text",
        "longtext",
        **_big(_MYSQL_BIG, lambda t: f"{len(t)}:{md5(t)}"),
    ),
    Column(
        "c_big_blob",
        "longblob",
        cast="UNHEX(%s)",
        **_big(
            "length({0}) || ':' || md5(encode({0}, 'hex'))",
            lambda t: f"{len(t) // 2}:{md5(t)}",
        ),
    ),
)


_DECIMALS = (
    (None, None),
    ("0", "0"),
    ("-1.5", "-1.5"),
    (
        "1234567890123456789012345678.0123456789",
        "1234567890123456789012345678.0123456789",
    ),
    ("0.0000000001", "0.0000000001"),
    ("1.10", "1.1"),
)
_VARBINARY = "CONVERT(varbinary(300), %s, 2)"

SQL_SERVER: tuple[Column, ...] = (
    Column("c_tinyint", "tinyint", same(None, "0", "255", "1")),
    Column("c_smallint", "smallint", same(None, "0", "-32768", "32767")),
    Column("c_int", "int", same(None, "0", "-2147483648", "2147483647")),
    Column(
        "c_bigint",
        "bigint",
        same(None, "0", "-9223372036854775808", "9223372036854775807"),
    ),
    Column(
        "c_real",
        "real",
        (
            (None, None),
            ("0", "0"),
            ("1.5", "1.5"),
            ("-3.40282e38", "-3.40282e+38"),
            ("0.1", "0.1"),
        ),
    ),
    Column(
        "c_float",
        "float",
        (
            (None, None),
            ("0", "0"),
            ("0.1", "0.1"),
            ("-123456.789", "-123456.789"),
            ("1.7976931348623157e308", "1.7976931348623157e+308"),
            ("2.2250738585072014e-308", "2.2250738585072014e-308"),
        ),
    ),
    Column("c_bit", "bit", ((None, None), ("1", "true"), ("0", "false"))),
    Column("c_decimal", "decimal(38, 10)", _DECIMALS),
    Column("c_numeric", "numeric(10, 0)", same(None, "0", "9999999999", "-9999999999")),
    Column(
        "c_money",
        "money",
        same(None, "0", "922337203685477.5807", "-922337203685477.5808", "0.99", "1"),
        cast="CAST(%s AS money)",
    ),
    Column(
        "c_smallmoney",
        "smallmoney",
        same(None, "0", "214748.3647", "-214748.3648", "0.0001"),
        cast="CAST(%s AS smallmoney)",
    ),
    Column(
        "c_char",
        "char(5)",
        ((None, None), ("", "     "), ("ab", "ab   "), ("abcde", "abcde")),
    ),
    Column(
        "c_nchar",
        "nchar(5)",
        ((None, None), ("", "     "), ("κλ", "κλ   "), ("abcde", "abcde")),
    ),
    Column("c_varchar", "varchar(40)", same(None, "", "a", "κλειδί", "x" * 40, "a  ")),
    Column("c_nvarchar", "nvarchar(40)", same(None, "", "a", "日本語 🎉", "x" * 40)),
    Column("c_varchar_max", "varchar(max)", _TEXTS),
    Column("c_nvarchar_max", "nvarchar(max)", _TEXTS),
    Column(
        "c_binary",
        "binary(4)",
        (
            (None, None),
            ("61", "\\x61000000"),
            ("deadbeef", "\\xdeadbeef"),
            ("00", "\\x00000000"),
        ),
        cast="CONVERT(binary(4), %s, 2)",
    ),
    Column(
        "c_varbinary",
        "varbinary(300)",
        _hex(None, "", "00", "deadbeef", bytes(range(256)).hex()),
        cast=_VARBINARY,
    ),
    Column(
        "c_date",
        "date",
        same(None, "1970-01-01", "0001-01-01", "9999-12-31", "2024-02-29"),
    ),
    Column(
        "c_time",
        "time",
        (
            (None, None),
            ("00:00:00", "00:00:00"),
            ("23:59:59.999999", "23:59:59.999999"),
            ("12:34:56.789", "12:34:56.789"),
            ("01:02:03", "01:02:03"),
            ("12:34:56.1234567", "12:34:56.123457"),
            ("23:59:59.9999999", "23:59:59.999999"),
        ),
    ),
    Column(
        "c_smalldatetime",
        "smalldatetime",
        same(None, "1900-01-01 00:00:00", "2079-06-06 23:59:00", "2024-02-29 12:34:00"),
    ),
    Column(
        "c_datetime",
        "datetime",
        same(
            None,
            "1753-01-01 00:00:00",
            "9999-12-31 23:59:59.997",
            "2024-02-29 12:34:56.123",
        ),
    ),
    Column(
        "c_datetime2",
        "datetime2",
        same(
            None,
            "1970-01-01 00:00:00",
            "0001-01-01 00:00:00",
            "9999-12-31 23:59:59.999999",
            "2024-02-29 12:34:56.123456",
        )
        + (("2024-02-29 12:34:56.1234567", "2024-02-29 12:34:56.123457"),),
    ),
    Column(
        "c_datetimeoffset",
        "datetimeoffset",
        (
            (None, None),
            ("1970-01-01 00:00:00 +00:00", "1970-01-01 00:00:00+00"),
            ("2024-02-29 12:34:56.123456 +00:00", "2024-02-29 12:34:56.123456+00"),
            ("9999-12-31 23:59:59.999999 +00:00", "9999-12-31 23:59:59.999999+00"),
            ("0001-01-01 00:00:00 +00:00", "0001-01-01 00:00:00+00"),
        ),
    ),
    Column(
        "c_uniqueidentifier",
        "uniqueidentifier",
        (
            (None, None),
            (
                "00000000-0000-0000-0000-000000000000",
                "00000000-0000-0000-0000-000000000000",
            ),
            (
                "FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF",
                "ffffffff-ffff-ffff-ffff-ffffffffffff",
            ),
            (
                "123E4567-E89B-12D3-A456-426614174000",
                "123e4567-e89b-12d3-a456-426614174000",
            ),
        ),
    ),
    Column(
        "c_time_text",
        "time",
        ((None, None), ("12:34:56.7890123", "12:34:56.789012300")),
        text=True,
    ),
    Column(
        "c_money_text",
        "money",
        same(None, "1234.5600"),
        cast="CAST(%s AS money)",
        text=True,
    ),
    Column("c_decimal_text", "decimal(20, 5)", same(None, "1.50000"), text=True),
    Column(
        "c_varbinary_text",
        "varbinary(16)",
        ((None, None), ("deadbeef", "3q2+7w==")),
        cast=_VARBINARY,
        text=True,
    ),
    Column(
        "c_big_varchar",
        "varchar(max)",
        **_big(_MYSQL_BIG, lambda t: f"{len(t)}:{md5(t)}"),
    ),
    Column(
        "c_big_nvarchar",
        "nvarchar(max)",
        **_big(_MYSQL_BIG, lambda t: f"{len(t)}:{md5(t)}"),
    ),
)
