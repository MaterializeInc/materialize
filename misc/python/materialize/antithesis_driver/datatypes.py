# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Value fidelity: every data type survives storage, maintenance and reads.

One table holds a column per data type. Every column of a row is a pure
function of the row's `seed` (`row_values`), NULLs and edge values included,
and an `UPDATE` changes the seed and every column in one statement. A reader
therefore knows what each column of a row must contain from the row alone,
whatever writes applied, so the check needs no model of the writes and stays
sound when their outcomes are unknown.

A check reads, in one `SELECT` and so at one timestamp, the table, an indexed
view and a materialized view over it, and two more materialized views: one
groups by each column, which uses every type as an arrangement key, and one
keeps the least and greatest value of each ordered column. It compares each
column's `::text` with the text its seed determines, the three row sets with
each other, and the groups and extremes with those the seeds determine.
"""

from __future__ import annotations

import datetime
import json
import threading
import time
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Any

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
    sometimes,
)

from materialize.antithesis_driver.client import Op, Outcome, Status
from materialize.antithesis_driver.context import RULE_WAIT_SECONDS, Context
from materialize.antithesis_driver.draw import Draw

SCHEMA = "datatypes"
CLUSTER = "driver"
MAX_ROWS = 48
SEEDS = 2**40
SETTINGS = {"cluster": CLUSTER, "transaction_isolation": "'strict serializable'"}
_MASK = 2**64 - 1


def mix(seed: int, salt: int) -> int:
    """64 well-mixed bits from `seed` and `salt` (SplitMix64's finalizer)."""
    z = (seed * 0x9E3779B97F4A7C15 + salt * 0xD1B54A32D192ED03 + 1) & _MASK
    z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & _MASK
    z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & _MASK
    return z ^ (z >> 31)


@dataclass(frozen=True)
class Value:
    input: str
    """Text the type's input function accepts, in a literal or in COPY."""
    text: str
    """What `::text` renders after the value was stored."""
    key: Any = None
    """Sorts as Materialize sorts the value, in an ordered column. None
    stands for `text`."""
    edge: bool = False

    @property
    def order(self) -> Any:
        return self.text if self.key is None else self.key


def _same(text: str) -> str:
    return text


@dataclass(frozen=True)
class Column:
    name: str
    sql: str
    edges: tuple[Value, ...]
    generate: Callable[[int], Value]
    ordered: bool = False
    """Whether `Value.key` sorts as Materialize sorts the column."""
    binary: bool = True
    """Whether the type has a binary wire format."""
    wire: Callable[[str], str] = _same
    """The text of a rendered value on the wire and inside an array or list,
    where it differs from `::text`."""
    rounds: bool = False
    """Whether an input can carry more precision than the column keeps. A
    cast rounds it. COPY FROM stores it unrounded as of v26.51.0, so COPY
    writes such a column's rendered text."""

    def pick(self, r: int) -> Value | None:
        """The value 61 random bits select: NULL, an edge or a generated one."""
        kind, r = r % 8, r >> 3
        if kind == 0:
            return None
        if kind <= 3:
            edge = self.edges[r % len(self.edges)]
            return Value(edge.input, edge.text, edge.key, True)
        return self.generate(r)


def _v(input: str, text: str | None = None, key: Any = None) -> Value:
    return Value(input, input if text is None else text, key)


def _integers(lo: int, hi: int) -> tuple[tuple[Value, ...], Callable[[int], Value]]:
    edges = tuple(_v(str(n), key=n) for n in sorted({lo, hi, 0, min(hi, 1), lo + 1}))

    def generate(r: int) -> Value:
        # All 64 bits, so that the widest types cover their whole range.
        n = lo + mix(r, 0) % (hi - lo + 1)
        return _v(str(n), key=n)

    return edges, generate


def _float_key(text: str) -> tuple[int, float]:
    # NaN sorts above every number, and -0 sorts with 0.
    value = float(text)
    return (1, 0.0) if value != value else (0, value)


def _float(input: str, text: str | None = None) -> Value:
    return _v(input, text, _float_key(input))


def _eighths(bits: int) -> Callable[[int], Value]:
    """Multiples of 1/8 whose shortest decimal form is their exact one."""

    def generate(r: int) -> Value:
        value = (r % 2**bits - 2 ** (bits - 1)) / 8
        text = str(int(value)) if value == int(value) else repr(value)
        return _float(text)

    return generate


def _plain(value: Decimal) -> str:
    """A numeric as stored: no exponent and no trailing fractional zeros."""
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text in ("-0", "") else text


def _numeric(input: str, text: str | None = None) -> Value:
    if input == "NaN":
        return _v(input, key=(1, Decimal(0)))
    value = Decimal(input)
    return _v(input, _plain(value) if text is None else text, (0, value))


def _numeric_generate(digits: int, scale: int) -> Callable[[int], Value]:
    def generate(r: int) -> Value:
        n = r % 10**digits - 10**digits // 2
        return _numeric(_plain(Decimal(n).scaleb(-((r >> 50) % (scale + 1)))))

    return generate


_ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789"


def _word(r: int, length: int) -> str:
    return "".join(_ALPHABET[(r >> (5 * i)) % 36] for i in range(length))


def _text_generate(max_length: int) -> Callable[[int], Value]:
    def generate(r: int) -> Value:
        return _v(_word(r >> 3, 1 + r % min(8, max_length)))

    return generate


def _bytes(data: bytes) -> Value:
    return _v("\\x" + data.hex(), key=data)


def _bytes_generate(r: int) -> Value:
    return _bytes((r >> 4).to_bytes(8, "big")[: 1 + r % 8])


def _fraction(micros: int) -> str:
    return f".{micros:06d}".rstrip("0") if micros else ""


def _date(ordinal: int) -> Value:
    date = datetime.date.fromordinal(ordinal)
    return _v(date.isoformat(), key=(date.year, date.month, date.day))


def _time(micros: int) -> Value:
    seconds, fraction = divmod(micros, 1_000_000)
    text = f"{seconds // 3600:02d}:{seconds // 60 % 60:02d}:{seconds % 60:02d}"
    return _v(text + _fraction(fraction), key=micros)


_UTC = datetime.UTC
_EPOCH = datetime.datetime(1, 1, 1, tzinfo=_UTC)
_MICROSECOND = datetime.timedelta(microseconds=1)
# Years 2 to 9998, so that a time zone offset stays within the type's range.
_INSTANTS = 9997 * 365 * 86_400_000_000
_OFFSETS = ((0, "+00"), (330, "+05:30"), (-480, "-08"), (840, "+14"))


def _stamp(at: datetime.datetime) -> str:
    # `strftime` does not pad the year.
    return f"{at.year:04d}-" + at.strftime("%m-%d %H:%M:%S") + _fraction(at.microsecond)


def _timestamp(text: str, input: str | None = None, zone: str = "") -> Value:
    """The timestamp `text` in UTC, written as `input` and rendered with `zone`."""
    at = datetime.datetime.fromisoformat(text).replace(tzinfo=_UTC)
    rendered = _stamp(at) + zone
    return _v(
        rendered if input is None else input, rendered, (at - _EPOCH) // _MICROSECOND
    )


def _timestamp_generate(step: int, zoned: bool) -> Callable[[int], Value]:
    def generate(r: int) -> Value:
        micros = 366 * 86_400_000_000 + (r >> 2) % _INSTANTS
        at = _EPOCH + (micros - micros % step) * _MICROSECOND
        if not zoned:
            return _timestamp(_stamp(at))
        minutes, suffix = _OFFSETS[r % 4]
        local = at + datetime.timedelta(minutes=minutes)
        return _timestamp(_stamp(at), _stamp(local) + suffix, "+00")

    return generate


def interval_text(months: int, days: int, micros: int) -> str:
    """An interval in the `postgres` output style."""
    parts: list[str] = []
    after_negative = False
    years = abs(months) // 12 * (-1 if months < 0 else 1)
    for value, unit in ((years, "year"), (months - years * 12, "mon"), (days, "day")):
        if value:
            sign = "+" if after_negative and value > 0 else ""
            parts.append(f"{sign}{value} {unit}{'' if value == 1 else 's'}")
            after_negative = value < 0
    if micros or not parts:
        seconds, fraction = divmod(abs(micros), 1_000_000)
        sign = "-" if micros < 0 else "+" if after_negative else ""
        parts.append(
            f"{sign}{seconds // 3600:02d}:{seconds // 60 % 60:02d}:{seconds % 60:02d}"
            + _fraction(fraction)
        )
    return " ".join(parts)


def _interval(months: int, days: int, micros: int, input: str | None = None) -> Value:
    text = interval_text(months, days, micros)
    # Materialize orders intervals by their fields, months first.
    return _v(text if input is None else input, text, (months, days, micros))


def _interval_generate(r: int) -> Value:
    return _interval(
        r % 2401 - 1200, (r >> 12) % 801 - 400, (r >> 24) % (2 * 10**11) - 10**11
    )


def _uuid_generate(r: int) -> Value:
    h = f"{mix(r, 1):016x}{mix(r, 2):016x}"
    return _v(f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}")


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def _jsonb_generate(r: int) -> Value:
    quarters = (r >> 30) % 100
    return _v(
        _json(
            {
                "n": r % 1000 - 500,
                "s": _word(r >> 10, 1 + r % 5),
                "a": [r % 7, None, r % 2 == 0, _word(r >> 20, 2)],
                # jsonb renders a whole number without a fraction.
                "o": {"x": quarters / 4 if quarters % 4 else quarters // 4},
            }
        )
    )


def quote(element: str, special: str = '{},"\\ \t\n\x0c\r') -> str:
    """An element of an array, list or range, quoted where Materialize quotes
    it (`strconv::ListElementEscaper`)."""
    if element and element.upper() != "NULL" and not any(c in special for c in element):
        return element
    return '"' + element.replace("\\", "\\\\").replace('"', '\\"') + '"'


def quote_in_map(element: str) -> str:
    """As `quote`, for a map's keys and values (`strconv::MapElementEscaper`)."""
    return quote(element, '{},"\\ \t\n\x0c\r=>')


def _collection(
    name: str, sql: str, of: Column, edges: Sequence[str] = (), nested: bool = False
) -> Column:
    """An array or list column whose elements are `of`'s values."""

    def braced(elements: Sequence[Value | None]) -> Value:
        def side(render: Callable[[Value], str]) -> str:
            # A list inside a list is not quoted.
            return (
                "{"
                + ",".join(
                    "NULL" if e is None else render(e) if nested else quote(render(e))
                    for e in elements
                )
                + "}"
            )

        return _v(side(lambda e: e.input), side(lambda e: of.wire(e.text)))

    def generate(r: int) -> Value:
        return braced([of.pick(mix(r, i)) for i in range(r % 4)])

    fixed = (_v("{}"), braced(of.edges), braced([None, of.edges[0]]))
    return Column(
        name,
        sql,
        fixed + tuple(_v(e) for e in edges),
        generate,
        binary=not sql.endswith("list"),
    )


def _map(name: str, of: Column) -> Column:
    def entries(r: int, count: int) -> Value:
        keys = sorted({_word(mix(r, 16 + i), 1 + i) for i in range(count)})
        values = [of.pick(mix(r, i)) for i in range(len(keys))]

        def side(render: Callable[[Value], str]) -> str:
            return (
                "{"
                + ",".join(
                    f"{k}=>{'NULL' if v is None else render(v)}"
                    for k, v in zip(keys, values)
                )
                + "}"
            )

        return _v(
            side(lambda v: quote_in_map(v.input)), side(lambda v: quote_in_map(v.text))
        )

    return Column(
        name,
        f"map[text=>{of.sql}]",
        (_v("{}"), entries(1, 3), _v('{"b c"=>NULL,a=>NULL}', '{a=>NULL,"b c"=>NULL}')),
        lambda r: entries(r, r % 4),
        False,
        False,
    )


def _range(name: str, sql: str, of: Column, discrete: bool) -> Column:
    """A range column over `of`, an ordered column."""

    def between(a: Value, b: Value, r: int) -> Value:
        lo, hi = sorted((a, b), key=lambda v: v.order)
        if lo.order == hi.order:
            return _v("empty")
        # A discrete range renders in its canonical `[)` form, so only that
        # form is written.
        left, right = ("[", ")") if discrete else ("[("[r % 2], ")]"[(r >> 1) % 2])
        return _v(
            f"{left}{lo.input},{hi.input}{right}",
            f"{left}{quote(lo.text)},{quote(hi.text)}{right}",
        )

    def generate(r: int) -> Value:
        return between(of.generate(mix(r, 1)), of.generate(mix(r, 2)), r)

    lo, hi = sorted((of.generate(1), of.generate(2)), key=lambda v: v.order)
    edges = (
        _v("empty"),
        _v("(,)"),
        _v(f"[{lo.input},)", f"[{quote(lo.text)},)"),
        _v(f"(,{hi.input})", f"(,{quote(hi.text)})"),
        between(lo, hi, 0),
    )
    return Column(name, sql, edges, generate)


def _columns() -> tuple[Column, ...]:
    def integers(name: str, sql: str, lo: int, hi: int) -> Column:
        edges, generate = _integers(lo, hi)
        return Column(name, sql, edges, generate, True)

    boolean = Column(
        "c_bool",
        "bool",
        (_v("true", key=1), _v("false", key=0)),
        lambda r: _v("true", key=1) if r % 2 else _v("false", key=0),
        True,
        wire=lambda text: text[0],
    )
    int4 = integers("c_int4", "int4", -(2**31), 2**31 - 1)
    float8 = Column(
        "c_float8",
        "float8",
        (
            _float("NaN"),
            _float("Infinity"),
            _float("-Infinity"),
            _float("-0"),
            _float("0"),
            _float("0.1"),
            _float("1.7976931348623157e308", "1.7976931348623157e+308"),
            _float("5e-324"),
            _float("1e21", "1e+21"),
            _float("1e-7"),
            _float("9007199254740993", "9007199254740992"),
        ),
        _eighths(32),
        True,
    )
    numeric = Column(
        "c_numeric",
        "numeric",
        (
            _numeric("NaN"),
            _numeric("0"),
            _numeric("-0", "0"),
            _numeric("1.50"),
            _numeric("99999999999999999999999999999999999999"),
            _numeric("-99999999999999999999999999999999999999"),
            _numeric("0.000000000000000000000000000000000000001"),
            _numeric("123456789.123456789"),
        ),
        _numeric_generate(18, 9),
        True,
    )
    text = Column(
        "c_text",
        "text",
        tuple(
            _v(s)
            for s in (
                "",
                " ",
                "ünïcødé ☃ 日本語 🎉",
                'it\'s "quoted"',
                "line1\nline2\ttab\r\n",
                "back\\slash \\N \\x41",
                " leading and trailing ",
                "NULL",
                "a,b;c|d",
                "{1,2}",
                "x" * 2000,
            )
        ),
        _text_generate(8),
        True,
    )
    bytea = Column(
        "c_bytea",
        "bytea",
        (
            _bytes(b""),
            _bytes(b"\x00"),
            _bytes(b"\xff\x00\xff"),
            _bytes(bytes(range(256))),
            _bytes(b"\\x41 text"),
        ),
        _bytes_generate,
        True,
    )
    date = Column(
        "c_date",
        "date",
        (
            _date(1),
            _date(datetime.date(9999, 12, 31).toordinal()),
            _date(datetime.date(2024, 2, 29).toordinal()),
            _date(datetime.date(1970, 1, 1).toordinal()),
            _v("262142-12-31", key=(262142, 12, 31)),
            _v("4714-11-24 BC", key=(-4713, 11, 24)),
        ),
        lambda r: _date(1 + r % datetime.date.max.toordinal()),
        True,
    )
    time_ = Column(
        "c_time",
        "time",
        (_time(0), _time(86_399_999_999), _time(45_296_500_000), _time(1)),
        lambda r: _time(r % 86_400_000_000),
        True,
    )
    timestamp = Column(
        "c_timestamp",
        "timestamp",
        (
            _timestamp("0001-01-01 00:00:00"),
            _timestamp("9999-12-31 23:59:59.999999"),
            _timestamp("2024-02-29 12:00:00.5"),
            _timestamp("2024-02-29 12:00:00.123457", "2024-02-29 12:00:00.123456789"),
        ),
        _timestamp_generate(1, False),
        True,
    )
    timestamptz = Column(
        "c_timestamptz",
        "timestamptz",
        (
            _timestamp("0001-01-01 00:00:00", zone="+00"),
            _timestamp("9999-12-31 23:59:59.999999", zone="+00"),
            _timestamp("2024-02-29 06:30:00", "2024-02-29 12:00:00+05:30", "+00"),
            _timestamp("2024-03-01 07:30:00", "2024-02-29 23:30:00-08", "+00"),
        ),
        _timestamp_generate(1, True),
        True,
    )
    interval = Column(
        "c_interval",
        "interval",
        (
            _interval(0, 0, 0, "0"),
            _interval(14, 3, 14_706_789_000, "1 year 2 months 3 days 04:05:06.789"),
            _interval(0, -1, 0, "-1 day"),
            _interval(-14, 3, -14_706_000_000),
            _interval(178_000_000 * 12, 0, 0),
            _interval(0, 0, 90_000_000_000, "25:00:00"),
            _interval(1, 0, 0),
            _interval(0, 30, 0),
            _interval(0, 0, 1),
        ),
        _interval_generate,
        True,
    )
    uuid = Column(
        "c_uuid",
        "uuid",
        (
            _v("00000000-0000-0000-0000-000000000000"),
            _v("ffffffff-ffff-ffff-ffff-ffffffffffff"),
            _v(
                "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11",
                "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
            ),
        ),
        _uuid_generate,
        True,
    )
    jsonb = Column(
        "c_jsonb",
        "jsonb",
        (
            _v("null"),
            _v("true"),
            _v('""'),
            _v("[]"),
            _v("{}"),
            _v("-0.5"),
            _v("1e3", "1000"),
            _v("123456789012345678901234567890"),
            _v(
                '{"b":1,"a":[1,2,null,true,"x"],"c":{"d":1.50}}',
                '{"a":[1,2,null,true,"x"],"b":1,"c":{"d":1.5}}',
            ),
            _v(_json('é\n"q" \\ 日本')),
        ),
        _jsonb_generate,
    )
    int8 = integers("c_int8", "int8", -(2**63), 2**63 - 1)
    return (
        boolean,
        integers("c_int2", "int2", -(2**15), 2**15 - 1),
        int4,
        int8,
        integers("c_uint2", "uint2", 0, 2**16 - 1),
        integers("c_uint4", "uint4", 0, 2**32 - 1),
        integers("c_uint8", "uint8", 0, 2**64 - 1),
        integers("c_oid", "oid", 0, 2**32 - 1),
        Column(
            "c_mz_timestamp",
            "mz_timestamp",
            _integers(0, 2**64 - 1)[0],
            lambda r: _v(
                str(1_700_000_000_000 + r % 10**11), key=1_700_000_000_000 + r % 10**11
            ),
            True,
        ),
        Column(
            "c_float4",
            "float4",
            (
                _float("NaN"),
                _float("Infinity"),
                _float("-Infinity"),
                _float("-0"),
                _float("0"),
                _float("16777216"),
                _float("3.4028235e38", "3.4028235e+38"),
                _float("1e-45"),
            ),
            _eighths(21),
            True,
        ),
        float8,
        numeric,
        Column(
            "c_numeric_scaled",
            "numeric(38, 10)",
            (
                _numeric("0"),
                _numeric("1.5"),
                _numeric("9999999999999999999999999999.9999999999"),
                _numeric("-1234567890123456789012345678.0123456789"),
                _numeric("0.00000000014", "0.0000000001"),
                _numeric("NaN"),
            ),
            _numeric_generate(20, 10),
            True,
        ),
        text,
        Column(
            "c_varchar",
            "varchar(8)",
            (_v(""), _v("abcdefgh"), _v("é☃ 日本"), _v("a'b\"c")),
            _text_generate(8),
            True,
        ),
        Column(
            "c_char",
            "char(6)",
            (_v(""), _v("ab"), _v("abcdef"), _v("a b"), _v("é☃")),
            _text_generate(6),
            True,
            # `::text` drops the padding, the wire keeps it.
            wire=lambda text: text.ljust(6),
        ),
        Column(
            "c_pg_char",
            '"char"',
            (_v("a"), _v("~"), _v("0"), _v("'")),
            lambda r: _v(chr(33 + r % 94)),
            True,
        ),
        bytea,
        date,
        time_,
        timestamp,
        Column(
            "c_timestamp_ms",
            "timestamp(3)",
            (
                _timestamp("0001-01-01 00:00:00"),
                _timestamp("2024-02-29 12:00:00.123", "2024-02-29 12:00:00.123456"),
            ),
            _timestamp_generate(1000, False),
            True,
            rounds=True,
        ),
        timestamptz,
        interval,
        uuid,
        jsonb,
        _collection("c_int4_array", "int4[]", int4, ("{{1,2},{3,4}}",)),
        _collection("c_text_array", "text[]", text),
        _collection("c_bool_array", "bool[]", boolean),
        # Without -0, whose grouping `expected_groups` leaves open only for a
        # scalar.
        _collection(
            "c_float8_array",
            "float8[]",
            replace(float8, edges=tuple(e for e in float8.edges if e.input != "-0")),
        ),
        _collection("c_numeric_array", "numeric[]", numeric),
        _collection("c_bytea_array", "bytea[]", bytea),
        _collection("c_timestamptz_array", "timestamptz[]", timestamptz),
        _collection("c_uuid_array", "uuid[]", uuid),
        _collection("c_jsonb_array", "jsonb[]", jsonb),
        _collection("c_int4_list", "int4 list", int4),
        _collection("c_text_list", "text list", text),
        _collection("c_date_list", "date list", date),
        _collection("c_interval_list", "interval list", interval),
        _collection("c_time_list", "time list", time_),
        _collection(
            "c_int4_list_list",
            "int4 list list",
            _collection("", "int4 list", int4),
            nested=True,
        ),
        _map("c_int4_map", int4),
        _map("c_text_map", text),
        _range("c_int4range", "int4range", int4, True),
        _range("c_int8range", "int8range", int8, True),
        _range("c_numrange", "numrange", numeric, False),
        _range("c_daterange", "daterange", date, True),
        _range("c_tsrange", "tsrange", timestamp, False),
        _range("c_tstzrange", "tstzrange", timestamptz, False),
    )


COLUMNS = _columns()
ORDERED = tuple(i for i, c in enumerate(COLUMNS) if c.ordered)


def row_values(seed: int) -> tuple[Value | None, ...]:
    """What each column of a row with this seed holds. None is SQL NULL."""
    return tuple(c.pick(mix(seed, i)) for i, c in enumerate(COLUMNS))


def literal(column: Column, value: Value | None) -> str:
    if value is None:
        return f"NULL::{column.sql}"
    return "'" + value.input.replace("'", "''") + f"'::{column.sql}"


def column_ddl() -> str:
    return ", ".join(f"{c.name} {c.sql}" for c in COLUMNS)


def setup_statements() -> list[str]:
    grouped = " UNION ALL ".join(
        f"SELECT {i}::bigint AS col, min(seed) AS lo, max(seed) AS hi, count(*) AS n"
        f" FROM {SCHEMA}.t GROUP BY {c.name}"
        for i, c in enumerate(COLUMNS)
    )
    extremes = " UNION ALL ".join(
        f"SELECT {i}::bigint AS col, {direction} AS dir, seed FROM (SELECT seed FROM"
        f" {SCHEMA}.t WHERE {COLUMNS[i].name} IS NOT NULL"
        f" ORDER BY {COLUMNS[i].name} {order}, seed LIMIT 1)"
        for i in ORDERED
        for direction, order in ((0, "ASC"), (1, "DESC"))
    )
    return [
        f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}",
        f"CREATE TABLE IF NOT EXISTS {SCHEMA}.t (id bigint, seed bigint, {column_ddl()})",
        f"CREATE VIEW IF NOT EXISTS {SCHEMA}.v AS SELECT * FROM {SCHEMA}.t",
        f"CREATE INDEX IF NOT EXISTS v_idx IN CLUSTER {CLUSTER} ON {SCHEMA}.v (id)",
        f"CREATE MATERIALIZED VIEW IF NOT EXISTS {SCHEMA}.mv IN CLUSTER {CLUSTER}"
        f" AS SELECT * FROM {SCHEMA}.t",
        f"CREATE MATERIALIZED VIEW IF NOT EXISTS {SCHEMA}.grouped IN CLUSTER {CLUSTER}"
        f" AS {grouped}",
        f"CREATE MATERIALIZED VIEW IF NOT EXISTS {SCHEMA}.extremes IN CLUSTER {CLUSTER}"
        f" AS {extremes}",
    ]


READ_PATHS = ("t", "v", "mv")


def read_query() -> str:
    texts = ", ".join(f"{c.name}::text" for c in COLUMNS)
    padding = ", NULL::text" * (len(COLUMNS) - 2)
    paths = [
        f"SELECT '{path}' AS tag, id, seed, {texts} FROM {SCHEMA}.{path}"
        for path in READ_PATHS
    ]
    return " UNION ALL ".join(
        paths
        + [
            f"SELECT 'grouped', col, lo, hi::text, n::text{padding} FROM {SCHEMA}.grouped",
            f"SELECT 'extremes', col, seed, dir::text, NULL::text{padding}"
            f" FROM {SCHEMA}.extremes",
        ]
    )


def expected_groups(
    seeds: Sequence[int], zeros_equal: bool = False
) -> Counter[tuple[int, int, int, int]]:
    """The rows of `grouped`: per column and distinct value, the least and
    greatest seed and the number of rows.

    Two values are distinct when their text is, as every type renders one
    value one way. With `zeros_equal`, the floats -0 and 0 share a group.
    """
    classes: dict[tuple[int, str | None], list[int]] = {}
    for seed in seeds:
        for i, value in enumerate(row_values(seed)):
            cls = None if value is None else value.text
            if zeros_equal and cls == "-0":
                cls = "0"
            classes.setdefault((i, cls), []).append(seed)
    return Counter(
        (i, min(members), max(members), len(members))
        for (i, _), members in classes.items()
    )


def expected_extremes(seeds: Sequence[int]) -> Counter[tuple[int, int, int]]:
    """The rows of `extremes`: per ordered column, the least seed among the
    rows that hold its least value, and among those that hold its greatest."""
    result: Counter[tuple[int, int, int]] = Counter()
    rows = [(seed, row_values(seed)) for seed in seeds]
    for i in ORDERED:
        keyed = [(v.order, seed) for seed, values in rows if (v := values[i])]
        if keyed:
            least, greatest = min(k for k, _ in keyed), max(k for k, _ in keyed)
            result[(i, 0, min(s for k, s in keyed if k == least))] += 1
            result[(i, 1, min(s for k, s in keyed if k == greatest))] += 1
    return result


@dataclass
class Verdict:
    paths_differ: list[Any]
    wrong_values: list[Any]
    wrong_groups: list[Any]
    wrong_extremes: list[Any]
    rows: dict[int, int]
    """Each row of the table: its id and seed."""
    kinds: set[tuple[int, str]]
    """Which columns the table's rows covered as `null`, `edge` and `generated`."""


def judge(rows: Sequence[Sequence[Any]]) -> Verdict:
    """Compare one read of `read_query` with what the rows' seeds determine."""
    paths: dict[str, Counter[tuple[Any, ...]]] = {p: Counter() for p in READ_PATHS}
    grouped: Counter[tuple[int, int, int, int]] = Counter()
    extremes: Counter[tuple[int, int, int]] = Counter()
    for tag, a, b, *texts in rows:
        if tag == "grouped":
            grouped[(a, b, int(texts[0]), int(texts[1]))] += 1
        elif tag == "extremes":
            extremes[(a, int(texts[0]), b)] += 1
        else:
            paths[tag][(a, b, *texts)] += 1
    table = paths["t"]
    paths_differ = [
        {"path": p, "missing": list(table - bag)[:2], "extra": list(bag - table)[:2]}
        for p, bag in paths.items()
        if bag != table
    ]
    wrong_values = []
    kinds: set[tuple[int, str]] = set()
    for id, seed, *texts in table:
        for i, (value, text) in enumerate(zip(row_values(seed), texts)):
            kinds.add(
                (i, "null" if value is None else "edge" if value.edge else "generated")
            )
            if (None if value is None else value.text) != text:
                wrong_values.append(
                    {
                        "id": id,
                        "seed": seed,
                        "column": COLUMNS[i].name,
                        "expected": None if value is None else value.text[:200],
                        "observed": None if text is None else text[:200],
                    }
                )
    seeds = [seed for _, seed, *_ in table.elements()]
    want_groups, want_extremes = expected_groups(seeds), expected_extremes(seeds)
    # Materialize orders -0 with 0 and yet groups them apart. The check takes
    # no side on the grouping.
    if grouped == expected_groups(seeds, zeros_equal=True):
        want_groups = grouped

    def named(bag: Counter[Any]) -> list[Any]:
        return [(COLUMNS[row[0]].name, *row[1:]) for row in list(bag)[:5]]

    return Verdict(
        paths_differ,
        wrong_values[:10],
        (
            [
                {
                    "missing": named(want_groups - grouped),
                    "extra": named(grouped - want_groups),
                }
            ]
            if grouped != want_groups
            else []
        ),
        (
            [
                {
                    "missing": named(want_extremes - extremes),
                    "extra": named(extremes - want_extremes),
                }
            ]
            if extremes != want_extremes
            else []
        ),
        {id: seed for id, seed, *_ in table},
        kinds,
    )


class DataTypes:
    def __init__(self, ctx: Context) -> None:
        self.ctx = ctx
        self._lock = threading.Lock()
        self._next_id = 1
        self.rows: dict[int, int] = {}
        """Each row's id and seed at the last check."""
        self.kinds: set[tuple[int, str]] = set()
        """What the checks so far covered, as `Verdict.kinds`."""
        self._unchecked = 0
        """Inserts sent since the last check. They bound the table while no
        check succeeds, as when the cluster of the views has no replica."""
        self.query = read_query()

    def _session(self, d: Draw) -> int:
        return d.integer(0, len(self.ctx.sessions.sessions) - 1)

    def write(self, d: Draw) -> Status:
        with self._lock:
            ids = sorted(self.rows)
            full = len(ids) + self._unchecked >= MAX_ROWS
        kind = d.choice(("insert", "insert", "update", "update", "delete"))
        if full and not ids:
            return Status.NOT_SENT
        if full:
            kind = "trim"
        elif not ids:
            kind = "insert"
        seed = d.integer(0, SEEDS - 1)
        values = row_values(seed)
        literals = [literal(c, v) for c, v in zip(COLUMNS, values)]
        if kind == "insert":
            with self._lock:
                id = self._next_id
                self._next_id += 1
                self._unchecked += 1
            statement = (
                f"INSERT INTO {SCHEMA}.t VALUES ({id}, {seed}, {', '.join(literals)})"
            )
        elif kind == "update":
            assignments = ", ".join(
                f"{c.name} = {lit}" for c, lit in zip(COLUMNS, literals)
            )
            statement = (
                f"UPDATE {SCHEMA}.t SET seed = {seed}, {assignments}"
                f" WHERE id = {d.choice(ids)}"
            )
        elif kind == "delete":
            statement = f"DELETE FROM {SCHEMA}.t WHERE id = {d.choice(ids)}"
        else:
            statement = f"DELETE FROM {SCHEMA}.t WHERE id <= {ids[len(ids) // 4]}"

        def execute(conn: psycopg.Connection) -> None:
            conn.execute(statement.encode())

        def complete(outcome: Outcome[None]) -> None:
            pass

        return self.ctx.sessions.call(
            self._session(d),
            Op(f"data types {kind}", execute, complete, SETTINGS),
            RULE_WAIT_SECONDS,
        )

    def _read_op(self, compared: list[bool]) -> Op:
        def execute(conn: psycopg.Connection) -> list[tuple[Any, ...]]:
            return conn.execute(self.query.encode()).fetchall()

        def complete(outcome: Outcome[list[tuple[Any, ...]]]) -> None:
            if not outcome.ok:
                return
            assert outcome.value is not None
            self.compare(outcome.value)
            compared.append(True)

        return Op("data types check", execute, complete, SETTINGS)

    def check(self, d: Draw) -> Status:
        return self.ctx.sessions.call(
            self._session(d), self._read_op([]), RULE_WAIT_SECONDS
        )

    def compare(self, rows: list[tuple[Any, ...]]) -> None:
        verdict = judge(rows)
        details = {"rows": len(verdict.rows)}
        always(
            not verdict.wrong_values,
            "data types: every column of a visible row renders the value its seed determines",
            {**details, "wrong": verdict.wrong_values},
        )
        always(
            not verdict.paths_differ,
            "data types: the indexed view and the materialized view return the table's rows",
            {**details, "differ": verdict.paths_differ},
        )
        always(
            not verdict.wrong_groups,
            "data types: grouping by a column yields the groups its values determine",
            {**details, "groups": verdict.wrong_groups},
        )
        always(
            not verdict.wrong_extremes,
            "data types: the least and greatest value of a column are those its values determine",
            {**details, "extremes": verdict.wrong_extremes},
        )
        with self._lock:
            reseeded = any(
                id in self.rows and self.rows[id] != seed
                for id, seed in verdict.rows.items()
            )
            self.rows = verdict.rows
            self._unchecked = 0
            self.kinds |= verdict.kinds
            covered = len(self.kinds)
        sometimes(
            reseeded,
            "data types: a check saw a row whose seed and columns an update replaced",
            details,
        )
        sometimes(
            covered == 3 * len(COLUMNS),
            "data types: the checks covered every column as NULL, as an edge value and as a generated value",
            {**details, "covered": covered, "of": 3 * len(COLUMNS)},
        )

    def eventually(self, deadline: float) -> None:
        compared: list[bool] = []
        while not compared and time.monotonic() < deadline:
            for i in range(len(self.ctx.sessions.sessions)):
                if (
                    self.ctx.sessions.call(i, self._read_op(compared), 60.0)
                    != Status.NOT_SENT
                ):
                    break
            if not compared:
                time.sleep(1)
        always(
            bool(compared),
            "data types: a read of every data type succeeds after faults stop",
            {},
        )
