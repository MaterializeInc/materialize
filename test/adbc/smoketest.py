# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Apache Arrow ADBC PostgreSQL driver tests.

The driver keys its Arrow type mapping off the *name* of each type's binary
receive function, which it reads from `pg_catalog.pg_type.typreceive`.
Materialize encodes a `regproc` over pgwire as a bare OID rather than as the
function name, so every lookup misses and the driver falls back to
`arrow.opaque` over binary. Connecting and streaming rows work. Per-column type
fidelity does not, so the tests that require it are marked `expectedFailure`
rather than deleted. They report an unexpected success once `regproc` renders as
a name.
"""

import unittest

# `adbc-driver-postgresql` and `duckdb` are pinned in `test/adbc/requirements.txt`
# and installed only inside this composition's test container, so they are absent
# from the repo-wide Python virtualenv that pyright resolves against.
import adbc_driver_postgresql.dbapi  # pyright: ignore[reportMissingImports]
import duckdb  # pyright: ignore[reportMissingImports]
import pyarrow as pa

MATERIALIZED_URL = "postgresql://materialize@materialized:6875/materialize"

# One non-null value per scalar type the driver has to resolve. Every value is
# non-null so that a broken binary decoder shows up as a wrong value rather
# than as a column of nulls.
SCALAR_QUERY = """
SELECT
    true::bool AS c_bool,
    1::int2 AS c_int2,
    2::int4 AS c_int4,
    3::int8 AS c_int8,
    1.5::float4 AS c_float4,
    2.5::float8 AS c_float8,
    123.45::numeric AS c_numeric,
    'abc'::text AS c_text,
    'def'::varchar AS c_varchar,
    '\\x0102'::bytea AS c_bytea,
    '2024-01-01'::date AS c_date,
    '12:34:56'::time AS c_time,
    '2024-01-01 12:34:56'::timestamp AS c_timestamp,
    '2024-01-01 12:34:56+00'::timestamptz AS c_timestamptz,
    '1 day'::interval AS c_interval,
    '00000000-0000-0000-0000-000000000001'::uuid AS c_uuid,
    '{"a": 1}'::jsonb AS c_jsonb
"""

# The Arrow type the driver must produce for each column of SCALAR_QUERY.
# `numeric` and `uuid` have no native Arrow type the driver maps them onto, so
# it wraps them in an `arrow.opaque` extension type over string or binary
# storage.
EXPECTED_SCALAR_TYPES = {
    "c_bool": pa.bool_(),
    "c_int2": pa.int16(),
    "c_int4": pa.int32(),
    "c_int8": pa.int64(),
    "c_float4": pa.float32(),
    "c_float8": pa.float64(),
    "c_numeric": pa.opaque(pa.string(), "numeric", "PostgreSQL"),
    "c_text": pa.string(),
    "c_varchar": pa.string(),
    "c_bytea": pa.binary(),
    "c_date": pa.date32(),
    "c_time": pa.time64("us"),
    "c_timestamp": pa.timestamp("us"),
    "c_timestamptz": pa.timestamp("us", tz="UTC"),
    "c_interval": pa.month_day_nano_interval(),
    "c_uuid": pa.opaque(pa.binary(), "uuid", "PostgreSQL"),
    "c_jsonb": pa.string(),
}


def query_arrow(sql: str) -> pa.Table:
    with adbc_driver_postgresql.dbapi.connect(MATERIALIZED_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetch_arrow_table()


class SmokeTest(unittest.TestCase):
    def test_connect(self) -> None:
        """Connecting runs the driver's type resolver, which reads the binary
        send and receive functions of every type out of `pg_catalog.pg_type`. A
        column missing there fails every connection before any user query, so
        connecting at all covers that catalog contract. This deliberately makes
        no claim about the values, to stay a test of the catalog rather than of
        Arrow type fidelity."""
        with adbc_driver_postgresql.dbapi.connect(MATERIALIZED_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                table = cur.fetch_arrow_table()
                self.assertEqual(table.num_rows, 1)
                self.assertEqual(table.num_columns, 1)

    # Materialize encodes `regproc` as an OID rather than a function name, so
    # the driver cannot key its type map and every column resolves to binary.
    @unittest.expectedFailure
    def test_scalar_types_resolve(self) -> None:
        """Each scalar type must arrive as its own Arrow type. A type the
        driver cannot resolve degrades to opaque binary instead of failing, so
        asserting the exact Arrow type is what catches it."""
        table = query_arrow(SCALAR_QUERY)
        self.assertEqual(table.num_rows, 1)
        self.assertEqual(
            sorted(table.schema.names), sorted(EXPECTED_SCALAR_TYPES.keys())
        )

        for name, expected in EXPECTED_SCALAR_TYPES.items():
            actual = table.schema.field(name).type
            self.assertEqual(
                actual,
                expected,
                f"column {name} resolved to Arrow type {actual}, expected {expected}",
            )

        # `bytea` is the only column that is legitimately Arrow binary. Any
        # other column landing there means the driver fell back to passing
        # bytes through untyped.
        for field in table.schema:
            if field.type == pa.binary():
                self.assertEqual(
                    field.name,
                    "c_bytea",
                    f"column {field.name} fell back to opaque binary",
                )

        row = table.to_pylist()[0]
        self.assertEqual(row["c_bool"], True)
        self.assertEqual(row["c_int4"], 2)
        self.assertEqual(row["c_text"], "abc")
        self.assertEqual(row["c_bytea"], b"\x01\x02")

    # Materialize encodes `regproc` as an OID rather than a function name, so
    # the driver cannot key its type map and the element type resolves to
    # binary.
    @unittest.expectedFailure
    def test_array_type(self) -> None:
        """An array must arrive as an Arrow list, not as an opaque blob."""
        table = query_arrow("SELECT ARRAY[1, 2, 3]::int4[] AS a")
        actual = table.schema.field("a").type
        self.assertEqual(
            actual,
            pa.list_(pa.int32()),
            f"column a resolved to Arrow type {actual}, expected a list of int32",
        )
        self.assertEqual(table.to_pylist(), [{"a": [1, 2, 3]}])

    # Materialize encodes `regproc` as an OID rather than a function name, so
    # the driver cannot key its type map and every column resolves to binary.
    @unittest.expectedFailure
    def test_null_handling(self) -> None:
        """Nulls round-trip while keeping the column's resolved Arrow type."""
        table = query_arrow("""
            SELECT
                NULL::int4 AS c_int4,
                NULL::text AS c_text,
                NULL::timestamptz AS c_timestamptz
            """)
        self.assertEqual(table.schema.field("c_int4").type, pa.int32())
        self.assertEqual(table.schema.field("c_text").type, pa.string())
        self.assertEqual(
            table.schema.field("c_timestamptz").type, pa.timestamp("us", tz="UTC")
        )
        self.assertEqual(
            table.to_pylist(),
            [{"c_int4": None, "c_text": None, "c_timestamptz": None}],
        )

    # Materialize encodes `regproc` as an OID rather than a function name, so
    # the driver cannot key its type map and DuckDB is handed blobs it
    # cannot aggregate.
    @unittest.expectedFailure
    def test_duckdb_handoff(self) -> None:
        """DuckDB reads the Arrow table Materialize produced directly, with no
        intermediate file or object store hop."""
        table = query_arrow("""
            SELECT n, n * 2 AS doubled, 'row' || n::text AS label
            FROM generate_series(1, 100) AS g(n)
            """)

        con = duckdb.connect()
        try:
            con.register("t", table)
            self.assertEqual(
                con.execute("SELECT count(*), sum(n), sum(doubled) FROM t").fetchall(),
                [(100, 5050, 10100)],
            )
            self.assertEqual(
                con.execute("SELECT sum(doubled) FROM t WHERE n > 50").fetchall(),
                [(7550,)],
            )
            self.assertEqual(
                con.execute("SELECT label FROM t WHERE n = 7").fetchall(),
                [("row7",)],
            )
        finally:
            con.close()

    def test_fetch_many_rows(self) -> None:
        """The driver fetches results as a binary COPY stream. This covers a
        result large enough to span multiple reads of that stream. Row and
        column counts hold regardless of how each column's type resolves, so
        this stays a hard assertion."""
        table = query_arrow("SELECT n FROM generate_series(1, 5000) AS g(n)")
        self.assertEqual(table.num_rows, 5000)
        self.assertEqual(table.num_columns, 1)
