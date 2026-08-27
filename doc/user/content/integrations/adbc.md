---
title: "ADBC (Arrow Database Connectivity)"
description: "Use the ADBC PostgreSQL driver to pull Materialize results into Apache Arrow"
menu:
  main:
    parent: "integrations"
    weight: 35
---

[ADBC (Arrow Database Connectivity)](https://arrow.apache.org/adbc/) is a
standard, columnar database API from Apache Arrow. Because Materialize is
**wire-compatible** with PostgreSQL, the community [ADBC PostgreSQL
driver](https://arrow.apache.org/adbc/current/driver/postgresql.html)
(`adbc_driver_postgresql`) connects to Materialize with no Materialize-specific
driver required.

The benefit over a standard PostgreSQL client is the result format. ADBC returns
query results as [Apache Arrow](https://arrow.apache.org/) tables, which you can
hand directly to Arrow-native tools such as [DuckDB](https://duckdb.org/),
[pandas](https://pandas.pydata.org/), and [Polars](https://pola.rs/) without an
intermediate object-store or file hop.

## Prerequisites

Install the driver manager, the PostgreSQL driver, and PyArrow. The examples on
this page also use DuckDB:

```bash
pip install adbc-driver-manager adbc-driver-postgresql pyarrow duckdb
```

{{< note >}}
Use a recent version of the ADBC PostgreSQL driver against a recent version of
Materialize. Correct Arrow type resolution depends on catalog fixes in
Materialize and on the driver's type mapping. The tested combination is
`adbc-driver-postgresql` 1.12.0.
{{< /note >}}

## Connect

Connect using `adbc_driver_postgresql.dbapi.connect` with a PostgreSQL
connection URI. Materialize requires SSL, so pass `sslmode=require`:

```python
import adbc_driver_postgresql.dbapi

uri = (
    "postgresql://MATERIALIZE_USERNAME:APP_SPECIFIC_PASSWORD"
    "@MATERIALIZE_HOST:6875/materialize?sslmode=require"
)

conn = adbc_driver_postgresql.dbapi.connect(uri)
```

For where to find your host name, database, and app password, see [SQL
clients](/integrations/sql-clients/).

## Query and fetch as Arrow

Execute a query with a cursor, then call `fetch_arrow_table` to get a
[`pyarrow.Table`](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html):

```python
with conn.cursor() as cur:
    cur.execute("SELECT n, n * 2 AS doubled FROM generate_series(1, 100) AS g(n)")
    table = cur.fetch_arrow_table()

print(table.schema)
print(table.num_rows)
```

## Hand off to DuckDB

Because the result is already an Arrow table, an Arrow-native engine can read it
in place. DuckDB queries the table directly, with no S3 or file hop:

```python
import duckdb

with conn.cursor() as cur:
    cur.execute("SELECT n, n * 2 AS doubled FROM generate_series(1, 100) AS g(n)")
    table = cur.fetch_arrow_table()

con = duckdb.connect()
con.register("t", table)
print(con.execute("SELECT sum(doubled) FROM t WHERE n > 50").fetchall())
```

The same pattern works for any Arrow consumer. For example,
`table.to_pandas()` produces a pandas DataFrame, and
`polars.from_arrow(table)` produces a Polars DataFrame.

## Type mapping

The driver maps Materialize (PostgreSQL) types to Arrow types as follows:

Materialize / PostgreSQL type | Arrow type
----------------------------- | ----------
`bool`                        | `bool`
`int2` (`smallint`)           | `int16`
`int4` (`integer`)            | `int32`
`int8` (`bigint`)             | `int64`
`float4` (`real`)             | `float32`
`float8` (`double precision`) | `float64`
`text`, `varchar`             | `string`
`bytea`                       | `binary`
`date`                        | `date32`
`time`                        | `time64` (microseconds)
`timestamp`                   | `timestamp` (microseconds)
`timestamptz`                 | `timestamp` (microseconds, UTC)
`interval`                    | `month_day_nano_interval`
`jsonb`                       | `json` (over `string`)
`numeric`                     | `opaque` extension (over `string`)
`uuid`                        | `opaque` extension (over `binary`)
arrays (for example `int4[]`) | `list` of the element type

{{< note >}}
`numeric` and `uuid` have no native Arrow type, so the driver wraps them in an
[Arrow `opaque` extension
type](https://arrow.apache.org/docs/format/CanonicalExtensions.html#opaque)
over `string` and `binary` storage respectively.
{{< /note >}}

The driver retrieves results using PostgreSQL's binary `COPY` protocol.

## Learn more

- [SQL clients](/integrations/sql-clients/) for connection parameters and app
  passwords.
- [Client libraries](/integrations/client-libraries/) for other language
  clients.
- [Apache Arrow ADBC documentation](https://arrow.apache.org/adbc/) for the ADBC
  API and driver details.
