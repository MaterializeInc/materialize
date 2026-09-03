---
title: "ADBC (Arrow Database Connectivity)"
description: "Use the ADBC PostgreSQL driver to pull Materialize results into Apache Arrow"
menu:
  main:
    parent: "integrations"
    weight: 35
---

{{< warn-if-unreleased "v26.40" >}}

[ADBC (Arrow Database Connectivity)](https://arrow.apache.org/adbc/) is a
standard, columnar database API from Apache Arrow. Because Materialize is
**wire-compatible** with PostgreSQL, the community [ADBC PostgreSQL
driver](https://arrow.apache.org/adbc/current/driver/postgresql.html)
(`adbc_driver_postgresql`) can connect directly to Materialize; i.e., no
Materialize-specific driver is required.

The benefit over a standard PostgreSQL client is the result format. ADBC returns
query results as [Apache Arrow](https://arrow.apache.org/) tables, which you can
hand directly to Arrow-native tools such as [DuckDB](https://duckdb.org/),
[pandas](https://pandas.pydata.org/), and [Polars](https://pola.rs/) without an
intermediate object-store or file hop.

## Prerequisites

Requires **Materialize v26.40 or later**. When it opens a connection, the driver
reads the binary send and receive functions of every type from
`pg_catalog.pg_type`. Earlier versions of Materialize do not expose
`pg_type.typsend`, so `connect()` fails before it can run a query.

Install the driver manager, the PostgreSQL driver, and PyArrow. The examples on
this page also use DuckDB:

```bash
pip install adbc-driver-manager adbc-driver-postgresql pyarrow duckdb
```

The examples were tested with `adbc-driver-manager` and
`adbc-driver-postgresql` 1.12.0, `pyarrow` 25.0.1, and `duckdb` 1.5.5.

## Connect

Connect using `adbc_driver_postgresql.dbapi.connect` with a PostgreSQL
connection URI. Use the connection as a context manager so that it closes when
the block exits:

```python
import adbc_driver_postgresql.dbapi
from urllib.parse import quote_plus

username = quote_plus("MATERIALIZE_USERNAME")
password = quote_plus("APP_SPECIFIC_PASSWORD")

uri = (
    f"postgresql://{username}:{password}"
    "@MATERIALIZE_HOST:6875/materialize?sslmode=require"
)

with adbc_driver_postgresql.dbapi.connect(uri) as conn:
    # Run queries here.
    ...
```

{{< tip >}}
Percent-encode the username and password before putting them in the URI, as in
the example above. Materialize usernames are email addresses, so they contain an
`@`, which delimits the host in a URI. App passwords can likewise contain
characters that a URI reserves.
{{< /tip >}}

{{< note >}}
`sslmode=require` applies to Materialize Cloud, which accepts only TLS
connections. A self-managed deployment accepts TLS only if you have configured
it to serve TLS, so set `sslmode` to match your deployment.
{{< /note >}}

For where to find your host name, database, and app password, see [SQL
clients](/integrations/sql-clients/).

## Query and fetch as Arrow

Execute a query with a cursor, then call `fetch_arrow_table` to get a
[`pyarrow.Table`](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html):

```python
with adbc_driver_postgresql.dbapi.connect(uri) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT n, n * 2 AS doubled FROM generate_series(1, 100) AS g(n)")
        table = cur.fetch_arrow_table()

print(table.schema)
print(table.num_rows)
```

```nofmt
n: int32
doubled: int32
100
```

The Arrow table holds the full result in memory, so it stays usable after the
connection closes.

## Hand off to DuckDB

Because the result is already an Arrow table, an Arrow-native engine can read it
in place. DuckDB queries the `table` from the previous section directly, with no
S3 or file hop:

```python
import duckdb

con = duckdb.connect()
con.register("t", table)
print(con.execute("SELECT sum(doubled) FROM t WHERE n > 50").fetchall())
```

```nofmt
[(7550,)]
```

The same pattern works for any Arrow consumer. For example,
`table.to_pandas()` produces a pandas DataFrame, and
`polars.from_arrow(table)` produces a Polars DataFrame.

## Type mapping

The driver maps the types Materialize shares with PostgreSQL to Arrow types as
follows:

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

The driver picks the Arrow type for a column from the name of that type's
PostgreSQL binary receive function. Materialize-specific types
([`list`](/sql/types/list/), [`map`](/sql/types/map/),
[`uint2`/`uint4`/`uint8`](/sql/types/uint/), `mz_timestamp`, and `mz_aclitem`)
have no PostgreSQL receive function. Rather than erroring, the driver falls back
to an `opaque` extension type over binary for them, which holds the raw binary
`COPY` encoding of the value. To get a usable Arrow value for one of these
types, cast it in the query, for example `my_list::text`.

The driver retrieves results using PostgreSQL's binary `COPY` protocol.

## Learn more

- [SQL clients](/integrations/sql-clients/) for connection parameters and app
  passwords.
- [Client libraries](/integrations/client-libraries/) for other language
  clients.
- [Apache Arrow ADBC documentation](https://arrow.apache.org/adbc/) for the ADBC
  API and driver details.
