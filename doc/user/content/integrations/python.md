---
title: "Python Cheatsheet"
description: "Use Python to connect, insert, manage, query and stream from Materialize."
aliases:
  - /guides/python/
menu:
  main:
    parent: "client-libraries"
    name: "Python"
---

Materialize is **PostgreSQL-compatible**, which means that Python applications can use any existing PostgreSQL client to interact with Materialize as if it were a PostgreSQL database. In this guide, we'll use [psycopg2 PostgreSQL database adapter](https://pypi.org/project/psycopg2/) to connect to Materialize and issue PostgreSQL commands.

## Connect

You connect to Materialize the same way you [connect to PostgreSQL with `psycopg2`](https://www.psycopg.org/docs/usage.html).

### Local Instance

You can connect to a local Materialize instance just as you would connect to a PostgreSQL instance:

```python
#!/usr/bin/env python3

import psycopg2
import sys

dsn = "postgresql://materialize@localhost:6875/materialize?sslmode=disable"
conn = psycopg2.connect(dsn)
```

## Stream

To take full advantage of incrementally updated materialized views from a Python application, instead of [querying](#query) Materialize for the state of a view at a point in time, use [a `TAIL` statement](/sql/tail/) to request a stream of updates as the view changes.

To read a stream of updates from an existing materialized view, open a long-lived transaction with `BEGIN` and use [`TAIL` with `FETCH`](/sql/tail/#tailing-with-fetch) to repeatedly fetch all changes to the view since the last query.

```python
#!/usr/bin/env python3

import psycopg2
import sys

dsn = "postgresql://materialize@localhost:6875/materialize?sslmode=disable"
conn = psycopg2.connect(dsn)

with conn.cursor() as cur:
    cur.execute("DECLARE c CURSOR FOR TAIL my_view")
    while True:
        cur.execute("FETCH ALL c")
        for row in cur:
            print(row)
```

The [TAIL Output format](/sql/tail/#output) of `res.rows` is an array of view update objects. When a row of a tailed view is **updated,** two objects will show up in the `rows` array:

```python
    ...
(Decimal('1648737001490'), 1, 'my_value_1')
(Decimal('1648737001490'), 1, 'my_value_2')
(Decimal('1648737001490'), 1, 'my_value_3')
(Decimal('1648737065479'), -1, 'my_value_3')
(Decimal('1648737065479'), 1, 'my_value_4')
    ...
```

A `mz_diff` value of `-1` indicates Materialize is deleting one row with the included values.  An update is just a deletion (`mz_diff: '-1'`) and an insertion (`mz_diff: '1'`) with the same `mz_timestamp`.

### Streaming with psycopg3

{{< warning >}}
psycopg3 is not yet stable.
The example here could break if their API changes.
{{< /warning >}}

Although psycopg3 can function identically as the psycopg2 example above,
it also has a `stream` feature where rows are not buffered and we can thus use `TAIL` directly.

```python
#!/usr/bin/env python3

import psycopg3
import sys

dsn = "postgresql://materialize@localhost:6875/materialize?sslmode=disable"
conn = psycopg3.connect(dsn)

conn = psycopg3.connect(dsn)
with conn.cursor() as cur:
    for row in cur.stream("TAIL t"):
        print(row)
```
## Query

Querying Materialize is identical to querying a traditional PostgreSQL database: Python executes the query, and Materialize returns the state of the view, source, or table at that point in time.

Because Materialize maintains materialized views in memory, response times are much faster than traditional database queries, and polling (repeatedly querying) a view doesn't impact performance.

Query a view `my_view` with a select statement:

```python
#!/usr/bin/env python3

import psycopg2
import sys

dsn = "postgresql://materialize@localhost:6875/materialize?sslmode=disable"
conn = psycopg2.connect(dsn)

with conn.cursor() as cur:
    cur.execute("SELECT * FROM my_view;")
    for row in cur:
        print(row)
```

For more details, see the [Psycopg](https://www.psycopg.org/docs/usage.html) documentation.

## Insert data into tables

Most data in Materialize will stream in via a `SOURCE`, but a [`TABLE` in Materialize](/sql/create-table/) can be helpful for supplementary data. For example, use a table to join slower-moving reference or lookup data with a stream.

**Basic Example:** [Insert a row](/sql/insert/) of data into a table named `countries` in Materialize.

```python
#!/usr/bin/env python3

import psycopg2
import sys

dsn = "postgresql://materialize@localhost:6875/materialize?sslmode=disable"
conn = psycopg2.connect(dsn)

cur = conn.cursor()
cur.execute("INSERT INTO countries (name, code) VALUES (%s, %s)", ('United States', 'US'))
cur.execute("INSERT INTO countries (name, code) VALUES (%s, %s)", ('Canada', 'CA'))
cur.execute("INSERT INTO countries (name, code) VALUES (%s, %s)", ('Mexico', 'MX'))
cur.execute("INSERT INTO countries (name, code) VALUES (%s, %s)", ('Germany', 'DE'))
conn.commit()
cur.close()

with conn.cursor() as cur:
    cur.execute("SELECT COUNT(*) FROM countries;")
    print(cur.fetchone())

conn.close()
```

## Manage sources, views, and indexes

Typically, you create sources, views, and indexes when deploying Materialize, although it is possible to use a Python app to execute common DDL statements.

### Create a source from Python

```python
#!/usr/bin/env python3

import psycopg2
import sys

dsn = "postgresql://materialize@localhost:6875/materialize?sslmode=disable"
conn = psycopg2.connect(dsn)
conn.autocommit = True

cur = conn.cursor()

with conn.cursor() as cur:
    cur.execute("CREATE SOURCE market_orders_raw_2 FROM PUBNUB " \
            "SUBSCRIBE KEY 'sub-c-4377ab04-f100-11e3-bffd-02ee2ddab7fe' " \
            "CHANNEL 'pubnub-market-orders'")

with conn.cursor() as cur:
    cur.execute("SHOW SOURCES")
    print(cur.fetchone())
```

For more information, see [`CREATE SOURCE`](/sql/create-source/).

### Create a view from Python

```python
#!/usr/bin/env python3

import psycopg2
import sys

dsn = "postgresql://materialize@localhost:6875/materialize?sslmode=disable"
conn = psycopg2.connect(dsn)
conn.autocommit = True

cur = conn.cursor()

with conn.cursor() as cur:
    cur.execute("CREATE VIEW market_orders_2 AS " \
            "SELECT " \
                "val->>'symbol' AS symbol, " \
                "(val->'bid_price')::float AS bid_price " \
            "FROM (SELECT text::jsonb AS val FROM market_orders_raw_2)")

with conn.cursor() as cur:
    cur.execute("SHOW VIEWS")
    print(cur.fetchone())
```

For more information, see [`CREATE VIEW`](/sql/create-view/).

## Python ORMs

Materialize doesn't currently support the full catalog of PostgreSQL system metadata API endpoints, including the system calls that object relational mapping systems (ORMs) like **SQLAlchemy** use to introspect databases and do extra work behind the scenes. This means that some ORM system attempts to interact with Materialize will currently fail. Once [full `pg_catalog` support](https://github.com/MaterializeInc/materialize/issues/2157) is implemented, the features that depend on `pg_catalog` may work properly.
