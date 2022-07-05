---
title: "Python cheatsheet"
description: "Use Python to connect, insert, manage, query and stream from Materialize."
aliases:
  - /guides/python/
menu:
  main:
    parent: "client-libraries"
    name: "Python"
---

MMaterialize is **wire-compatible** with PostgreSQL, which means that Python applications can use common PostgreSQL clients to interact with Materialize. In this guide, we'll use the [`psycopg2`](https://pypi.org/project/psycopg2/) adapter to connect to Materialize and issue SQL commands.

## Connect

To [connect](https://www.psycopg.org/docs/usage.html) to a local Materialize instance using `psycopg2`:

```python
#!/usr/bin/env python3

import psycopg2
import sys

dsn = "postgresql://materialize@localhost:6875/materialize?sslmode=disable"
conn = psycopg2.connect(dsn)
```

## Stream

To take full advantage of incrementally updated materialized views from a Python application, instead of [querying](#query) Materialize for the state of a view at a point in time, use a [`TAIL` statement](/sql/tail/) to request a stream of updates as the view changes.

To read a stream of updates from an existing materialized view, open a long-lived transaction with `BEGIN` and use [`TAIL` with `FETCH`](/sql/tail/#tailing-with-fetch) to repeatedly fetch all changes to the view since the last query:

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

The [TAIL output format](/sql/tail/#output) of `cur` is a data access object that can be used to iterate over the set of rows. When a row of a tailed view is **updated,** two objects will show up in the `rows` array:

```python
    ...
(Decimal('1648737001490'), 1, 'my_value_1')
(Decimal('1648737001490'), 1, 'my_value_2')
(Decimal('1648737001490'), 1, 'my_value_3')
(Decimal('1648737065479'), -1, 'my_value_3')
(Decimal('1648737065479'), 1, 'my_value_4')
    ...
```

A `mz_diff` value of `-1` indicates Materialize is deleting one row with the included values.  An update is just a retraction (`mz_diff: '-1'`) and an insertion (`mz_diff: '1'`) with the same timestamp.

### Streaming with `psycopg3`

{{< warning >}}
`psycopg3` is **not yet stable**. The following example may break if its API changes.
{{< /warning >}}

Although `psycopg3` can function identically as the `psycopg2` example above,
it provides has a `stream` feature where rows are not buffered, which allows you to use `TAIL` directly:

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

Querying Materialize is identical to querying a PostgreSQL database: Python executes the query, and Materialize returns the state of the view, source, or table at that point in time.

Because Materialize maintains materialized views in memory, response times are much faster than traditional database queries, and polling (repeatedly querying) a view doesn't impact performance.

To query a view `my_view` with a `SELECT` statement:

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

Most data in Materialize will stream in via an external system, but a [table](/sql/create-table/) can be helpful for supplementary data. For example, use a table to join slower-moving reference or lookup data with a stream.

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

ORM frameworks like **SQLAlchemy** tend to run complex introspection queries that may use configuration settings, system tables or features not yet implemented in Materialize. This means that even if a tool is compatible with PostgreSQL, it’s not guaranteed that the same integration will work out-of-the-box.

The level of support for these tools will improve as we extend the coverage of `pg_catalog` in Materialize {{% gh 2157 %}} and join efforts with each community to make the integrations Just Work™️.
