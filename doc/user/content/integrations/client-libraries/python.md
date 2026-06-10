---
title: "Python cheatsheet"
description: "Use Python to connect, insert, manage, query and stream from Materialize."
aliases:
  - /guides/python/
  - /integrations/python/
menu:
  main:
    parent: "client-libraries"
    name: "Python"
---

Materialize is **wire-compatible** with PostgreSQL, which means that Python applications can use common PostgreSQL clients to interact with Materialize. In this guide, we'll use the [`psycopg2`](https://pypi.org/project/psycopg2/) adapter to connect to Materialize and issue SQL commands.

## Connect

To [connect](https://www.psycopg.org/docs/usage.html) to a local Materialize instance using `psycopg2`:

```python
#!/usr/bin/env python3

import psycopg2
import sys

dsn = "user=MATERIALIZE_USERNAME password=MATERIALIZE_PASSWORD host=MATERIALIZE_HOST port=6875 dbname=materialize sslmode=require"
conn = psycopg2.connect(dsn)
```

## Create tables

Most data in Materialize will stream in via an external system, but a [table](/sql/create-table/) can be helpful for supplementary data. For example, use a table to join slower-moving reference or lookup data with a stream.

To create a table named `countries` in Materialize:

```python
#!/usr/bin/env python3

import psycopg2
import sys

dsn = "user=MATERIALIZE_USERNAME password=MATERIALIZE_PASSWORD host=MATERIALIZE_HOST port=6875 dbname=materialize sslmode=require"
conn = psycopg2.connect(dsn)

with conn.cursor() as cur:
    cur.execute("CREATE TABLE IF NOT EXISTS countries (code CHAR(2), name TEXT);")

with conn.cursor() as cur:
    cur.execute("SHOW TABLES")
    print(cur.fetchone())
```

## Insert data into tables

**Basic Example:** [Insert a row](/sql/insert/) of data into a table named `countries` in Materialize.

```python
#!/usr/bin/env python3

import psycopg2
import sys

dsn = "user=MATERIALIZE_USERNAME password=MATERIALIZE_PASSWORD host=MATERIALIZE_HOST port=6875 dbname=materialize sslmode=require"
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

## Query

Querying Materialize is identical to querying a PostgreSQL database: Python executes the query, and Materialize returns the state of the view, source, or table at that point in time.

Because Materialize keeps results incrementally updated, response times are much faster than traditional database queries, and polling (repeatedly querying) a view doesn't impact performance.

To query the `countries` table using a `SELECT` statement:

```python
#!/usr/bin/env python3

import psycopg2
import sys

dsn = "user=MATERIALIZE_USERNAME password=MATERIALIZE_PASSWORD host=MATERIALIZE_HOST port=6875 dbname=materialize sslmode=require"
conn = psycopg2.connect(dsn)

with conn.cursor() as cur:
    cur.execute("SELECT * FROM countries;")
    for row in cur:
        print(row)
```

For more details, see the [Psycopg](https://www.psycopg.org/docs/usage.html) documentation.

## Manage sources, views, and indexes

Typically, you create sources, views, and indexes when deploying Materialize, although it is possible to use a Python app to execute common DDL statements.

### Create a source from Python

```python
#!/usr/bin/env python3

import psycopg2
import sys

dsn = "user=MATERIALIZE_USERNAME password=MATERIALIZE_PASSWORD host=MATERIALIZE_HOST port=6875 dbname=materialize sslmode=require"
conn = psycopg2.connect(dsn)
conn.autocommit = True

with conn.cursor() as cur:
    cur.execute("CREATE SOURCE auction FROM LOAD GENERATOR AUCTION FOR ALL TABLES;")

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

dsn = "user=MATERIALIZE_USERNAME password=MATERIALIZE_PASSWORD host=MATERIALIZE_HOST port=6875 dbname=materialize sslmode=require"
conn = psycopg2.connect(dsn)
conn.autocommit = True

with conn.cursor() as cur:
    cur.execute("CREATE MATERIALIZED VIEW IF NOT EXISTS amount_sum AS " \
            "SELECT sum(amount)" \
            "FROM bids;")

with conn.cursor() as cur:
    cur.execute("SHOW VIEWS")
    print(cur.fetchone())
```

For more information, see [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view/).

## Stream

To take full advantage of incrementally updated materialized views from a Python application, instead of [querying](#query) Materialize for the state of a view at a point in time, use a [`SUBSCRIBE` statement](/sql/subscribe/) to request a stream of updates as the view changes.

To read a stream of updates from an existing materialized view, open a long-lived transaction with `BEGIN` and use [`SUBSCRIBE` with `FETCH`](/sql/subscribe/#subscribing-with-fetch) to repeatedly fetch all changes to the view since the last query:

```python
#!/usr/bin/env python3

import psycopg2
import sys

dsn = "user=MATERIALIZE_USERNAME password=MATERIALIZE_PASSWORD host=MATERIALIZE_HOST port=6875 dbname=materialize sslmode=require"
conn = psycopg2.connect(dsn)

with conn.cursor() as cur:
    cur.execute("DECLARE c CURSOR FOR SUBSCRIBE amount_sum")
    while True:
        cur.execute("FETCH ALL c")
        for row in cur:
            print(row)
```

The [SUBSCRIBE output format](/sql/subscribe/#output) of `cur` is a data access object that can be used to iterate over the set of rows. When a row of a subscribed view is **updated,** two objects will show up in the `rows` array:

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

Although `psycopg3` can function identically as the `psycopg2` example above,
it provides a `stream` feature where rows are not buffered, which allows you to use `SUBSCRIBE` directly:

```python
#!/usr/bin/env python3

import psycopg
import sys

dsn = "user=MATERIALIZE_USERNAME password=MATERIALIZE_PASSWORD host=MATERIALIZE_HOST port=6875 dbname=materialize sslmode=require"
conn = psycopg.connect(dsn)

with conn.cursor() as cur:
    for row in cur.stream("SUBSCRIBE amount_sum"):
        print(row)
```

## Clean up

To clean up the sources, views, and tables that we created, first connect to Materialize using a [PostgreSQL client](/integrations/sql-clients/) and then, run the following commands:

```mzsql
DROP MATERIALIZED VIEW IF EXISTS amount_sum;
DROP SOURCE IF EXISTS auction CASCADE;
DROP TABLE IF EXISTS countries;
```

## Python ORMs

ORM frameworks tend to run complex introspection queries that may use configuration settings, system tables or features not yet implemented in Materialize. This means that even if a tool is compatible with PostgreSQL, it’s not guaranteed that the same integration will work out-of-the-box.

The level of support for these tools will improve as we extend the coverage of `pg_catalog` in Materialize and join efforts with each community to make the integrations Just Work™️.

Check out the [integrations page](/integrations/) for a list of ORM frameworks
that are known to work well with Materialize.
