<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/self-managed/v25.2/)
 /  [Overview](/docs/self-managed/v25.2/transform-data/)
 /  [Patterns](/docs/self-managed/v25.2/transform-data/patterns/)

</div>

# Partitioning and filter pushdown

A few types of Materialize collections are durably written to storage:
[materialized
views](/docs/self-managed/v25.2/sql/create-materialized-view/),
[tables](/docs/self-managed/v25.2/sql/create-table), and
[sources](/docs/self-managed/v25.2/sql/create-source).

Internally, each collection is stored as a set of **runs** of data, each
of which is sorted and then partitioned up into individual **parts**,
and those parts are written to object storage and fetched only when
necessary to satisfy a query. Materialize will also periodically
**compact** the data it stores, to consolidate small parts into larger
ones or discard deleted rows.

Using the `PARTITION BY` option, you can specify the internal ordering
that Materialize will use to sort, partition, and store these runs of
data. A well-chosen partitioning can unlock optimizations like [filter
pushdown](#filter-pushdown), which in turn can make queries and other
operations more efficient.

<div class="note">

**NOTE:** The `PARTITION BY` option has no impact on the order in which
records are returned by queries. If you want to return results in a
specific order, use an `ORDER BY` clause on your [`SELECT`
statement](/docs/self-managed/v25.2/sql/select/).

</div>

## Syntax

The option `PARTITION BY <column list>` declares that a [materialized
view](/docs/self-managed/v25.2/sql/create-materialized-view/#with_options)
or [table](/docs/self-managed/v25.2/sql/create-table/#with_options)
should be partitioned by the listed columns. For example, a table that
stores an append-only collection of events may want to partition the
data by time:

<div class="highlight">

``` chroma
CREATE TABLE events (event_ts timestamptz, body jsonb)
WITH (
    PARTITION BY (event_ts)
);
```

</div>

This `PARTITION BY` clause declares that events with similar `event_ts`
timestamps should be stored together.

When multiple columns are specified, rows are partitioned
lexicographically. For example, `PARTITION BY (event_date, event_time)`
would partition first by the created date; if many rows have the same
`event_date`, those rows would be partitioned by the `event_time`
column. Durable collections without a `PARTITION BY` option can be
partitioned arbitrarily.

<div class="note">

**NOTE:** The `PARTITION BY` option does not mean that rows with
different values for the specified columns will be stored in different
parts, only that rows with similar values for those columns should be
stored together.

</div>

## Requirements

Materialize currently imposes some restrictions on the list of columns
in the `PARTITION BY` clause.

- This clause must list a prefix of the columns in the collection. For
  example:
  - if you’re creating a table that partitions by a single column, that
    column must be the first column in the table’s schema definition;
  - if you’re creating a table that partitions by two columns, those
    columns must be the first two columns in the table’s schema
    definition and listed in the same order.
- Only certain types of columns are supported. This includes:
  - all fixed-width integer types, including `smallint`, `integer`, and
    `bigint`;
  - date and time types, including `date`, `time`, `timestamp`,
    `timestamptz`, and `mz_timestamp`;
  - string types like `text` and `bytea`;
  - `boolean` and `uuid`;
  - `record` types where all fields types are supported.

We intend to relax some of these restrictions in the future.

## Filter pushdown

Suppose that our example `events` table has accumulated years’ worth of
data, but we’re running a query with a [temporal
filter](/docs/self-managed/v25.2/transform-data/patterns/temporal-filters/)
that matches only rows with recent timestamps.

<div class="highlight">

``` chroma
SELECT * FROM events WHERE mz_now() <= event_ts + INTERVAL '5min';
```

</div>

This query returns only rows with similar values for `event_ts`:
timestamps in the last five minutes. Since we declared that our `events`
table is partitioned by `event_ts`, that means all the rows that pass
this filter will be stored in the same small subset of parts.

Materialize tracks a small amount of metadata for every part, including
the range of possible values for many columns. When it can determine
that none of the data in a part will match a filter, it will skip
fetching that data from object storage. This optimization is called
*filter pushdown*, and when you’re querying with a selective filter
against a large collection, it can save a great deal of time and
computation.

Materialize will always try to apply filter pushdown to your query, but
that filtering is usually only effective when similar rows are stored
together. If you want to make sure that the filter pushdown optimization
is effective for your query, you can:

- Use a `PARTITION BY` clause on the relevant column to ensure that data
  with similar values for that column are stored close together.
- Add a filter to your query that only returns true for a narrow range
  of values in that column.

Filters that consist of arithmetic, date math, and comparisons are
generally eligible for pushdown, including all the examples in this
page. However, more complex filters might not be. You can check whether
the filters in your query can be pushed down using [an `EXPLAIN`
statement](/docs/self-managed/v25.2/sql/explain-plan/). In the following
example, we can be confident our temporal filter will be pushed down
because it’s present in the `pushdown` list at the bottom of the output.

<div class="highlight">

``` chroma
EXPLAIN SELECT * FROM events WHERE mz_now() <= event_ts + INTERVAL '5min';
----
Explained Query:
[...]
Source materialize.public.events
  [...]
  pushdown=((mz_now() <= timestamp_to_mz_timestamp((#0 + 00:05:00))))
```

</div>

Some common functions, such as casting from a string to a timestamp, can
prevent filter pushdown for a query. For similar functions that *do*
allow pushdown, see [the pushdown functions
documentation](/docs/self-managed/v25.2/sql/functions/pushdown/).

## Examples

These examples create real objects. After you have tried the examples,
make sure to drop these objects and spin down any resources you may have
created.

### Partitioning by timestamp

For timeseries or “event”-type collections, it’s often useful to
partition the data by timestamp.

1.  First, create a table called `events`.

    <div class="highlight">

    ``` chroma
    -- Create a table of timestamped events. Note that the `event_ts` column is
    -- first in the column list and in the parition-by clause.
    CREATE TABLE events (
        event_ts timestamptz,
        content text
    ) WITH (
        PARTITION BY (event_ts)
    );
    ```

    </div>

2.  Insert a few records, one “older” record and one more recent.

    <div class="highlight">

    ``` chroma
    INSERT INTO events VALUES (now()::timestamp - '5 minutes', 'hello');
    INSERT INTO events VALUES (now(), 'world');
    ```

    </div>

3.  Run a select statement against the data within the next five
    minutes. This should return only the more recent of the two rows.

    <div class="highlight">

    ``` chroma
    SELECT * FROM events WHERE event_ts + '2 minutes' > mz_now();
    ```

    </div>

4.  To verify that Materialize fetched only the parts that contain data
    with the recent timestamps, run an `EXPLAIN FILTER PUSHDOWN`
    statement.

    <div class="highlight">

    ``` chroma
    EXPLAIN FILTER PUSHDOWN FOR
    SELECT * FROM events WHERE event_ts + '2 minutes' > mz_now();
    ```

    </div>

If you wait a few minutes longer until there are no events that match
the temporal filter, you’ll notice that not only does the query return
zero rows, but the explain shows that we fetched zero parts.

<div class="note">

**NOTE:** The exact numbers you see here may vary: parts can be much
larger than a single row, and the actual level of filtering may
fluctuate for small datasets as data is compacted together internally.
However, datasets of a few gigabytes or larger should reliably see
benefits from this optimization.

</div>

### Partitioning by category

Other datasets don’t have a strong timeseries component, but they do
have a clear notion of type or category. For example, suppose you have a
collection of music venues spread across the world that you regularly
query by a single country.

1.  First, create a table called `venues`, partitioned by country.

    <div class="highlight">

    ``` chroma
    -- Create a table for our venue data.
    -- Once again, the partition column is listed first.
    CREATE TABLE venues (
        country_code text,
        id bigint,
        name text
    ) WITH (
        PARTITION BY (country_code)
    );
    ```

    </div>

2.  Insert a few records with different country codes.

    <div class="highlight">

    ``` chroma
    INSERT INTO venues VALUES ('US', 1, 'Rock World');
    INSERT INTO venues VALUES ('CA', 2, 'Friendship Cove');
    ```

    </div>

3.  Query for venues in particular countries.

    <div class="highlight">

    ``` chroma
    SELECT * FROM venues WHERE country_code IN ('US', 'MX');
    ```

    </div>

4.  Run `EXPLAIN FILTER PUSHDOWN` to check that we’re filtering out
    parts that don’t include data that’s relevant to the query.

    <div class="highlight">

    ``` chroma
    EXPLAIN FILTER PUSHDOWN FOR
    SELECT * FROM venues WHERE country_code IN ('US', 'MX');
    ```

    </div>

<div class="note">

**NOTE:** As before, we’re not guaranteed to see much or any benefit
from filter pushdown on small collections… but for datasets of over a
few gigabytes, we should reliably be able to filter down to a subset of
the parts we’d otherwise need to fetch.

</div>

</div>

<a href="#top" class="back-to-top">Back to top ↑</a>

<div class="theme-switcher">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzeXN0ZW0iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+U3lzdGVtIFRoZW1lPC90aXRsZT4KICAgICAgICA8cGF0aCBkPSJNMjU2IDE3NmE4MCA4MCAwIDEwODAgODAgODAuMjQgODAuMjQgMCAwMC04MC04MHptMTcyLjcyIDgwYTE2NS41MyAxNjUuNTMgMCAwMS0xLjY0IDIyLjM0bDQ4LjY5IDM4LjEyYTExLjU5IDExLjU5IDAgMDEyLjYzIDE0Ljc4bC00Ni4wNiA3OS41MmExMS42NCAxMS42NCAwIDAxLTE0LjE0IDQuOTNsLTU3LjI1LTIzYTE3Ni41NiAxNzYuNTYgMCAwMS0zOC44MiAyMi42N2wtOC41NiA2MC43OGExMS45MyAxMS45MyAwIDAxLTExLjUxIDkuODZoLTkyLjEyYTEyIDEyIDAgMDEtMTEuNTEtOS41M2wtOC41Ni02MC43OEExNjkuMyAxNjkuMyAwIDAxMTUxLjA1IDM5M0w5My44IDQxNmExMS42NCAxMS42NCAwIDAxLTE0LjE0LTQuOTJMMzMuNiAzMzEuNTdhMTEuNTkgMTEuNTkgMCAwMTIuNjMtMTQuNzhsNDguNjktMzguMTJBMTc0LjU4IDE3NC41OCAwIDAxODMuMjggMjU2YTE2NS41MyAxNjUuNTMgMCAwMTEuNjQtMjIuMzRsLTQ4LjY5LTM4LjEyYTExLjU5IDExLjU5IDAgMDEtMi42My0xNC43OGw0Ni4wNi03OS41MmExMS42NCAxMS42NCAwIDAxMTQuMTQtNC45M2w1Ny4yNSAyM2ExNzYuNTYgMTc2LjU2IDAgMDEzOC44Mi0yMi42N2w4LjU2LTYwLjc4QTExLjkzIDExLjkzIDAgMDEyMDkuOTQgMjZoOTIuMTJhMTIgMTIgMCAwMTExLjUxIDkuNTNsOC41NiA2MC43OEExNjkuMyAxNjkuMyAwIDAxMzYxIDExOWw1Ny4yLTIzYTExLjY0IDExLjY0IDAgMDExNC4xNCA0LjkybDQ2LjA2IDc5LjUyYTExLjU5IDExLjU5IDAgMDEtMi42MyAxNC43OGwtNDguNjkgMzguMTJhMTc0LjU4IDE3NC41OCAwIDAxMS42NCAyMi42NnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="system" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzdW4iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+TGlnaHQgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0yMzQgMjZoNDR2OTJoLTQ0ek0yMzQgMzk0aDQ0djkyaC00NHpNMzM4LjAyNSAxNDIuODU3bDY1LjA1NC02NS4wNTQgMzEuMTEzIDMxLjExMy02NS4wNTQgNjUuMDU0ek03Ny44MTUgNDAzLjA3NGw2NS4wNTQtNjUuMDU0IDMxLjExMyAzMS4xMTMtNjUuMDU0IDY1LjA1NHpNMzk0IDIzNGg5MnY0NGgtOTJ6TTI2IDIzNGg5MnY0NEgyNnpNMzM4LjAyOSAzNjkuMTRsMzEuMTEyLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMiAzMS4xMTJ6TTc3LjgwMiAxMDguOTJsMzEuMTEzLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMyAzMS4xMTJ6TTI1NiAzNThhMTAyIDEwMiAwIDExMTAyLTEwMiAxMDIuMTIgMTAyLjEyIDAgMDEtMTAyIDEwMnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="sun" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJtb29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgPHRpdGxlPkRhcmsgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0xNTIuNjIgMTI2Ljc3YzAtMzMgNC44NS02Ni4zNSAxNy4yMy05NC43N0M4Ny41NCA2Ny44MyAzMiAxNTEuODkgMzIgMjQ3LjM4IDMyIDM3NS44NSAxMzYuMTUgNDgwIDI2NC42MiA0ODBjOTUuNDkgMCAxNzkuNTUtNTUuNTQgMjE1LjM4LTEzNy44NS0yOC40MiAxMi4zOC02MS44IDE3LjIzLTk0Ljc3IDE3LjIzLTEyOC40NyAwLTIzMi42MS0xMDQuMTQtMjMyLjYxLTIzMi42MXoiIC8+CiAgICAgIDwvc3ZnPg=="
class="moon" />

</div>

<div>

<a
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/transform-data/patterns/partition-by.md"
class="btn-ghost"><img
src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTgiIGhlaWdodD0iMTgiIHZpZXdib3g9IjAgMCAyMyAyMyIgZmlsbD0iY3VycmVudENvbG9yIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPgogICAgICAgIDxwYXRoIGQ9Ik0yMC44OTQ1IDExLjQ5NjhDMjAuODk0NSAxMC4yMzk0IDIwLjYxNTEgOS4wNTE5IDIwLjEyNjEgNy44NjQzN0MxOS42MzcxIDYuNzQ2NjkgMTguOTM4NSA1LjY5ODg4IDE4LjE3MDEgNC45MzA0N0MxNy40MDE3IDQuMTYyMDcgMTYuMzUzOSAzLjQ2MzUgMTUuMjM2MiAyLjk3NDUyQzE0LjExODUgMi40ODU1MyAxMi44NjExIDIuMjA2MTMgMTEuNjAzOCAyLjIwNjEzQzEwLjM0NjQgMi4yMDYxMyA5LjE1ODg0IDIuNDg1NTMgNy45NzEzIDIuOTc0NTJDNi44NTM2MiAzLjQ2MzUgNS44MDU3OSA0LjE2MjA3IDUuMDM3MzggNC45MzA0N0M0LjI2ODk4IDUuNjk4ODggMy41NzA0NCA2Ljc0NjY5IDMuMDgxNDUgNy44NjQzN0MyLjU5MjQ3IDguOTgyMDUgMi4zMTMwNCAxMC4yMzk0IDIuMzEzMDQgMTEuNDk2OEMyLjMxMzA0IDEzLjUyMjYgMi45NDE3NCAxNS4zMzg5IDQuMTI5MjggMTcuMDE1NEM1LjMxNjgxIDE4LjY5MTkgNi45MjM0NyAxOS44MDk2IDguODA5NTYgMjAuMzY4NFYxNy45MjM1QzguMjUwNzIgMTcuOTkzNCA3Ljk3MTI5IDE3Ljk5MzMgNy44MzE1OCAxNy45OTMzQzYuNzgzNzYgMTcuOTkzMyA2LjAxNTM1IDE3LjUwNDQgNS41OTYyMiAxNi41MjY0QzUuNDU2NTEgMTYuMTc3MSA1LjI0Njk1IDE1LjgyNzggNS4wMzczOCAxNS42MTgzQzQuOTY3NTMgMTUuNTQ4NCA0Ljg5NzY4IDE1LjQ3ODYgNC43NTc5NyAxNS4zMzg5QzQuNjE4MjYgMTUuMTk5MiA0LjQ3ODU0IDE1LjEyOTMgNC4zMzg4MyAxNC45ODk2QzQuMTk5MTIgMTQuODQ5OSA0LjEyOTI4IDE0Ljc4IDQuMTI5MjggMTQuNzhDNC4xMjkyOCAxNC42NDAzIDQuMjY4OTggMTQuNjQwMyA0LjU0ODQgMTQuNjQwM0M0LjgyNzgyIDE0LjY0MDMgNS4xMDcyNCAxNC43MTAyIDUuMzE2ODEgMTQuODQ5OUM1LjUyNjM3IDE0Ljk4OTYgNS43MzU5NCAxNS4xMjkzIDUuODc1NjUgMTUuMzM4OUM2LjAxNTM2IDE1LjU0ODQgNi4xNTUwNyAxNS43NTggNi4zNjQ2MyAxNS45Njc2QzYuNTA0MzQgMTYuMTc3MSA2LjcxMzkxIDE2LjMxNjggNi45MjM0OCAxNi40NTY1QzcuMTMzMDQgMTYuNTk2MyA3LjQxMjQ2IDE2LjY2NjEgNy43NjE3MyAxNi42NjYxQzguMTgwODYgMTYuNjY2MSA4LjUzMDE0IDE2LjU5NjMgOC45NDkyNyAxNi40NTY1QzkuMDg4OTggMTUuODk3NyA5LjQzODI1IDE1LjQ3ODYgOS44NTczOCAxNS4xMjkzQzguMjUwNzIgMTQuOTg5NiA3LjA2MzE4IDE0LjU3MDUgNi4yOTQ3NyAxMy45NDE4QzUuNTI2MzcgMTMuMzEzMSA1LjEwNzI0IDEyLjE5NTQgNS4xMDcyNCAxMC42NTg2QzUuMTA3MjQgOS41NDA4OSA1LjQ1NjUyIDguNTYyOTQgNi4xNTUwNyA3Ljc5NDUzQzYuMDE1MzYgNy4zNzU0IDUuOTQ1NSA2Ljk1NjI2IDUuOTQ1NSA2LjUzNzEzQzUuOTQ1NSA1Ljk3ODI5IDYuMDg1MjEgNS40MTk0NiA2LjM2NDYzIDQuOTMwNDdDNi45MjM0NyA0LjkzMDQ3IDcuNDEyNDUgNS4wMDAzMiA3LjgzMTU4IDUuMjA5ODlDOC4yNTA3MSA1LjQxOTQ1IDguNzM5NyA1LjY5ODg2IDkuMjk4NTQgNi4xMTc5OUMxMC4wNjY5IDUuOTc4MjggMTAuODM1NCA1LjgzODU4IDExLjc0MzUgNS44Mzg1OEMxMi41MTE5IDUuODM4NTggMTMuMjgwMyA1LjkwODQ1IDEzLjk3ODggNi4wNDgxNkMxNC41Mzc3IDUuNjI5MDMgMTUuMDI2NyA1LjM0OTYgMTUuNDQ1OCA1LjIwOTg5QzE1Ljg2NDkgNS4wMDAzMiAxNi4zNTM5IDQuOTMwNDcgMTYuOTEyNyA0LjkzMDQ3QzE3LjE5MjIgNS40MTk0NiAxNy4zMzE5IDUuOTc4MjkgMTcuMzMxOSA2LjUzNzEzQzE3LjMzMTkgNi45NTYyNiAxNy4yNjIgNy4zNzU0IDE3LjEyMjMgNy43MjQ2N0MxNy44MjA5IDguNDkzMDggMTguMTcwMSA5LjQ3MTA1IDE4LjE3MDEgMTAuNTg4N0MxOC4xNzAxIDEyLjEyNTUgMTcuNzUxIDEzLjE3MzQgMTYuOTgyNiAxMy44NzE5QzE2LjIxNDIgMTQuNTcwNSAxNS4wMjY2IDE0LjkxOTcgMTMuNDIgMTUuMDU5NEMxNC4xMTg1IDE1LjU0ODQgMTQuMzk4IDE2LjE3NzEgMTQuMzk4IDE2Ljk0NTVWMjAuMjI4N0MxNi4zNTM5IDE5LjYgMTcuODkwNyAxOC40ODIzIDE5LjA3ODIgMTYuODc1N0MyMC4yNjU4IDE1LjMzODkgMjAuODk0NSAxMy41MjI2IDIwLjg5NDUgMTEuNDk2OFpNMjIuNzEwNyAxMS40OTY4QzIyLjcxMDcgMTMuNTIyNiAyMi4yMjE3IDE1LjQwODcgMjEuMjQzOCAxNy4wODUyQzIwLjI2NTggMTguODMxNiAxOC44Njg3IDIwLjE1ODggMTcuMTkyMiAyMS4xMzY4QzE1LjQ0NTggMjIuMTE0OCAxMy42Mjk2IDIyLjYwMzggMTEuNjAzOCAyMi42MDM4QzkuNTc3OTYgMjIuNjAzOCA3LjY5MTg4IDIyLjExNDggNi4wMTUzNiAyMS4xMzY4QzQuMjY4OTggMjAuMTU4OCAyLjk0MTc0IDE4Ljc2MTggMS45NjM3NyAxNy4wODUyQzAuOTg1Nzk2IDE1LjMzODkgMC40OTY4MDcgMTMuNTIyNiAwLjQ5NjgwNyAxMS40OTY4QzAuNDk2ODA3IDkuNDcxMDQgMC45ODU3OTYgNy41ODQ5NiAxLjk2Mzc3IDUuOTA4NDRDMi45NDE3NCA0LjE2MjA2IDQuMzM4ODQgMi44MzQ4MyA2LjAxNTM2IDEuODU2ODZDNy43NjE3MyAwLjg3ODg4NiA5LjU3Nzk2IDAuMzg5ODk3IDExLjYwMzggMC4zODk4OTdDMTMuNjI5NiAwLjM4OTg5NyAxNS41MTU2IDAuODc4ODg2IDE3LjE5MjIgMS44NTY4NkMxOC45Mzg1IDIuODM0ODMgMjAuMjY1OCA0LjIzMTkyIDIxLjI0MzggNS45MDg0NEMyMi4yMjE3IDcuNTg0OTYgMjIuNzEwNyA5LjQ3MTA0IDIyLjcxMDcgMTEuNDk2OFoiIC8+CiAgICAgIDwvc3ZnPg==" />
Edit this page</a>

</div>

<div class="footer-links">

[Home](https://materialize.com) [Status](https://status.materialize.com)
[GitHub](https://github.com/MaterializeInc/materialize)
[Blog](https://materialize.com/blog)
[Contact](https://materialize.com/contact)

Cookie Preferences

[Privacy Policy](https://materialize.com/privacy-policy/)

</div>

© 2025 Materialize Inc.

</div>
