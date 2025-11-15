---
title: "Partitioning and filter pushdown"
description: "Declare how collections are stored."
aliases:
  - /guides/partition-by/
  - /sql/patterns/partition-by/
menu:
  main:
    parent: 'sql-patterns'
---

[//]: # "TODO link to the source table docs once that feature is documented."

A few types of Materialize collections are durably written to storage: [materialized views](/sql/create-materialized-view/), [tables](/sql/create-table), and [sources](/sql/create-source).

Internally, each collection is stored as a set of **runs** of data, each of which is sorted and then partitioned up into individual **parts**, and those parts are written to object storage and fetched only when necessary to satisfy a query. Materialize will also periodically **compact** the data it stores, to consolidate small parts into larger ones or discard deleted rows.

Using the `PARTITION BY` option, you can specify the internal ordering that
Materialize will use to sort, partition, and store these runs of data.
A well-chosen partitioning can unlock optimizations like [filter pushdown](#filter-pushdown), which in turn can make queries and other operations more efficient.

{{< note >}}
The `PARTITION BY` option has no impact on the order in which records are returned by queries.
If you want to return results in a specific order, use an `ORDER BY` clause on your [`SELECT` statement](/sql/select/).
{{< /note >}}

## Syntax

The option `PARTITION BY <column list>` declares that a [materialized view](/sql/create-materialized-view/#with_options) or [table](/sql/create-table/#syntax) should be partitioned by the listed columns.
For example, a table that stores an append-only collection of events may want to partition the data by time:

```mzsql
CREATE TABLE events (event_ts timestamptz, body jsonb)
WITH (
    PARTITION BY (event_ts)
);
```

This `PARTITION BY` clause declares that events with similar `event_ts` timestamps should be stored together.

When multiple columns are specified, rows are partitioned lexicographically.
For example, `PARTITION BY (event_date, event_time)` would partition first by the created date;
if many rows have the same `event_date`, those rows would be partitioned by the `event_time` column.
Durable collections without a `PARTITION BY` option can be partitioned arbitrarily.

{{< note >}}
The `PARTITION BY` option does not mean that rows with different values for the specified columns will be stored in different parts, only that rows with similar values for those columns should be stored together.
{{< /note >}}

## Requirements

Materialize currently imposes some restrictions on the list of columns in the `PARTITION BY` clause.

- This clause must list a prefix of the columns in the collection. For example:
  - if you're creating a table that partitions by a single column, that column must be the first column in the table's schema definition;
  - if you're creating a table that partitions by two columns, those columns must be the first two columns in the table's schema definition and listed in the same order.
- Only certain types of columns are supported. This includes:
    - all fixed-width integer types, including `smallint`, `integer`, and `bigint`;
    - date and time types, including `date`, `time`, `timestamp`, `timestamptz`, and `mz_timestamp`;
    - string types like `text` and `bytea`;
    - `boolean` and `uuid`;
    - `record` types where all fields types are supported.

We intend to relax some of these restrictions in the future.

## Filter pushdown

Suppose that our example `events` table has accumulated years' worth of data, but we're running a query with a [temporal filter](/transform-data/patterns/temporal-filters/) that matches only rows with recent timestamps.

```mzsql
SELECT * FROM events WHERE mz_now() <= event_ts + INTERVAL '5min';
```

This query returns only rows with similar values for `event_ts`: timestamps in the last five minutes.
Since we declared that our `events` table is partitioned by `event_ts`, that means all the rows that pass this filter will be stored in the same small subset of parts.

Materialize tracks a small amount of metadata for every part, including the range of possible values for many columns. When it can determine that none of the data in a part will match a filter, it will skip fetching that data from object storage. This optimization is called _filter pushdown_, and when you're querying with a selective filter against a large collection, it can save a great deal of time and computation.

Materialize will always try to apply filter pushdown to your query, but that filtering is usually only effective when similar rows are stored together.
If you want to make sure that the filter pushdown optimization is effective for your query, you can:

- Use a `PARTITION BY` clause on the relevant column to ensure that data with similar values for that column are stored close together.
- Add a filter to your query that only returns true for a narrow range of values in that column.

Filters that consist of arithmetic, date math, and comparisons are generally eligible for pushdown, including all the examples in this page. However, more complex filters might not be. You can check whether the filters in your query can be pushed down using [an `EXPLAIN` statement](/sql/explain-plan/). In the following example, we can be confident our temporal filter will be pushed down because it's present in the `pushdown` list at the bottom of the output.

```mzsql
EXPLAIN SELECT * FROM events WHERE mz_now() <= event_ts + INTERVAL '5min';
----
Explained Query:
[...]
Source materialize.public.events
  [...]
  pushdown=((mz_now() <= timestamp_to_mz_timestamp((#0 + 00:05:00))))
```

Some common functions, such as casting from a string to a timestamp, can prevent filter pushdown for a query. For similar functions that _do_ allow pushdown, see [the pushdown functions documentation](/sql/functions/pushdown/).

## Examples

These examples create real objects. After you have tried the examples, make sure to drop these objects and spin down any resources you may have created.

### Partitioning by timestamp

For timeseries or "event"-type collections, it's often useful to partition the data by timestamp.

1. First, create a table called `events`.
    ```mzsql
    -- Create a table of timestamped events. Note that the `event_ts` column is
    -- first in the column list and in the parition-by clause.
    CREATE TABLE events (
        event_ts timestamptz,
        content text
    ) WITH (
        PARTITION BY (event_ts)
    );
    ```

1. Insert a few records, one "older" record and one more recent.
    ```mzsql
    INSERT INTO events VALUES (now()::timestamp - '5 minutes', 'hello');
    INSERT INTO events VALUES (now(), 'world');
    ```

1. Run a select statement against the data within the next five minutes. This should return only the more recent of the two rows.
    ```mzsql
    SELECT * FROM events WHERE event_ts + '2 minutes' > mz_now();
    ```

1. To verify that Materialize fetched only the parts that contain data with the
   recent timestamps, run an `EXPLAIN FILTER PUSHDOWN` statement.
    ```mzsql
    EXPLAIN FILTER PUSHDOWN FOR
    SELECT * FROM events WHERE event_ts + '2 minutes' > mz_now();
    ```

If you wait a few minutes longer until there are no events that match the temporal filter, you'll notice that not only does the query return zero rows, but the explain shows that we fetched zero parts.

{{< note >}}

The exact numbers you see here may vary: parts can be much larger than a single row, and the actual level of filtering may fluctuate for small datasets as data is compacted together internally. However, datasets of a few gigabytes or larger should reliably see benefits from this optimization.

{{< /note >}}

### Partitioning by category

Other datasets don't have a strong timeseries component, but they do have a clear notion of type or category. For example, suppose you have a collection of music venues spread across the world that you regularly query by a single country.

1. First, create a table called `venues`, partitioned by country.
    ```mzsql
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

1. Insert a few records with different country codes.
    ```mzsql
    INSERT INTO venues VALUES ('US', 1, 'Rock World');
    INSERT INTO venues VALUES ('CA', 2, 'Friendship Cove');
    ```

1. Query for venues in particular countries.
    ```mzsql
    SELECT * FROM venues WHERE country_code IN ('US', 'MX');
    ```

1. Run `EXPLAIN FILTER PUSHDOWN` to check that we're filtering out parts that don't include data that's relevant to the query.
    ```mzsql
    EXPLAIN FILTER PUSHDOWN FOR
    SELECT * FROM venues WHERE country_code IN ('US', 'MX');
    ```

{{< note >}}

As before, we're not guaranteed to see much or any benefit from filter pushdown on small collections... but for datasets of over a few gigabytes, we should reliably be able to filter down to a subset of the parts we'd otherwise need to fetch.

{{< /note >}}
