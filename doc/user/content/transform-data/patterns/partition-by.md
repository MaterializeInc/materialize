---
title: "Partitioning and filter pushdown"
description: "Declare how collections are stored."
aliases:
  - /guides/partition-by/
  - /sql/patterns/partition-by/
  - /self-managed/v25.2/transform-data/patterns/partition-by/
menu:
  main:
    parent: 'sql-patterns'
---

[//]: # "TODO link to the source table docs once that feature is documented."

A few types of Materialize collections are durably written to storage: [materialized views](/sql/create-materialized-view/), [tables](/sql/create-table), and [sources](/sql/create-source).

Internally, each collection is stored as a set of **runs** of data, each of which is sorted and then partitioned up into individual **parts**, and those parts are written to object storage and fetched only when necessary to satisfy a query. Materialize will also periodically **compact** the data it stores, to consolidate small parts into larger ones or discard deleted rows.

For [materialized views](/sql/create-materialized-view/) and
[tables](/sql/create-table) (including read-only tables created from sources),
you can use the `PARTITION BY` option to declare the **expected** internal
ordering of the data. If the data has that ordering, optimizations like [filter
pushdown](#filter-pushdown) can be more effective, which in turn can make
queries and other operations more efficient.

{{< warning >}}
The `PARTITION BY` option declares the expected layout of your data. It does not
change how the data is stored. Materialize validates the option against the
[requirements](#requirements) below, but otherwise stores your data as it would
without the option. As a result, adding or removing `PARTITION BY` does not
affect query performance.

The requirements are what make this possible. The option can only name a prefix
of the collection's columns, which is the ordering Materialize already uses
internally, so a valid `PARTITION BY` clause never asks for a layout that
differs from the default one. The option records your expectation so that
Materialize can preserve it, and it lets you find out at creation time if the
ordering you want is not one Materialize can provide.

If you are adding `PARTITION BY` to make a specific query faster, see [Filter
pushdown](#filter-pushdown) instead: whether pushdown helps depends on your data
and your filters, not on this option.
{{< /warning >}}

{{< note >}}
The `PARTITION BY` option has no impact on the order in which records are returned by queries.
If you want to return results in a specific order, use an `ORDER BY` clause on your [`SELECT` statement](/sql/select/).
{{< /note >}}

## Syntax

The option `PARTITION BY <column list>` declares that a [materialized view](/sql/create-materialized-view/#syntax) or [table](/sql/create-table/) should be partitioned by the listed columns.
For example, a table that stores an append-only collection of events may want to partition the data by time:

```mzsql
CREATE TABLE events (event_ts timestamptz, body jsonb)
WITH (
    PARTITION BY (event_ts)
);
```

This `PARTITION BY` clause declares that events with similar `event_ts` timestamps should be stored together.

{{< note >}}
The `PARTITION BY` option described here is unrelated to the `PARTITION BY`
option of [`CREATE SINK ... INTO KAFKA`](/sql/create-sink/kafka/#partitioning),
which chooses the Kafka partition that a sink writes each row to.
{{< /note >}}

When multiple columns are specified, rows are partitioned lexicographically.
For example, `PARTITION BY (event_date, event_time)` would partition first by the created date;
if many rows have the same `event_date`, those rows would be partitioned by the `event_time` column.
Durable collections without a `PARTITION BY` option can be partitioned arbitrarily.

{{< note >}}
The `PARTITION BY` option does not mean that rows with different values for the specified columns will be stored in different parts, only that rows with similar values for those columns should be stored together.
{{< /note >}}

## Requirements

Materialize currently imposes some restrictions on the list of columns in the `PARTITION BY` clause.
These restrictions describe the orderings Materialize can provide, and are enforced when you create the object.

- This clause must list a prefix of the columns in the collection. For example:
  - if you're creating a table that partitions by a single column, that column must be the first column in the table's schema definition;
  - if you're creating a table that partitions by two columns, those columns must be the first two columns in the table's schema definition and listed in the same order.
- Only certain types of columns are supported. This includes:
    - all fixed-width integer types, including `smallint`, `integer`, and `bigint`;
    - date and time types, including `date`, `time`, `timestamp`, `timestamptz`, and `mz_timestamp`;
    - string types like `text` and `bytea`;
    - `boolean` and `uuid`;
    - `record` types where all fields types are supported.


## Filter pushdown

Suppose that our example `events` table has accumulated years' worth of data, but we're running a query with a [temporal filter](/transform-data/patterns/temporal-filters/) that matches only rows with recent timestamps.

```mzsql
SELECT * FROM events WHERE mz_now() <= event_ts + INTERVAL '5min';
```

This query returns only rows with similar values for `event_ts`: timestamps in the last five minutes.
If rows with similar `event_ts` values are stored close together, the rows that pass this filter live in a small subset of parts, and Materialize can skip fetching the rest.

Materialize tracks a small amount of metadata for every part, including the range of possible values for many columns. When it can determine that none of the data in a part will match a filter, it will skip fetching that data from object storage. This optimization is called _filter pushdown_, and when you're querying with a selective filter against a large collection, it can save a great deal of time and computation.

Materialize always attempts to apply filter pushdown, but it is most effective when similar rows are stored together.
Whether rows are stored together depends on your data and the order in which the data was written.
You cannot currently control this layout. `PARTITION BY` declares the layout you expect rather than creating it.

To maximize the effectiveness of filter pushdown, you can:

- Add a filter that only matches a narrow range of values in a single column.
- Filter on a column that appears early in the collection's column list, and whose values correlate with the order in which rows were written. A timestamp on an append-only collection is a straightforward example, as is an identifier that increases over time (e.g., UUIDv7).

To measure the effectiveness of filter pushdown, use [`EXPLAIN FILTER PUSHDOWN`](/sql/explain-filter-pushdown/) to see the number of parts and bytes your query would need to fetch.

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

The `PARTITION BY` clauses below declare the ordering each collection expects. Because the option does not change how data is stored, these examples store and fetch the same data without them. The clause still records the expected ordering, and Materialize validates it when you create the object.

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

As before, filter pushdown on small collections may provide little or no benefit. With larger datasets, filter pushdown can reduce the number of parts that need to be fetched. However, a category column like `country_code` is less favorable for filter pushdown than a timestamp: venues from the same country aren't necessarily grouped into the same parts, so the benefit depends on the order in which rows arrived.

{{< /note >}}
