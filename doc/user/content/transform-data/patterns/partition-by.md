---
title: "PARTITION BY"
description: "Specifying the sort order of the data."
aliases:
  - /guides/partition-by/
  - /sql/patterns/partition-by/
menu:
  main:
    parent: 'sql-patterns'
---

A few types of Materialize collections are durably written to storage: [materialized views](/sql/create-materialized-view/), [tables](/sql/create-table), and [sources](/sql/create-source).

Internally, Materialize stores these durable collections in an [LSM-tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree)-like structure. Each collection is made up of a set of
**runs** of data, each run is sorted and then split up into individual **parts**, and those parts are written to object storage and retrieved only when necessary to satisfy a query. Materialize will also periodically **compact** the data it stores to consolidate small parts into larger ones or discard deleted rows.

Materialize lets you specify the ordering it will use to sort these runs of data internally. A well-chosen sort order can unlock optimizations like [filter pushdown](#filter-pushdown), which in turn can make queries and other operations more efficient.

## Syntax

The option `PARTITION BY <column list>` declares that a [materialized view](/sql/create-materialized-view/#with_options), [table](/sql/create-table/#with_options), or source table should be ordered by the listed columns. For example, a table that stores an append-only collection of events may look like:

```mzsql
CREATE TABLE events (created_at timestamptz, body jsonb)
WITH (
    PARTITION BY (created_at)
);
```

When multiple columns are specified, rows are ordered lexicographically:
`PARTITION BY (created_date, created_time)` would sort first by the created date, then order rows with equal created date by time. Durable collections without a `PARTITION BY` option might be stored in any order.

Note that this declaration has no impact on the order in which records are returned by queries. If you want to return results in a specific order, use an
`ORDER BY` clause on your select statement.

## Filter pushdown

As mentioned above, Materialize stores durable collections as a set of parts. If it can prove that a particular part in a collection is not needed to answer a particular query, it can skip fetching it, saving a substantial amount of time and computation. For example:

- If a materialized view has a temporal filter that preserves only the last two days of data, we'd like to filter out parts that contain data from a month ago.
- If a select statement filters for only data from a particular country, we'd like to avoid fetching parts that only contain data from other countries.

This optimization tends to be important for append-only timeseries datasets, but it can be useful for any dataset where most queries look at only a particular "range" of the data. If the query is a select statement, it can make that select statement more performant; if the query is an index or a materialized view, it can make it much faster to bootstrap.

Materialize will always try to filter out parts it doesn't need for a particular query, but that filtering is usually only effective when similar rows are stored together. If you want to make sure that filter pushdown reliably kicks in for your query, you should:

- Use a `PARTITION BY` clause on the relevant column to ensure that data with similar values for that column are stored close together.
- Add a filter to your query that only returns true for a particular range of values in that column.

Filters that consist of arithmetic, date math, and comparisons are generally eligible for pushdown, including all the examples in this page. However, more complex filters might not be. You can check whether the filters in your query can be pushed down by using [the
`filter_pushdown` option](/sql/explain-plan/#output-modifiers) in an `EXPLAIN` statement.

Some common functions, such as casting from a string to a timestamp, can prevent filter pushdown for a query. For similar functions that _do_ allow pushdown, see [the pushdown functions documentation](/sql/functions/pushdown/).

## Requirements

Materialize imposes some restrictions on the list of columns in the partition-by clause.

- This clause must list a prefix of the columns in the collection. For example, if you're creating a table that partitions by a single column, that column must be the first column in the table's schema definition.
- Only certain types of columns are supported. This includes:
    - all fixed-width integer types, including `smallint`, `integer`, and `bigint`;
    - date and time types, including `date`, `time`, `timestamp`, `timestamptz`, and `mz_timestamp`;
    - string types like `text` and `bytea`;
    - `boolean` and `uuid`;
    - `record` types where all fields types are supported.

We intend to relax some of these restrictions in the future.

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
    INSERT INTO events VALUES (now()::timestamp - '1 minute', 'hello');
    INSERT INTO events VALUES (now(), 'world');
    ```

1. If we run a select statement against the data sometime in the next couple of minutes, we return one row but not the other.
    ```mzsql
    SELECT * FROM events WHERE event_ts > mz_now() - '2 minutes';
    ```

1. An `EXPLAIN FILTER PUSHDOWN` statement shows that we're able to fetch a subset of the parts in our collection... only those parts that contain data with recent timestamps.
    ```mzsql
    EXPLAIN FILTER PUSHDOWN FOR
    SELECT * FROM events WHERE event_ts + '2 minutes' > mz_now();
    ```

If you wait a few minutes longer until there are no events that match the temporal filter, you'll notice that not only does the query return zero rows, but the explain shows that we fetched zero parts.

Note that the exact numbers you see here may very: parts can be much larger than a single row, and the actual level of filtering may vary for small datasets as data is compacted together internally. However, datasets of a few gigabytes or larger should reliably see benefits from this optimization.

### Partitioning by primary key

Other datasets don't have a strong timeseries component, but they do have a clear notion of type or category. For example, let's suppose we manage a collection of music venues spread across the world, but regularly want to target queries to just those that exist in a single country.

1. First, create a table called `venues`, partitioned by country.
    ```mzsql
    -- Create a table for our venue data.
    -- Once again, the partition column is listed first.
    CREATE TABLE venues (
        country_code text,
        id uuid,
    ) WITH (
        PARTITION BY (country_code)
    );
    ```

1. Insert a few records, one "older" record and one more recent.
    ```mzsql
    INSERT INTO venues VALUES ('US', uuid_generate_v5('venue', 'Rock World'));
    INSERT INTO venues VALUES ('CA', uuid_generate_v5('venue', 'Friendship Cove'));
    ```

1. We can filter down our list of venues to just those in a specific country.
    ```mzsql
    SELECT * FROM venues WHERE country_code = 'US';
    ```

1. An `EXPLAIN FILTER PUSHDOWN` statement shows that we're only fetching a subset of parts we've stored.
    ```mzsql
    EXPLAIN FILTER PUSHDOWN FOR
    SELECT * FROM venues WHERE country_code = 'US';
    ```

As before, we're not guaranteed to see much or any benefit from filter pushdown on small collections... but for datasets of over a few gigabytes, we should reliably be able to filter down to a subset of the parts we'd otherwise need to fetch.
