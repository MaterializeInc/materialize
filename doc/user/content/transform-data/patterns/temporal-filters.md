---
title: "Temporal filters (sliding, hopping & tumbling windows)"
description: "Perform time-windowed computation over temporal data."
aliases:
  - /guides/temporal-filters/
  - /sql/patterns/temporal-filters/
menu:
  main:
    parent: 'sql-patterns'
---

Temporal filters allow you to implement several windowing idioms (tumbling, hopping, and sliding), in addition to more nuanced temporal queries.

A "temporal filter" is a `WHERE` or `HAVING` clause which uses the function [`mz_now`](/sql/functions/now_and_mz_now) to represent the current time through which your data is viewed.
Temporal filters have their own name because they are one of very few instances where you can materialize a view containing a function that changes on its own rather than as a result of changes to your data.
Even if your input data does not change, your query results can change as the time represented by `mz_now()` changes.

## Details

You can only use `mz_now()` to establish a temporal filter under the following conditions:

-   `mz_now` appears in a `WHERE` or `HAVING` clause.
-   The clause must directly compare `mz_now()` to a [`numeric`](/sql/types/numeric) expression not containing `mz_now()`,
    or be part of a conjunction phrase (`AND`) which directly compares `mz_now()` to a [`numeric`](/sql/types/numeric) expression not containing `mz_now()`.
-   The comparison must be one of `=`, `<`, `<=`, `>`, or `>=`, or operators that desugar to them or a conjunction of them (for example, `BETWEEN`).

    At the moment, you can't use the `!=` operator with `mz_now()`.

Let's take a look at an example.

The `SELECT` query below uses one value of `mz_now()` to filter out records inserted after or deleted before that value, which is filled in with the time at which the query executes.

```sql
SELECT count(*)
FROM events
WHERE mz_now() >= insert_ms
  AND mz_now() < delete_ms;
```

The query counts `events` with `insert_ms` before or at the time represented by `mz_now()` and `delete_ms` after that time.

We can create a materialized view of the same query:

```sql
CREATE MATERIALIZED VIEW active_events AS
SELECT count(*)
FROM events
WHERE mz_now() >= insert_ms
  AND mz_now() < delete_ms;
```

At each logical time, the view `active_events` contains exactly the results of the `SELECT` query above.
As `mz_now()` moves forward in time, the events included change, newer events now meeting the time requirement and older events no longer doing so.
You do not need to insert a deletion event and can rely on the query to maintain the result for you as time advances.

## Windowing idioms

Temporal filters allow you to implement several windowing idioms for data that comes with logical timestamps.
The patterns discussed below are all instances of temporal filters. Each pattern is a generalization of the last.

### Tumbling windows

**Tumbling windows** are what we call windows when their duration equals their period (the amount of time before a new window begins). This creates fixed-size, contiguous, non-overlapping time intervals where each record belongs to exactly one interval.

Here we specify a tumbling window with the duration and period `PERIOD_MS`:

```sql
CREATE MATERIALIZED VIEW tumbling AS
SELECT content, insert_ms
FROM events
-- The event should appear in only one interval of duration `PERIOD_MS`.
-- The interval begins here ...
WHERE mz_now() >= PERIOD_MS * (insert_ms / PERIOD_MS)
-- ... and ends here.
  AND mz_now() < PERIOD_MS * (1 + insert_ms / PERIOD_MS)
```

Tumbling windows are useful for maintaining constantly refreshed answers to questions like "How many orders did we receive during each hour of each day?"

### Hopping windows

**Hopping windows** are windows whose duration is an integer multiple of their period. This creates fixed-size windows that may overlap, with records belonging to multiple windows.

{{< note >}}
In some systems, these are called "sliding windows". Materialize reserves that term for a different use case, discussed below.
{{< /note >}}

Here we specify a hopping window that has the period `PERIOD_MS` and covers `INTERVALS` number of intervals:

```sql
CREATE MATERIALIZED VIEW hopping AS
SELECT content, insert_ms
FROM events
-- The event should appear in `INTERVALS` intervals each of width `PERIOD_MS`.
-- The interval begins here ...
WHERE mz_now() >= PERIOD_MS * (insert_ms / PERIOD_MS)
-- ... and ends here.
  AND mz_now() < INTERVALS * (PERIOD_MS + insert_ms / PERIOD_MS)
```

Note that when `INTERVALS` is one, this query is identical to the query above it.

### Sliding windows

{{< note >}}
In some systems, "sliding windows" are used to mean windows whose duration is an integer multiple of their period.
At Materialize, we call those "hopping windows" instead.
{{< /note >}}

**Sliding windows** are windows whose period approaches the limit of 0. This creates fixed-size windows that appear to slide continuously forward in time. Records may belong to more than one interval.

Here we specify a sliding windows that has the duration `INTERVAL_MS`:

```sql
CREATE MATERIALIZED VIEW sliding AS
SELECT content, insert_ms
FROM events
-- The event should appear inside the interval that begins at
-- `insert_ms` and ends at  `insert_ms + INTERVAL_MS`.
-- The interval begins here ..
WHERE mz_now() >= insert_ms
-- ... and ends here.
  AND mz_now() < insert_ms + INTERVAL_MS
```

Sliding windows are useful for maintaining the answers to questions like "How many orders did we get in the past five minutes?"

## Grace periods

Obviously, a record must be present for it to pass a temporal filter.
If a record is presented to Materialize at a time later than its `insert_ms` column (or the equivalent), it will only be included at the later time.

Streaming data often arrives just a bit later than intended. You can account for this in the temporal filter by introducing a grace period, adding a fixed
amount to each expression you compare to `mz_now()`.

If you think your data may arrive up to ten seconds late, you could add a grace period of ten seconds to each time.
This holds back the production of correct answers by ten seconds, but does not omit data up to ten seconds late.

```sql
-- Use a grace period to allow for records as late as `GRACE_MS`.
CREATE MATERIALIZED VIEW grace AS
SELECT content, insert_ms, delete_ms
FROM events
WHERE mz_now() >= insert_ms + GRACE_MS
  AND mz_now() < delete_ms + GRACE_MS;
```

Grace periods can be applied to any of the moving window idioms.

## Example

### Windowing

<!-- This example also appears in now_and_mz_now -->

For this example, you'll need to create a sample data source and create a materialized view from it for later reference.

```sql
--Create a table of timestamped events.
CREATE TABLE events (
    content text,
    insert_ms numeric,
    delete_ms numeric
);

--Create a materialized view of events valid at a given logical time.
CREATE MATERIALIZED VIEW valid AS
SELECT content, insert_ms, delete_ms
FROM events
WHERE mz_now() >= insert_ms
  AND mz_now() < delete_ms;
```

Next, you'll populate the table with timestamp data.
The epoch extracted from `now()` is measured in seconds, so it's multiplied by 1000 to match the milliseconds in `mz_now()`.

```sql
INSERT INTO events VALUES (
    'hello',
    extract(epoch from now()) * 1000,
    (extract(epoch from now()) * 1000) + 100000
);
INSERT INTO events VALUES (
    'welcome',
    extract(epoch from now()) * 1000,
    (extract(epoch from now()) * 1000) + 150000
);
INSERT INTO events VALUES (
    'goodbye',
    (extract(epoch from now()) * 1000),
    (extract(epoch from now()) * 1000) + 200000
);
```

Then, before 100,000 ms (or 1.67 minutes) elapse, run the following query to see all the records:

```sql
SELECT *, mz_now() FROM valid;
```

```nofmt
 content |   insert_ms   |   delete_ms   | mz_now
---------+---------------+---------------+----------------------
 hello   | 1620853325858 | 1620853425858 |        1620853337180
 goodbye | 1620853325862 | 1620853525862 |        1620853337180
 welcome | 1620853325860 | 1620853475860 |        1620853337180
(3 rows)
```

If you run this query again after 1.67 minutes from the first insertion, you'll see only two results, because the first result no longer satisfies the predicate.

### Bucketing

It is common to use buckets to represent the time window where a row belongs. Materialize [date functions](/sql/functions/#date-and-time-func) will help us here. <br/>
Let's continue with the last example:

```sql
CREATE MATERIALIZED VIEW valid_buckets AS
SELECT
  content,
  insert_ms,
  delete_ms,
  -- Trunc timestamps to the minute and use them as buckets
  DATE_TRUNC('minute', to_timestamp(insert_ms / 1000)) as insert_bucket,
  DATE_TRUNC('minute', to_timestamp(delete_ms / 1000)) as delete_bucket
FROM events
WHERE mz_now() >= insert_ms
  AND mz_now() < delete_ms;
```

Re-insert some rows to make sure there is data available, and run the following query to see all the records and their respective buckets:

```sql
SELECT * FROM valid_buckets;
```

```nofmt
 content |   insert_ms   |   delete_ms   |     insert_bucket      |     delete_bucket
---------+---------------+---------------+------------------------+------------------------
 goodbye | 1647551752960 | 1647551952960 | 2022-03-17 21:15:00+00 | 2022-03-17 21:19:00+00
 hello   | 1647551752687 | 1647551852687 | 2022-03-17 21:15:00+00 | 2022-03-17 21:17:00+00
 welcome | 1647551752694 | 1647551902694 | 2022-03-17 21:15:00+00 | 2022-03-17 21:18:00+00
(3 rows)
```

You can then use these time buckets in aggregations:

```sql
SELECT
  insert_bucket,
  COUNT(1)
FROM valid_buckets
GROUP BY 1;
```

```nofmt
     insert_bucket      | count
------------------------+-------
 2022-03-17 21:15:00+00 |     3
(1 row)
```

For this case, querying and aggregating is relatively effortless;
if the aggregation query were expensive, creating a materialized view doing the aggregation would make more sense.

## Temporal filter pushdown

{{< alpha />}}

In the example above, all of the temporal queries we’ve written only return results based on recently-added events: for example, our sliding window query will only return events that were inserted within the last `INTERVAL_MS`. This is a very common pattern; it’s much more likely we’re interested in the last five minutes of data than in five minutes of data from a year ago!

Materialize can "push down" filters that match this pattern all the way down to its storage layer, skipping over old data that’s not relevant to the query. For `SELECT` statements, this can substantially improve query latency. Temporal filters of materialized views can reduce the time it takes to start serving results after being created or after the cluster is restarted.

For this optimization to apply to your query or view, two things need to be true:

- The columns filtered should correlate with the insertion or update time of the row.

  In the examples above, the values `insert_ms` and `delete_ms` in each event correlate with the time the event was inserted. Filters that reference these columns should reduce query latency.

  However, the values in our `content` column are not
  correlated with insertion time in any way, and filters
  against `content` will probably not be pushed down.
- Temporal filters that consist of arithmetic, date math, and comparisons are eligible for pushdown, including all the examples in this page.

  However, more complex filters might not be. You can check whether the filters in your query can be pushed down by using [the `filter_pushdown` option](../../../sql/explain/#output-modifiers) to `EXPLAIN`.

  For example:

  ```sql
  EXPLAIN WITH(filter_pushdown)
  SELECT count(*)
  FROM events
  WHERE mz_now() >= insert_ms
  AND mz_now() < delete_ms;
  ----
  Explained Query:
  [...]
  Source materialize.public.events
    filter=((mz_now() >= numeric_to_mz_timestamp(#1)) AND (mz_now() < numeric_to_mz_timestamp(#2)))
    pushdown=((mz_now() >= numeric_to_mz_timestamp(#1)) AND (mz_now() < numeric_to_mz_timestamp(#2)))
  ```

  Both of the filters in our query appear in the `pushdown=` list at the bottom of the output, so our filter pushdown optimization will be able to filter out irrelevant ranges of data in that source and make the overall query more efficient.
