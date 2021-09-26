---
title: "Temporal Filters"
description: "Perform time-windowed computation over temporal data."
weight: 30
menu:
  main:
    parent: guides
---

"Temporal filter" is the name given to `WHERE` or `HAVING` clauses containing the function [`mz_logical_timestamp`](/sql/functions/now_and_mz_logical_timestamp), which represents the current time through which your data are changing.
Temporal filters have their own name because they are one of very few instances where you can materialize a view containing a function that changes on its own, rather than as a result of changes to your data.
Even if your input data do not change, your query results can change as `mz_logical_timestamp()` itself changes.

Temporal filters allow you to describe *when* records should be included, rather than *which* records should be included.
This is very powerful, and allows you to implement several windowing idioms (tumbling, stepping, and sliding), in addition to more nuanced temporal queries.
We will walk through each of these idioms in the [Examples](idk) section.

## Demonstration

The following `SELECT` query will use one value of `mz_logical_timestamp()` to filter out events inserted after or deleted before that value.
The value is filled in to be the moment at which the query executes (which also determines the contents of `events`).
The query counts the records with `insert_ms` less or equal to that time and `delete_ms` after that time.
```sql
SELECT count(*)
FROM events
WHERE mz_logical_timestamp() >= insert_ms
  AND mz_logical_timestamp() < delete_ms;
```

The above query also explains not only *if* each event should be be included, but *when* each should be included as `mz_logical_timestamp()` moves forward.
We can create a materialized view of the same query:
```sql
CREATE MATERIALIZED VIEW active_events AS
SELECT count(*)
FROM events
WHERE mz_logical_timestamp() >= insert_ms
  AND mz_logical_timestamp() < delete_ms;
```
The view `active_events` will contain exactly the results of the `SELECT` query above, at each logical time.
The view will automatically update itself as `mz_logical_timestamp()` advances, introducing and eventually removing each event, based on their `insert_ms` and `delete_ms` columns.
You would not need to insert a deletion event, and can rely on the query to maintain the result for you as time advances.

<!-- You can use temporal filters to implement several time-windowing idioms.
You can use temporal filters to implement time-windowed computations over temporal data, such as creating self-updating views that report on "Updates from the last ten seconds" or "Orders placed more than a day but less than a week ago".
You can use temporal filters to implement "event time", in which fields of your record indicate when they should first be included, and when they should be removed.

Windows can be sliding or tumbling. Sliding windows are fixed-size time intervals that you drag over temporal data, like "Show me updates from the last ten seconds". Tumbling or hopping windows are sliding windows that slide one unit at a time, like "Show me updates from the last day for each one-hour interval".

Temporal filters are defined using the function [`mz_logical_timestamp`](/sql/functions/now_and_mz_logical_timestamp). For a more detailed overview, see our blog post on [temporal filters](https://materialize.com/temporal-filters/). -->

## Restrictions

You can only use `mz_logical_timestamp()` to establish a temporal filter in a `WHERE` or `HAVING` clause.
The clause must directly compare `mz_logical_timestamp()` to a [`numeric`](/sql/types/numeric) expression not containing `mz_logical_timestamp()`,
or be part of a conjunction phrase (`AND`) which directly compares `mz_logical_timestamp()` to a [`numeric`](/sql/types/numeric) expression not containing `mz_logical_timestamp()`.
The comparison must be one of `=`, `<`, `<=`, `>`, or `>=`, or operators that desugar to them or a conjunction of them (for example, `BETWEEN`).
At the moment, you can't use the `!=` operator with `mz_logical_timestamp()` (we're working on it).

## Late Data

A record must be present for it to pass a temporal filter.
If a record is presented to Materialize at a time later than its `insert_ms` column (or the equivalent) it will only be included at the later time.
Temporal filters are not able to move records backwards in time and cause them to appear earlier than they arrive.

You can introduce a "grace period" by adding a fixed amount to each expression you compare to `mz_logical_timestamp()`.

## Example

<!-- This example also appears in now_and_mz_logical_timestamp -->
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
WHERE mz_logical_timestamp() >= insert_ms
  AND mz_logical_timestamp() < delete_ms;
```

Next, you'll populate the table with timestamp data.
The epoch extracted from `now()` is measured in seconds, so it's multiplied by 1000 to match the milliseconds in `mz_logical_timestamp()`.

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
SELECT *, mz_logical_timestamp() FROM valid;
```
```nofmt
 content |   insert_ms   |   delete_ms   | mz_logical_timestamp
---------+---------------+---------------+----------------------
 hello   | 1620853325858 | 1620853425858 |        1620853337180
 goodbye | 1620853325862 | 1620853525862 |        1620853337180
 welcome | 1620853325860 | 1620853475860 |        1620853337180
(3 rows)
```

If you run this query again after 1.67 minutes from the first insertion, you'll see only two results, because the first result no longer satisfies the predicate.

### Windows

Temporal filters allow you to implement several "windowing" idioms for data bearing logical timestamps.
"Tumbling" windows are defined by a period of time, and at each for each disjoint interval of time produce exactly the records with timestamps in that interval.
"Stepping" windows also have a period of time, but include all records with timestamps in a fixed number of intervals (more than one interval, if not a tumbling window).
"Sliding" windows cover a fixed interval of time, and at each time contain exactly the records with timestamp within that interval behind the time.
These patterns are each slight generalizations of the next, and all are instances of temporal filters.

Tumbling windows are defined by their period, here `PERIOD_MS` in milliseconds:
```sql
CREATE MATERIALIZED VIEW tumbling AS
SELECT content, insert_ms
FROM events
-- The event should appear in only one interval of width `PERIOD_MS`.
WHERE mz_logical_timestamp() >= PERIOD_MS * (insert_ms / PERIOD_MS)
  AND mz_logical_timestamp() < PERIOD_MS * (1 + insert_ms / PERIOD_MS)
```

Stepping windows are defined by their period and the number of intervals, here `PERIOD_MS` and `INTERVALS`:
```sql
CREATE MATERIALIZED VIEW stepping AS
SELECT content, insert_ms
FROM events
-- The event should appear in `INTERVALS` intervals each of width `PERIOD_MS`.
WHERE mz_logical_timestamp() >= PERIOD_MS * (insert_ms / PERIOD_MS)
  AND mz_logical_timestamp() < PERIOD_MS * (INTERVALS + insert_ms / PERIOD_MS)
```
Notice that when `INTERVALS` is one, a stepping window would be a tumbling window, and this query is identical to the query above it.

Sliding windows are defined by their interval of time, here `INTERVAL_MS`:
```sql
CREATE MATERIALIZED VIEW sliding AS
SELECT content, insert_ms
FROM events
-- The event should appear for exactly the next `INTERVAL_MS`.
WHERE mz_logical_timestamp() >= insert_ms
  AND mz_logical_timestamp() < insert_ms + INTERVAL_MS
```

### Grace Periods

Streaming data often arrives just a bit later than intended.
If you think your data may arrive up to ten seconds late, you could add a grace period of ten seconds to each time.
This holds back the production of correct answers by ten seconds, but does not omit data up to ten seconds late.
```sql
-- Use a grace period to allow for records as late as `GRACE_MS`.
CREATE MATERIALIZED VIEW grace AS
SELECT content, insert_ms, delete_ms
FROM events
WHERE mz_logical_timestamp() >= insert_ms + GRACE_MS
  AND mz_logical_timestamp() < delete_ms + GRACE_MS;
```

Grace periods can be applied to each of the moving window idioms as well.
