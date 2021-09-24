---
title: "Temporal Filters"
description: "Perform time-windowed computation over temporal data."
weight: 30
menu:
  main:
    parent: guides
---

You can use temporal filters to perform time-windowed computations over temporal data, such as creating self-updating views that report on "Updates from the last ten seconds" or "Orders placed more than a day but less than a week ago".

Windows can be sliding or tumbling. Sliding windows are fixed-size time intervals that you drag over temporal data, like "Show me updates from the last ten seconds". Tumbling or hopping windows are sliding windows that slide one unit at a time, like "Show me updates from the last day for each one-hour interval".

Temporal filters are defined using the function [`mz_logical_timestamp`](/sql/functions/now_and_mz_logical_timestamp), which represents the logical time at which the query executes, based on the system time defined by the system on which `materialized` is installed. For a more detailed overview, see our blog post on [temporal filters](https://materialize.com/temporal-filters/).

## Restrictions

You can only use `mz_logical_timestamp()` to establish a temporal filter in the following situations:

* In `WHERE` clauses, where `mz_logical_timestamp()` must be directly compared to [`numeric`](/sql/types/numeric) expressions not containing `mz_logical_timestamp()`
* As part of a conjunction phrase (`AND`), where `mz_logical_timestamp()` must be directly compared to [`numeric`](/sql/types/numeric) expressions not containing `mz_logical_timestamp()`.

At the moment, you can't use the `!=` operator with `mz_logical_timestamp` (we're working on it).

## Late-arriving data

Records that arrive late (that is, with a timestamp that is too old to pass the filter) are not shown in the current window. Think of a temporal filter as being like a query to a database for records updated between certain timestamps: if the record is added to the database with a time before the earlier timestamp boundary, it will not appear in the query results.

## Interactions with Materialize compaction

Materialize periodically [compacts](/ops/deployment/#compaction) data to prevent memory usage from growing without bounds. This compaction does not affect temporal filters.

## Example

<!-- This example also appears in now_and_mz_logical_timestamp -->
For this example, you'll need to create a sample data source and create a materialized view from it for later reference.

```sql
--Create table
CREATE TABLE events (
    content text,
    insert_ts numeric,
    delete_ts numeric
);
--Create materialized view
CREATE MATERIALIZED VIEW valid AS
SELECT content, insert_ts, delete_ts
FROM events
WHERE mz_logical_timestamp() >= insert_ts
  AND mz_logical_timestamp() < delete_ts;
```

Next, you'll populate the table with timestamp data. The epoch extracted from `now()` is measured in seconds, so it's multiplied by 1000 to match the milliseconds in `mz_logical_timestamp()`.

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
content |   insert_ts   |   delete_ts   | mz_logical_timestamp
---------+---------------+---------------+----------------------
 hello   | 1620853325858 | 1620853425858 |        1620853337180
 goodbye | 1620853325862 | 1620853525862 |        1620853337180
 welcome | 1620853325860 | 1620853475860 |        1620853337180
(3 rows)
```

If you run the query again after 1.67 minutes, you'll see only two results, because the first result has aged out of the view.

## Related pages

* [`now` and `mz_logical_timestamp`](/sql/functions/now_and_mz_logical_timestamp)
