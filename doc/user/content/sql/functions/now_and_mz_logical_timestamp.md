---
title: "now and mz_logical_timestamp functions"
description: "In Materialize mz_logical_timestamp() acts the way now() does in most systems."
menu:
  main:
    parent: 'sql-functions'
---

In Materialize, `now()` doesn't represent the system time, as it does in most systems; it represents the time with timezone when the query was executed. It cannot be used when creating views.

`mz_logical_timestamp()` comes closer to what `now()` typically indicates. It represents the logical time at which a query executes. Its typical uses are:

* Internal debugging
* Defining [temporal filters](/sql/patterns/temporal-filters/)

## Internal debugging

`mz_logical_timestamp()` can be useful if you need to understand the time up to which data has been ingested by `materialized`. It corresponds to the timestamp column of [`SUBSCRIBE`](/sql/subscribe). Generally a `CREATE SOURCE` command will cause Materialize to ingest data and produce timestamps that correspond to milliseconds since the Unix epoch.

## Temporal filters

You can use `mz_logical_timestamp()` to define temporal filters for materialized view, which implement various windowing idioms.

For more information, see [Temporal Filters](/sql/patterns/temporal-filters/).

## Restrictions

You can only use `mz_logical_timestamp()` to establish a temporal filter in one of the following types of clauses:

* WHERE clauses, where `mz_logical_timestamp()` must be directly compared to [`numeric`](/sql/types/numeric) expressions not containing `mz_logical_timestamp()`
* A conjunction (AND), where `mz_logical_timestamp()` must be directly compared to [`numeric`](/sql/types/numeric) expressions not containing `mz_logical_timestamp()`.

  At the moment, you can't use the `!=` operator with `mz_logical_timestamp` (we're working on it).

## Example

### Temporal filter using mz_logical_timestamp()

<!-- This example also appears in temporal-filters -->
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

### Query using now()

The epoch extracted from `now()` is measured in seconds, so it's multiplied by 1000 to match the milliseconds in `mz_logical_timestamp()`.

```sql
SELECT * FROM valid
  WHERE insert_ts <= (extract(epoch from now()) * 1000);
```
```nofmt
 content |   insert_ts   |   delete_ts
---------+---------------+---------------
 goodbye | 1621279636402 | 1621279836402
 welcome | 1621279636400 | 1621279786400
```
