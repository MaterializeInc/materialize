---
title: "now and mz_now functions"
description: "In Materialize mz_now() acts the way now() does in most systems."
menu:
  main:
    parent: 'sql-functions'
---

In Materialize, `now()` doesn't represent the system time, as it does in most systems; it represents the time with timezone when the query was executed. It cannot be used when creating views.

`mz_now()` comes closer to what `now()` typically indicates. It represents the logical time at which a query executes. Its typical uses are:

* Internal debugging
* Defining [temporal filters](/sql/patterns/temporal-filters/)

## Internal debugging

`mz_now()` can be useful if you need to understand the time up to which data has been ingested by `materialized`. It corresponds to the timestamp column of [`SUBSCRIBE`](/sql/subscribe). Generally a `CREATE SOURCE` command will cause Materialize to ingest data and produce timestamps that correspond to milliseconds since the Unix epoch.

## Temporal filters

You can use `mz_now()` to define temporal filters for materialized view, which implement various windowing idioms.

For more information, see [Temporal Filters](/sql/patterns/temporal-filters/).

## Restrictions

You can only use `mz_now()` to establish a temporal filter in one of the following types of clauses:

* WHERE clauses, where `mz_now()` must be directly compared to [`mztimestamp`](/sql/types/mztimestamp) expressions not containing `mz_now()`
* A conjunction (AND), where `mz_now()` must be directly compared to [`mztimestamp`](/sql/types/mztimestamp) expressions not containing `mz_now()`.

  At the moment, you can't use the `!=` operator with `mz_now` (we're working on it).

## Example

### Temporal filter using mz_now()

<!-- This example also appears in temporal-filters -->
For this example, you'll need to create a sample data source and create a materialized view from it for later reference.

```sql
--Create a table of timestamped events.
CREATE TABLE events (
    content text,
    insert_t timestamp,
    delete_t timestamp
);

--Create a materialized view of events valid at a given logical time.
CREATE MATERIALIZED VIEW valid AS
SELECT content, insert_t, delete_t
FROM events
WHERE mz_now() >= insert_t
  AND mz_now() < delete_t;
```

Next, you'll populate the table with timestamp data.
The epoch extracted from `now()` is measured in seconds, so it's multiplied by 1000 to match the milliseconds in `mz_now()`.

```sql
INSERT INTO events VALUES (
    'hello',
    now(),
    now() + '100s'
);
INSERT INTO events VALUES (
    'welcome',
    now(),
    now() + '150s'
);
INSERT INTO events VALUES (
    'goodbye',
    now(),
    now() + '200s'
);
```

Then, before 100 seconds (or 1.67 minutes) elapse, run the following query to see all the records:

```sql
SELECT *, mz_now() FROM valid;
```
```nofmt
 content |        insert_t         |        delete_t         |    mz_now
---------+-------------------------+-------------------------+---------------
 hello   | 2022-09-27 23:52:34.831 | 2022-09-27 23:54:14.831 | 1664322794280
 goodbye | 2022-09-27 23:53:04.262 | 2022-09-27 23:56:24.262 | 1664322794280
 welcome | 2022-09-27 23:53:03.142 | 2022-09-27 23:55:33.142 | 1664322794280
(3 rows)
```

If you run this query again after 1.67 minutes from the first insertion, you'll see only two results, because the first result no longer satisfies the predicate.

### Query using now()

The epoch extracted from `now()` is measured in seconds, so it's multiplied by 1000 to match the milliseconds in `mz_now()`.

```sql
SELECT * FROM valid
  WHERE insert_t <= now();
```
```nofmt
 content |        insert_t         |        delete_t
---------+-------------------------+-------------------------
 goodbye | 2022-09-27 23:53:04.262 | 2022-09-27 23:56:24.262
 welcome | 2022-09-27 23:53:03.142 | 2022-09-27 23:55:33.142
```
