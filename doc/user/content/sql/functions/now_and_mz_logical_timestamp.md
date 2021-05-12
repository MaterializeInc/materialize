---
title: "now and mz_logical_timestamp functions"
description: "In Materialize mz_logical_timestamp() acts the way now() does in most systems."
menu:
  main:
    parent: 'sql-functions'
---

In Materialize, `now()` doesn't represent the system time; it represents the time with timezone when the query was executed. By contrast, `mz_logical_timestamp()` represents the logical time at which the query executes.

In most Materialize queries, you should use `mz_logical_timestamp()` where you might use `now()` in other systems, for example, when defining [temporal filters](https://materialize.com/temporal-filters/) or when creating materialized views from multiple sources with potentially conflicting system times.

## Example

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
  AND mz_logical_timestamp()  < delete_ts;
```

Next, you'll populate the table with timestamp data.

```sql
insert into events VALUES (
    'hello',
    mz_logical_timestamp(),
    mz_logical_timestamp() + 100000
);
insert into events VALUES (
    'welcome',
    mz_logical_timestamp(),
    mz_logical_timestamp() + 150000
);
insert into events VALUES (
    'goodbye',
    mz_logical_timestamp(),
    mz_logical_timestamp() + 200000
);
```

Then, before 100,000 ms (or 1.67 minutes) elapse, run the following query to see all the records:

```sql
SELECT *, mz_logical_timestamp() FROM valid;
```
```
content |   insert_ts   |   delete_ts   | mz_logical_timestamp
---------+---------------+---------------+----------------------
 hello   | 1620853325858 | 1620853425858 |        1620853337180
 goodbye | 1620853325862 | 1620853525862 |        1620853337180
 welcome | 1620853325860 | 1620853475860 |        1620853337180
(3 rows)
```

If you run the query again after 1.67 minutes, you'll see only two results, because the first result has aged out of the view.
