---
title: "now and mz_logical_timestamp functions"
description: "In Materialize mz_logical_timestamp() acts the way now() does in most systems."
menu:
  main:
    parent: 'sql-functions'
---

In Materialize, `now()` doesn't represent the system time, as it does in most systems; it represents the time with timezone when the query was executed. It cannot be used when creating views.

`mz_logical_timestamp()` comes closer to what `now()` typically indicates. It represents the logical time at which the query executes, based on the system time defined by the system on which `materialized` is installed. Its typical uses are:

* Internal debugging
* Defining [temporal filters](https://materialize.com/temporal-filters/)

## Internal debugging

`mz_logical_timestamp()` can be useful if you need to understand the time up to which data has been ingested by `materialized`. It corresponds to the timestamp column of [TAIL](/sql/tail). Generally a `CREATE SOURCE` command will cause Materialize to ingest data and produce timestamps that correspond to milliseconds since the Unix epoch.

## Temporal filters

You can use `mz_logical_timestamp()` to define temporal filters for materialized view, or computations for fixed windows of time. For more information, see [Temporal Filters: Enabling Windowed Queries in Materialize](https://materialize.com/temporal-filters/).

## Example

### Temporal filter using mz_logical_timestamp()

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
