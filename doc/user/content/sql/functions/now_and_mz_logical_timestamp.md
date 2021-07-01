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

You can use `mz_logical_timestamp()` to define temporal filters for materialized view, or computations for fixed windows of time.

For more information, see [Temporal Filters: Enabling Windowed Queries in Materialize](https://materialize.com/temporal-filters/).

## Restrictions

You can only use `mz_logical_timestamp()` to establish a temporal filter in one of the following types of clauses:

* WHERE clauses, where `mz_logical_timestamp()` must be directly compared to [`numeric`](/sql/types/numeric) expressions not containing `mz_logical_timestamp()`
* A conjunction (AND), where `mz_logical_timestamp()` must be directly compared to [`numeric`](/sql/types/numeric) expressions not containing `mz_logical_timestamp()`.

At the moment, you can't use the `!=` operator with `mz_logical_timestamp` (we're working on it).

## Example

### Temporal filter using mz_logical_timestamp()

{{% mz_logical_timestamp_example %}}

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
