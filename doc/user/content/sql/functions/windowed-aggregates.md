---
title: "Windowed Aggregate Functions"
description: "Aggregates timestamp data into well-defined windows."
menu:
  main:
    parent: 'sql-functions'
---

When handling [`timestamp`]-valued data, you might want to perform aggregate
computations on values that occur within the same window of time, e.g. in
non-overlapping 5-minute intervals.

To support this, Materialize offers what we call "windowed aggregate functions."
These functions calculate which windows of time each row belongs to, providing a
value to `GROUP BY` in in aggregate functions.

Note that these functions greatly differ from Materialize's [windowing
idioms](/guides/temporal-filters/#windowing-idioms). Our windowing idioms let
users control which data views return as a function of `mz_logical_timestamp()`,
whereas windowed aggregate functions' primary function is to express `timestamp`
data as well-defined windows.

{{< experimental v0.11.1 />}}

## Return values

All windowed aggregate functions return a column with the name of the function
whose type is a [`record`](../../types/record) with the following fields:

Field | Type | Description
------|------|-------------
`window_start` | [`timestamp`] | The window's start time, inclusive.
`window_ned` | [`timestamp`] | The window's end time, not inclusive.

## Tumbling windows

Tumbling windows represent non-overlapping windows of a fixed size starting at
the Unix epoch.

{{< diagram "func-at-time-zone.svg" >}}

Parameter | Type | Description
----------|------|------------
_ts_ | [`timestamp`] | The value to place within the window The target time zone.
_i_ | [`interval`](../../types/interval) | The tumbling window's width.

### Input/output positions

`window_tumbling` is a scalar function and must be used in the list of values a
query returns.

### Examples

```sql
SELECT (window_tumbling(ts, INTERVAL '1m')).window_start, sum(v)
FROM (
    VALUES
        ('2001-01-01 01:02:34'::timestamp, 1),
        ('2001-01-01 01:02:35'::timestamp, 10),
        ('2001-01-01 01:03:35'::timestamp, 2)
) vals (ts, v)
GROUP BY window_start;
```
```nofmt
    window_start     | sum 
---------------------+-----
 2001-01-01 01:02:00 |  11
 2001-01-01 01:03:00 |   2
```
