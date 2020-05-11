---
title: "Timestamp Data Types"
description: "Expresses a date and time"
menu:
  main:
    parent: 'sql-types'
aliases:
    - /docs/sql/types/timestamptz
---

`timestamp` and `timestamp with time zone` data expresses a date and time in UTC.

Detail | Info
-------|------
**Quick Syntax** | `TIMESTAMP '2007-02-01 15:04:05'` <br/> `TIMESTAMP WITH TIME ZONE '2007-02-01 15:04:05+06'`
**Size** | 8 bytes
**Min value** | 4713 BC
**Max value** | 294276 AD
**Resolution** | 1 microsecond / 14 digits

For convenience and compatibility with PostgreSQL, `timestamptz` is accepted
as an alias for `timestamp with time zone`.

## Syntax

{{< diagram "type-timestamp.html" >}}

Field | Use
------|-----
**WITH TIME ZONE** | Apply the _tz&lowbar;offset_ field. If not specified, don't.
**TIMESTAMPTZ** | Apply the _tz&lowbar;offset_ field.
_date&lowbar;str_ | A string representing a date in `Y-M-D` format.
_time&lowbar;str_ | A string representing a time of day in `H:M:S.NS` format.
_tz&lowbar;offset_ | The timezone's distance, in hours, from UTC.

## Details

- `timestamp` and `timestamptz` store data in [UTC](https://en.wikipedia.org/wiki/Coordinated_Universal_Time).
- The difference between the two types is that `timestamptz` can read or write timestamps with the offset specified by the timezone. Importantly, `timestamptz` itself doesn't store any timezone data; Materialize simply performs the conversion from the time provided and UTC.
- Materialize assumes all clients expect UTC time, and does not currently support any other timezones.

### Valid casts

#### From `timestamp`

You can [cast](../../functions/cast) `timestamp` or `timestamptz` to:

- [`date`](../date)
- [`text`](../text)
- `timestamp`
- `timestamptz`

#### To `timestamp`

You can [cast](../../functions/cast) the following types to `timestamp` or `timestamptz`:

- [`date`](../date)
- [`text`](../text)
- `timestamp`
- `timestamptz`

### Valid operations

`timestamp` data supports the following operations with other types.

Operation | Computes
----------|------------
[`date`](../date) `+` [`interval`](../interval) | [`timestamp`](../timestamp)
[`date`](../date) `-` [`interval`](../interval) | [`timestamp`](../timestamp)
[`date`](../date) `+` [`time`](../time) | [`timestamp`](../timestamp)
[`timestamp`](../timestamp) `+` [`interval`](../interval) | [`timestamp`](../timestamp)
[`timestamp`](../timestamp) `-` [`interval`](../interval) | [`timestamp`](../timestamp)
[`timestamp`](../timestamp) `-` [`timestamp`](../timestamp) | [`interval`](../interval)

## Examples

```sql
SELECT TIMESTAMP '2007-02-01 15:04:05' AS ts_v;
```
```nofmt
        ts_v
---------------------
 2007-02-01 15:04:05
```

<hr/>

```sql
SELECT TIMESTAMPTZ '2007-02-01 15:04:05+06' AS tstz_v;
```
```nofmt
         tstz_v
-------------------------
 2007-02-01 09:04:05 UTC
```
