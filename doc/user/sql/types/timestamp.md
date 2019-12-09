---
title: "timestamp Data Type"
description: "Expresses a date and time"
aliases:
    - /docs/sql/types/timestamptz
menu:
  main:
    parent: 'sql-types'
---

`timestamp` and `timestamptz` data expresses a date and time in UTC.

Detail | Info
-------|------
**Quick Syntax** | `TIMESTAMP '2007-02-01 15:04:05'` <br/> `TIMESTAMPTZ '2007-02-01 15:04:05+06'`
**Size** | 8 bytes
**Min value** | 4713 BC
**Max value** | 294276 AD
**Resolution** | 1 microsecond / 14 digits

## Syntax

### TIMESTAMP

{{< diagram "type-timestamp.html" >}}

### TIMESTAMPTZ

{{< diagram "type-timestamptz.html" >}}

Field | Use
------|-----
_tz&lowbar;offset_ | The timezone's distance, in hours, from UTC.

## Details

- `timestamp` and `timestamptz` store data in [UTC](https://en.wikipedia.org/wiki/Coordinated_Universal_Time).
- The difference between the two types is that `timestamptz` can read or write timestamps with the offset specified by the timezone. Importantly, `timestamptz` itself doesn't store any timezone data; Materialize simply performs the conversion from the time provided and UTC.
- Materialize assumes all clients expect UTC time, and does not currently support any other timezones.

### Valid casts

#### From `timestamp`

You can [cast](../../functions/cast) `timestamp` or `timestamptz` to:

- [`string`](../string)

#### To `timestamp`

You cannot cast `timestamp` or `timestamptz` to any other type.

Notably, you also cannot cast between `timestamp` and `timestamptz`.

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
