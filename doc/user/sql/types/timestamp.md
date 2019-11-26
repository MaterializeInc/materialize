---
title: "TIMESTAMP Data Type"
description: "Expresses a date and time"
menu:
  main:
    parent: 'sql-types'
---

`TIMESTAMP` and `TIMESTAMPTZ` data expresses a date and time in UTC.

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

- `TIMESTAMP` and `TIMESTAMPTZ` store data in [UTC](https://en.wikipedia.org/wiki/Coordinated_Universal_Time).
- The difference between the two types is that `TIMESTAMPTZ` displays the UTC time with the offset specified by the timezone. Importantly, `TIMESTAMPTZ` itself doesn't store any timezone data; it merely enables displaying its values in a time other than UTC.
- Materialize assumes all clients expect UTC time, and does not currently support any other timezones.

## Examples

```sql
SELECT TIMESTAMP '2007-02-01 15:04:05' AS ts_v;
```
```shell
        ts_v
---------------------
 2007-02-01 15:04:05
```

<hr/>

```sql
SELECT TIMESTAMPTZ '2007-02-01 15:04:05+06' AS tstz_v;
```
```shell
         tstz_v
-------------------------
 2007-02-01 09:04:05 UTC
```
