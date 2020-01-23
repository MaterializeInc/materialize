---
title: "date_trunc Function"
description: "Truncates a timestamp at the specified time component"
menu:
  main:
    parent: 'sql-functions'
---

`date_trunc` returns a [`timestamp`](../../types/timestamp) with the "floor value" of the specified time component, i.e. the largest time component less than or equal to the provided value.

## Signatures

{{< diagram "func-date-trunc.html" >}}

Parameter | Type | Description
----------|------|------------
_ts&lowbar;val_ | [`timestamp`](../../types/timestamp), [`timestamptz`](../../types/timestamptz) | The value you want to truncate.

### Return value

`date_trunc` returns a [`timestamp`](../../types/float) value.

## Examples

```sql
SELECT date_trunc('hour', TIMESTAMP '2019-11-26 15:56:46.241150') AS hour_trunc;
```
```nofmt
          hour_trunc
-------------------------------
 2019-11-26 15:00:00.000000000
```
<hr/>
```sql
SELECT date_trunc('year', TIMESTAMP '2019-11-26 15:56:46.241150') AS year_trunc;
```
```nofmt
          year_trunc
-------------------------------
 2019-01-01 00:00:00.000000000
```
