---
title: "date_trunc function"
description: "Truncates a timestamp at the specified time component"
menu:
  main:
    parent: 'sql-functions'
---

`date_trunc` computes _ts_val_'s "floor value" of the specified time component,
i.e. the largest time component less than or equal to the provided value.

To align values along arbitrary values, see [`date_bin`].

## Signatures

{{< diagram "func-date-trunc.svg" >}}

Parameter | Type | Description
----------|------|------------
_val_ | [`timestamp`], [`timestamp with time zone`], [`interval`] | The value you want to truncate.

### Return value

`date_trunc` returns the same type as _val_.

## Examples

```sql
SELECT date_trunc('hour', TIMESTAMP '2019-11-26 15:56:46.241150') AS hour_trunc;
```
```nofmt
          hour_trunc
-------------------------------
 2019-11-26 15:00:00.000000000
```

```sql
SELECT date_trunc('year', TIMESTAMP '2019-11-26 15:56:46.241150') AS year_trunc;
```
```nofmt
          year_trunc
-------------------------------
 2019-01-01 00:00:00.000000000
```

```sql
SELECT date_trunc('millennium', INTERVAL '1234 years 11 months 23 days 23:59:12.123456789') AS millenium_trunc;
```
```nofmt
          millenium_trunc
-------------------------------
 1000 years
```

[`date_bin`]: ../date-bin
[`interval`]: ../../types/interval/
[`timestamp`]: ../../types/timestamp
[`timestamp with time zone`]: ../../types/timestamptz
