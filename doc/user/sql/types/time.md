---
title: "time Data Type"
description: "Expresses a time without a specific date"
menu:
  main:
    parent: 'sql-types'
---

`time` data expresses a time without a specific date.

Detail | Info
-------|------
**Quick Syntax** | `TIME '01:23:45'`
**Size** | 4 bytes
**Min value** | `TIME '00:00:00'`
**Max value** | `TIME '23:59:59.999999'`

## Syntax

{{< diagram "type-time.html" >}}

Field | Use
------|------------
_time&lowbar;str_ | A string representing a time of day in `H:M:S.NS` format.

## Details

### Valid casts

You cannot cast `time` to or from any other type.

### Valid operations

`time` data supports the following operations with other types.

Operation | Computes
----------|------------
[`date`](../date) `+` [`time`](../time) | [`timestamp`](../timestamp)
[`time`](../time) `+` [`interval`](../interval) | `time`
[`time`](../time) `-` [`interval`](../interval) | `time`
[`time`](../time) `-` [`time`](../time) | [`interval`](../interval)

## Examples

```sql
SELECT TIME '01:23:45' AS t_v;
```
```nofmt
   t_v
----------
 01:23:45
```

<hr/>

```sql
SELECT DATE '2001-02-03' + TIME '45:53:24' AS d_t;
```
```nofmt
         d_t
---------------------
 2001-02-03 12:34:45
```
