---
title: "date Data Type"
description: "Expresses a date without a specified time"
menu:
  main:
    parent: 'sql-types'
---

`date` data expresses a date without a specified time.

Detail | Info
-------|------
**Quick Syntax** | `DATE '2007-02-01'`
**Size** | 1 byte
**Min value** | 4713 BC
**Max value** | 5874897 AD
**Resolution** | 1 day

## Syntax

{{< diagram "type-date.html" >}}

Field | Use
------|----
_date&lowbar;str_ | A string representing a date in `Y-M-D` format.

## Details

### Valid casts

#### From `date`

You can [cast](../../functions/cast) `date` to:

- [`timestamp`](../timestamp)
- [`timestamptz`](../timestamp)
- [`string`](../string)

#### To `date`

You cannot cast any other type to `date`.

### Valid operations

`time` data supports the following operations with other types.

Operation | Computes
----------|------------
[`date`](../date) `+` [`interval`](../interval) | [`timestamp`](../timestamp)
[`date`](../date) `-` [`interval`](../interval) | [`timestamp`](../timestamp)
[`date`](../date) `+` [`time`](../time) | [`timestamp`](../timestamp)
[`date`](../date) `-` [`date`](../date) | [`interval`](../interval)

## Examples

```sql
SELECT DATE '2007-02-01' AS date_v;
```
```nofmt
   date_v
------------
 2007-02-01
```
