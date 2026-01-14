---
title: "date type"
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
**Catalog name** | `pg_catalog.date`
**OID** | 1082
**Min value** | 4714-11-24 BC
**Max value** | 262143-12-31 AD
**Resolution** | 1 day

## Syntax

{{% include-syntax file="sql_types/date" example="syntax" %}}

## Details

### Valid casts

#### From `date`

You can [cast](../../functions/cast) `date` to:

- [`text`](../text) (by assignment)
- [`timestamp`](../timestamp) (implicitly)
- [`timestamptz`](../timestamp) (implicitly)

#### To `date`

You can [cast](../../functions/cast) from the following types to `date`:

- [`text`](../text) (explicitly)
- [`timestamp`](../timestamp) (by assignment)
- [`timestamptz`](../timestamp) (by assignment)

### Valid operations

`time` data supports the following operations with other types.

Operation | Computes
----------|------------
[`date`](../date) `+` [`interval`](../interval) | [`timestamp`](../timestamp)
[`date`](../date) `-` [`interval`](../interval) | [`timestamp`](../timestamp)
[`date`](../date) `+` [`time`](../time) | [`timestamp`](../timestamp)
[`date`](../date) `-` [`date`](../date) | [`interval`](../interval)

## Examples

```mzsql
SELECT DATE '2007-02-01' AS date_v;
```
```nofmt
   date_v
------------
 2007-02-01
```
