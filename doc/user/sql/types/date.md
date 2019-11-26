---
title: "DATE Data Type"
description: "Expresses a date without a specified time"
menu:
  main:
    parent: 'sql-types'
---

`DATE` data expresses a date without a specified time.

Detail | Info
-------|------
**Quick Syntax** | `DATE '2007-02-01'`
**Size** | 1 byte
**Min value** | 4713 BC
**Max value** | 5874897 AD
**Resolution** | 1 day

## Syntax

{{< diagram "type-date.html" >}}

## Examples

```sql
SELECT DATE '2007-02-01' AS date_v;
```
```shell
   date_v
------------
 2007-02-01
```
