---
title: "Aggregate function filters"
description: "Use FILTER to specify which rows are sent to an aggregate function"
menu:
  main:
    parent: 'sql-functions'
---

You can use a `FILTER` clause on an aggregate function to specify which rows are sent to an [aggregate function](../#aggregate-func). Rows for which the `filter_clause` evaluates to false are discarded.

## Syntax

{{< diagram "aggregate-with-filter.svg" >}}

## Examples

```sql
SELECT
    COUNT(*) AS unfiltered,
    COUNT(*) FILTER (WHERE i < 5) AS filtered
FROM generate_series(1,10) AS s(i)
```
