---
title: "Aggregate Function Filters"
description: "If FILTER is specified, then only the input rows for which the filter_clause evaluates to true are fed to the aggregate function; other rows are discarded"
menu:
main:
parent: 'sql-functions'
---

If `FILTER` is specified, then only the input rows for which the `filter_clause` evaluates to true are fed to the [aggregate function](../#aggregate-func); other rows are discarded

## Syntax

{{< diagram "aggregate-with-filter.svg" >}}

## Details

## Examples

```sql
SELECT
    COUNT(*) AS unfiltered,
    COUNT(*) FILTER (WHERE i < 5) AS filtered
FROM generate_series(1,10) AS s(i)
```
