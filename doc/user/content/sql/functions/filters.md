---
title: "Aggregate function filters"
description: "Use FILTER to specify which rows are sent to an aggregate function"
menu:
  main:
    parent: 'sql-functions'
---

You can use a `FILTER` clause on an aggregate function to specify which rows are sent to an [aggregate function](/sql/functions/#aggregate-functions). Rows for which the `filter_clause` evaluates to true contribute to the aggregation.

Temporal filters cannot be used in aggregate function filters.

## Syntax

{{< diagram "aggregate-with-filter.svg" >}}

## Examples

```mzsql
SELECT
    COUNT(*) AS unfiltered,
    -- The FILTER guards the evaluation which might otherwise error.
    COUNT(1 / (5 - i)) FILTER (WHERE i < 5) AS filtered
FROM generate_series(1,10) AS s(i)
```
