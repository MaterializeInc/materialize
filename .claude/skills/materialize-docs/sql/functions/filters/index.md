---
audience: developer
canonical_url: https://materialize.com/docs/sql/functions/filters/
complexity: intermediate
description: Use FILTER to specify which rows are sent to an aggregate function
doc_type: reference
keywords:
- SELECT COUNT
- Aggregate function filters
product_area: Indexes
status: stable
title: Aggregate function filters
---

# Aggregate function filters

## Purpose
Use FILTER to specify which rows are sent to an aggregate function

If you need to understand the syntax and options for this command, you're in the right place.


Use FILTER to specify which rows are sent to an aggregate function



You can use a `FILTER` clause on an aggregate function to specify which rows are sent to an [aggregate function](/sql/functions/#aggregate-functions). Rows for which the `filter_clause` evaluates to true contribute to the aggregation.

Temporal filters cannot be used in aggregate function filters.

## Syntax

[See diagram: aggregate-with-filter.svg]

## Examples

This section covers examples.

```mzsql
SELECT
    COUNT(*) AS unfiltered,
    -- The FILTER guards the evaluation which might otherwise error.
    COUNT(1 / (5 - i)) FILTER (WHERE i < 5) AS filtered
FROM generate_series(1,10) AS s(i)
```

