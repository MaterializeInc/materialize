---
title: "Temporal Filters"
description: "Perform time-windowed computation over temporal data."
weight: 10
menu:
  main:
    parent: guides
---

You can use temporal filters to perform time-windowed computations over temporal data, such as creating self-updating views that report on "Updates from the last ten seconds" or "Orders placed more than a day but less than a week ago".

Windows can be sliding or tumbling. Sliding windows are fixed-size time intervals that you drag over temporal data, like "Show me updates from the last ten seconds". Tumbling or hopping windows are sliding windows that slide one unit at a time, like "Show me updates from the last day for each one-hour interval".

Temporal filters are defined using the function [`mz_logical_timestamp`](/sql/functions/now_and_mz_logical_timestamp). For a more detailed overview, see our blog post on [temporal filters](https://materialize.com/temporal-filters/).

## Restrictions

You can only use `mz_logical_timestamp()` to establish a temporal filter in the following situations:

* In `WHERE` clauses, where `mz_logical_timestamp()` must be directly compared to [`numeric`](/sql/types/numeric) expressions not containing `mz_logical_timestamp()`
* As part of a conjunction phrase (`AND`), where `mz_logical_timestamp()` must be directly compared to [`numeric`](/sql/types/numeric) expressions not containing `mz_logical_timestamp()`.

At the moment, you can't use the `!=` operator with `mz_logical_timestamp` (we're working on it).

## Example

{{% mz_logical_timestamp_example %}}
