---
title: "User-defined functions"
description: "Write custom per-row logic in Materialize with jq programs."
disable_list: true
menu:
  main:
    parent: transform-data
    weight: 45
    identifier: user-defined-functions
---

{{< warn-if-unreleased "v26.50" >}}

## `jq()`-based user-defined functions

Some per-row logic is awkward or expensive to express in SQL. Sorting,
filtering, or deduplicating a list stored in a single row is a common example.
In SQL, you unnest the list, aggregate it back, and join the result to the
original row. Materialize maintains that query incrementally, so each of those
steps keeps its own copy of the data in memory.

The [`jq`](/sql/functions/jq/) function lets you write this logic as a
[jq program](https://jqlang.org/manual/) that runs on a
[`jsonb`](/sql/types/jsonb/) value. Materialize evaluates the program one row
at a time, as it reads the row. It does not keep extra copies
of the data, so list operations use less memory than the equivalent SQL.

jq is expressive enough for more than list operations, such as string
normalization or similarity scores.

- [Write functions with jq](/transform-data/user-defined-functions/jq/): a
  short tutorial.
- [`jq` function reference](/sql/functions/jq/): syntax and behavior.
