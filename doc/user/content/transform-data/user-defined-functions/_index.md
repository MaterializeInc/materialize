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

{{< warn-if-unreleased "v26.51" >}}

## `jq()`-based user-defined functions

The [`jq`](/sql/functions/jq/) function runs a
[jq program](https://jqlang.org/manual/) on a [`jsonb`](/sql/types/jsonb/)
value, one row at a time. List operations such as sorting and deduplicating
use less memory than the equivalent SQL.

| Guide | Description |
|-------|-------------|
| [Write functions with jq](/transform-data/user-defined-functions/jq/) | Sort and deduplicate lists in a `jsonb` column, and compare the plan with SQL. |
| [`jq` function reference](/sql/functions/jq/) | Syntax, behavior, and examples. |
