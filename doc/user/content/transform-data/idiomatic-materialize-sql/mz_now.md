---
title: "mz_now() expressions"
description: " `mz_now()` expressions can only take comparison operators. `mz_now()` expressions cannot be used with disjunctions `OR`."
menu:
  main:
    parent: idiomatic-materialize-sql
    identifier: idiomatic-materialize-mz_now
    weight: 45
---

## Overview

In Materialize, [`mz_now()`](/sql/functions/now_and_mz_now/) function returns
Materialize's current virtual timestamp (i.e., returns
[`mz_timestamp`](/sql/types/mz_timestamp/)). The function can be used in
[temporal filters](/transform-data/patterns/temporal-filters/) to reduce the
working dataset.

`mz_now()` expression has the following form:

```mzsql
mz_now() <comparison_operator> <numeric_expr | timestamp_expr>
```

## Idiomatic Materialize SQL

### `mz_now()` expressions to calculate past or future timestamp

**Idiomatic Materialize SQL**: {{< include-md
file="shared-content/mz_now_operators.md" >}}

For example:

{{< yaml-table data="mz_now/mz_now_operators" noHeader="true" >}}

### Disjunctions (`OR`)

The `mz_now()` clauses can only be combined using an `AND`, and all top-level
`WHERE` or `HAVING` conditions must be combined using an `AND`, even if the
`mz_now()` clause is nested.

For example:

{{< yaml-table data="mz_now/mz_now_combination" >}}

**Idiomatic Materialize SQL**: To use disjunctions (`OR`) when using `mz_now()`
in your query, rewrite the query to use `UNION ALL` instead.

For example:

{{< yaml-table data="mz_now/mz_now_disjunction_alternatives" noHeader="true" >}}
