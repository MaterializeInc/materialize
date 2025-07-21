---
title: "mz_now() expressions"
description: " `mz_now()` expressions can only take comparison operators. `mz_now()` expressions cannot be used with disjunctions `OR` in view definitions."
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

#### Examples

{{< yaml-table data="mz_now/mz_now_operators" noHeader="true" >}}

### Disjunctions (`OR`)

{{< include-md file="shared-content/mz_now_clause_disjunction_restrictions.md"
>}}

For example:

{{< yaml-table data="mz_now/mz_now_combination" >}}


**Idiomatic Materialize SQL**: When `mz_now()` is included in a materialized
view definition, a view definition that is being indexed, or a `SUBSCRIBE`
statement, instead of using disjunctions (`OR`) when using `mz_now()`, rewrite
the query to use `UNION ALL` or `UNION` instead, deduplicating as necessary:

- In some cases, you may need to modify the conditions to deduplicate results
  when using `UNION ALL`. For example, you might add the negation of one input's
  condition to the other as a conjunction.

- In some cases, using `UNION` instead of `UNION ALL` may suffice if the inputs
  do not contain other duplicates that need to be retained.

#### Examples

{{< yaml-table data="mz_now/mz_now_disjunction_alternatives" noHeader="true" >}}
