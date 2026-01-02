---
audience: developer
canonical_url: https://materialize.com/docs/transform-data/idiomatic-materialize-sql/mz_now/
complexity: beginner
description: '`mz_now()` expressions can only take comparison operators. `mz_now()`
  expressions cannot be used with disjunctions `OR` in view definitions.'
doc_type: overview
keywords:
- Idiomatic Materialize SQL
- CREATE THE
- CREATE AN
- mz_now() expressions
product_area: SQL
status: stable
title: mz_now() expressions
---

# mz_now() expressions

## Purpose
`mz_now()` expressions can only take comparison operators. `mz_now()` expressions cannot be used with disjunctions `OR` in view definitions.

This page provides detailed documentation for this topic.


 `mz_now()` expressions can only take comparison operators. `mz_now()` expressions cannot be used with disjunctions `OR` in view definitions.



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

This section covers idiomatic materialize sql.

### `mz_now()` expressions to calculate past or future timestamp

**Idiomatic Materialize SQL**: `mz_now()` must be used with one of the following comparison operators: `=`,
`<`, `<=`, `>`, `>=`, or an operator that desugars to them or to a conjunction
(`AND`) of them (for example, `BETWEEN...AND...`). That is, you cannot use
date/time operations directly on  `mz_now()` to calculate a timestamp in the
past or future. Instead, rewrite the query expression to move the operation to
the other side of the comparison.


#### Examples

<!-- Dynamic table: mz_now/mz_now_operators - see original docs -->

### Disjunctions (`OR`)

When used in a materialized view definition, a view definition that is being
indexed (i.e., although you can create the view and perform ad-hoc query on
the view, you cannot create an index on that view), or a `SUBSCRIBE`
statement:

- `mz_now()` clauses can only be combined using an `AND`, and

- All top-level `WHERE` or `HAVING` conditions must be combined using an `AND`,
  even if the `mz_now()` clause is nested.


For example:

<!-- Dynamic table: mz_now/mz_now_combination - see original docs -->


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

<!-- Dynamic table: mz_now/mz_now_disjunction_alternatives - see original docs -->

