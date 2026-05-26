---
title: "`NOT IN` subquery"
description: "Use idiomatic Materialize SQL for `NOT IN (subquery)` predicates to avoid a cross join in the dataflow plan."
menu:
  main:
    parent: idiomatic-materialize-sql
    identifier: idiomatic-materialize-not-in
    weight: 7
---

## Overview

The `fieldX NOT IN (<subquery>)` predicate returns `true` if `fieldX` does not
equal any value returned by the subquery. For predicates where `fieldX` or the
`<subquery>` can contain `NULL` values, Materialize provides idiomatic SQL
alternatives.

### Materialize and `NOT IN (<subquery>)`

When evaluating a `WHERE fieldX NOT IN (<subquery>)` predicate involving
possible `NULL` values for `fieldX` or `<subquery>`, Materialize performs a
cross join between the outer relation and the subquery to preserve SQL `NULL`
semantics, which can significantly increase memory usage. If possible, rewrite
the query to avoid the cross join.

## Idiomatic Materialize SQL

For `fieldX NOT IN (<subquery>)` predicates involving possible `NULL` values,
the following rewrites are available:

{{< note >}}

Neither rewrite is strictly equivalent to `NOT IN (<subquery>)`.

Both rewrites avoid the `NULL` propagation semantics of `NOT IN`; that is, they
treat subquery `NULL` values as non-matches rather than allowing them to
invalidate the comparison. In addition, the `NOT EXISTS` rewrite retains outer
rows whose value is `NULL`, whereas both `NOT IN` and the filter-`NULL`s rewrite
exclude them.

{{</ note >}}

- Rewrite to [`NOT EXISTS`](/sql/functions/#not-exists) with a correlated
  subquery.
- Retain `NOT IN`, but filter out `NULL` values from both the outer field and
  the subquery.

<table>
<thead>
<tr>
<th></th>
<th></th>
</tr>
</thead>
<tbody>
<tr>
<td><blue>Materialize SQL</blue></td>
<td class="copyableCode">

{{% include-from-yaml data="idiomatic_mzsql/patterns_general" name="not-in-subquery" field="syntax_idiomatic" %}}

</td>
</tr>
<tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

{{% include-from-yaml data="idiomatic_mzsql/patterns_general" name="not-in-subquery" field="syntax_anti_pattern" %}}

</td>
</tr>

</tbody>
</table>

If the subquery uses [`UNNEST()`](/sql/functions/#unnest) on a column whose
value depends on the outer row:
- Factor the `UNNEST()` into an uncorrelated [Common Table Expression
  (CTE)](/sql/select/#common-table-expressions-ctes) first.
- Then apply the rewrite against the CTE. See the [example
  below](#find-items-not-currently-on-sale).

## Examples

{{< note >}}

The example data can be found in the
[Appendix](/transform-data/idiomatic-materialize-sql/appendix/example-orders).

{{</ note >}}

### Find items not currently on sale

Using idiomatic Materialize SQL, the following examples find items in the
`items` table whose `item` value (declared `NOT NULL`) does not appear in any of
this week's sales arrays in `sales_items`, a nullable `text[]`. The subquery
uses [`UNNEST()`](/sql/functions/#unnest) to expand each week's `items` array
into individual values for comparison.

If the subquery uses [`UNNEST()`](/sql/functions/#unnest) on a column whose
value depends on the outer row:

- First, factor the `UNNEST()` into an uncorrelated [Common Table Expression
  (CTE)](/sql/select/#common-table-expressions-ctes).
- Then, apply the rewrite against the CTE.

<table>
<thead>
<tr>
<th></th>
<th></th>
</tr>
</thead>
<tbody>

<tr>
<td><blue>Materialize SQL</blue> ✅</td>
<td class="copyableCode">

{{% include-from-yaml data="idiomatic_mzsql/patterns_general" name="not-in-subquery" field="example_idiomatic" %}}

</td>
</tr>

<tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

{{% include-from-yaml data="idiomatic_mzsql/patterns_general" name="not-in-subquery" field="example_anti_pattern" %}}

</td>
</tr>

</tbody>
</table>

## See also

- [`NOT EXISTS`](/sql/functions/#not-exists)

- [Idiomatic Materialize SQL
  Chart](/transform-data/idiomatic-materialize-sql/appendix/idiomatic-sql-chart/)
