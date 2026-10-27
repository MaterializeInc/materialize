---
title: "`ANY()` equi-join condition"
description: "Use idiomatic Materialize SQL for equi-join whose `ON` expression includes the `ANY()` operator."
menu:
  main:
    parent: idiomatic-materialize-sql
    identifier: idiomatic-materialize-any
    weight: 5
---

## Overview

The "`field = ANY(...)`" equality condition returns true if the equality
comparison is true for any of the values in the `ANY()` expression.

For equi-join whose `ON` expression includes an [`ANY` operator
expression](/sql/functions/#expression-bool_op-any),
Materialize provides an idiomatic SQL as an alternative to the `ANY()`
expression.

{{< callout >}}

### Materialize and equi-join `ON fieldX = ANY(<array|list|map>)`

When evaluating an equi-join whose `ON` expression includes the [`ANY` operator
expression](/sql/functions/#expression-bool_op-any)
(i.e., `ON fieldX = ANY(<array|list|map>)`), Materialize performs a cross join,
which can lead to a significant increase in memory usage. If possible, rewrite
the query to perform an equi-join on the unnested values.

{{</ callout >}}

## Idiomatic Materialize SQL

**Idiomatic Materialize SQL:**  For equi-join whose `ON` expression includes
the [`ANY` operator expression](/sql/functions/#expression-bool_op-any) (`ON
fieldX = ANY(<array|list|map>)`), use [UNNEST()](/sql/functions/#unnest) in a
[Common Table Expression (CTE)](/sql/select/#common-table-expressions-ctes) to
unnest the values and perform the equi-join on the unnested values. If the
array/list/map contains duplicates, include [`DISTINCT`](/sql/select/#select-distinct) to remove duplicates.

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

{{% include-from-yaml data="idiomatic_mzsql/patterns_general" name="any-equi-join" field="syntax_idiomatic" %}}

</td>
</tr>
<tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

{{% include-from-yaml data="idiomatic_mzsql/patterns_general" name="any-equi-join" field="syntax_anti_pattern" %}}

</td>
</tr>

</tbody>
</table>


## Examples

{{< note >}}

The example data can be found in the
[Appendix](/transform-data/idiomatic-materialize-sql/appendix/example-orders).

{{</ note >}}

### Find orders with any sales items

Using idiomatic Materialize SQL, the following example finds orders that contain
any of the sales items for the week of the order. That is, the example uses a
CTE to [`UNNEST()`](/sql/functions/#unnest) (or
[`DISTINCT`](/sql/select/#select-distinct)[`UNNEST()`](/sql/functions/#unnest))
the `items` field from the `sales_items` table, and then performs an equi-join
with the `orders` table on the unnested values.

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

{{% include-from-yaml data="idiomatic_mzsql/patterns_general" name="any-equi-join" field="example_idiomatic" %}}

</td>
</tr>

<tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

{{% include-from-yaml data="idiomatic_mzsql/patterns_general" name="any-equi-join" field="example_anti_pattern" %}}

</td>
</tr>

</tbody>
</table>

## See also

- [`ANY()`](/sql/functions/#expression-bool_op-any)

- [Common Table Expression (CTE)](/sql/select/#common-table-expressions-ctes)

- [Idiomatic Materialize SQL
  Chart](/transform-data/idiomatic-materialize-sql/appendix/idiomatic-sql-chart/)

- [`UNNEST()`](/sql/functions/#unnest)
