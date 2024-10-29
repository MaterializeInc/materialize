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

**If no duplicates exist in the unnested field:** Use a Common Table
Expression (CTE) to [`UNNEST()`](/sql/functions/#unnest) the array of values and
perform the equi-join on the unnested values.

<br>
<div style="background-color: var(--code-block)">

```mzsql
-- array_field contains no duplicates.--

WITH my_expanded_values AS
(SELECT UNNEST(array_field) AS fieldZ FROM tableB)
SELECT a.fieldA, ...
FROM tableA a
JOIN my_expanded_values t ON a.fieldZ = t.fieldZ
;
```

</td>
</tr>
<tr>
<td><blue>Materialize SQL</blue></td>
<td class="copyableCode">

**Duplicates may exist in the unnested field:** Use a Common Table
Expression (CTE) to [`DISTINCT`](/sql/select/#select-distinct)
[`UNNEST()`](/sql/functions/#unnest) the array of values and perform the
equi-join on the unnested values.

<br>
<div style="background-color: var(--code-block)">


```mzsql
-- array_field may contain duplicates.--

WITH my_expanded_values AS
(SELECT DISTINCT UNNEST(array_field) AS fieldZ FROM tableB)
SELECT a.fieldA, ...
FROM tableA a
JOIN my_expanded_values t ON a.fieldZ = t.fieldZ
;
```

</td>
</tr>
<tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

<red>Avoid the use of [`ANY(...)` function](/sql/functions/#expression-bool_op-any) for equi-join
conditions.</red>

<br>
<div style="background-color: var(--code-block)">

```nofmt
-- Anti-pattern. Avoid. --
SELECT a.fieldA, ...
FROM tableA a, tableB b
WHERE a.fieldZ = ANY(b.array_field) -- Anti-pattern. Avoid.
;

```

</div>
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

***If no duplicates in the unnested field***

```mzsql
-- sales_items.items contains no duplicates. --

WITH individual_sales_items AS
(SELECT unnest(items) as item, week_of FROM sales_items)
SELECT s.week_of, o.order_id, o.item, o.quantity
FROM orders o
JOIN individual_sales_items s ON o.item = s.item
WHERE date_trunc('week', o.order_date) = s.week_of
ORDER BY s.week_of, o.order_id, o.item, o.quantity
;
```

***To omit duplicates that may exist in the unnested field***

```mzsql
-- sales_items.items may contains duplicates --

WITH individual_sales_items AS
(SELECT DISTINCT unnest(items) as item, week_of FROM sales_items)
SELECT s.week_of, o.order_id, o.item, o.quantity
FROM orders o
JOIN individual_sales_items s ON o.item = s.item
WHERE date_trunc('week', o.order_date) = s.week_of
ORDER BY s.week_of, o.order_id, o.item, o.quantity
;
```

</td>
</tr>

<tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

<red>Avoid the use of [`ANY()`](/sql/functions/#expression-bool_op-any) for the equi-join condition.</red>

<br>
<div style="background-color: var(--code-block)">

```nofmt
-- Anti-pattern. Avoid. --
SELECT s.week_of, o.order_id, o.item, o.quantity
FROM orders o
JOIN sales_items s ON o.item = ANY(s.items)
WHERE date_trunc('week', o.order_date) = s.week_of
ORDER BY s.week_of, o.order_id, o.item, o.quantity
;
```

</div>

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
