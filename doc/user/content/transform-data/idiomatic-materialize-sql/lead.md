---
title: "Lead over"
description: "Use idiomatic Materialize SQL to access the next row's value (lead) when ordered by a field that advances in a regular pattern, such as in regular intervals."
menu:
  main:
    parent: idiomatic-materialize-sql
    identifier: idiomatic-materialize-lead
    weight: 40
---

## Overview

The "lead over" query pattern accesses the field value of the next row as
determined by some ordering.

For "lead over (order by)" queries whose ordering can be represented by some
equality condition (such as when ordering by a field that increases at a regular
interval), Materialize provides an idiomatic SQL as an alternative to the window
function.

{{< callout >}}

### Materialize and window functions

{{< idiomatic-sql/materialize-window-functions >}}

{{</ callout >}}

## Idiomatic Materialize SQL

{{< important >}}

Do not use if the "lead over (order by)" ordering cannot be represented by an
equality match.

{{</ important >}}

### Exclude the last row in results

**Idiomatic Materialize SQL:** To access the lead (next row's field value)
ordered by some field that increases in **regular** intervals, use a self join
that specifies an **equality condition** on the order by field (e.g., `WHERE
t1.order_field = t2.order_field - 1`, `WHERE t1.order_field = t2.order_field *
2`, etc.). The query *excludes* the last row in the results since it does not
have a next row.

<table>
<thead>
<tr>
<th></th>
<th></th>
</tr>
</thead>
<tbody>
<tr>
<td><blue>Idiomatic Materialize SQL</blue></td>
<td class="copyableCode">

Use a self join that specifies an **equality match** on the lead's order by
field (e.g., `fieldA`). The order by field must increment in a regular pattern
in order to be represented by an equality condition (e.g., `WHERE t1.fieldA =
t2.fieldA - ...`). The query *excludes* the last row in the results since it
does not have a next row.

{{< important >}}

The idiomatic Materialize SQL applies only to those "lead over" queries whose
ordering can be represented by some **equality condition**.

{{</ important >}}

<br>

```mzsql
-- Excludes the last row in the results --
SELECT t1.fieldA, t2.fieldB as next_row_value
FROM tableA t1, tableA t2
WHERE t1.fieldA = t2.fieldA - ...  -- or some other operand
ORDER BY fieldA;
```

</td>
</tr>

<tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

<red>

Avoid the use of [`LEAD(fieldZ) OVER (ORDER BY ...) window
function`](/sql/functions/#lead) when the order by field increases in a regular pattern.

</red>

<br>

<div style="background-color: var(--code-block)">

```nofmt
-- Anti-pattern. Avoid. --
SELECT fieldA, ...
    LEAD(fieldZ) OVER (ORDER BY fieldA) as next_row_value
FROM tableA;
```

</div>

</td>
</tr>

</tbody>
</table>

### Include the last row in results

**Idiomatic Materialize SQL:** To access the lead (next row's field value)
ordered by some field that increases in **regular** intervals, use a self [`LEFT
JOIN/LEFT OUTER JOIN`](/sql/select/join/#left-outer-join) that specifies an
**equality condition** on the order by field (e.g., `ON t1.order_field =
t2.order_field - 1`, `ON t1.order_field = t2.order_field * 2`, etc.). The `LEFT
JOIN/LEFT OUTER JOIN` query *includes* the last row, returning `null` as its
lead value.

<table>
<thead>
<tr>
<th></th>
<th></th>
</tr>
</thead>
<tbody>
<tr>
<td><blue>Idiomatic Materialize SQL</blue></td>
<td class="copyableCode">

Use a self [`LEFT JOIN/LEFT OUTER JOIN`](/sql/select/join/#left-outer-join)
(e.g., `FROM tableA t1 LEFT JOIN tableA t2`) that specifies an **equality
match** on the lag's order by field (e.g., `fieldA`).  The order by field must
increment in a regular pattern in order to be represented by an equality
condition (e.g., `ON t1.fieldA = t2.fieldA - ...`). The query *includes* the
last row, returning `null` as its lead value.

{{< important >}}

The idiomatic Materialize SQL applies only to those "lead over" queries whose
ordering can be represented by some **equality condition**.

{{</ important >}}


```mzsql
-- Includes the last row in the response --
SELECT t1.fieldA, t2.fieldB as next_row_value
FROM tableA t1
LEFT JOIN tableA t2
ON t1.fieldA = t2.fieldA - ... -- or some other operand
ORDER BY fieldA;
```

</td>
</tr>

<tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

<red>

Avoid the use of [`LEAD(fieldZ) OVER (ORDER BY ...) window
function`](/sql/functions/#lead) when the order by field increases in regular
intervals.

</red>

<br>

<div style="background-color: var(--code-block)">

```nofmt
SELECT fieldA, ...
    LEAD(fieldZ) OVER (ORDER BY fieldA) as next_row_value
FROM tableA;
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

### Find next row's value (exclude the last row in results)

Using idiomatic Materialize SQL, the following example finds the next day's
order total. That is, the example uses a self join on `orders_daily_totals`. The
row ordering on the `order_date` field is represented by an **equality
condition** using an [interval of `1
DAY`](https://materialize.com/docs/sql/types/interval/#valid-operations). The
query excludes the last row in the results since the last row does not have a
next row.

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

```mzsql
-- Excludes the last row in results --
SELECT o1.order_date, o1.daily_total,
    o2.daily_total as next_daily_total
FROM orders_daily_totals o1, orders_daily_totals o2
WHERE o1.order_date = o2.order_date - INTERVAL '1' DAY
ORDER BY order_date;
```

{{< important >}}

The idiomatic Materialize SQL applies only to those "lead over" queries whose
ordering can be represented by some **equality condition**.

{{</ important >}}


</td>
</tr>

<tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

<red>Avoid the use of [`LEAD() OVER (ORDER BY ...)`
window function](/sql/functions/#lead) to access next row's value if the
order by field increases in regular intervals.</red>

<br>
<div style="background-color: var(--code-block)">

```nofmt
-- Anti-pattern. Includes the last row's value. --
SELECT order_date, daily_total,
    LEAD(daily_total) OVER (ORDER BY order_date) as next_daily_total
FROM orders_daily_totals;
```

</td>
</tr>
</tbody>
</table>

### Find next row's value (include the last row in results)

Using idiomatic Materialize SQL, the following example finds the next day's
order total. The example uses a self [`LEFT JOIN/LEFT OUTER
JOIN`](/sql/select/join/#left-outer-join) on `orders_daily_totals`. The row
ordering on the `order_date` field is represented by an **equality condition**
using an [interval of `1
DAY`](https://materialize.com/docs/sql/types/interval/#valid-operations)). The
query includes the last row in the results, using `null` as the next row's
value.

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

```mzsql
-- Include the last row in the results --
SELECT o1.order_date, o1.daily_total,
    o2.daily_total as next_daily_total
FROM orders_daily_totals o1
LEFT JOIN orders_daily_totals o2
ON o1.order_date = o2.order_date - INTERVAL '1' DAY
ORDER BY order_date;
```

{{< important >}}

The idiomatic Materialize SQL applies only to those "lead over" queries whose
ordering can be represented by some **equality condition**.

{{</ important >}}


</td>
</tr>

<tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

<red>Avoid the use of [`LEAD() OVER (ORDER BY ...)`
window function](/sql/functions/#lead) to access next row's value if the
order by field increases in a regular pattern.</red>

<br>
<div style="background-color: var(--code-block)">

```nofmt
-- Anti-pattern. Includes the last row in results. --
SELECT order_date, daily_total,
    LEAD(daily_total) OVER (ORDER BY order_date) as next_daily_total
FROM orders_daily_totals;
```

</td>
</tr>

</tbody>
</table>

## See also

- [Lag over](/transform-data/idiomatic-materialize-sql/lag)
- [`INTERVAL`](https://materialize.com/docs/sql/types/interval/)
- [`LEFT JOIN/LEFT OUTER JOIN`](/sql/select/join/#left-outer-join)
- [`LEAD()`](/sql/functions/#lead)
- [Window functions](/sql/functions/#window-functions)
