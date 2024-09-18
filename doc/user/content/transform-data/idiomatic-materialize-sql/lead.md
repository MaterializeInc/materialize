---
title: "Lead over"
description: "Use idiomatic Materialize SQL to access the next row's value (lead) when ordered by a field that advances in regular intervals."
menu:
  main:
    parent: idiomatic-materialize-sql
    identifier: idiomatic-materialize-lead
    weight: 40
---

## Overview

Lead queries access the field value of the next row as determined by some
ordering. The following idiomatic Materialize SQL queries refer to lead queries
whose order by field increases in **regular** intervals.

## Idiomatic Materialize SQL

### Exclude the last row in results

**Idiomatic Materialize SQL:** To access the lead (next row's field value)
ordered by some field that increases in **regular** intervals, use a self join
that specifies an equality match on the regularly increasing field, taking into
consideration the interval ([`WHERE a.field = b.field - INTERVAL
...`](https://materialize.com/docs/sql/types/interval/#valid-operations)). The
query *excludes* the last row in the results since it does not have a next row.

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

Use a self join that specifies an equality match on the regularly increasing
field, taking into consideration the interval (e.g., [`WHERE a.field = b.field -
INTERVAL
...`](https://materialize.com/docs/sql/types/interval/#valid-operations)). The
query *excludes* the last row since it does not have a next row.

<br>

```mzsql
-- Excludes the last row in the results --
SELECT t1.fieldA, t2.fieldB as next_row_value
FROM tableA t1, tableA t2
WHERE t1.fieldA = t2.fieldA - INTERVAL ...
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
JOIN/LEFT OUTER JOIN`](/sql/select/join/#left-outer-join) on the regularly
increasing field, taking into consideration the
[interval](https://materialize.com/docs/sql/types/interval/#valid-operations)).
The `LEFT JOIN/LEFT OUTER JOIN` query *includes* the last row, returning `null`
as its lead value.

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
(e.g., `FROM tableA t1 LEFT JOIN tableA t2`) on the regularly increasing field
(e.g., `fieldA`), taking into consideration the interval (e.g., [`ON t1.fieldA =
t2.fieldA - INTERVAL ...
...`](https://materialize.com/docs/sql/types/interval/#valid-operations)). The
query *includes* the last row, returning `null` as its lead value.

<br>

```mzsql
-- Includes the last row in the response --
SELECT t1.fieldA, t2.fieldB as next_row_value
FROM tableA t1
LEFT JOIN tableA t2
ON t1.fieldA = t2.fieldA - INTERVAL ...
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
order total. The example uses a self join by the regularly increasing field
`order_date`, taking into consideration the [interval of `1
DAY`](https://materialize.com/docs/sql/types/interval/#valid-operations)). The
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
JOIN`](/sql/select/join/#left-outer-join) on the regularly increasing field
`order_date`, taking into consideration the [interval of `1
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

- [`INTERVAL`](https://materialize.com/docs/sql/types/interval/)
- [`LEFT JOIN/LEFT OUTER JOIN`](/sql/select/join/#left-outer-join)
- [`LEAD()`](/sql/functions/#lead)
- [Window functions](/sql/functions/#window-functions)
