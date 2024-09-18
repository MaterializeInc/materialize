---
title: "Lag over"
description: "Use idiomatic Materialize SQL to access the previous row's value (lag) when ordered by a field that advances in regular intervals."
menu:
  main:
    parent: idiomatic-materialize-sql
    identifier: idiomatic-materialize-lag
    weight: 20
---

## Overview

Lag queries access the field value of the previous row as determined by some
ordering. The following idiomatic Materialize SQL queries refer to lag queries
whose order by field increases in **regular** intervals.

## Idiomatic Materialize SQL

### Exclude the first row in results

**Idiomatic Materialize SQL:** To access the lag (previous row's field value)
ordered by some field that increases in **regular** intervals, use a self join
that specifies an equality match on the regularly increasing field, taking into
consideration the interval ([`WHERE a.field = b.field + INTERVAL
...`](https://materialize.com/docs/sql/types/interval/#valid-operations)). The
query *excludes* the first row since it does not have a previous row.

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
field, taking into consideration the interval (e.g., [`WHERE a.field = b.field +
INTERVAL
...`](https://materialize.com/docs/sql/types/interval/#valid-operations)). The
query *excludes* the first row in the results since it does not have a previous
row.

<br>

```mzsql
-- Excludes the first row in the results --
SELECT t1.fieldA, t2.fieldB as previous_row_value
FROM tableA t1, tableA t2
WHERE t1.fieldA = t2.fieldA + INTERVAL ...
ORDER BY fieldA;
```

</td>
</tr>

<tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

<red>

Avoid the use of [`LAG(fieldZ) OVER (ORDER BY ...) window
function`](/sql/functions/#lag) when the order by field increases in regular
intervals.

</red>

<br>

<div style="background-color: var(--code-block)">

```nofmt
-- Anti-pattern. Avoid. --
SELECT fieldA, ...
    LAG(fieldZ) OVER (ORDER BY fieldA) as previous_row_value
FROM tableA;
```

</div>

</td>
</tr>

</tbody>
</table>

### Include the first row in results

**Idiomatic Materialize SQL:** To access the lag (previous row's field value)
ordered by some field that increases in **regular** intervals, use a self [`LEFT
JOIN/LEFT OUTER JOIN`](/sql/select/join/#left-outer-join) on the regularly
increasing field, taking into consideration the
[interval](https://materialize.com/docs/sql/types/interval/#valid-operations)).
The `LEFT JOIN/LEFT OUTER JOIN` query *includes* the first row, returning `null`
as its lag value.

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
t2.fieldA + INTERVAL ...
...`](https://materialize.com/docs/sql/types/interval/#valid-operations)). The
query *includes* the first row, returning `null` as its lag value.

<br>

```mzsql
-- Includes the first row in the results --
SELECT t1.fieldA, t2.fieldB as previous_row_value
FROM tableA t1
LEFT JOIN tableA t2
ON t1.fieldA = t2.fieldA + INTERVAL ...
ORDER BY fieldA;
```

</td>
</tr>

<tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

<red>

Avoid the use of [`LAG(fieldZ) OVER (ORDER BY ...) window
function`](/sql/functions/#lag) when the order by field increases in regular
intervals.

</red>

<br>

<div style="background-color: var(--code-block)">

```nofmt
SELECT fieldA, ...
    LAG(fieldZ) OVER (ORDER BY fieldA) as previous_row_value
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

### Find previous row's value (exclude the first row in results)

Using idiomatic Materialize SQL, the following example finds the previous day's
order total. The example uses a self join by the regularly increasing field
`order_date`, taking into consideration the [interval of `1
DAY`](https://materialize.com/docs/sql/types/interval/#valid-operations)). The
query excludes the first row in the results since the first row does not have a
previous row.

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
-- Excludes the first row in results --
SELECT o1.order_date, o1.daily_total,
    o2.daily_total as previous_daily_total
FROM orders_daily_totals o1, orders_daily_totals o2
WHERE o1.order_date = o2.order_date + INTERVAL '1' DAY
ORDER BY order_date;
```

</td>
</tr>

<tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

<red>Avoid the use of [`LAG() OVER (ORDER BY ...)`
window function](/sql/functions/#lag) to access previous row's value if the
order by field increases in regular intervals.</red>

<br>
<div style="background-color: var(--code-block)">

```nofmt
-- Anti-pattern. Includes the first row's value. --
SELECT order_date, daily_total,
    LAG(daily_total) OVER (ORDER BY order_date) as previous_daily_total
FROM orders_daily_totals;
```

</td>
</tr>
</tbody>
</table>

### Find previous row's value (include the first row in results)

Using idiomatic Materialize SQL, the following example finds the previous day's
order total. The example uses a self [`LEFT JOIN/LEFT OUTER
JOIN`](/sql/select/join/#left-outer-join) on the regularly increasing field
`order_date`, taking into consideration the [interval of `1
DAY`](https://materialize.com/docs/sql/types/interval/#valid-operations)). The
query includes the first row in the results, using `null` as the previous value.

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
-- Include the first row in results --
SELECT o1.order_date, o1.daily_total,
    o2.daily_total as previous_daily_total
FROM orders_daily_totals o1
LEFT JOIN orders_daily_totals o2
ON o1.order_date = o2.order_date + INTERVAL '1' DAY
ORDER BY order_date;
```

</td>
</tr>

<tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

<red>Avoid the use of [`LAG() OVER (ORDER BY ...)`
window function](/sql/functions/#lag) to access previous row's value if the
order by field increases in regular intervals.</red>

<br>
<div style="background-color: var(--code-block)">

```nofmt
-- Anti-pattern. Includes the first row's value. --
SELECT order_date, daily_total,
    LAG(daily_total) OVER (ORDER BY order_date) as previous_daily_total
FROM orders_daily_totals;
```

</td>
</tr>

</tbody>
</table>

## See also

- [`INTERVAL`](https://materialize.com/docs/sql/types/interval/)
- [`LEFT JOIN/LEFT OUTER JOIN`](/sql/select/join/#left-outer-join)
- [`LAG()`](/sql/functions/#lag)
- [Window functions](/sql/functions/#window-functions)
