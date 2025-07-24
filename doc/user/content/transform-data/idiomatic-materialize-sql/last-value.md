---
title: "Last value in group"
description: "Use idiomatic Materialize SQL to find the last value in each group."
menu:
  main:
    parent: idiomatic-materialize-sql
    identifier: idiomatic-materialize-last-value
    weight: 30
---

## Overview

The "last value in each group" query pattern returns the last value, according
to some ordering, in each group.

{{< callout >}}

### Materialize and window functions

{{< idiomatic-sql/materialize-window-functions >}}

{{</ callout >}}

## Idiomatic Materialize SQL

**Idiomatic Materialize SQL:** To find the last value in each group, use the
[MIN()](/sql/functions/#min) or [MAX()](/sql/functions/#max) aggregate function
in a subquery.

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

Use a subquery that uses the [MIN()](/sql/functions/#min) or
[MAX()](/sql/functions/#max) aggregate function.

<br>
<div style="background-color: var(--code-block)">

```mzsql
SELECT tableA.fieldA, tableA.fieldB, minmax.Z
 FROM tableA,
 (SELECT fieldA,
    MAX(fieldZ),
    MIN(fieldZ)
 FROM tableA
 GROUP BY fieldA) minmax
WHERE tableA.fieldA = minmax.fieldA
ORDER BY fieldA ... ;
```

</td>
</tr>

<tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

<red>Do not use [`LAST_VALUE() OVER (PARTITION BY ... ORDER BY ... RANGE
...)` window function](/sql/functions/#last_value) for last value in each group
queries.</red>

{{< note >}}

Materialize does not support `RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED
FOLLOWING`.

{{</ note >}}

<br>
<div style="background-color: var(--code-block)">

```nofmt
-- Unsupported --
SELECT fieldA, fieldB,
  LAST_VALUE(fieldZ)
    OVER (PARTITION BY fieldA ORDER BY fieldZ
          RANGE BETWEEN
            UNBOUNDED PRECEDING AND
            UNBOUNDED FOLLOWING),
  LAST_VALUE(fieldZ)
    OVER (PARTITION BY fieldA ORDER BY fieldZ DESC
          RANGE BETWEEN
            UNBOUNDED PRECEDING AND
            UNBOUNDED FOLLOWING)
FROM tableA
ORDER BY fieldA, ...;
```

</div>
</td>
</tr>

</tbody>
</table>

### Query hints

To further improve the memory usage of the idiomatic Materialize SQL, you can
specify a [`AGGREGATE INPUT GROUP SIZE` query hint](/sql/select/#query-hints) in
the idiomatic Materialize SQL.

```mzsql
SELECT tableA.fieldA, tableA.fieldB, minmax.Z
 FROM tableA,
 (SELECT fieldA,
    MAX(fieldZ),
    MIN(fieldZ)
 FROM tableA
 GROUP BY fieldA
 OPTIONS (AGGREGATE INPUT GROUP SIZE = ...)
 ) minmax
WHERE tableA.fieldA = minmax.fieldA
ORDER BY fieldA ... ;
```

For more information on setting `AGGREGATE INPUT GROUP SIZE`, see
[Optimization](/transform-data/optimization/#query-hints).

## Examples

{{< note >}}

The example data can be found in the
[Appendix](/transform-data/idiomatic-materialize-sql/appendix/example-orders).

{{</ note >}}

### Use MAX() to find the last value

Using idiomatic Materialize SQL, the following example finds the highest item
price in each order and calculates the difference between the price of each item
in the order and the highest price. The example uses a subquery that groups by
the `order_id` and selects [`MAX(price)`](/sql/functions/#max)  to find the
highest price (i.e., the last price if ordered by ascending price values):

<table>
<thead>
<tr>
<th></th>
<th></th>
</tr>
</thead>
<tbody>
<tr>
<td><blue>Idiomatic Materialize SQL</blue> ✅</td>
<td class="copyableCode">


```mzsql
SELECT o.order_id, minmax.highest_price, o.item, o.price,
  o.price - minmax.highest_price AS diff_highest_price
FROM orders_view o,
     (SELECT order_id,
        MAX(price) AS highest_price
     FROM orders_view
     GROUP BY order_id) minmax
WHERE o.order_id = minmax.order_id
ORDER BY o.order_id, o.item;
```

</td>
</tr>

<tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

<red>Do not use of `LAST_VALUE() OVER (PARTITION BY ... ORDER BY ... RANGE ...)`
for last value in each group queries.</red>

{{< note >}}

Materialize does not support `RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED
FOLLOWING`.

{{</ note >}}

<div style="background-color: var(--code-block)">

```nofmt
-- Unsupported --
SELECT order_id,
  LAST_VALUE(price)
    OVER (PARTITION BY order_id ORDER BY price
          RANGE BETWEEN
            UNBOUNDED PRECEDING AND
            UNBOUNDED FOLLOWING) AS highest_price,
  item,
  price,
  price - LAST_VALUE(price)
    OVER (PARTITION BY order_id ORDER BY price
           RANGE BETWEEN
            UNBOUNDED PRECEDING AND
            UNBOUNDED FOLLOWING) AS diff_highest_price
FROM orders_view
ORDER BY order_id, item;
```

</div>
</td>
</tr>
</tbody>
</table>

### Use MIN() to find the last values

Using idiomatic Materialize SQL, the following example finds the lowest item
price in each order and calculates the difference between the price of each item
in the order and the lowest price.  That is, use a subquery that groups by the
`order_id` and selects [`MIN(price)`](/sql/functions/#min)  as the lowest price
(i.e.,  last price if ordered by descending price value)

<table>
<thead>
<tr>
<th></th>
<th></th>
</tr>
</thead>
<tbody>
<tr>
<td><blue>Idiomatic Materialize SQL</blue> ✅</td>
<td class="copyableCode">


```mzsql
SELECT o.order_id, minmax.lowest_price, o.item, o.price,
  o.price - minmax.lowest_price AS diff_lowest_price
FROM orders_view o,
     (SELECT order_id,
        MIN(price) AS lowest_price
     FROM orders_view
     GROUP BY order_id) minmax
WHERE o.order_id = minmax.order_id
ORDER BY o.order_id, o.item;
```

</td>
</tr>

<tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

<red>Do not use `LAST_VALUE() OVER (PARTITION BY ... ORDER BY ... RANGE ... )`
for last value in each group queries.</red>

{{< note >}}

Materialize does not support `RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED
FOLLOWING`.

{{</ note >}}

<div style="background-color: var(--code-block)">

```nofmt
-- Unsupported --
SELECT order_id,
  LAST_VALUE(price)
    OVER (PARTITION BY order_id ORDER BY price DESC
          RANGE BETWEEN
            UNBOUNDED PRECEDING AND
            UNBOUNDED FOLLOWING) AS lowest_price,
  item,
  price,
  price - LAST_VALUE(price)
    OVER (PARTITION BY order_id ORDER BY price DESC
          RANGE BETWEEN
            UNBOUNDED PRECEDING AND
            UNBOUNDED FOLLOWING) AS diff_lowest_price
FROM orders_view
ORDER BY order_id, item;
```

</div>
</td>
</tr>
</tbody>
</table>

### Use MIN() and MAX() to find the last values

Using idiomatic Materialize SQL, the following example finds the lowest and
highest item price in each order and calculate the difference for each item in
the order from these prices. That is, use a subquery that groups by the
`order_id` and selects [`MIN(price)`](/sql/functions/#min) as the lowest price
(i.e., last value if ordered by descending price values) and
[`MAX(price)`](/sql/functions/#max) as the highest price (i.e., last value if
ordered by ascending price values).

<table>
<thead>
<tr>
<th></th>
<th></th>
</tr>
</thead>
<tbody>
<tr>
<td><blue>Idiomatic Materialize SQL</blue> ✅</td>
<td class="copyableCode">

```mzsql
SELECT o.order_id, minmax.lowest_price, minmax.highest_price, o.item, o.price,
  o.price - minmax.lowest_price AS diff_lowest_price,
  o.price - minmax.highest_price AS diff_highest_price
FROM orders_view o,
      (SELECT order_id,
         MIN(price) AS lowest_price,
         MAX(price) AS highest_price
      FROM orders_view
      GROUP BY order_id) minmax
WHERE o.order_id = minmax.order_id
ORDER BY o.order_id, o.item;
```

</td>
</tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

<red>Do not use `LAST_VALUE() OVER (PARTITION BY ... ORDER BY
)` for last value within groups queries.</red>

{{< note >}}

Materialize does not support `RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED
FOLLOWING`.

{{</ note >}}

<div style="background-color: var(--code-block)">

```nofmt
-- Unsupported --
SELECT order_id,
  LAST_VALUE(price)
    OVER (PARTITION BY order_id ORDER BY price DESC
          RANGE BETWEEN
            UNBOUNDED PRECEDING AND
            UNBOUNDED FOLLOWING) AS lowest_price,
  LAST_VALUE(price)
    OVER (PARTITION BY order_id ORDER BY price
          RANGE BETWEEN
            UNBOUNDED PRECEDING AND
            UNBOUNDED FOLLOWING) AS highest_price,
  item,
  price,
  price - LAST_VALUE(price)
    OVER (PARTITION BY order_id ORDER BY price DESC
          RANGE BETWEEN
            UNBOUNDED PRECEDING AND
            UNBOUNDED FOLLOWING) AS diff_lowest_price,
  price - LAST_VALUE(price)
    OVER (PARTITION BY order_id ORDER BY price
           RANGE BETWEEN
            UNBOUNDED PRECEDING AND
            UNBOUNDED FOLLOWING) AS diff_highest_price
FROM orders_view
ORDER BY order_id, item;
```

</div>
</td>
</tr>

</tbody>
</table>

## See also

- [First value in a
  group](/transform-data/idiomatic-materialize-sql/first-value)
- [`MIN()`](/sql/functions/#min)
- [`MAX()`](/sql/functions/#max)
- [Query hints for MIN/MAX](/transform-data/optimization/#query-hints)
- [Window functions](/sql/functions/#window-functions)
