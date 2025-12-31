---
audience: developer
canonical_url: https://materialize.com/docs/transform-data/idiomatic-materialize-sql/
complexity: advanced
description: Learn about idiomatic Materialize SQL. Materialize offers various idiomatic
  query patterns, such as for top-k query pattern, first value/last value query paterrns,
  etc.
doc_type: reference
keywords:
- Idiomatic Materialize SQL
- If no duplicates in the unnested field
- SELECT DISTINCT
- 'Idiomatic Materialize SQL:'
- 'Duplicates may exist in the unnested field:'
- SELECT A
- 'If no duplicates exist in the unnested field:'
- 'Note:'
- SELECT UNNEST
product_area: SQL
status: stable
title: Idiomatic Materialize SQL
---

# Idiomatic Materialize SQL

## Purpose
Learn about idiomatic Materialize SQL. Materialize offers various idiomatic query patterns, such as for top-k query pattern, first value/last value query paterrns, etc.

If you need to understand the syntax and options for this command, you're in the right place.


Learn about idiomatic Materialize SQL. Materialize offers various idiomatic query patterns, such as for top-k query pattern, first value/last value query paterrns, etc.


Materialize follows the SQL standard (SQL-92) implementation and strives for
compatibility with the PostgreSQL dialect. However, for some use cases,
Materialize provides its own idiomatic query patterns that can provide better
performance.

## Window functions

<!-- Dynamic table: idiomatic_mzsql/toc_window_functions - see original docs -->

## General query patterns

<!-- Dynamic table: idiomatic_mzsql/toc_query_patterns - see original docs -->


---

## `ANY()` equi-join condition


This section covers `any()` equi-join condition.

## Overview

The "`field = ANY(...)`" equality condition returns true if the equality
comparison is true for any of the values in the `ANY()` expression.

For equi-join whose `ON` expression includes an [`ANY` operator
expression](/sql/functions/#expression-bool_op-any),
Materialize provides an idiomatic SQL as an alternative to the `ANY()`
expression.


### Materialize and equi-join `ON fieldX = ANY(<array|list|map>)`

When evaluating an equi-join whose `ON` expression includes the [`ANY` operator
expression](/sql/functions/#expression-bool_op-any)
(i.e., `ON fieldX = ANY(<array|list|map>)`), Materialize performs a cross join,
which can lead to a significant increase in memory usage. If possible, rewrite
the query to perform an equi-join on the unnested values.


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
```text

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
```text

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

```text

</div>
</td>
</tr>

</tbody>
</table>


## Examples

> **Note:** 

The example data can be found in the
[Appendix](/transform-data/idiomatic-materialize-sql/appendix/example-orders).


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
```text

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
```text

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
```text

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


---

## Appendix


---

## First value in group


This section covers first value in group.

## Overview

The "first value in each group" query pattern returns the first value, according
to some ordering, in each group.


### Materialize and window functions


## Idiomatic Materialize SQL

**Idiomatic Materialize SQL:** To find the first value in each group, use
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
<td><blue>Materialize SQL</blue></td>
<td class="copyableCode">

Use a subquery that uses the [MIN()](/sql/functions/#min) or
[MAX()](/sql/functions/#max) aggregate function.

<br>
<div style="background-color: var(--code-block)">

```mzsql
SELECT tableA.fieldA, tableA.fieldB, minmax.Z
 FROM tableA,
 (SELECT fieldA,
    MIN(fieldZ),
    MAX(fieldZ)
 FROM tableA
 GROUP BY fieldA) minmax
WHERE tableA.fieldA = minmax.fieldA
ORDER BY fieldA ... ;
```text

</td>
</tr>

<tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

<red>Avoid the use of [`FIRST_VALUE() OVER (PARTITION BY ... ORDER BY ...)`
window function](/sql/functions/#first_value) for first value within groups
queries.</red>

<br>
<div style="background-color: var(--code-block)">

```nofmt
-- Anti-pattern. Avoid. --
SELECT fieldA, fieldB,
 FIRST_VALUE(fieldZ) OVER (PARTITION BY fieldA ORDER BY ...),
 FIRST_VALUE(fieldZ) OVER (PARTITION BY fieldA ORDER BY ... DESC)
FROM tableA
ORDER BY fieldA, ...;
```text

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
    MIN(fieldZ),
    MAX(fieldZ)
 FROM tableA
 GROUP BY fieldA
 OPTIONS (AGGREGATE INPUT GROUP SIZE = ...)
 ) minmax
WHERE tableA.fieldA = minmax.fieldA
ORDER BY fieldA ... ;
```text

For more information on setting `AGGREGATE INPUT GROUP SIZE`, see
[Optimization](/transform-data/optimization/#query-hints).

## Examples

> **Note:** 

The example data can be found in the
[Appendix](/transform-data/idiomatic-materialize-sql/appendix/example-orders).


### Use MIN() to find the first value

Using idiomatic Materialize SQL, the following example finds the lowest item
price in each order and calculates the difference between the price of each item
in the order and the lowest price. The example uses a subquery that groups by
the `order_id` and selects `MIN(price)` to find the lowest price (i.e., first
value if ordered by ascending price values).

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
SELECT o.order_id, minmax.lowest_price, o.item, o.price,
  o.price - minmax.lowest_price AS diff_lowest_price
FROM orders_view o,
      (SELECT order_id,
         MIN(price) AS lowest_price
      FROM orders_view
      GROUP BY order_id) minmax
WHERE o.order_id = minmax.order_id
ORDER BY o.order_id, o.item;
```text

</td>
</tr>
<tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

<red>Avoid the use of [`FIRST_VALUE() OVER (PARTITION BY ... ORDER BY ...)`
window function](/sql/functions/#first_value) for first value within groups queries.</red>

<br>
<div style="background-color: var(--code-block)">


```nofmt
-- Anti-pattern --
SELECT order_id,
  FIRST_VALUE(price)
    OVER (PARTITION BY order_id ORDER BY price) AS lowest_price,
  item,
  price,
  price - FIRST_VALUE(price)
    OVER (PARTITION BY order_id ORDER BY price) AS diff_lowest_price
FROM orders_view
ORDER BY order_id, item;
```text

</div>
</td>
</tr>
</tbody>
</table>

### Use MAX() to find the first value

Using idiomatic Materialize SQL, the following example finds the highest item
price in each order and calculates the difference between the price of each item
in the order and the highest price. The example uses a subquery that groups by
the `order_id` and selects `MAX(price)` to find the highest price (i.e., first
value if ordered by descending price values).

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
SELECT o.order_id, minmax.highest_price, o.item, o.price,
  o.price - minmax.highest_price AS diff_highest_price
FROM orders_view o,
      (SELECT order_id,
         MAX(price) AS highest_price
      FROM orders_view
      GROUP BY order_id) minmax
WHERE o.order_id = minmax.order_id
ORDER BY o.order_id, o.item;
```text

</td>
</tr>
<tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

<red>Avoid the use of [`FIRST_VALUE() OVER (PARTITION BY ... ORDER BY ...)`
window function](/sql/functions/#first_value) for first value within groups
queries.</red>

<br>
<div style="background-color: var(--code-block)">


```nofmt
-- Anti-pattern --
SELECT order_id,
  FIRST_VALUE(price)
    OVER (PARTITION BY order_id ORDER BY price DESC) AS highest_price,
  item,
  price,
  price - FIRST_VALUE(price)
    OVER (PARTITION BY order_id ORDER BY price DESC) AS diff_highest_price
FROM orders_view
ORDER BY order_id, item;
```text

</div>
</td>
</tr>

</tbody>
</table>

### Use MIN() and MAX() to find the first values

Using idiomatic Materialize SQL, the following example finds the lowest and the
highest item price in each order and calculates the difference between each item
in the order and these prices. The example uses a subquery that groups by the
`order_id` and selects `MIN(price)` as the lowest price (i.e., first
value if ordered by price values) and `MAX(price)` as the
highest price (i.e., first
value if ordered by descending price values)

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
```text

</td>
</tr>
<tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

<red>Avoid the use of [`FIRST_VALUE() OVER (PARTITION BY ... ORDER BY ...)`
window function](/sql/functions/#first_value) for first value within groups
queries.</red>

<br>
<div style="background-color: var(--code-block)">


```nofmt
-- Anti-pattern --
SELECT order_id,
  FIRST_VALUE(price)
    OVER (PARTITION BY order_id ORDER BY price) AS lowest_price,
  FIRST_VALUE(price)
    OVER (PARTITION BY order_id ORDER BY price DESC) AS highest_price,
  item,
  price,
  price - FIRST_VALUE(price)
    OVER (PARTITION BY order_id ORDER BY price) AS diff_lowest_price,
  price - FIRST_VALUE(price)
    OVER (PARTITION BY order_id ORDER BY price DESC) AS diff_highest_price
FROM orders_view
ORDER BY order_id, item;
```text
</div>
</td>
</tr>
</tbody>
</table>

## See also

- [Last value in a group](/transform-data/idiomatic-materialize-sql/last-value)
- [`MIN()`](/sql/functions/#min)
- [`MAX()`](/sql/functions/#max)
- [Query hints for MIN/MAX](/transform-data/optimization/#query-hints)
- [Window functions](/sql/functions/#window-functions)


---

## Lag over


This section covers lag over.

## Overview

The "lag over (order by )" query pattern accesses the field value of the
previous row as determined by some ordering.

For "lag over (order by)" queries whose ordering can be represented by some
equality condition (such as when ordering by a field that increases at a regular
interval), Materialize provides an idiomatic SQL as an alternative to the window
function.


### Materialize and window functions


## Idiomatic Materialize SQL

> **Important:** 

Do not use if the "lag over (order by)" ordering cannot be represented by an
equality match.


### Exclude the first row in results

**Idiomatic Materialize SQL:** To access the lag (previous row's field value)
ordered by some field that increases in a **regular** pattern, use a self join
that specifies an **equality condition** on the order by field (e.g., `WHERE
t1.order_field = t2.order_field + 1`, `WHERE t1.order_field = t2.order_field *
2`, etc.). The query *excludes* the first row since it does not have a previous
row.

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

Use a self join that specifies an **equality match** on the lag's order by field
(e.g., `fieldA`). The order by field must increment in a regular pattern in
order to be represented by an equality condition (e.g., `WHERE t1.fieldA =
t2.fieldA + ...`). The
query *excludes* the first row in the results since it does not have a previous
row.

> **Important:** 

The idiomatic Materialize SQL applies only to those "lag over" queries whose
ordering can be represented by some **equality condition**.


<br>

```mzsql
-- Excludes the first row in the results --
SELECT t1.fieldA, t2.fieldB as previous_row_value
FROM tableA t1, tableA t2
WHERE t1.fieldA = t2.fieldA + ... -- or some other operand
ORDER BY fieldA;
```text

</td>
</tr>

<tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

<red>

Avoid the use of [`LAG(fieldZ) OVER (ORDER BY ...)`](/sql/functions/#lag) window
function when the order by field increases in a regular pattern.

</red>

<br>

<div style="background-color: var(--code-block)">

```nofmt
-- Anti-pattern. Avoid. --
SELECT fieldA, ...
    LAG(fieldZ) OVER (ORDER BY fieldA) as previous_row_value
FROM tableA;
```text

</div>

</td>
</tr>

</tbody>
</table>

### Include the first row in results

**Idiomatic Materialize SQL:** To access the lag (previous row's field value)
ordered by some field that increases in a **regular** pattern, use a self
[`LEFT JOIN/LEFT OUTER JOIN`](/sql/select/join/#left-outer-join) that specifies
an **equality condition** on the order by field (e.g., `ON t1.order_field =
t2.order_field + 1`, `ON t1.order_field = t2.order_field * 2`, etc.). The `LEFT
JOIN/LEFT OUTER JOIN` query *includes* the first row, returning `null` as its
lag value.

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
match** on the lag's order by field (e.g., `fieldA`). The order by field must
increment in a regular pattern in order to be represented by an equality
condition (e.g., `ON t1.fieldA = t2.fieldA + ...`). The
query *includes* the first row, returning `null` as its lag value.

> **Important:** 

The idiomatic Materialize SQL applies only to those "lag over" queries whose
ordering can be represented by some **equality condition**.


<br>

```mzsql
-- Includes the first row in the results --
SELECT t1.fieldA, t2.fieldB as previous_row_value
FROM tableA t1
LEFT JOIN tableA t2
ON t1.fieldA = t2.fieldA + ... -- or some other operand
ORDER BY fieldA;
```text

</td>
</tr>

<tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

<red>

Avoid the use of [`LAG(fieldZ) OVER (ORDER BY ...) window
function`](/sql/functions/#lag) when the order by field increases in a regular
pattern.

</red>

<br>

<div style="background-color: var(--code-block)">

```nofmt
SELECT fieldA, ...
    LAG(fieldZ) OVER (ORDER BY fieldA) as previous_row_value
FROM tableA;
```text

</div>
</td>
</tr>

</tbody>
</table>


## Examples

> **Note:** 

The example data can be found in the
[Appendix](/transform-data/idiomatic-materialize-sql/appendix/example-orders).


### Find previous row's value (exclude the first row in results)

Using idiomatic Materialize SQL, the following example finds the previous day's
order total. That is, the example uses a self join on `orders_daily_totals`. The
row ordering on the `order_date` field is represented by an **equality
condition** using an [interval of `1
DAY`](/sql/types/interval/#valid-operations). The
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
```text

> **Important:** 

The idiomatic Materialize SQL applies only to those "lag over" queries whose
ordering can be represented by some **equality condition**.


</td>
</tr>

<tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

<red>Avoid the use of [`LAG() OVER (ORDER BY ...)` window
function](/sql/functions/#lag) to access previous row's value if the order by
field increases in a regular pattern.</red>

<br>
<div style="background-color: var(--code-block)">

```nofmt
-- Anti-pattern. Includes the first row's value. --
SELECT order_date, daily_total,
    LAG(daily_total) OVER (ORDER BY order_date) as previous_daily_total
FROM orders_daily_totals;
```text

</td>
</tr>
</tbody>
</table>

### Find previous row's value (include the first row in results)

Using idiomatic Materialize SQL, the following example finds the previous day's
order total. The example uses a self [`LEFT JOIN/LEFT OUTER
JOIN`](/sql/select/join/#left-outer-join) on `orders_daily_totals`. The
row ordering on the `order_date` field is represented by an **equality
condition** using an [interval of `1
DAY`](/sql/types/interval/#valid-operations). The
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
```text

> **Important:** 

The idiomatic Materialize SQL applies only to those "lag over" queries whose
ordering can be represented by some **equality condition**.


</td>
</tr>

<tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

<red>Avoid the use of [`LAG() OVER (ORDER BY ...)`
window function](/sql/functions/#lag) to access previous row's value if the
order by field increases in a regular pattern.</red>

<br>
<div style="background-color: var(--code-block)">

```nofmt
-- Anti-pattern. Includes the first row's value. --
SELECT order_date, daily_total,
    LAG(daily_total) OVER (ORDER BY order_date) as previous_daily_total
FROM orders_daily_totals;
```text

</td>
</tr>

</tbody>
</table>

## See also

- [Lead over](/transform-data/idiomatic-materialize-sql/lead)
- [`INTERVAL`](/sql/types/interval/)
- [`LEFT JOIN/LEFT OUTER JOIN`](/sql/select/join/#left-outer-join)
- [`LAG()`](/sql/functions/#lag)
- [Window functions](/sql/functions/#window-functions)


---

## Last value in group


This section covers last value in group.

## Overview

The "last value in each group" query pattern returns the last value, according
to some ordering, in each group.


### Materialize and window functions


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
```text

</td>
</tr>

<tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

<red>Do not use [`LAST_VALUE() OVER (PARTITION BY ... ORDER BY ... RANGE
...)` window function](/sql/functions/#last_value) for last value in each group
queries.</red>

> **Note:** 

Materialize does not support `RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED
FOLLOWING`.


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
```text

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
```text

For more information on setting `AGGREGATE INPUT GROUP SIZE`, see
[Optimization](/transform-data/optimization/#query-hints).

## Examples

> **Note:** 

The example data can be found in the
[Appendix](/transform-data/idiomatic-materialize-sql/appendix/example-orders).


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
```text

</td>
</tr>

<tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

<red>Do not use of `LAST_VALUE() OVER (PARTITION BY ... ORDER BY ... RANGE ...)`
for last value in each group queries.</red>

> **Note:** 

Materialize does not support `RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED
FOLLOWING`.


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
```text

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
```text

</td>
</tr>

<tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

<red>Do not use `LAST_VALUE() OVER (PARTITION BY ... ORDER BY ... RANGE ... )`
for last value in each group queries.</red>

> **Note:** 

Materialize does not support `RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED
FOLLOWING`.


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
```text

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
```text

</td>
</tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

<red>Do not use `LAST_VALUE() OVER (PARTITION BY ... ORDER BY
)` for last value within groups queries.</red>

> **Note:** 

Materialize does not support `RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED
FOLLOWING`.


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
```text

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


---

## Lead over


This section covers lead over.

## Overview

The "lead over" query pattern accesses the field value of the next row as
determined by some ordering.

For "lead over (order by)" queries whose ordering can be represented by some
equality condition (such as when ordering by a field that increases at a regular
interval), Materialize provides an idiomatic SQL as an alternative to the window
function.


### Materialize and window functions


## Idiomatic Materialize SQL

> **Important:** 

Do not use if the "lead over (order by)" ordering cannot be represented by an
equality match.


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

> **Important:** 

The idiomatic Materialize SQL applies only to those "lead over" queries whose
ordering can be represented by some **equality condition**.


<br>

```mzsql
-- Excludes the last row in the results --
SELECT t1.fieldA, t2.fieldB as next_row_value
FROM tableA t1, tableA t2
WHERE t1.fieldA = t2.fieldA - ...  -- or some other operand
ORDER BY fieldA;
```text

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
```text

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

> **Important:** 

The idiomatic Materialize SQL applies only to those "lead over" queries whose
ordering can be represented by some **equality condition**.


```mzsql
-- Includes the last row in the response --
SELECT t1.fieldA, t2.fieldB as next_row_value
FROM tableA t1
LEFT JOIN tableA t2
ON t1.fieldA = t2.fieldA - ... -- or some other operand
ORDER BY fieldA;
```text

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
```text

</div>
</td>
</tr>

</tbody>
</table>

## Examples

> **Note:** 

The example data can be found in the
[Appendix](/transform-data/idiomatic-materialize-sql/appendix/example-orders).


### Find next row's value (exclude the last row in results)

Using idiomatic Materialize SQL, the following example finds the next day's
order total. That is, the example uses a self join on `orders_daily_totals`. The
row ordering on the `order_date` field is represented by an **equality
condition** using an [interval of `1
DAY`](/sql/types/interval/#valid-operations). The
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
```text

> **Important:** 

The idiomatic Materialize SQL applies only to those "lead over" queries whose
ordering can be represented by some **equality condition**.


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
```text

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
DAY`](/sql/types/interval/#valid-operations)). The
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
```text

> **Important:** 

The idiomatic Materialize SQL applies only to those "lead over" queries whose
ordering can be represented by some **equality condition**.


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
```text

</td>
</tr>

</tbody>
</table>

## See also

- [Lag over](/transform-data/idiomatic-materialize-sql/lag)
- [`INTERVAL`](/sql/types/interval/)
- [`LEFT JOIN/LEFT OUTER JOIN`](/sql/select/join/#left-outer-join)
- [`LEAD()`](/sql/functions/#lead)
- [Window functions](/sql/functions/#window-functions)


---

## mz_now() expressions


This section covers mz_now() expressions.

## Overview

In Materialize, [`mz_now()`](/sql/functions/now_and_mz_now/) function returns
Materialize's current virtual timestamp (i.e., returns
[`mz_timestamp`](/sql/types/mz_timestamp/)). The function can be used in
[temporal filters](/transform-data/patterns/temporal-filters/) to reduce the
working dataset.

`mz_now()` expression has the following form:

```mzsql
mz_now() <comparison_operator> <numeric_expr | timestamp_expr>
```bash

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


---

## Top-K in group


This section covers top-k in group.

## Overview

The "Top-K in group" query pattern groups by some key and return the first K
elements within each group according to some ordering.


### Materialize and window functions


## Idiomatic Materialize SQL

This section covers idiomatic materialize sql.

### For K >= 1

**Idiomatic Materialize SQL**: For Top-K queries where K >= 1, use a subquery to
[SELECT DISTINCT](/sql/select/#select-distinct) on the grouping key and perform
a [LATERAL](/sql/select/join/#lateral-subqueries) join (by the grouping key)
with another subquery that specifies the ordering and the limit K.

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

Use a subquery to
[SELECT DISTINCT](/sql/select/#select-distinct) on the grouping key (e.g.,
`fieldA`), and perform a [LATERAL](/sql/select/join/#lateral-subqueries) join
(by the grouping key `fieldA`) with another subquery that specifies the ordering
(e.g., `fieldZ [ASC|DESC]`) and the limit K.

```mzsql
SELECT fieldA, fieldB, ...
FROM (SELECT DISTINCT fieldA FROM tableA) grp,
     LATERAL (SELECT fieldB, ... , fieldZ FROM tableA
        WHERE fieldA = grp.fieldA
        ORDER BY fieldZ ... LIMIT K)   -- K is a number >= 1
ORDER BY fieldA, fieldZ ... ;
```text

</td>
</tr>
<tr>
<td><red>Anti-pattern</red></td>
<td>

<red>Avoid the use of `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)` for Top-K queries.</red>

<br>
<div style="background-color: var(--code-block)">

```nofmt
-- Anti-pattern. Avoid. --
SELECT fieldA, fieldB, ...
FROM (
   SELECT fieldA, fieldB, ... , fieldZ,
      ROW_NUMBER() OVER (PARTITION BY fieldA
      ORDER BY fieldZ ... ) as rn
   FROM tableA)
WHERE rn <= K     -- K is a number >= 1
ORDER BY fieldA, fieldZ ...;
```text

</div>
</td>
</tr>
</tbody>
</table>

#### Query hints

To further improve the memory usage of the idiomatic Materialize SQL, you can
specify a [`LIMIT INPUT GROUP SIZE` query hint](/sql/select/#query-hints) in the
idiomatic Materialize SQL.

```mzsql
SELECT fieldA, fieldB, ...
FROM (SELECT DISTINCT fieldA FROM tableA) grp,
     LATERAL (SELECT fieldB, ... , fieldZ FROM tableA
        WHERE fieldA = grp.fieldA
        OPTIONS (LIMIT INPUT GROUP SIZE = ...)
        ORDER BY fieldZ ... LIMIT K)   -- K is a number >= 1
ORDER BY fieldA, fieldZ ... ;
```text

For more information on setting `LIMIT INPUT GROUP SIZE`, see
[Optimization](/transform-data/optimization/#query-hints).

### For K = 1

**Idiomatic Materialize SQL**: For K = 1, use a [SELECT DISTINCT
ON()](/sql/select/#select-distinct-on) on the grouping key (e.g., `fieldA`) and
order the results first by the `DISTINCT ON` key and then the Top-K ordering
key (e.g., `fieldA, fieldZ [ASC|DESC]`).

Alternatively, you can also use the more general [Top-K where K >= 1](#for-k--1)
pattern, specifying 1 as the limit.

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

```mzsql
SELECT DISTINCT ON(fieldA) fieldA, fieldB, ...
FROM tableA
ORDER BY fieldA, fieldZ ... ;
```text

</div>
</td>
</tr>

<tr>
<td><red>Anti-pattern</red></td>
<td>

<red>Avoid the use of `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)` for Top-K queries.</red>

<br>
<div style="background-color: var(--code-block)">

```nofmt
-- Anti-pattern. Avoid. --
SELECT fieldA, fieldB, ...
FROM (
   SELECT fieldA, fieldB, ... , fieldZ,
      ROW_NUMBER() OVER (PARTITION BY fieldA
      ORDER BY fieldZ ... ) as rn
   FROM tableA)
WHERE rn = 1
ORDER BY fieldA, fieldZ ...;
```text

</div>
</td>
</tr>
</tbody>
</table>

### Query hints

To further improve the memory usage of the idiomatic Materialize SQL, you can
specify a [`DISTINCT ON INPUT GROUP SIZE` query hint](/sql/select/#query-hints)
in the idiomatic Materialize SQL.

```mzsql
SELECT DISTINCT ON(fieldA) fieldA, fieldB, ...
FROM tableA
OPTIONS (DISTINCT ON INPUT GROUP SIZE = ...)
ORDER BY fieldA, fieldZ ... ;
```text

For more information on setting `DISTINCT ON INPUT GROUP SIZE`, see
[`EXPLAIN ANALYZE HINTS`](/sql/explain-analyze/#explain-analyze-hints).

## Examples

> **Note:** 

The example data can be found in the
[Appendix](/transform-data/idiomatic-materialize-sql/appendix/example-orders).


### Select Top-3 items

Using idiomatic Materialize SQL, the following example finds the top 3 items (by
descending subtotal) in each order. The example uses a subquery to [SELECT
DISTINCT](/sql/select/#select-distinct) on the grouping key (`order_id`), and
performs a [LATERAL](/sql/select/join/#lateral-subqueries) join (by the grouping
key) with another subquery that specifies the ordering (`ORDER BY subtotal
DESC`) and limits its results to 3 (`LIMIT 3`).

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

```mzsql
SELECT order_id, item, subtotal
FROM (SELECT DISTINCT order_id FROM orders_view) grp,
     LATERAL (SELECT item, subtotal FROM orders_view
        WHERE order_id = grp.order_id
        ORDER BY subtotal DESC LIMIT 3)
ORDER BY order_id, subtotal DESC;
```text

</td>
</tr>

<tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

<red>Avoid the use of `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)` for Top-K queries.</red>

<br>
<div style="background-color: var(--code-block)">

```nofmt
-- Anti-pattern --
SELECT order_id, item, subtotal
FROM (
   SELECT order_id, item, subtotal,
      ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY subtotal DESC) as rn
   FROM orders_view)
WHERE rn <= 3
ORDER BY order_id, subtotal DESC;
```text

</div>
</td>
</tr>

</tbody>
</table>

### Select Top-1 item

Using idiomatic Materialize SQL, the following example finds the top 1 item (by
descending subtotal) in each order. The example uses a query to [SELECT DISTINCT
ON()](/sql/select/#select-distinct-on) on the grouping key (`order_id`) with an
`ORDER BY order_id, subtotal DESC` (i.e., ordering first by the `DISTINCT
ON`/grouping key, then the descending subtotal). [^1]

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

```mzsql
SELECT DISTINCT ON(order_id) order_id, item, subtotal
FROM orders_view
ORDER BY order_id, subtotal DESC;
```text

</td>
</tr>
<tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

<red>Avoid the use of `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)` for Top-K queries.</red>

<br>
<div style="background-color: var(--code-block)">

```nofmt
-- Anti-pattern --
SELECT order_id, item, subtotal
FROM (
   SELECT order_id, item, subtotal,
      ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY subtotal DESC) as rn
   FROM orders_view)
WHERE rn = 1
ORDER BY order_id, subtotal DESC;
```

</div>
</td>
</tr>
</tbody>
</table>

[^1]: Alternatively, you can also use the [idiomatic Materialize SQL for the
    more general Top K query](#for-k--1), specifying 1 as the limit.

## See also

- [SELECT DISTINCT](/sql/select/#select-distinct)
- [LATERAL subqueries](/sql/select/join/#lateral-subqueries)
- [Query hints for Top K](/transform-data/optimization/#query-hints)
- [Window functions](/sql/functions/#window-functions)