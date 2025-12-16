<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/self-managed/v25.2/)
 /  [Overview](/docs/self-managed/v25.2/transform-data/)  /  [Idiomatic
Materialize
SQL](/docs/self-managed/v25.2/transform-data/idiomatic-materialize-sql/)

</div>

# Last value in group

## Overview

The “last value in each group” query pattern returns the last value,
according to some ordering, in each group.

<div class="callout">

<div>

### Materialize and window functions

For [window
functions](/docs/self-managed/v25.2/sql/functions/#window-functions),
when an input record in a partition (as determined by the `PARTITION BY`
clause of your window function) is added/removed/changed, Materialize
recomputes the results for the entire window partition. This means that
when a new batch of input data arrives (that is, every second), **the
amount of computation performed is proportional to the total size of the
touched partitions**.

For example, assume that in a given second, 20 input records change, and
these records belong to **10** different partitions, where the average
size of each partition is **100**. Then, amount of work to perform is
proportional to computing the window function results for
**10\*100=1000** rows.

To avoid performance issues that may arise as the number of records
grows, consider rewriting your query to use idiomatic Materialize SQL
instead of window functions. If your query cannot be rewritten without
the window functions and the performance of window functions is
insufficient for your use case, please [contact our
team](/docs/self-managed/v25.2/support/).

</div>

</div>

## Idiomatic Materialize SQL

**Idiomatic Materialize SQL:** To find the last value in each group, use
the [MIN()](/docs/self-managed/v25.2/sql/functions/#min) or
[MAX()](/docs/self-managed/v25.2/sql/functions/#max) aggregate function
in a subquery.

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<tbody>
<tr>
<td>Idiomatic Materialize SQL</td>
<td class="copyableCode"><p>Use a subquery that uses the <a
href="/docs/self-managed/v25.2/sql/functions/#min">MIN()</a> or <a
href="/docs/self-managed/v25.2/sql/functions/#max">MAX()</a> aggregate
function.</p>
<br />
&#10;<div style="background-color: var(--code-block)">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>SELECT tableA.fieldA, tableA.fieldB, minmax.Z
 FROM tableA,
 (SELECT fieldA,
    MAX(fieldZ),
    MIN(fieldZ)
 FROM tableA
 GROUP BY fieldA) minmax
WHERE tableA.fieldA = minmax.fieldA
ORDER BY fieldA ... ;</code></pre>
</div>
</div></td>
</tr>
<tr>
<td>Anti-pattern ❌</td>
<td><p>Do not use <a
href="/docs/self-managed/v25.2/sql/functions/#last_value"><code>LAST_VALUE() OVER (PARTITION BY ... ORDER BY ... RANGE ...)</code>
window function</a> for last value in each group queries.</p>
<div class="note">
<strong>NOTE:</strong> Materialize does not support
<code>RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING</code>.
</div>
<br />
&#10;<div style="background-color: var(--code-block)">
<pre tabindex="0"><code>-- Unsupported --
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
ORDER BY fieldA, ...;</code></pre>
</div></td>
</tr>
</tbody>
</table>

### Query hints

To further improve the memory usage of the idiomatic Materialize SQL,
you can specify a [`AGGREGATE INPUT GROUP SIZE` query
hint](/docs/self-managed/v25.2/sql/select/#query-hints) in the idiomatic
Materialize SQL.

<div class="highlight">

``` chroma
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

</div>

For more information on setting `AGGREGATE INPUT GROUP SIZE`, see
[Optimization](/docs/self-managed/v25.2/transform-data/optimization/#query-hints).

## Examples

<div class="note">

**NOTE:** The example data can be found in the
[Appendix](/docs/self-managed/v25.2/transform-data/idiomatic-materialize-sql/appendix/example-orders).

</div>

### Use MAX() to find the last value

Using idiomatic Materialize SQL, the following example finds the highest
item price in each order and calculates the difference between the price
of each item in the order and the highest price. The example uses a
subquery that groups by the `order_id` and selects
[`MAX(price)`](/docs/self-managed/v25.2/sql/functions/#max) to find the
highest price (i.e., the last price if ordered by ascending price
values):

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<tbody>
<tr>
<td>Idiomatic Materialize SQL ✅</td>
<td class="copyableCode"><div class="highlight">
<pre class="chroma" tabindex="0"><code>SELECT o.order_id, minmax.highest_price, o.item, o.price,
  o.price - minmax.highest_price AS diff_highest_price
FROM orders_view o,
     (SELECT order_id,
        MAX(price) AS highest_price
     FROM orders_view
     GROUP BY order_id) minmax
WHERE o.order_id = minmax.order_id
ORDER BY o.order_id, o.item;</code></pre>
</div></td>
</tr>
<tr>
<td>Anti-pattern ❌</td>
<td><p>Do not use of
<code>LAST_VALUE() OVER (PARTITION BY ... ORDER BY ... RANGE ...)</code>
for last value in each group queries.</p>
<div class="note">
<strong>NOTE:</strong> Materialize does not support
<code>RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING</code>.
</div>
<div style="background-color: var(--code-block)">
<pre tabindex="0"><code>-- Unsupported --
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
ORDER BY order_id, item;</code></pre>
</div></td>
</tr>
</tbody>
</table>

### Use MIN() to find the last values

Using idiomatic Materialize SQL, the following example finds the lowest
item price in each order and calculates the difference between the price
of each item in the order and the lowest price. That is, use a subquery
that groups by the `order_id` and selects
[`MIN(price)`](/docs/self-managed/v25.2/sql/functions/#min) as the
lowest price (i.e., last price if ordered by descending price value)

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<tbody>
<tr>
<td>Idiomatic Materialize SQL ✅</td>
<td class="copyableCode"><div class="highlight">
<pre class="chroma" tabindex="0"><code>SELECT o.order_id, minmax.lowest_price, o.item, o.price,
  o.price - minmax.lowest_price AS diff_lowest_price
FROM orders_view o,
     (SELECT order_id,
        MIN(price) AS lowest_price
     FROM orders_view
     GROUP BY order_id) minmax
WHERE o.order_id = minmax.order_id
ORDER BY o.order_id, o.item;</code></pre>
</div></td>
</tr>
<tr>
<td>Anti-pattern ❌</td>
<td><p>Do not use
<code>LAST_VALUE() OVER (PARTITION BY ... ORDER BY ... RANGE ... )</code>
for last value in each group queries.</p>
<div class="note">
<strong>NOTE:</strong> Materialize does not support
<code>RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING</code>.
</div>
<div style="background-color: var(--code-block)">
<pre tabindex="0"><code>-- Unsupported --
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
ORDER BY order_id, item;</code></pre>
</div></td>
</tr>
</tbody>
</table>

### Use MIN() and MAX() to find the last values

Using idiomatic Materialize SQL, the following example finds the lowest
and highest item price in each order and calculate the difference for
each item in the order from these prices. That is, use a subquery that
groups by the `order_id` and selects
[`MIN(price)`](/docs/self-managed/v25.2/sql/functions/#min) as the
lowest price (i.e., last value if ordered by descending price values)
and [`MAX(price)`](/docs/self-managed/v25.2/sql/functions/#max) as the
highest price (i.e., last value if ordered by ascending price values).

</div>

</div>
