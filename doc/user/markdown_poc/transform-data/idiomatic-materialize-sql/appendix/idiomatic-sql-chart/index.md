<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [Overview](/docs/transform-data/)  /  [Idiomatic
Materialize SQL](/docs/transform-data/idiomatic-materialize-sql/)
 /  [Appendix](/docs/transform-data/idiomatic-materialize-sql/appendix/)

</div>

# Idiomatic Materialize SQL chart

Materialize follows the SQL standard (SQL-92) implementation and strives
for compatibility with the PostgreSQL dialect. However, for some use
cases, Materialize provides its own idiomatic query patterns that can
provide better performance.

## General

### Query Patterns

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th></th>
<th>Idiomatic Materialize SQL Pattern</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>ANY()</code> Equi-join condition</td>
<td class="copyableCode"><p><em><strong>If no duplicates in the unnested
field</strong></em></p>
<div class="highlight">
<pre class="chroma" tabindex="0"><code>WITH my_expanded_values AS
(SELECT UNNEST(array|list|map) AS fieldZ FROM tableB)
SELECT a.fieldA, ...
FROM tableA a
JOIN my_expanded_values t ON a.fieldZ = t.fieldZ
;</code></pre>
</div>
<p><em><strong>If duplicates exist in the unnested
field</strong></em></p>
<div class="highlight">
<pre class="chroma" tabindex="0"><code>WITH my_expanded_values AS
(SELECT DISTINCT UNNEST(array|list|map) AS fieldZ FROM tableB)
SELECT a.fieldA, ...
FROM tableA a
JOIN my_expanded_values t ON a.fieldZ = t.fieldZ
;</code></pre>
</div></td>
</tr>
<tr>
<td><code>mz_now()</code> cannot be used with date/time operators</td>
<td>Rewrite the query expression; specifically, move the operation to
the other side of the comparison.</td>
</tr>
<tr>
<td><code>mz_now()</code> cannot be used with <code>OR</code>s in
materialized/indexed view definitions and <code>SUBSCRIBE</code>
statements</td>
<td>Rewrite as <code>UNION ALL</code> or <code>UNION</code>,
deduplicating as necessary:
<ul>
<li>In some cases, you may need to modify the conditions to deduplicate
results when using <code>UNION ALL</code>. For example, you might add
the negation of one input's condition to the other as a
conjunction.</li>
<li>In some cases, using <code>UNION</code> instead of
<code>UNION ALL</code> may suffice if the inputs do not contain other
duplicates that need to be retained.</li>
</ul></td>
</tr>
</tbody>
</table>

### Examples

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th></th>
<th>Idiomatic Materialize SQL</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>ANY()</code> Equi-join condition</td>
<td class="copyableCode"><p><em><strong>If no duplicates in the unnested
field</strong></em></p>
<div class="highlight">
<pre class="chroma" tabindex="0"><code>-- sales_items.items contains no duplicates. --
&#10;WITH individual_sales_items AS
(SELECT unnest(items) as item, week_of FROM sales_items)
SELECT s.week_of, o.order_id, o.item, o.quantity
FROM orders o
JOIN individual_sales_items s ON o.item = s.item
WHERE date_trunc(&#39;week&#39;, o.order_date) = s.week_of;</code></pre>
</div>
<p><em><strong>If duplicates exist in the unnested
field</strong></em></p>
<div class="highlight">
<pre class="chroma" tabindex="0"><code>-- sales_items.items may contains duplicates --
&#10;WITH individual_sales_items AS
(SELECT DISTINCT unnest(items) as item, week_of FROM sales_items)
SELECT s.week_of, o.order_id, o.item, o.quantity
FROM orders o
JOIN individual_sales_items s ON o.item = s.item
WHERE date_trunc(&#39;week&#39;, o.order_date) = s.week_of
ORDER BY s.week_of, o.order_id, o.item, o.quantity
;</code></pre>
</div></td>
</tr>
<tr>
<td><code>mz_now()</code> cannot be used with date/time operators</td>
<td class="copyableCode"><div class="highlight">
<pre class="chroma" tabindex="0"><code>SELECT * from orders
WHERE mz_now() &gt; order_date + INTERVAL &#39;5min&#39;
;</code></pre>
</div></td>
</tr>
<tr>
<td><code>mz_now()</code> cannot be used with <code>OR</code>s in
materialized/indexed view definitions and <code>SUBSCRIBE</code>
statements</td>
<td class="copyableCode"><p><strong>Rewrite as <code>UNION ALL</code>
with possible duplicates</strong></p>
<div class="highlight">
<pre class="chroma" tabindex="0"><code>CREATE MATERIALIZED VIEW forecast_completed_orders_duplicates_possible AS
SELECT item, quantity, status from orders
WHERE status = &#39;Shipped&#39;
UNION ALL
SELECT item, quantity, status from orders
WHERE order_date + interval &#39;30&#39; minutes &gt;= mz_now()
;</code></pre>
</div>
<p><strong>Rewrite as UNION ALL that avoids duplicates across
queries</strong></p>
<div class="highlight">
<pre class="chroma" tabindex="0"><code>CREATE MATERIALIZED VIEW forecast_completed_orders_deduplicated_union_all AS
SELECT item, quantity, status from orders
WHERE status = &#39;Shipped&#39;
UNION ALL
SELECT item, quantity, status from orders
WHERE order_date + interval &#39;30&#39; minutes &gt;= mz_now()
AND status != &#39;Shipped&#39; -- Deduplicate by excluding those with status &#39;Shipped&#39;
;</code></pre>
</div>
<p><strong>Rewrite as UNION to deduplicate any and all duplicated
results</strong></p>
<div class="highlight">
<pre class="chroma" tabindex="0"><code>CREATE MATERIALIZED VIEW forecast_completed_orders_deduplicated_results AS
SELECT item, quantity, status from orders
WHERE status = &#39;Shipped&#39;
UNION
SELECT item, quantity, status from orders
WHERE order_date + interval &#39;30&#39; minutes &gt;= mz_now()
;</code></pre>
</div></td>
</tr>
</tbody>
</table>

## Window Functions

<div class="callout">

<div>

### Materialize and window functions

For [window functions](/docs/sql/functions/#window-functions), when an
input record in a partition (as determined by the `PARTITION BY` clause
of your window function) is added/removed/changed, Materialize
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
team](/docs/support/).

</div>

</div>

### Query Patterns

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th></th>
<th>Idiomatic Materialize SQL Pattern</th>
</tr>
</thead>
<tbody>
<tr>
<td>Top-K over partition<br />
(K &gt;= 1)</td>
<td class="copyableCode"><div class="highlight">
<pre class="chroma" tabindex="0"><code>SELECT fieldA, fieldB, ...
FROM (SELECT DISTINCT fieldA FROM tableA) grp,
      LATERAL (SELECT fieldB, ... , fieldZ FROM tableA
         WHERE fieldA = grp.fieldA
         ORDER BY fieldZ ... LIMIT K)   -- K is a number &gt;= 1
ORDER BY fieldA, fieldZ ... ;</code></pre>
</div></td>
</tr>
<tr>
<td>Top-K over partition<br />
(K = 1)</td>
<td class="copyableCode"><div class="highlight">
<pre class="chroma" tabindex="0"><code>SELECT DISTINCT ON(fieldA) fieldA, fieldB, ...
FROM tableA
ORDER BY fieldA, fieldZ ...  -- Top-K where K is 1;</code></pre>
</div></td>
</tr>
<tr>
<td>First value over partition<br />
order by ...</td>
<td class="copyableCode"><div class="highlight">
<pre class="chroma" tabindex="0"><code>SELECT tableA.fieldA, tableA.fieldB, minmax.Z
   FROM tableA,
   (SELECT fieldA,
      MIN(fieldZ)      -- Or MAX()
   FROM tableA
   GROUP BY fieldA) minmax
WHERE tableA.fieldA = minmax.fieldA
ORDER BY fieldA ... ;</code></pre>
</div></td>
</tr>
<tr>
<td>Last value over partition<br />
order by ...<br />
range between unbounded preceding<br />
and unbounded following</td>
<td class="copyableCode"><div class="highlight">
<pre class="chroma" tabindex="0"><code>SELECT tableA.fieldA, tableA.fieldB, minmax.Z
   FROM tableA,
   (SELECT fieldA,
      MAX(fieldZ)      -- Or MIN()
   FROM tableA
   GROUP BY fieldA) minmax
WHERE tableA.fieldA = minmax.fieldA
ORDER BY fieldA ... ;</code></pre>
</div></td>
</tr>
<tr>
<td><p>Lag over (order by) whose ordering can be represented by some
equality condition.</p></td>
<td class="copyableCode"><p><em><strong>To exclude the first row since
it has no previous row</strong></em></p>
<div class="highlight">
<pre class="chroma" tabindex="0"><code>SELECT t1.fieldA, t2.fieldB
FROM tableA t1, tableA t2
WHERE t1.fieldA = t2.fieldA +  ...
ORDER BY fieldA;</code></pre>
</div>
<p><em><strong>To include the first row</strong></em></p>
<div class="highlight">
<pre class="chroma" tabindex="0"><code>SELECT t1.fieldA, t2.fieldB
FROM tableA t1
LEFT JOIN tableA t2
ON t1.fieldA = t2.fieldA +  ...
ORDER BY fieldA;</code></pre>
</div></td>
</tr>
<tr>
<td><p>Lead over (order by) whose ordering can be represented by some
equality condition.</p></td>
<td class="copyableCode"><p><em><strong>To exclude the last row since it
has no next row</strong></em></p>
<div class="highlight">
<pre class="chroma" tabindex="0"><code>SELECT t1.fieldA, t2.fieldB
FROM tableA t1, tableA t2
WHERE t1.fieldA = t2.fieldA - ...
ORDER BY fieldA;</code></pre>
</div>
<p><em><strong>To include the last row</strong></em></p>
<div class="highlight">
<pre class="chroma" tabindex="0"><code>SELECT t1.fieldA, t2.fieldB
FROM tableA t1
LEFT JOIN tableA t2
ON t1.fieldA = t2.fieldA -  ...
ORDER BY fieldA;</code></pre>
</div></td>
</tr>
</tbody>
</table>

### Examples

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th></th>
<th>Idiomatic Materialize SQL</th>
</tr>
</thead>
<tbody>
<tr>
<td>Top-K over partition<br />
(K &gt;= 1)</td>
<td class="copyableCode"><div class="highlight">
<pre class="chroma" tabindex="0"><code>SELECT order_id, item, subtotal
FROM (SELECT DISTINCT order_id FROM orders_view) grp,
        LATERAL (SELECT item, subtotal FROM orders_view
        WHERE order_id = grp.order_id
        ORDER BY subtotal DESC LIMIT 3) -- For Top 3
ORDER BY order_id, subtotal DESC;</code></pre>
</div></td>
</tr>
<tr>
<td>Top-K over partition<br />
(K = 1)</td>
<td class="copyableCode"><div class="highlight">
<pre class="chroma" tabindex="0"><code>SELECT DISTINCT ON(order_id) order_id, item, subtotal
FROM orders_view
ORDER BY order_id, subtotal DESC;  -- For Top 1</code></pre>
</div></td>
</tr>
<tr>
<td>First value over partition<br />
order by ...</td>
<td class="copyableCode"><div class="highlight">
<pre class="chroma" tabindex="0"><code>SELECT o.order_id, minmax.lowest_price, minmax.highest_price,
    o.item,
    o.price,
    o.price - minmax.lowest_price AS diff_lowest_price,
    o.price - minmax.highest_price AS diff_highest_price
FROM orders_view o,
        (SELECT order_id,
            MIN(price) AS lowest_price,
            MAX(price) AS highest_price
        FROM orders_view
        GROUP BY order_id) minmax
WHERE o.order_id = minmax.order_id
ORDER BY o.order_id, o.item;</code></pre>
</div></td>
</tr>
<tr>
<td>Last value over partition<br />
order by...<br />
range between unbounded preceding<br />
and unbounded following</td>
<td class="copyableCode"><div class="highlight">
<pre class="chroma" tabindex="0"><code>SELECT o.order_id, minmax.lowest_price, minmax.highest_price,
    o.item,
    o.price,
    o.price - minmax.lowest_price AS diff_lowest_price,
    o.price - minmax.highest_price AS diff_highest_price
FROM orders_view o,
        (SELECT order_id,
            MIN(price) AS lowest_price,
            MAX(price) AS highest_price
        FROM orders_view
        GROUP BY order_id) minmax
WHERE o.order_id = minmax.order_id
ORDER BY o.order_id, o.item;</code></pre>
</div></td>
</tr>
<tr>
<td><p>Lag over (order by) whose ordering can be represented by some
equality condition.</p></td>
<td class="copyableCode"><p><em><strong>If suppressing the first row
since it has no previous row</strong></em></p>
<div class="highlight">
<pre class="chroma" tabindex="0"><code>SELECT o1.order_date, o1.daily_total,
    o2.daily_total as previous_daily_total
FROM orders_daily_totals o1, orders_daily_totals o2
WHERE o1.order_date = o2.order_date + INTERVAL &#39;1&#39; DAY
ORDER BY order_date;</code></pre>
</div>
<p><em><strong>To include the first row</strong></em></p>
<div class="highlight">
<pre class="chroma" tabindex="0"><code>SELECT o1.order_date, o1.daily_total,
    o2.daily_total as previous_daily_total
FROM orders_daily_totals o1
LEFT JOIN orders_daily_totals o2
ON o1.order_date = o2.order_date + INTERVAL &#39;1&#39; DAY
ORDER BY order_date;</code></pre>
</div></td>
</tr>
<tr>
<td><p>Lead over (order by) whose ordering can be represented by some
equality condition.</p></td>
<td class="copyableCode"><p><em><strong>To suppress the last row since
it has no next row</strong></em></p>
<div class="highlight">
<pre class="chroma" tabindex="0"><code>SELECT o1.order_date, o1.daily_total,
    o2.daily_total as previous_daily_total
FROM orders_daily_totals o1, orders_daily_totals o2
WHERE o1.order_date = o2.order_date - INTERVAL &#39;1&#39; DAY
ORDER BY order_date;</code></pre>
</div>
<p><em><strong>To include the last row</strong></em></p>
<div class="highlight">
<pre class="chroma" tabindex="0"><code>SELECT o1.order_date, o1.daily_total,
    o2.daily_total as previous_daily_total
FROM orders_daily_totals o1
LEFT JOIN orders_daily_totals o2
ON o1.order_date = o2.order_date - INTERVAL &#39;1&#39; DAY
ORDER BY order_date;</code></pre>
</div></td>
</tr>
</tbody>
</table>

## See also

- [SQL Functions](/docs/sql/functions/)
- [SQL Types](/docs/sql/types/)
- [SELECT](/docs/sql/select/)
- [DISTINCT](/docs/sql/select/#select-distinct)
- [DISTINCT ON](/docs/sql/select/#select-distinct-on)

</div>

<a href="#top" class="back-to-top">Back to top ↑</a>

<div class="theme-switcher">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzeXN0ZW0iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+U3lzdGVtIFRoZW1lPC90aXRsZT4KICAgICAgICA8cGF0aCBkPSJNMjU2IDE3NmE4MCA4MCAwIDEwODAgODAgODAuMjQgODAuMjQgMCAwMC04MC04MHptMTcyLjcyIDgwYTE2NS41MyAxNjUuNTMgMCAwMS0xLjY0IDIyLjM0bDQ4LjY5IDM4LjEyYTExLjU5IDExLjU5IDAgMDEyLjYzIDE0Ljc4bC00Ni4wNiA3OS41MmExMS42NCAxMS42NCAwIDAxLTE0LjE0IDQuOTNsLTU3LjI1LTIzYTE3Ni41NiAxNzYuNTYgMCAwMS0zOC44MiAyMi42N2wtOC41NiA2MC43OGExMS45MyAxMS45MyAwIDAxLTExLjUxIDkuODZoLTkyLjEyYTEyIDEyIDAgMDEtMTEuNTEtOS41M2wtOC41Ni02MC43OEExNjkuMyAxNjkuMyAwIDAxMTUxLjA1IDM5M0w5My44IDQxNmExMS42NCAxMS42NCAwIDAxLTE0LjE0LTQuOTJMMzMuNiAzMzEuNTdhMTEuNTkgMTEuNTkgMCAwMTIuNjMtMTQuNzhsNDguNjktMzguMTJBMTc0LjU4IDE3NC41OCAwIDAxODMuMjggMjU2YTE2NS41MyAxNjUuNTMgMCAwMTEuNjQtMjIuMzRsLTQ4LjY5LTM4LjEyYTExLjU5IDExLjU5IDAgMDEtMi42My0xNC43OGw0Ni4wNi03OS41MmExMS42NCAxMS42NCAwIDAxMTQuMTQtNC45M2w1Ny4yNSAyM2ExNzYuNTYgMTc2LjU2IDAgMDEzOC44Mi0yMi42N2w4LjU2LTYwLjc4QTExLjkzIDExLjkzIDAgMDEyMDkuOTQgMjZoOTIuMTJhMTIgMTIgMCAwMTExLjUxIDkuNTNsOC41NiA2MC43OEExNjkuMyAxNjkuMyAwIDAxMzYxIDExOWw1Ny4yLTIzYTExLjY0IDExLjY0IDAgMDExNC4xNCA0LjkybDQ2LjA2IDc5LjUyYTExLjU5IDExLjU5IDAgMDEtMi42MyAxNC43OGwtNDguNjkgMzguMTJhMTc0LjU4IDE3NC41OCAwIDAxMS42NCAyMi42NnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="system" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzdW4iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+TGlnaHQgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0yMzQgMjZoNDR2OTJoLTQ0ek0yMzQgMzk0aDQ0djkyaC00NHpNMzM4LjAyNSAxNDIuODU3bDY1LjA1NC02NS4wNTQgMzEuMTEzIDMxLjExMy02NS4wNTQgNjUuMDU0ek03Ny44MTUgNDAzLjA3NGw2NS4wNTQtNjUuMDU0IDMxLjExMyAzMS4xMTMtNjUuMDU0IDY1LjA1NHpNMzk0IDIzNGg5MnY0NGgtOTJ6TTI2IDIzNGg5MnY0NEgyNnpNMzM4LjAyOSAzNjkuMTRsMzEuMTEyLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMiAzMS4xMTJ6TTc3LjgwMiAxMDguOTJsMzEuMTEzLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMyAzMS4xMTJ6TTI1NiAzNThhMTAyIDEwMiAwIDExMTAyLTEwMiAxMDIuMTIgMTAyLjEyIDAgMDEtMTAyIDEwMnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="sun" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJtb29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgPHRpdGxlPkRhcmsgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0xNTIuNjIgMTI2Ljc3YzAtMzMgNC44NS02Ni4zNSAxNy4yMy05NC43N0M4Ny41NCA2Ny44MyAzMiAxNTEuODkgMzIgMjQ3LjM4IDMyIDM3NS44NSAxMzYuMTUgNDgwIDI2NC42MiA0ODBjOTUuNDkgMCAxNzkuNTUtNTUuNTQgMjE1LjM4LTEzNy44NS0yOC40MiAxMi4zOC02MS44IDE3LjIzLTk0Ljc3IDE3LjIzLTEyOC40NyAwLTIzMi42MS0xMDQuMTQtMjMyLjYxLTIzMi42MXoiIC8+CiAgICAgIDwvc3ZnPg=="
class="moon" />

</div>

<div>

<a
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/transform-data/idiomatic-materialize-sql/appendix/idiomatic-sql-chart.md"
class="btn-ghost"><img
src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTgiIGhlaWdodD0iMTgiIHZpZXdib3g9IjAgMCAyMyAyMyIgZmlsbD0iY3VycmVudENvbG9yIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPgogICAgICAgIDxwYXRoIGQ9Ik0yMC44OTQ1IDExLjQ5NjhDMjAuODk0NSAxMC4yMzk0IDIwLjYxNTEgOS4wNTE5IDIwLjEyNjEgNy44NjQzN0MxOS42MzcxIDYuNzQ2NjkgMTguOTM4NSA1LjY5ODg4IDE4LjE3MDEgNC45MzA0N0MxNy40MDE3IDQuMTYyMDcgMTYuMzUzOSAzLjQ2MzUgMTUuMjM2MiAyLjk3NDUyQzE0LjExODUgMi40ODU1MyAxMi44NjExIDIuMjA2MTMgMTEuNjAzOCAyLjIwNjEzQzEwLjM0NjQgMi4yMDYxMyA5LjE1ODg0IDIuNDg1NTMgNy45NzEzIDIuOTc0NTJDNi44NTM2MiAzLjQ2MzUgNS44MDU3OSA0LjE2MjA3IDUuMDM3MzggNC45MzA0N0M0LjI2ODk4IDUuNjk4ODggMy41NzA0NCA2Ljc0NjY5IDMuMDgxNDUgNy44NjQzN0MyLjU5MjQ3IDguOTgyMDUgMi4zMTMwNCAxMC4yMzk0IDIuMzEzMDQgMTEuNDk2OEMyLjMxMzA0IDEzLjUyMjYgMi45NDE3NCAxNS4zMzg5IDQuMTI5MjggMTcuMDE1NEM1LjMxNjgxIDE4LjY5MTkgNi45MjM0NyAxOS44MDk2IDguODA5NTYgMjAuMzY4NFYxNy45MjM1QzguMjUwNzIgMTcuOTkzNCA3Ljk3MTI5IDE3Ljk5MzMgNy44MzE1OCAxNy45OTMzQzYuNzgzNzYgMTcuOTkzMyA2LjAxNTM1IDE3LjUwNDQgNS41OTYyMiAxNi41MjY0QzUuNDU2NTEgMTYuMTc3MSA1LjI0Njk1IDE1LjgyNzggNS4wMzczOCAxNS42MTgzQzQuOTY3NTMgMTUuNTQ4NCA0Ljg5NzY4IDE1LjQ3ODYgNC43NTc5NyAxNS4zMzg5QzQuNjE4MjYgMTUuMTk5MiA0LjQ3ODU0IDE1LjEyOTMgNC4zMzg4MyAxNC45ODk2QzQuMTk5MTIgMTQuODQ5OSA0LjEyOTI4IDE0Ljc4IDQuMTI5MjggMTQuNzhDNC4xMjkyOCAxNC42NDAzIDQuMjY4OTggMTQuNjQwMyA0LjU0ODQgMTQuNjQwM0M0LjgyNzgyIDE0LjY0MDMgNS4xMDcyNCAxNC43MTAyIDUuMzE2ODEgMTQuODQ5OUM1LjUyNjM3IDE0Ljk4OTYgNS43MzU5NCAxNS4xMjkzIDUuODc1NjUgMTUuMzM4OUM2LjAxNTM2IDE1LjU0ODQgNi4xNTUwNyAxNS43NTggNi4zNjQ2MyAxNS45Njc2QzYuNTA0MzQgMTYuMTc3MSA2LjcxMzkxIDE2LjMxNjggNi45MjM0OCAxNi40NTY1QzcuMTMzMDQgMTYuNTk2MyA3LjQxMjQ2IDE2LjY2NjEgNy43NjE3MyAxNi42NjYxQzguMTgwODYgMTYuNjY2MSA4LjUzMDE0IDE2LjU5NjMgOC45NDkyNyAxNi40NTY1QzkuMDg4OTggMTUuODk3NyA5LjQzODI1IDE1LjQ3ODYgOS44NTczOCAxNS4xMjkzQzguMjUwNzIgMTQuOTg5NiA3LjA2MzE4IDE0LjU3MDUgNi4yOTQ3NyAxMy45NDE4QzUuNTI2MzcgMTMuMzEzMSA1LjEwNzI0IDEyLjE5NTQgNS4xMDcyNCAxMC42NTg2QzUuMTA3MjQgOS41NDA4OSA1LjQ1NjUyIDguNTYyOTQgNi4xNTUwNyA3Ljc5NDUzQzYuMDE1MzYgNy4zNzU0IDUuOTQ1NSA2Ljk1NjI2IDUuOTQ1NSA2LjUzNzEzQzUuOTQ1NSA1Ljk3ODI5IDYuMDg1MjEgNS40MTk0NiA2LjM2NDYzIDQuOTMwNDdDNi45MjM0NyA0LjkzMDQ3IDcuNDEyNDUgNS4wMDAzMiA3LjgzMTU4IDUuMjA5ODlDOC4yNTA3MSA1LjQxOTQ1IDguNzM5NyA1LjY5ODg2IDkuMjk4NTQgNi4xMTc5OUMxMC4wNjY5IDUuOTc4MjggMTAuODM1NCA1LjgzODU4IDExLjc0MzUgNS44Mzg1OEMxMi41MTE5IDUuODM4NTggMTMuMjgwMyA1LjkwODQ1IDEzLjk3ODggNi4wNDgxNkMxNC41Mzc3IDUuNjI5MDMgMTUuMDI2NyA1LjM0OTYgMTUuNDQ1OCA1LjIwOTg5QzE1Ljg2NDkgNS4wMDAzMiAxNi4zNTM5IDQuOTMwNDcgMTYuOTEyNyA0LjkzMDQ3QzE3LjE5MjIgNS40MTk0NiAxNy4zMzE5IDUuOTc4MjkgMTcuMzMxOSA2LjUzNzEzQzE3LjMzMTkgNi45NTYyNiAxNy4yNjIgNy4zNzU0IDE3LjEyMjMgNy43MjQ2N0MxNy44MjA5IDguNDkzMDggMTguMTcwMSA5LjQ3MTA1IDE4LjE3MDEgMTAuNTg4N0MxOC4xNzAxIDEyLjEyNTUgMTcuNzUxIDEzLjE3MzQgMTYuOTgyNiAxMy44NzE5QzE2LjIxNDIgMTQuNTcwNSAxNS4wMjY2IDE0LjkxOTcgMTMuNDIgMTUuMDU5NEMxNC4xMTg1IDE1LjU0ODQgMTQuMzk4IDE2LjE3NzEgMTQuMzk4IDE2Ljk0NTVWMjAuMjI4N0MxNi4zNTM5IDE5LjYgMTcuODkwNyAxOC40ODIzIDE5LjA3ODIgMTYuODc1N0MyMC4yNjU4IDE1LjMzODkgMjAuODk0NSAxMy41MjI2IDIwLjg5NDUgMTEuNDk2OFpNMjIuNzEwNyAxMS40OTY4QzIyLjcxMDcgMTMuNTIyNiAyMi4yMjE3IDE1LjQwODcgMjEuMjQzOCAxNy4wODUyQzIwLjI2NTggMTguODMxNiAxOC44Njg3IDIwLjE1ODggMTcuMTkyMiAyMS4xMzY4QzE1LjQ0NTggMjIuMTE0OCAxMy42Mjk2IDIyLjYwMzggMTEuNjAzOCAyMi42MDM4QzkuNTc3OTYgMjIuNjAzOCA3LjY5MTg4IDIyLjExNDggNi4wMTUzNiAyMS4xMzY4QzQuMjY4OTggMjAuMTU4OCAyLjk0MTc0IDE4Ljc2MTggMS45NjM3NyAxNy4wODUyQzAuOTg1Nzk2IDE1LjMzODkgMC40OTY4MDcgMTMuNTIyNiAwLjQ5NjgwNyAxMS40OTY4QzAuNDk2ODA3IDkuNDcxMDQgMC45ODU3OTYgNy41ODQ5NiAxLjk2Mzc3IDUuOTA4NDRDMi45NDE3NCA0LjE2MjA2IDQuMzM4ODQgMi44MzQ4MyA2LjAxNTM2IDEuODU2ODZDNy43NjE3MyAwLjg3ODg4NiA5LjU3Nzk2IDAuMzg5ODk3IDExLjYwMzggMC4zODk4OTdDMTMuNjI5NiAwLjM4OTg5NyAxNS41MTU2IDAuODc4ODg2IDE3LjE5MjIgMS44NTY4NkMxOC45Mzg1IDIuODM0ODMgMjAuMjY1OCA0LjIzMTkyIDIxLjI0MzggNS45MDg0NEMyMi4yMjE3IDcuNTg0OTYgMjIuNzEwNyA5LjQ3MTA0IDIyLjcxMDcgMTEuNDk2OFoiIC8+CiAgICAgIDwvc3ZnPg==" />
Edit this page</a>

</div>

<div class="footer-links">

[Home](https://materialize.com) [Status](https://status.materialize.com)
[GitHub](https://github.com/MaterializeInc/materialize)
[Blog](https://materialize.com/blog)
[Contact](https://materialize.com/contact)

Cookie Preferences

[Privacy Policy](https://materialize.com/privacy-policy/)

</div>

© 2025 Materialize Inc.

</div>
