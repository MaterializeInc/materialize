# Idiomatic Materialize SQL chart
Cheatsheet of idiomatic Materialize SQL.
Materialize follows the SQL standard (SQL-92) implementation and strives for
compatibility with the PostgreSQL dialect. However, for some use cases,
Materialize provides its own idiomatic query patterns that can provide better
performance.

## General

### Query Patterns




<table>
<thead>
<tr>
<th></th>
<th>Idiomatic Materialize SQL Pattern</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>ANY()</code> Equi-join condition</td>
<td class="copyableCode">

***If no duplicates in the unnested field***

```mzsql
WITH my_expanded_values AS
(SELECT UNNEST(array|list|map) AS fieldZ FROM tableB)
SELECT a.fieldA, ...
FROM tableA a
JOIN my_expanded_values t ON a.fieldZ = t.fieldZ
;
```

***If duplicates exist in the unnested field***
```mzsql
WITH my_expanded_values AS
(SELECT DISTINCT UNNEST(array|list|map) AS fieldZ FROM tableB)
SELECT a.fieldA, ...
FROM tableA a
JOIN my_expanded_values t ON a.fieldZ = t.fieldZ
;
```

</td>
</tr>
<tr>
<td><code>mz_now()</code> cannot be used with date/time operators</td>
<td>
Rewrite the query expression; specifically, move the operation to the other side of the comparison.
</td>
</tr>
<tr>
<td><code>mz_now()</code> cannot be used with <code>OR</code>s in materialized/indexed view definitions and <code>SUBSCRIBE</code> statements</td>
<td>
Rewrite as <code>UNION ALL</code> or <code>UNION</code>, deduplicating as
necessary:

<ul>
<li>In some cases, you may need to modify the conditions to deduplicate results
when using <code>UNION ALL</code>. For example, you might add the negation of
one input's condition to the other as a conjunction.</li>

<li>In some cases, using <code>UNION</code> instead of <code>UNION ALL</code>
may suffice if the inputs do not contain other duplicates that need to be
retained.</li>

</ul>

</td>
</tr>
</tbody>
</table>


### Examples




<table>
<thead>
<tr>
<th></th>
<th>Idiomatic Materialize SQL</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>ANY()</code> Equi-join condition</td>
<td class="copyableCode">

***If no duplicates in the unnested field***

```mzsql
-- sales_items.items contains no duplicates. --

WITH individual_sales_items AS
(SELECT unnest(items) as item, week_of FROM sales_items)
SELECT s.week_of, o.order_id, o.item, o.quantity
FROM orders o
JOIN individual_sales_items s ON o.item = s.item
WHERE date_trunc('week', o.order_date) = s.week_of;
```

***If duplicates exist in the unnested field***

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
<td><code>mz_now()</code> cannot be used with date/time operators</td>
<td class="copyableCode">

```mzsql
SELECT * from orders
WHERE mz_now() > order_date + INTERVAL '5min'
;
```

</td>
</tr>
<tr>
<td><code>mz_now()</code> cannot be used with <code>OR</code>s in materialized/indexed view definitions and <code>SUBSCRIBE</code> statements</td>
<td class="copyableCode">

**Rewrite as `UNION ALL` with possible duplicates**

```mzsql
CREATE MATERIALIZED VIEW forecast_completed_orders_duplicates_possible AS
SELECT item, quantity, status from orders
WHERE status = 'Shipped'
UNION ALL
SELECT item, quantity, status from orders
WHERE order_date + interval '30' minutes >= mz_now()
;
```

**Rewrite as UNION ALL that avoids duplicates across queries**

```mzsql
CREATE MATERIALIZED VIEW forecast_completed_orders_deduplicated_union_all AS
SELECT item, quantity, status from orders
WHERE status = 'Shipped'
UNION ALL
SELECT item, quantity, status from orders
WHERE order_date + interval '30' minutes >= mz_now()
AND status != 'Shipped' -- Deduplicate by excluding those with status 'Shipped'
;
```

**Rewrite as UNION to deduplicate any and all duplicated results**

```mzsql
CREATE MATERIALIZED VIEW forecast_completed_orders_deduplicated_results AS
SELECT item, quantity, status from orders
WHERE status = 'Shipped'
UNION
SELECT item, quantity, status from orders
WHERE order_date + interval '30' minutes >= mz_now()
;
```

</td>
</tr>

</tbody>
</table>




## Window Functions
> ### Materialize and window functions
> For [window functions](/sql/functions/#window-functions), when an input record
> in a partition (as determined by the `PARTITION BY` clause of your window
> function) is added/removed/changed, Materialize recomputes the results for the
> entire window partition. This means that when a new batch of input data arrives
> (that is, every second), **the amount of computation performed is proportional
> to the total size of the touched partitions**.
> For example, assume that in a given second, 20 input records change, and these
> records belong to **10** different partitions, where the average size of each
> partition is **100**. Then, amount of work to perform is proportional to
> computing the window function results for **10\*100=1000** rows.
> To avoid performance issues that may arise as the number of records grows,
> consider rewriting your query to use idiomatic Materialize SQL instead of window
> functions. If your query cannot be rewritten without the window functions and
> the performance of window functions is insufficient for your use case, please
> [contact our team](/support/).




### Query Patterns




<table>
<thead>
<tr>
<th></th>
<th>Idiomatic Materialize SQL Pattern</th>
</tr>
</thead>
<tbody>
<tr>
<td>Top-K over partition<br>(K >= 1)</td>
<td class="copyableCode">

```mzsql
SELECT fieldA, fieldB, ...
FROM (SELECT DISTINCT fieldA FROM tableA) grp,
      LATERAL (SELECT fieldB, ... , fieldZ FROM tableA
         WHERE fieldA = grp.fieldA
         ORDER BY fieldZ ... LIMIT K)   -- K is a number >= 1
ORDER BY fieldA, fieldZ ... ;
```

</td>
</tr>
<tr>
<td>Top-K over partition<br>(K = 1)</td>
<td class="copyableCode">

```mzsql
SELECT DISTINCT ON(fieldA) fieldA, fieldB, ...
FROM tableA
ORDER BY fieldA, fieldZ ...  -- Top-K where K is 1;
```

</td>
</tr>

<tr>
<td>First value over partition<br>order by ... </td>
<td class="copyableCode">

```mzsql
SELECT tableA.fieldA, tableA.fieldB, minmax.Z
   FROM tableA,
   (SELECT fieldA,
      MIN(fieldZ)      -- Or MAX()
   FROM tableA
   GROUP BY fieldA) minmax
WHERE tableA.fieldA = minmax.fieldA
ORDER BY fieldA ... ;
```

</td>
</tr>

<tr>
<td>Last value over partition<br>order by ...  <br>range between unbounded preceding<br>and unbounded following</td>
<td class="copyableCode">

```mzsql
SELECT tableA.fieldA, tableA.fieldB, minmax.Z
   FROM tableA,
   (SELECT fieldA,
      MAX(fieldZ)      -- Or MIN()
   FROM tableA
   GROUP BY fieldA) minmax
WHERE tableA.fieldA = minmax.fieldA
ORDER BY fieldA ... ;
```

</td>
</tr>

<tr>
<td>

Lag over (order by) whose ordering can be represented by some equality
condition.

</td>
<td class="copyableCode">

***To exclude the first row since it has no previous row***

```mzsql
SELECT t1.fieldA, t2.fieldB
FROM tableA t1, tableA t2
WHERE t1.fieldA = t2.fieldA +  ...
ORDER BY fieldA;
```

***To include the first row***

```mzsql
SELECT t1.fieldA, t2.fieldB
FROM tableA t1
LEFT JOIN tableA t2
ON t1.fieldA = t2.fieldA +  ...
ORDER BY fieldA;
```

</td>
</tr>

<tr>
<td>

Lead over (order by) whose ordering can be represented by some equality
condition.

</td>
<td class="copyableCode">

***To exclude the last row since it has no next row***

```mzsql
SELECT t1.fieldA, t2.fieldB
FROM tableA t1, tableA t2
WHERE t1.fieldA = t2.fieldA - ...
ORDER BY fieldA;
```

***To include the last row***

```mzsql
SELECT t1.fieldA, t2.fieldB
FROM tableA t1
LEFT JOIN tableA t2
ON t1.fieldA = t2.fieldA -  ...
ORDER BY fieldA;
```

</td>
</tr>

</tbody>
</table>


### Examples




<table>
<thead>
<tr>
<th></th>
<th>Idiomatic Materialize SQL</th>
</tr>
</thead>
<tbody>
<tr>
<td>Top-K over partition<br>(K >= 1)</td>
<td class="copyableCode">

```mzsql
SELECT order_id, item, subtotal
FROM (SELECT DISTINCT order_id FROM orders_view) grp,
        LATERAL (SELECT item, subtotal FROM orders_view
        WHERE order_id = grp.order_id
        ORDER BY subtotal DESC LIMIT 3) -- For Top 3
ORDER BY order_id, subtotal DESC;
```

</td>
</tr>
<tr>
<td>Top-K over partition<br>(K = 1)</td>
<td class="copyableCode">

```mzsql
SELECT DISTINCT ON(order_id) order_id, item, subtotal
FROM orders_view
ORDER BY order_id, subtotal DESC;  -- For Top 1
```

</td>
</tr>

<tr>
<td>First value over partition<br>order by ...</td>
<td class="copyableCode">

```mzsql
SELECT o.order_id, minmax.lowest_price, minmax.highest_price,
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
ORDER BY o.order_id, o.item;
```

</td>
</tr>

<tr>
<td>Last value over partition<br>order by...<br>range between unbounded preceding<br>and unbounded following</td>
<td class="copyableCode">

```mzsql
SELECT o.order_id, minmax.lowest_price, minmax.highest_price,
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
ORDER BY o.order_id, o.item;
```

</td>
</tr>

<tr>
<td>

Lag over (order by) whose ordering can be represented by some equality
condition.

</td>
<td class="copyableCode">

***If suppressing the first row since it has no previous row***

```mzsql
SELECT o1.order_date, o1.daily_total,
    o2.daily_total as previous_daily_total
FROM orders_daily_totals o1, orders_daily_totals o2
WHERE o1.order_date = o2.order_date + INTERVAL '1' DAY
ORDER BY order_date;
```

***To include the first row***

```mzsql
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
<td>

Lead over (order by) whose ordering can be represented by some equality
condition.

</td>
<td class="copyableCode">

***To suppress the last row since it has no next row***

  ```mzsql
  SELECT o1.order_date, o1.daily_total,
      o2.daily_total as previous_daily_total
  FROM orders_daily_totals o1, orders_daily_totals o2
  WHERE o1.order_date = o2.order_date - INTERVAL '1' DAY
  ORDER BY order_date;
  ```

  ***To include the last row***

  ```mzsql
  SELECT o1.order_date, o1.daily_total,
      o2.daily_total as previous_daily_total
  FROM orders_daily_totals o1
  LEFT JOIN orders_daily_totals o2
  ON o1.order_date = o2.order_date - INTERVAL '1' DAY
  ORDER BY order_date;
  ```

</td>
</tr>

</tbody>
</table>




## See also

- [SQL Functions](/sql/functions/)
- [SQL Types](/sql/types/)
- [SELECT](/sql/select/)
- [DISTINCT](/sql/select/#select-distinct)
- [DISTINCT ON](/sql/select/#select-distinct-on)
