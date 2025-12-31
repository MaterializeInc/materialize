# Appendix

Idiomatic Materialize appendix, containing example data and cheatsheets






---

## Example data: items and orders


The following sample data is used in the various Idiomaitc Materialize SQL
pages:

```mzsql
CREATE TABLE orders (
    order_id int NOT NULL,
    order_date timestamp NOT NULL,
    item text NOT NULL,
    quantity int NOT NULL,
    status text NOT NULL
);

INSERT INTO orders VALUES
(1,current_timestamp - (1 * interval '3 day') - (35 * interval '1 minute'),'brownies',12, 'Complete'),
(1,current_timestamp - (1 * interval '3 day') - (35 * interval '1 minute'),'cupcake',12, 'Complete'),
(1,current_timestamp - (1 * interval '3 day') - (35 * interval '1 minute'),'chocolate cake',1, 'Complete'),
(2,current_timestamp - (1 * interval '3 day') - (15 * interval '1 minute'),'cheesecake',1, 'Complete'),
(3,current_timestamp - (1 * interval '3 day'),'chiffon cake',1, 'Complete'),
(3,current_timestamp - (1 * interval '3 day'),'egg tart',6, 'Complete'),
(3,current_timestamp - (1 * interval '3 day'),'fruit tart',6, 'Complete'),
(4,current_timestamp - (1 * interval '2 day')- (30 * interval '1 minute'),'cupcake',6, 'Shipped'),
(4,current_timestamp - (1 * interval '2 day')- (30 * interval '1 minute'),'cupcake',6, 'Shipped'),
(5,current_timestamp - (1 * interval '2 day'),'chocolate cake',1, 'Processing'),
(6,current_timestamp,'brownie',10, 'Pending'),
(6,current_timestamp,'chocolate cake',1, 'Pending'),
(7,current_timestamp,'chocolate chip cookie',20, 'Processing'),
(8,current_timestamp,'coffee cake',1, 'Complete'),
(8,current_timestamp,'fruit tart',4, 'Complete'),
(9,current_timestamp + (15 * interval '1 minute'),'chocolate chip cookie',20, 'Pending'),
(9,current_timestamp + (15 * interval '1 minute'),'brownie',20, 'Processing'),
(10,current_timestamp + (30 * interval '1 minute'),'sugar cookie',10, 'Pending'),
(10,current_timestamp + (30 * interval '1 minute'),'donut',36, 'Pending'),
(11,current_timestamp + (30 * interval '1 minute'),'chiffon cake',2, 'Pending'),
(11,current_timestamp + (30 * interval '1 minute'),'egg tart',6, 'Pending'),
(12,current_timestamp + (1 * interval '1 day') + (35 * interval '1 minute'),'cheesecake',1, 'Pending'),
(13,current_timestamp + (1 * interval '1 day')+ (35 * interval '1 minute'),'chocolate chip cookie',20, 'Pending'),
(14,current_timestamp + (1 * interval '1 day')+ (35 * interval '1 minute'),'brownie',20, 'Pending'),
(14,current_timestamp + (1 * interval '1 day')+ (35 * interval '1 minute'),'cheesecake',1, 'Pending'),
(14,current_timestamp + (1 * interval '1 day')+ (35 * interval '1 minute'),'cupcake',6, 'Pending'),
(15,current_timestamp + (1 * interval '1 day')+ (35 * interval '1 minute'),'chocolate cake',1, 'Pending'),
(16,current_timestamp + (1 * interval '2 day'),'chocolate cake',1, 'Pending'),
(17,current_timestamp + (1 * interval '2 day')+ (10 * interval '1 minute'),'coffee cake',1, 'Pending'),
(17,current_timestamp + (1 * interval '2 day')+ (10 * interval '1 minute'),'egg tart',12, 'Pending'),
(18,current_timestamp + (1 * interval '2 day')+ (15 * interval '1 minute'),'chocolate chip cookie',12, 'Pending'),
(18,current_timestamp + (1 * interval '2 day')+ (15 * interval '1 minute'),'brownie',12, 'Pending'),
(18,current_timestamp + (1 * interval '2 day')+ (15 * interval '1 minute'),'sugar cookie',12, 'Pending'),
(18,current_timestamp + (1 * interval '2 day')+ (15 * interval '1 minute'),'donut',12, 'Pending'),
(19,current_timestamp + (1 * interval '2 day')+ (30 * interval '1 minute'),'cupcake',6, 'Pending'),
(20,current_timestamp + (1 * interval '3 day'),'chiffon cake',1, 'Pending'),
(20,current_timestamp + (1 * interval '3 day'),'egg tart',6, 'Pending'),
(20,current_timestamp + (1 * interval '3 day'),'fruit tart',6, 'Pending'),
(21,current_timestamp + (1 * interval '3 day') + (15 * interval '1 minute'),'cheesecake',1, 'Pending'),
(22,current_timestamp + (1 * interval '3 day') + (35 * interval '1 minute'),'brownies',12, 'Pending'),
(22,current_timestamp + (1 * interval '3 day') + (35 * interval '1 minute'),'cupcake',12, 'Pending'),
(22,current_timestamp + (1 * interval '3 day') + (35 * interval '1 minute'),'chocolate cake',1, 'Pending')
;

CREATE TABLE items(
    item text NOT NULL,
    price numeric(8,4) NOT NULL,
    currency text NOT NULL DEFAULT 'USD'
);

INSERT INTO items VALUES
('brownie',2.25,'USD'),
('cheesecake',40,'USD'),
('chiffon cake',30,'USD'),
('chocolate cake',30,'USD'),
('chocolate chip cookie',2.5,'USD'),
('coffee cake',25,'USD'),
('cupcake',3,'USD'),
('donut',1.25,'USD'),
('egg tart',2.5,'USD'),
('fruit tart',4.5,'USD'),
('sugar cookie',2.5,'USD');

CREATE VIEW orders_view AS
SELECT o.*,i.price,o.quantity * i.price as subtotal
FROM orders as o
JOIN items as i
ON o.item = i.item;

CREATE VIEW orders_daily_totals AS
SELECT date_trunc('day',order_date) AS order_date,
       sum(subtotal) AS daily_total
FROM orders_view
GROUP BY date_trunc('day',order_date);


CREATE TABLE sales_items (
  week_of date NOT NULL,
  items text[]
);

INSERT INTO sales_items VALUES
(date_trunc('week', current_timestamp),ARRAY['brownie','chocolate chip cookie','chocolate cake']),
(date_trunc('week', current_timestamp + (1* interval '7 day')),ARRAY['chocolate chip cookie','donut','cupcake']);
```




---

## Idiomatic Materialize SQL chart


Materialize follows the SQL standard (SQL-92) implementation and strives for
compatibility with the PostgreSQL dialect. However, for some use cases,
Materialize provides its own idiomatic query patterns that can provide better
performance.

## General

### Query Patterns

{{% idiomatic-sql/general-syntax-table %}}

### Examples

{{% idiomatic-sql/general-example-table %}}

## Window Functions
{{< callout >}}

### Materialize and window functions

{{< idiomatic-sql/materialize-window-functions >}}

{{</ callout >}}

### Query Patterns

{{% idiomatic-sql/window-functions-syntax-table %}}

### Examples

{{% idiomatic-sql/window-functions-example-table %}}

## See also

- [SQL Functions](/sql/functions/)
- [SQL Types](/sql/types/)
- [SELECT](/sql/select/)
- [DISTINCT](/sql/select/#select-distinct)
- [DISTINCT ON](/sql/select/#select-distinct-on)




---

## Window function to idiomatic Materialize


Materialize offers a wide range of [window
functions](/sql/functions/#window-functions). However, for some
[`LAG()`](/sql/functions/#lag), [`LEAD()`](/sql/functions/#lead),
[`ROW_NUMBER()`](/sql/functions/#row_number),
[`FIRST_VALUE()`](/sql/functions/#first_value), and
[`LAST_VALUE()`](/sql/functions/#last_value) use cases, Materialize provides its
own idiomatic query patterns that do <red>not</red> use the window functions and
can provide better performance.

{{< callout >}}

### Materialize and window functions

{{< idiomatic-sql/materialize-window-functions >}}

{{</ callout >}}

<table>
<thead>
<tr>
<th>
Windows function anti-pattern
</th>
<th>
Materialize idiomatic SQL
</th>
</tr>
</thead>
<tbody>

<tr>
<td colspan=2>

**First value within groups.** For more information and examples, see [Idiomatic Materialize SQL: First
value](/transform-data/idiomatic-materialize-sql/first-value/).

</td>
</tr>
<tr>
<td>
<div style="background-color: var(--code-block)">

```nofmt
-- Anti-pattern. Avoid. --
SELECT fieldA, fieldB,
 FIRST_VALUE(fieldZ)
   OVER (PARTITION BY fieldA ORDER BY ...)
FROM tableA
ORDER BY fieldA, ...;
```

</div>
</td>
<td class="copyableCode">

```mzsql
SELECT tableA.fieldA, tableA.fieldB, minmax.Z
FROM tableA,
     (SELECT fieldA,
        MIN(fieldZ)
      FROM tableA
      GROUP BY fieldA) minmax
WHERE tableA.fieldA = minmax.fieldA
ORDER BY fieldA ... ;
```

</td>
</tr>

<tr>
<td colspan=2>

**Lag over whose order by field advances in a regular pattern.**
For more information and examples, see [Idiomatic Materialize SQL: Lag
over](/transform-data/idiomatic-materialize-sql/lag/).

</td>
</tr>
<tr>
<td>
<div style="background-color: var(--code-block)">

```nofmt
-- Anti-pattern. Avoid --
SELECT fieldA, ...
  LAG(fieldZ)
    OVER (ORDER BY fieldA) as previous_row_value
FROM tableA;
```

</div>
</td>
<td class="copyableCode">

```mzsql
-- Excludes the first row in the results --
SELECT t1.fieldA, t2.fieldB as previous_row_value
FROM tableA t1, tableA t2
WHERE t1.fieldA = t2.fieldA + ...
ORDER BY fieldA;
```

</td>
</tr>

<tr>
<td colspan=2>

**Last value within groups.** For more information and examples, see [Idiomatic Materialize SQL: Last value in
group](/transform-data/idiomatic-materialize-sql/last-value/).


</td>
</tr>
<tr>
<td>
<div style="background-color: var(--code-block)">

```nofmt
-- Anti-pattern. Unsupported range. --
SELECT fieldA, fieldB,
  LAST_VALUE(fieldZ)
    OVER (PARTITION BY fieldA ORDER BY fieldZ
          RANGE BETWEEN
            UNBOUNDED PRECEDING AND
            UNBOUNDED FOLLOWING)
FROM tableA
ORDER BY fieldA, ...;
```

</div>
</td>
<td class="copyableCode">

```mzsql
SELECT tableA.fieldA, tableA.fieldB, minmax.Z
 FROM tableA,
      (SELECT fieldA,
         MAX(fieldZ)
       FROM tableA
       GROUP BY fieldA) minmax
WHERE tableA.fieldA = minmax.fieldA
ORDER BY fieldA ... ;
```

</td>
</tr>

<tr>
<td colspan=2>

**Lead over whose order by field advances in a regular pattern.** For more
information and examples, see [Idiomatic Materialize SQL: Lead
over](/transform-data/idiomatic-materialize-sql/lead/).

</td>
</tr>
<tr>
<td>
<div style="background-color: var(--code-block)">

```nofmt
-- Anti-pattern. Avoid. --
SELECT fieldA, ...
    LEAD(fieldZ)
      OVER (ORDER BY fieldA) as next_row_value
FROM tableA;
```

</div>
</td>
<td class="copyableCode">

```mzsql
-- Excludes the last row in the results --
SELECT t1.fieldA, t2.fieldB as next_row_value
FROM tableA t1, tableA t2
WHERE t1.fieldA = t2.fieldA - ...
ORDER BY fieldA;
```

</td>
</tr>

<tr>
<td colspan=2>

**Top-K queries.** For more information and examples, see [Idiomatic Materialize SQL: Top-K in
group](/transform-data/idiomatic-materialize-sql/top-k/).

</td>
</tr>
<tr>
<td>
<div style="background-color: var(--code-block)">

```nofmt
-- Anti-pattern. Avoid. --
SELECT fieldA, fieldB, ...
FROM (
  SELECT fieldA, fieldB, ... , fieldZ,
     ROW_NUMBER() OVER (PARTITION BY fieldA
     ORDER BY fieldZ ... ) as rn
  FROM tableA)
WHERE rn <= K
ORDER BY fieldA, fieldZ ...;
```

</div>
</td>
<td class="copyableCode">

```mzsql
SELECT fieldA, fieldB, ...
FROM (SELECT DISTINCT fieldA FROM tableA) grp,
  LATERAL (SELECT fieldB, ... , fieldZ FROM tableA
           WHERE fieldA = grp.fieldA
           ORDER BY fieldZ ... LIMIT K)
ORDER BY fieldA, fieldZ ... ;
```

</td>
</tr>
</tbody>
</table>



