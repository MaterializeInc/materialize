---
title: "Top-K in group"
description: "Use idiomatic Materialize SQL to find the top-k/top-n elements in each group."
menu:
  main:
    parent: idiomatic-materialize-sql
    identifier: idiomatic-materialize-top-k
    weight: 50
aliases:
  - /sql/patterns/patterns/top-k/
  - /transform-data/patterns/top-k/
  - /guides/top-k/
  - /docs/sql/patterns/top-k/
---

## Overview

The "Top-K in group" query pattern groups by some key and return the first K
elements within each group according to some ordering.

{{< callout >}}

### Materialize and window functions

{{< idiomatic-sql/materialize-window-functions >}}

{{</ callout >}}

## Idiomatic Materialize SQL

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
```

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
```

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
```

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
```

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
```

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
```

For more information on setting `DISTINCT ON INPUT GROUP SIZE`, see
[Optimization](/transform-data/optimization/#query-hints).

## Examples

{{< note >}}

The example data can be found in the
[Appendix](/transform-data/idiomatic-materialize-sql/appendix/example-orders).

{{</ note >}}

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
```

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
```

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
```

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
