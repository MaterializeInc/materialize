---
title: "Idiomatic Materialize SQL"
description: "Learn about idiomatic Materialize SQL. Materialize offers various idiomatic query patterns, such as for top-k query pattern, first value/last value query paterrns, etc."
disable_list: true
menu:
  main:
    parent: transform-data
    weight: 10
    identifier: idiomatic-materialize-sql

aliases:
  - /transform-data/patterns/window-functions/
---

Materialize follows the SQL standard (SQL-92) implementation and strives for
compatibility with the PostgreSQL dialect. However, for some use cases,
Materialize provides its own idiomatic query patterns that can provide better
performance.

<table>
<thead>
<tr>
<th>
Query
</th>
<th>
Idiomatic Materialize
</th>
</tr>
</thead>
<tbody>

<tr>
<td>

[`ANY()` Equi-join condition](/transform-data/idiomatic-materialize-sql/any/)

</td>
<td>

[Use `UNNEST()` or `DISTINCT UNNEST()` to expand the values and
join](/transform-data/idiomatic-materialize-sql/any/).

</td>
</tr>

<tr>
<td>

[First value within groups](/transform-data/idiomatic-materialize-sql/first-value/)

</td>
<td>

[Use `MIN/MAX ... GROUP BY` subquery](/transform-data/idiomatic-materialize-sql/first-value/).

</td>
</tr>

<tr>
<td>

[Lag over a regularly increasing
field](/transform-data/idiomatic-materialize-sql/lag/)

</td>
<td>

[Use self join or a self `LEFT JOIN/LEFT OUTER JOIN` by an **equality match** on
the regularly increasing field](/transform-data/idiomatic-materialize-sql/lag/).

</td>
</tr>

<tr>
<td>

[Last value within groups](/transform-data/idiomatic-materialize-sql/last-value/)

</td>
<td>

[Use `MIN/MAX ... GROUP BY` subquery](/transform-data/idiomatic-materialize-sql/last-value/)

</td>
</tr>

<tr>
<td>

[Lead over a regularly increasing
field](/transform-data/idiomatic-materialize-sql/lead/)

</td>
<td>

[Use self join or a self `LEFT JOIN/LEFT OUTER JOIN` by an **equality match** on
the regularly increasing field](/transform-data/idiomatic-materialize-sql/lead/).

</td>
</tr>


<tr>
<td>

[Top-K](/transform-data/idiomatic-materialize-sql/top-k/)

</td>
<td>

[Use an `ORDER BY ... LIMIT` subquery with a `LATERAL JOIN` on a `DISTINCT`
subquery (or, for K=1,  a `SELECT DISTINCT ON ... ORDER BY ... LIMIT`
query)](/transform-data/idiomatic-materialize-sql/top-k/)

</td>
</tr>

</tbody>
</table>
