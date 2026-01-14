<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [SQL commands](/docs/sql/)
 /  [SELECT](/docs/sql/select/)

</div>

# JOIN

`JOIN` lets you combine two or more table expressions into a single
table expression.

## Conceptual framework

Much like an RDBMS, Materialize can join together any two table
expressions (in our case, either [sources](/docs/sql/create-source) or
[views](/docs/sql/create-view)) into a single table expression.

Materialize has much broader support for `JOIN` than most streaming
platforms, i.e. we support all types of SQL joins in all of the
conditions you would expect.

## Syntax

<div class="highlight">

``` chroma
<select_pred>
[NATURAL] <join_type> JOIN
  [LATERAL] ( <select_stmt> | <table_func_call> | <table_ref> )
  [USING ( <col_ref> [, ...] ) [AS <join_using_alias>] | ON <expression>]
<select_post>
```

</div>

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th>Syntax element</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>&lt;select_pred&gt;</code></td>
<td>The predicating <a href="/docs/sql/select"><code>SELECT</code></a>
clauses you want to use, e.g.
<code>SELECT col_ref FROM table_ref...</code>. The
<code>&lt;table_ref&gt;</code> from the <code>&lt;select_pred&gt;</code>
is the left-hand table.</td>
</tr>
<tr>
<td><strong>NATURAL</strong></td>
<td>Optional. Join table expressions on all columns with the same names
in both tables. This is similar to the <code>USING</code> clause naming
all identically named columns in both tables.</td>
</tr>
<tr>
<td><code>&lt;join_type&gt;</code></td>
<td><p>The type of <code>JOIN</code> you want to use. Valid join
types:</p>
<table>
<thead>
<tr>
<th>Join Type</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>INNER</code></td>
<td>(Default) Return all tuples from both tables where the join
condition is valid.</td>
</tr>
<tr>
<td><code>LEFT</code> [<strong>OUTER</strong>]</td>
<td>Return all tuples from the left-hand-side table, and all tuples from
the right-hand-side table that match the join condition. Tuples from the
left-hand table that are not joined contain <code>NULL</code> wherever
the right-hand table is referenced.</td>
</tr>
<tr>
<td><code>RIGHT</code> [<strong>OUTER</strong>]</td>
<td>Return all tuples from the right-hand-side table, and all tuples
from the left-hand-side table that match the join condition. Tuples from
the right-hand table that are not joined contain <code>NULL</code>
wherever the left-hand table is referenced.</td>
</tr>
<tr>
<td><code>FULL</code> [<strong>OUTER</strong>]</td>
<td>Return all tuples from both tables, joining them together where the
join conditions are met. Tuples that are not joined contain
<code>NULL</code> wherever the other table is referenced.</td>
</tr>
<tr>
<td><code>CROSS</code></td>
<td>Return the Cartesian product of the two tables, i.e. all
combinations of tuples from the left-hand table combined with tuples
from the right-hand table.</td>
</tr>
</tbody>
</table></td>
</tr>
<tr>
<td><strong>LATERAL</strong></td>
<td>Optional. Let the following subquery or table function call refer to
columns from join’s left-hand side. See <a
href="#lateral-subqueries"><code>LATERAL</code> subqueries</a> for
details.</td>
</tr>
<tr>
<td>( <code>&lt;select_stmt&gt;</code> |
<code>&lt;table_func_call&gt;</code> | <code>&lt;table_ref&gt;</code>
)</td>
<td>The table expression you want to join, i.e. the right-hand table.
Can be a <a href="/docs/sql/select"><code>SELECT</code> statement</a>, a
<a href="/docs/sql/functions/#table-functions">table function call</a>,
or a table reference.</td>
</tr>
<tr>
<td><strong>USING</strong> ( <code>&lt;col_ref&gt;</code> [, …] )
[<strong>AS</strong> <code>&lt;join_using_alias&gt;</code>]</td>
<td>Optional. If the join condition does not require table-level
qualification (i.e. joining tables on columns with the same name), the
columns to join the tables on. For example,
<code>USING (customer_id)</code>. The optional <code>AS</code> clause
provides a table alias for the join columns. The columns will remain
referenceable by their original names. For example, given
<code>lhs JOIN rhs USING (c) AS joint</code>, the column <code>c</code>
will be referenceable as <code>lhs.c</code>, <code>rhs.c</code>, and
<code>joint.c</code>.</td>
</tr>
<tr>
<td><strong>ON</strong> <code>&lt;expression&gt;</code></td>
<td>Optional. The condition on which to join the tables. For example
<code>ON purchase.customer_id = customer.id</code>.</td>
</tr>
<tr>
<td><code>&lt;select_post&gt;</code></td>
<td>The remaining <a href="/docs/sql/select"><code>SELECT</code></a>
clauses you want to use, e.g.
<code>...WHERE expr GROUP BY col_ref HAVING expr</code>.</td>
</tr>
</tbody>
</table>

**Note**: It’s possible to join together table expressions as inner
joins without using this clause whatsoever, e.g.
`SELECT cols... FROM t1, t2 WHERE t1.x = t2.x GROUP BY cols...`

## Details

Unlike most other streaming platforms, `JOIN`s in Materialize have very
few, if any, restrictions. For example, Materialize:

- Does not require time windows when joining streams.
- Does not require any kind of partitioning.

Instead, `JOIN`s work over the available history of both streams, which
ultimately provides an experience more similar to an RDBMS than other
streaming platforms.

### `LATERAL` subqueries

To permit subqueries on the right-hand side of a `JOIN` to access the
columns defined by the left-hand side, declare the subquery as
`LATERAL`. Normally, a subquery only has access to the columns within
its own context.

Table function invocations always have implicit access to the columns
defined by the left-hand side of the join, so declaring them as
`LATERAL` is a permitted no-op.

When a join contains a `LATERAL` cross-reference, the right-hand
relation is recomputed for each row in the left-hand relation, then
joined to the left-hand row according to the usual rules of the selected
join type.

<div class="warning">

**WARNING!** `LATERAL` subqueries can be very expensive to compute. For
best results, do not materialize a view containing a `LATERAL` subquery
without first inspecting the plan via the
[`EXPLAIN PLAN`](/docs/sql/explain-plan/) statement. In many common
patterns involving `LATERAL` joins, Materialize can optimize away the
join entirely.

</div>

As a simple example, the following query uses `LATERAL` to count from 1
to `x` for all the values of `x` in `xs`.

<div class="highlight">

``` chroma
SELECT * FROM
  (VALUES (1), (3)) xs (x)
  CROSS JOIN LATERAL generate_series(1, x) y;
```

</div>

```
 x | y
---+---
 1 | 1
 3 | 1
 3 | 2
 3 | 3
```

For a real-world example of a `LATERAL` subquery, see the [Top-K by
group idiom](/docs/transform-data/idiomatic-materialize-sql/top-k/).

## Examples

For these examples, we’ll use a small data set:

**Employees**

```
 id |  name
----+--------
  1 | Frank
  2 | Arjun
  3 | Nikhil
  4 | Cuong
```

**Managers**

```
 id | name  | manages
----+-------+---------
  1 | Arjun |       4
  2 | Cuong |       3
  3 | Frank |
```

In this table:

- `Arjun` and `Frank` do not have managers.
- `Frank` is a manager but has no reports.

### Inner join

Inner joins return all tuples from both tables where the join condition
is valid.

![inner join diagram](/docs/images/join-inner.png)

<div class="highlight">

``` chroma
SELECT
  employees."name" AS employee,
  managers."name" AS manager
FROM employees
INNER JOIN managers ON employees.id = managers.manages;
```

</div>

```
 employee | manager
----------+---------
 Cuong    | Arjun
 Nikhil   | Cuong
```

### Left outer join

Left outer joins (also known as left joins) return all tuples from the
left-hand-side table, and all tuples from the right-hand-side table that
match the join condition. Tuples on from the left-hand table that are
not joined with a tuple from the right-hand table contain `NULL`
wherever the right-hand table is referenced.

![left outer join diagram](/docs/images/join-left-outer.png)

<div class="highlight">

``` chroma
SELECT
  employees."name" AS employee,
  managers."name" AS manager
FROM employees
LEFT OUTER JOIN managers ON employees.id = managers.manages;
```

</div>

```
 employee | manager
----------+---------
 Cuong    | Arjun
 Nikhil   | Cuong
 Arjun    |
 Frank    |
```

### Right outer join

Right outer joins (also known as right joins) are simply the
right-hand-side equivalent of left outer joins.

Right outer joins return all tuples from the right-hand-side table, and
all tuples from the left-hand-side table that match the join condition.
Tuples on from the right-hand table that are not joined with a tuple
from the left-hand table contain `NULL` wherever the left-hand table is
referenced.

![right outer join diagram](/docs/images/join-right-outer.png)

<div class="highlight">

``` chroma
SELECT
  employees."name" AS employee,
  managers."name" AS manager
FROM employees
RIGHT OUTER JOIN managers ON employees.id = managers.manages;
```

</div>

```
 employee | manager
----------+---------
 Cuong    | Arjun
 Nikhil   | Cuong
          | Frank
```

### Full outer join

Full outer joins perform both a left outer join and a right outer join.
They return all tuples from both tables, and join them together where
the join conditions are met.

Tuples that are not joined with the other table contain `NULL` wherever
the other table is referenced.

![full outer join diagram](/docs/images/join-full-outer.png)

<div class="highlight">

``` chroma
SELECT
  employees."name" AS employee,
  managers."name" AS manager
FROM employees
FULL OUTER JOIN managers ON employees.id = managers.manages;
```

</div>

```
 employee | manager
----------+---------
 Cuong    | Arjun
 Nikhil   | Cuong
          | Frank
 Arjun    |
 Frank    |
```

### Cross join

Cross joins return the [Cartesian
product](https://en.wikipedia.org/wiki/Cartesian_product) of the two
tables, i.e. all combinations of tuples from the left-hand table
combined with tuples from the right-hand table.

![cross join diagram](/docs/images/join-cross.png)

Our example dataset doesn’t have a meaningful cross-join query, but the
above diagram shows how cross joins form the Cartesian product.

## Related pages

- [`SELECT`](/docs/sql/select)
- [`CREATE VIEW`](/docs/sql/create-view)

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/select/join.md"
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

© 2026 Materialize Inc.

</div>
