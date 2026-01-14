<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [SQL commands](/docs/sql/)

</div>

# SELECT

The `SELECT` statement is the root of a SQL query, and is used both to
bind SQL queries to named [views](../create-view) or [materialized
views](../create-materialized-view), and to interactively query data
maintained in Materialize. For interactive queries, you should consider
creating [indexes](../create-index) on the underlying relations based on
common query patterns.

## Syntax

<div class="highlight">

``` chroma
[WITH <cte_binding> [, ...]]
SELECT [ALL | DISTINCT [ON ( <col_ref> [, ...] )]]
  <target_elem> [, ...]
[FROM <table_expr> [, ...] [<join_expr>]]
[WHERE <expression>]
[GROUP BY <col_ref> [, ...]]
[OPTIONS ( <option> = <val> [, ...] )]
[HAVING <expression>]
[ORDER BY <col_ref> [ASC | DESC] [NULLS FIRST | NULLS LAST] [, ...]]
[LIMIT <expression>]
[OFFSET <integer>]
[{UNION | INTERSECT | EXCEPT} [ALL | DISTINCT] <another_select_stmt>]
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
<td><strong>WITH</strong> <code>&lt;cte_binding&gt;</code> [, …]</td>
<td>Optional. <a href="#common-table-expressions-ctes">Common table
expressions</a> (CTEs) for this query. See <a
href="#regular-ctes">Regular CTEs</a> for details.</td>
</tr>
<tr>
<td><strong>ALL</strong> | <strong>DISTINCT</strong>
[<strong>ON</strong> ( <code>&lt;col_ref&gt;</code> [, …] )]</td>
<td><p>Optional. Specifies which rows to return:</p>
<table>
<thead>
<tr>
<th>Option</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>ALL</code></td>
<td>Return all rows from query (default).</td>
</tr>
<tr>
<td><code>DISTINCT</code></td>
<td><span id="select-distinct"></span>Return only distinct values.</td>
</tr>
<tr>
<td><code>DISTINCT ON ( &lt;col_ref&gt; [, ...] )</code></td>
<td><span id="select-distinct-on"></span>Return only the first row with
a distinct value for <code>&lt;col_ref&gt;</code>. If an
<code>ORDER BY</code> clause is also present, then
<code>DISTINCT ON</code> will respect that ordering when choosing which
row to return for each distinct value. You should start the
<code>ORDER BY</code> clause with the same <code>&lt;col_ref&gt;</code>
as the <code>DISTINCT ON</code> clause.</td>
</tr>
</tbody>
</table></td>
</tr>
<tr>
<td><code>&lt;target_elem&gt;</code> [, …]</td>
<td>The columns or expressions to return. Can include column names,
functions, or expressions.</td>
</tr>
<tr>
<td><strong>FROM</strong> <code>&lt;table_expr&gt;</code> [, …]</td>
<td>The tables you want to read from. These can be table names, other
<code>SELECT</code> statements, <a
href="#common-table-expressions-ctes">Common Table Expressions</a>
(CTEs), or <a href="/docs/sql/functions/table-functions">table function
calls</a>.</td>
</tr>
<tr>
<td><code>&lt;join_expr&gt;</code></td>
<td>Optional. A join expression to combine table expressions. For more
details, see the <a href="/docs/sql/select/join/"><code>JOIN</code>
documentation</a>.</td>
</tr>
<tr>
<td><strong>WHERE</strong> <code>&lt;expression&gt;</code></td>
<td>Optional. Filter tuples by <code>&lt;expression&gt;</code>.</td>
</tr>
<tr>
<td><strong>GROUP BY</strong> <code>&lt;col_ref&gt;</code> [, …]</td>
<td>Optional. Group aggregations by <code>&lt;col_ref&gt;</code>. Column
references may be the name of an output column, the ordinal number of an
output column, or an arbitrary expression of only input columns.</td>
</tr>
<tr>
<td><strong>OPTIONS</strong> ( <code>&lt;option&gt;</code> =
<code>&lt;val&gt;</code> [, …] )</td>
<td><p>Optional. Specify one or more <a href="#query-hints">query
hints</a>. Valid hints:</p>
<table>
<thead>
<tr>
<th>Hint</th>
<th>Value type</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>AGGREGATE INPUT GROUP SIZE</code></td>
<td><code>uint8</code></td>
<td>How many rows will have the same group key in an aggregation.
Materialize can render <code>min</code> and <code>max</code> expressions
more efficiently with this information.</td>
</tr>
<tr>
<td><code>DISTINCT ON INPUT GROUP SIZE</code></td>
<td><code>uint8</code></td>
<td>How many rows will have the same group key in a
<code>DISTINCT ON</code> expression. Materialize can render <a
href="/docs/transform-data/idiomatic-materialize-sql/top-k/">Top K
patterns</a> based on <code>DISTINCT ON</code> more efficiently with
this information.</td>
</tr>
<tr>
<td><code>LIMIT INPUT GROUP SIZE</code></td>
<td><code>uint8</code></td>
<td>How many rows will be given as a group to a <code>LIMIT</code>
restriction. Materialize can render <a
href="/docs/transform-data/idiomatic-materialize-sql/top-k/">Top K
patterns</a> based on <code>LIMIT</code> more efficiently with this
information.</td>
</tr>
</tbody>
</table></td>
</tr>
<tr>
<td><strong>HAVING</strong> <code>&lt;expression&gt;</code></td>
<td>Optional. Filter aggregations by
<code>&lt;expression&gt;</code>.</td>
</tr>
<tr>
<td><strong>ORDER BY</strong> <code>&lt;col_ref&gt;</code>
[<strong>ASC</strong> | <strong>DESC</strong>] [<strong>NULLS
FIRST</strong> | <strong>NULLS LAST</strong>] [, …]</td>
<td>Optional. Sort results in either <code>ASC</code> (default) or
<code>DESC</code> order. Use the <code>NULLS FIRST</code> and
<code>NULLS LAST</code> options to determine whether nulls appear before
or after non-null values in the sort ordering (default:
<code>NULLS LAST</code> for <code>ASC</code>, <code>NULLS FIRST</code>
for <code>DESC</code>). Column references may be the name of an output
column, the ordinal number of an output column, or an arbitrary
expression of only input columns.</td>
</tr>
<tr>
<td><strong>LIMIT</strong> <code>&lt;expression&gt;</code></td>
<td>Optional. Limit the number of returned results to
<code>&lt;expression&gt;</code>.</td>
</tr>
<tr>
<td><strong>OFFSET</strong> <code>&lt;integer&gt;</code></td>
<td>Optional. Skip the first <code>&lt;integer&gt;</code> number of
rows.</td>
</tr>
<tr>
<td><strong>UNION</strong> [<strong>ALL</strong> |
<strong>DISTINCT</strong>] <code>&lt;another_select_stmt&gt;</code></td>
<td>Optional. Records present in <code>select_stmt</code> or
<code>another_select_stmt</code>. <code>DISTINCT</code> returns only
unique rows from these results (implied default). With <code>ALL</code>
specified, each record occurs a number of times equal to the sum of the
times it occurs in each input statement.</td>
</tr>
<tr>
<td><strong>INTERSECT</strong> [<strong>ALL</strong> |
<strong>DISTINCT</strong>] <code>&lt;another_select_stmt&gt;</code></td>
<td>Optional. Records present in both <code>select_stmt</code> and
<code>another_select_stmt</code>. <code>DISTINCT</code> returns only
unique rows from these results (implied default). With <code>ALL</code>
specified, each record occurs a number of times equal to the lesser of
the times it occurs in each input statement.</td>
</tr>
<tr>
<td><strong>EXCEPT</strong> [<strong>ALL</strong> |
<strong>DISTINCT</strong>] <code>&lt;another_select_stmt&gt;</code></td>
<td>Optional. Records present in <code>select_stmt</code> but not in
<code>another_select_stmt</code>. <code>DISTINCT</code> returns only
unique rows from these results (implied default). With <code>ALL</code>
specified, each record occurs a number of times equal to the times it
occurs in <code>select_stmt</code> less the times it occurs in
<code>another_select_stmt</code>, or not at all if the former is greater
than latter.</td>
</tr>
</tbody>
</table>

### Common table expressions (CTEs)

#### Regular CTEs

<div class="highlight">

``` chroma
WITH <cte_ident> [( <col_ident> [, ...] )] AS ( <select_stmt> )
  [, <cte_ident> [( <col_ident> [, ...] )] AS ( <select_stmt> ) [, ...]]
<select_stmt>
```

</div>

| Syntax element | Description |
|----|----|
| `<cte_ident>` | The name of the common table expression (CTE). |
| ( `<col_ident>` \[, …\] ) | Optional. Rename the CTE’s columns to the list of identifiers. The number of identifiers must match the number of columns returned by the CTE’s `select_stmt`. |
| **AS** ( `<select_stmt>` ) | The `SELECT` statement that defines the CTE. Any `cte_ident` alias can be referenced in subsequent `cte_binding` definitions and in the final `select_stmt`. |

#### Recursive CTEs

<div class="highlight">

``` chroma
WITH MUTUALLY RECURSIVE
  [((RETURN AT | ERROR AT) RECURSION LIMIT <limit>)]
  <cte_ident> ( <col_ident> <col_type> [, ...] ) AS ( <select_stmt> )
  [, <cte_ident> ( <col_ident> <col_type> [, ...] ) AS ( <select_stmt> ) [, ...]]
<select_stmt>
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
<td><strong>(RETURN AT | ERROR AT) RECURSION LIMIT</strong>
<code>&lt;limit&gt;</code></td>
<td><p>Optional. Control the recursion behavior:</p>
<table>
<thead>
<tr>
<th>Option</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>RETURN AT RECURSION LIMIT &lt;limit&gt;</code></td>
<td>Stop the fixpoint computation after <code>&lt;limit&gt;</code>
iterations and use the current values computed for each recursive CTE
binding in the <code>select_stmt</code>. Useful when debugging and
validating the correctness of recursive queries.</td>
</tr>
<tr>
<td><code>ERROR AT RECURSION LIMIT &lt;limit&gt;</code></td>
<td>Stop the fixpoint computation after <code>&lt;limit&gt;</code>
iterations and fail the query with an error. A good safeguard against
accidentally running a non-terminating dataflow in production
clusters.</td>
</tr>
</tbody>
</table></td>
</tr>
<tr>
<td><code>&lt;cte_ident&gt;</code> ( <code>&lt;col_ident&gt;</code>
<code>&lt;col_type&gt;</code> [, …] )</td>
<td>A binding that gives the SQL fragment defined under
<code>select_stmt</code> a <code>cte_ident</code> alias. Unlike regular
CTEs, a recursive CTE binding must explicitly state its type as a
comma-separated list of (<code>col_ident</code> <code>col_type</code>)
pairs. This alias can be used in the same binding or in all other
(preceding and subsequent) bindings in the enclosing recursive CTE
block.</td>
</tr>
<tr>
<td><strong>AS</strong> ( <code>&lt;select_stmt&gt;</code> )</td>
<td>The <code>SELECT</code> statement that defines the recursive CTE.
Any <code>cte_ident</code> alias can be referenced in all
<code>recursive_cte_binding</code> definitions that live under the same
block, as well as in the final <code>select_stmt</code> for that
block.</td>
</tr>
</tbody>
</table>

For details and examples, see the [Recursive
CTEs](/docs/sql/select/recursive-ctes) page.

## Details

Because Materialize works very differently from a traditional RDBMS,
it’s important to understand the implications that certain features of
`SELECT` will have on Materialize.

### Creating materialized views

Creating a [materialized view](/docs/sql/create-materialized-view)
generates a persistent dataflow, which has a different performance
profile from performing a `SELECT` in an RDBMS.

A materialized view has resource and latency costs that should be
carefully considered depending on its main usage. Materialize must
maintain the results of the query in durable storage, but often it must
also maintain additional intermediate state.

### Creating indexes

Creating an [index](/docs/sql/create-index) also generates a persistent
dataflow. The difference from a materialized view is that the results
are maintained in memory rather than on persistent storage. This allows
ad hoc queries to perform efficient point-lookups in indexes.

### Ad hoc queries

An ad hoc query (a.k.a. one-off `SELECT`) simply performs the query once
and returns the results. Ad hoc queries can either read from an existing
index, or they can start an ephemeral dataflow to compute the results.

Performing a `SELECT` on an **indexed** source, view or materialized
view is Materialize’s ideal operation. When Materialize receives such a
`SELECT` query, it quickly returns the maintained results from memory.
Materialize also quickly returns results for queries that only filter,
project, transform with scalar functions, and re-order data that is
maintained by an index.

Queries that can’t simply read out from an index will create an
ephemeral dataflow to compute the results. These dataflows are bound to
the active [cluster](/docs/concepts/clusters/), which you can change
using:

<div class="highlight">

``` chroma
SET cluster = <cluster name>;
```

</div>

Materialize will remove the dataflow as soon as it has returned the
query results to you.

#### Known limitations

CTEs have the following limitations, which we are working to improve:

- `INSERT`/`UPDATE`/`DELETE` (with `RETURNING`) is not supported inside
  a CTE.
- SQL99-compliant `WITH RECURSIVE` CTEs are not supported (use the
  [non-standard flavor](/docs/sql/select/recursive-ctes) instead).

### Query hints

Users can specify query hints to help Materialize optimize queries.

The following query hints are valid within the `OPTIONS` clause.

| Hint | Value type | Description |
|----|----|----|
| `AGGREGATE INPUT GROUP SIZE` | `uint8` | How many rows will have the same group key in an aggregation. Materialize can render `min` and `max` expressions more efficiently with this information. |
| `DISTINCT ON INPUT GROUP SIZE` | `uint8` | How many rows will have the same group key in a `DISTINCT ON` expression. Materialize can render [Top K patterns](/docs/transform-data/idiomatic-materialize-sql/top-k/) based on `DISTINCT ON` more efficiently with this information. To determine the query hint size, see [`EXPLAIN ANALYZE HINTS`](/docs/sql/explain-analyze/#explain-analyze-hints). |
| `LIMIT INPUT GROUP SIZE` | `uint8` | How many rows will be given as a group to a `LIMIT` restriction. Materialize can render [Top K patterns](/docs/transform-data/idiomatic-materialize-sql/top-k/) based on `LIMIT` more efficiently with this information. |

For examples, see the
[Optimization](/docs/transform-data/optimization/#query-hints) page.

### Column references

Within a given `SELECT` statement, we refer to the columns from the
tables in the `FROM` clause as the **input columns**, and columns in the
`SELECT` list as the **output columns**.

Expressions in the `SELECT` list, `WHERE` clause, and `HAVING` clause
may refer only to input columns.

Column references in the `ORDER BY` and `DISTINCT ON` clauses may be the
name of an output column, the ordinal number of an output column, or an
arbitrary expression of only input columns. If an unqualified name
refers to both an input and output column, `ORDER BY` chooses the output
column.

Column references in the `GROUP BY` clause may be the name of an output
column, the ordinal number of an output column, or an arbitrary
expression of only input columns. If an unqualified name refers to both
an input and output column, `GROUP BY` chooses the input column.

### Connection pooling

Because Materialize is wire-compatible with PostgreSQL, you can use any
PostgreSQL connection pooler with Materialize. For example in using
PgBouncer, see [Connection
Pooling](/docs/integrations/connection-pooling).

## Examples

### Creating an indexed view

This assumes you’ve already [created a source](../create-source).

The following query creates a view representing the total of all
purchases made by users per region, and then creates an index on this
view.

<div class="highlight">

``` chroma
CREATE VIEW purchases_by_region AS
    SELECT region.id, sum(purchase.total)
    FROM mysql_simple_purchase AS purchase
    JOIN mysql_simple_user AS user ON purchase.user_id = user.id
    JOIN mysql_simple_region AS region ON user.region_id = region.id
    GROUP BY region.id;

CREATE INDEX purchases_by_region_idx ON purchases_by_region(id);
```

</div>

In this case, Materialize will create a dataflow to maintain the results
of this query, and that dataflow will live on until the index it’s
maintaining is dropped.

### Reading from a view

Assuming you’ve created the indexed view listed above, named
`purchases_by_region`, you can simply read from the index with an ad hoc
`SELECT` query:

<div class="highlight">

``` chroma
SELECT * FROM purchases_by_region;
```

</div>

In this case, Materialize simply returns the results that the index is
maintaining, by reading from memory.

### Ad hoc querying

<div class="highlight">

``` chroma
SELECT region.id, sum(purchase.total)
FROM mysql_simple_purchase AS purchase
JOIN mysql_simple_user AS user ON purchase.user_id = user.id
JOIN mysql_simple_region AS region ON user.region_id = region.id
GROUP BY region.id;
```

</div>

In this case, Materialize will spin up a similar dataflow as it did for
creating the above indexed view, but it will tear down the dataflow once
it’s returned its results to the client. If you regularly want to view
the results of this query, you may want to create an
[index](/docs/sql/create-index) (in memory) and/or a [materialized
view](/docs/sql/create-materialized-view) (on persistent storage) for
it.

### Using regular CTEs

<div class="highlight">

``` chroma
WITH
  regional_sales (region, total_sales) AS (
    SELECT region, sum(amount)
    FROM orders
    GROUP BY region
  ),
  top_regions AS (
    SELECT region
    FROM regional_sales
    ORDER BY total_sales DESC
    LIMIT 5
  )
SELECT region,
       product,
       SUM(quantity) AS product_units,
       SUM(amount) AS product_sales
FROM orders
WHERE region IN (SELECT region FROM top_regions)
GROUP BY region, product;
```

</div>

Both `regional_sales` and `top_regions` are CTEs. You could write a
query that produces the same results by replacing references to the CTE
with the query it names, but the CTEs make the entire query simpler to
understand.

With regard to dataflows, this is similar to [ad hoc
querying](#ad-hoc-querying) above: Materialize tears down the created
dataflow after returning the results.

## Privileges

The privileges required to execute this statement are:

- `SELECT` privileges on all **directly** referenced relations in the
  query. If the directly referenced relation is a view or materialized
  view:

- `USAGE` privileges on the schemas that contain the relations in the
  query.

- `USAGE` privileges on the active cluster.

## Related pages

- [`CREATE VIEW`](../create-view)
- [`CREATE MATERIALIZED VIEW`](../create-materialized-view)
- [`SHOW FULL VIEWS`](../show-views)

[JOIN](/docs/sql/select/join/)

[Recursive CTEs](/docs/sql/select/recursive-ctes/)

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/select/_index.md"
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
