# SELECT

`SELECT` binds SQL queries to named views or materialized views, and allows to interactively query data maintained in Materialize .



[//]: # "TODO(morsapaes) More than adapting this to the new architecture,
rewrite the page entirely at some point."

The `SELECT` statement is the root of a SQL query, and is used both to bind SQL
queries to named [views](../create-view) or [materialized views](../create-materialized-view),
 and to interactively query data maintained in Materialize. For interactive queries, you should consider creating [indexes](../create-index)
on the underlying relations based on common query patterns.

## Syntax



```mzsql
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

| Syntax element | Description |
| --- | --- |
| **WITH** `<cte_binding>` [, ...] | Optional. [Common table expressions](#common-table-expressions-ctes) (CTEs) for this query. See [Regular CTEs](#regular-ctes) for details.  |
| **ALL** \| **DISTINCT** [**ON** ( `<col_ref>` [, ...] )] | Optional. Specifies which rows to return:  \| Option \| Description \| \|--------\|-------------\| \| `ALL` \| Return all rows from query (default). \| \| `DISTINCT` \| <a id="select-distinct"></a>Return only distinct values. \| \| `DISTINCT ON ( <col_ref> [, ...] )` \| <a id="select-distinct-on"></a>Return only the first row with a distinct value for `<col_ref>`. If an `ORDER BY` clause is also present, then `DISTINCT ON` will respect that ordering when choosing which row to return for each distinct value. You should start the `ORDER BY` clause with the same `<col_ref>` as the `DISTINCT ON` clause. \|  |
| `<target_elem>` [, ...] | The columns or expressions to return. Can include column names, functions, or expressions.  |
| **FROM** `<table_expr>` [, ...] | The tables you want to read from. These can be table names, other `SELECT` statements, [Common Table Expressions](#common-table-expressions-ctes) (CTEs), or [table function calls](/sql/functions/table-functions).  |
| `<join_expr>` | Optional. A join expression to combine table expressions. For more details, see the [`JOIN` documentation](/sql/select/join/).  |
| **WHERE** `<expression>` | Optional. Filter tuples by `<expression>`.  |
| **GROUP BY** `<col_ref>` [, ...] | Optional. Group aggregations by `<col_ref>`. Column references may be the name of an output column, the ordinal number of an output column, or an arbitrary expression of only input columns.  |
| **OPTIONS** ( `<option>` = `<val>` [, ...] ) | Optional. Specify one or more [query hints](#query-hints). Valid hints:  \| Hint \| Value type \| Description \| \|------\|------------\|-------------\| \| `AGGREGATE INPUT GROUP SIZE` \| `uint8` \| How many rows will have the same group key in an aggregation. Materialize can render `min` and `max` expressions more efficiently with this information. \| \| `DISTINCT ON INPUT GROUP SIZE` \| `uint8` \| How many rows will have the same group key in a `DISTINCT ON` expression. Materialize can render [Top K patterns](/transform-data/idiomatic-materialize-sql/top-k/) based on `DISTINCT ON` more efficiently with this information. \| \| `LIMIT INPUT GROUP SIZE` \| `uint8` \| How many rows will be given as a group to a `LIMIT` restriction. Materialize can render [Top K patterns](/transform-data/idiomatic-materialize-sql/top-k/) based on `LIMIT` more efficiently with this information. \|  |
| **HAVING** `<expression>` | Optional. Filter aggregations by `<expression>`.  |
| **ORDER BY** `<col_ref>` [**ASC** \| **DESC**] [**NULLS FIRST** \| **NULLS LAST**] [, ...] | Optional. Sort results in either `ASC` (default) or `DESC` order. Use the `NULLS FIRST` and `NULLS LAST` options to determine whether nulls appear before or after non-null values in the sort ordering (default: `NULLS LAST` for `ASC`, `NULLS FIRST` for `DESC`). Column references may be the name of an output column, the ordinal number of an output column, or an arbitrary expression of only input columns.  |
| **LIMIT** `<expression>` | Optional. Limit the number of returned results to `<expression>`.  |
| **OFFSET** `<integer>` | Optional. Skip the first `<integer>` number of rows.  |
| **UNION** [**ALL** \| **DISTINCT**] `<another_select_stmt>` | Optional. Records present in `select_stmt` or `another_select_stmt`. `DISTINCT` returns only unique rows from these results (implied default). With `ALL` specified, each record occurs a number of times equal to the sum of the times it occurs in each input statement.  |
| **INTERSECT** [**ALL** \| **DISTINCT**] `<another_select_stmt>` | Optional. Records present in both `select_stmt` and `another_select_stmt`. `DISTINCT` returns only unique rows from these results (implied default). With `ALL` specified, each record occurs a number of times equal to the lesser of the times it occurs in each input statement.  |
| **EXCEPT** [**ALL** \| **DISTINCT**] `<another_select_stmt>` | Optional. Records present in `select_stmt` but not in `another_select_stmt`. `DISTINCT` returns only unique rows from these results (implied default). With `ALL` specified, each record occurs a number of times equal to the times it occurs in `select_stmt` less the times it occurs in `another_select_stmt`, or not at all if the former is greater than latter.  |


### Common table expressions (CTEs)

#### Regular CTEs



```mzsql
WITH <cte_ident> [( <col_ident> [, ...] )] AS ( <select_stmt> )
  [, <cte_ident> [( <col_ident> [, ...] )] AS ( <select_stmt> ) [, ...]]
<select_stmt>

```

| Syntax element | Description |
| --- | --- |
| `<cte_ident>` | The name of the common table expression (CTE).  |
| ( `<col_ident>` [, ...] ) | Optional. Rename the CTE's columns to the list of identifiers. The number of identifiers must match the number of columns returned by the CTE's `select_stmt`.  |
| **AS** ( `<select_stmt>` ) | The `SELECT` statement that defines the CTE. Any `cte_ident` alias can be referenced in subsequent `cte_binding` definitions and in the final `select_stmt`.  |


#### Recursive CTEs



```mzsql
WITH MUTUALLY RECURSIVE
  [((RETURN AT | ERROR AT) RECURSION LIMIT <limit>)]
  <cte_ident> ( <col_ident> <col_type> [, ...] ) AS ( <select_stmt> )
  [, <cte_ident> ( <col_ident> <col_type> [, ...] ) AS ( <select_stmt> ) [, ...]]
<select_stmt>

```

| Syntax element | Description |
| --- | --- |
| **(RETURN AT \| ERROR AT) RECURSION LIMIT** `<limit>` | Optional. Control the recursion behavior:  \| Option \| Description \| \|--------\|-------------\| \| `RETURN AT RECURSION LIMIT <limit>` \| Stop the fixpoint computation after `<limit>` iterations and use the current values computed for each recursive CTE binding in the `select_stmt`. Useful when debugging and validating the correctness of recursive queries. \| \| `ERROR AT RECURSION LIMIT <limit>` \| Stop the fixpoint computation after `<limit>` iterations and fail the query with an error. A good safeguard against accidentally running a non-terminating dataflow in production clusters. \|  |
| `<cte_ident>` ( `<col_ident>` `<col_type>` [, ...] ) | A binding that gives the SQL fragment defined under `select_stmt` a `cte_ident` alias. Unlike regular CTEs, a recursive CTE binding must explicitly state its type as a comma-separated list of (`col_ident` `col_type`) pairs. This alias can be used in the same binding or in all other (preceding and subsequent) bindings in the enclosing recursive CTE block.  |
| **AS** ( `<select_stmt>` ) | The `SELECT` statement that defines the recursive CTE. Any `cte_ident` alias can be referenced in all `recursive_cte_binding` definitions that live under the same block, as well as in the final `select_stmt` for that block.  |


For details and examples, see the [Recursive CTEs](/sql/select/recursive-ctes) page.

## Details

Because Materialize works very differently from a traditional RDBMS, it's
important to understand the implications that certain features of `SELECT` will
have on Materialize.

### Creating materialized views

Creating a [materialized view](/sql/create-materialized-view) generates a persistent dataflow, which has a
different performance profile from performing a `SELECT` in an RDBMS.

A materialized view has resource and latency costs that should
be carefully considered depending on its main usage. Materialize must maintain
the results of the query in durable storage, but often it must also maintain
additional intermediate state.

### Creating indexes

Creating an [index](/sql/create-index) also generates a persistent dataflow. The difference from a materialized view is that the results are maintained in memory rather than on persistent storage. This allows ad hoc queries to perform efficient point-lookups in indexes.

### Ad hoc queries

An ad hoc query (a.k.a. one-off `SELECT`) simply performs the query once and returns the results. Ad hoc queries can either read from an existing index, or they can start an ephemeral dataflow to compute the results.

Performing a `SELECT` on an **indexed** source, view or materialized view is
Materialize's ideal operation. When Materialize receives such a `SELECT` query,
it quickly returns the maintained results from memory.
Materialize also quickly returns results for queries that only filter, project, transform with scalar functions,
and re-order data that is maintained by an index.

Queries that can't simply read out from an index will create an ephemeral dataflow to compute
the results. These dataflows are bound to the active [cluster](/concepts/clusters/),
 which you can change using:

```mzsql
SET cluster = <cluster name>;
```

Materialize will remove the dataflow as soon as it has returned the query results to you.


#### Known limitations

CTEs have the following limitations, which we are working to improve:

- `INSERT`/`UPDATE`/`DELETE` (with `RETURNING`) is not supported inside a CTE.
- SQL99-compliant `WITH RECURSIVE` CTEs are not supported (use the [non-standard flavor](/sql/select/recursive-ctes) instead).

### Query hints

Users can specify query hints to help Materialize optimize queries.

The following query hints are valid within the `OPTIONS` clause.

Hint | Value type | Description
------|------------|------------
`AGGREGATE INPUT GROUP SIZE` | `uint8` | How many rows will have the same group key in an aggregation. Materialize can render `min` and `max` expressions more efficiently with this information.
`DISTINCT ON INPUT GROUP SIZE` | `uint8` | How many rows will have the same group key in a `DISTINCT ON` expression. Materialize can render [Top K patterns](/transform-data/idiomatic-materialize-sql/top-k/) based on `DISTINCT ON` more efficiently with this information. To determine the query hint size, see [`EXPLAIN ANALYZE HINTS`](/sql/explain-analyze/#explain-analyze-hints).
`LIMIT INPUT GROUP SIZE` | `uint8` | How many rows will be given as a group to a `LIMIT` restriction. Materialize can render [Top K patterns](/transform-data/idiomatic-materialize-sql/top-k/) based on `LIMIT` more efficiently with this information.

For examples, see the [Optimization](/transform-data/optimization/#query-hints) page.

### Column references

Within a given `SELECT` statement, we refer to the columns from the tables in
the `FROM` clause as the **input columns**, and columns in the `SELECT` list as
the **output columns**.

Expressions in the `SELECT` list, `WHERE` clause, and `HAVING` clause may refer
only to input columns.

Column references in the `ORDER BY` and `DISTINCT ON` clauses may be the name of
an output column, the ordinal number of an output column, or an arbitrary
expression of only input columns. If an unqualified name refers to both an input
and output column, `ORDER BY` chooses the output column.

Column references in the `GROUP BY` clause may be the name of an output column,
the ordinal number of an output column, or an arbitrary expression of only input
columns. If an unqualified name refers to both an input and output column,
`GROUP BY` chooses the input column.

### Connection pooling

Because Materialize is wire-compatible with PostgreSQL, you can use any
PostgreSQL connection pooler with Materialize. For example in using PgBouncer,
see [Connection Pooling](/integrations/connection-pooling).

## Examples

### Creating an indexed view

This assumes you've already [created a source](../create-source).

The following query creates a view representing the total of all
purchases made by users per region, and then creates an index on this view.

```mzsql
CREATE VIEW purchases_by_region AS
    SELECT region.id, sum(purchase.total)
    FROM mysql_simple_purchase AS purchase
    JOIN mysql_simple_user AS user ON purchase.user_id = user.id
    JOIN mysql_simple_region AS region ON user.region_id = region.id
    GROUP BY region.id;

CREATE INDEX purchases_by_region_idx ON purchases_by_region(id);
```

In this case, Materialize will create a dataflow to maintain the results of
this query, and that dataflow will live on until the index it's maintaining is
dropped.

### Reading from a view

Assuming you've created the indexed view listed above, named `purchases_by_region`, you can simply read from the index with an ad hoc `SELECT` query:

```mzsql
SELECT * FROM purchases_by_region;
```

In this case, Materialize simply returns the results that the index is maintaining, by reading from memory.

### Ad hoc querying

```mzsql
SELECT region.id, sum(purchase.total)
FROM mysql_simple_purchase AS purchase
JOIN mysql_simple_user AS user ON purchase.user_id = user.id
JOIN mysql_simple_region AS region ON user.region_id = region.id
GROUP BY region.id;
```

In this case, Materialize will spin up a similar dataflow as it did for creating
the above indexed view, but it will tear down the dataflow once it's returned its
results to the client. If you regularly want to view the results of this query,
you may want to create an [index](/sql/create-index) (in memory) and/or a [materialized view](/sql/create-materialized-view) (on persistent storage) for it.

### Using regular CTEs

```mzsql
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

Both `regional_sales` and `top_regions` are CTEs. You could write a query that
produces the same results by replacing references to the CTE with the query it
names, but the CTEs make the entire query simpler to understand.

With regard to dataflows, this is similar to [ad hoc querying](#ad-hoc-querying)
above: Materialize tears down the created dataflow after returning the results.

## Privileges

The privileges required to execute this statement are:

- `SELECT` privileges on all **directly** referenced relations in the query. If
  the directly referenced relation is a view or materialized view: - `SELECT` privileges are required only on the directly referenced
  view/materialized view. `SELECT` privileges are **not** required for the
  underlying relations referenced in the view/materialized view definition
  unless those relations themselves are directly referenced in the query.

- However, the owner of the view/materialized view (including those with
  **superuser** privileges) must have all required `SELECT` and `USAGE`
  privileges to run the view definition regardless of who is selecting from the
  view/materialized view.

- `USAGE` privileges on the schemas that contain the relations in the query.
- `USAGE` privileges on the active cluster.

## Related pages

- [`CREATE VIEW`](../create-view)
- [`CREATE MATERIALIZED VIEW`](../create-materialized-view)
- [`SHOW FULL VIEWS`](../show-views)



---

## JOIN


`JOIN` lets you combine two or more table expressions into a single table
expression.

## Conceptual framework

Much like an RDBMS, Materialize can join together any two table expressions (in
our case, either [sources](/sql/create-source) or [views](/sql/create-view)) into
a single table expression.

Materialize has much broader support for `JOIN` than most streaming platforms,
i.e. we support all types of SQL joins in all of the conditions you would
expect.

## Syntax



```mzsql
<select_pred>
[NATURAL] <join_type> JOIN
  [LATERAL] ( <select_stmt> | <table_func_call> | <table_ref> )
  [USING ( <col_ref> [, ...] ) [AS <join_using_alias>] | ON <expression>]
<select_post>

```

| Syntax element | Description |
| --- | --- |
| `<select_pred>` | The predicating [`SELECT`](/sql/select) clauses you want to use, e.g. `SELECT col_ref FROM table_ref...`. The `<table_ref>` from the `<select_pred>` is the left-hand table.  |
| **NATURAL** | Optional. Join table expressions on all columns with the same names in both tables. This is similar to the `USING` clause naming all identically named columns in both tables.  |
| `<join_type>` | The type of `JOIN` you want to use. Valid join types:  \| Join Type \| Description \| \|-----------\|-------------\| \| `INNER` \| (Default) Return all tuples from both tables where the join condition is valid. \| \| `LEFT` [**OUTER**] \| Return all tuples from the left-hand-side table, and all tuples from the right-hand-side table that match the join condition. Tuples from the left-hand table that are not joined contain `NULL` wherever the right-hand table is referenced. \| \| `RIGHT` [**OUTER**] \| Return all tuples from the right-hand-side table, and all tuples from the left-hand-side table that match the join condition. Tuples from the right-hand table that are not joined contain `NULL` wherever the left-hand table is referenced. \| \| `FULL` [**OUTER**] \| Return all tuples from both tables, joining them together where the join conditions are met. Tuples that are not joined contain `NULL` wherever the other table is referenced. \| \| `CROSS` \| Return the Cartesian product of the two tables, i.e. all combinations of tuples from the left-hand table combined with tuples from the right-hand table. \|  |
| **LATERAL** | Optional. Let the following subquery or table function call refer to columns from join's left-hand side. See [`LATERAL` subqueries](#lateral-subqueries) for details.  |
| ( `<select_stmt>` \| `<table_func_call>` \| `<table_ref>` ) | The table expression you want to join, i.e. the right-hand table. Can be a [`SELECT` statement](/sql/select), a [table function call](/sql/functions/#table-functions), or a table reference.  |
| **USING** ( `<col_ref>` [, ...] ) [**AS** `<join_using_alias>`] | Optional. If the join condition does not require table-level qualification (i.e. joining tables on columns with the same name), the columns to join the tables on. For example, `USING (customer_id)`. The optional `AS` clause provides a table alias for the join columns. The columns will remain referenceable by their original names. For example, given `lhs JOIN rhs USING (c) AS joint`, the column `c` will be referenceable as `lhs.c`, `rhs.c`, and `joint.c`.  |
| **ON** `<expression>` | Optional. The condition on which to join the tables. For example `ON purchase.customer_id = customer.id`.  |
| `<select_post>` | The remaining [`SELECT`](/sql/select) clauses you want to use, e.g. `...WHERE expr GROUP BY col_ref HAVING expr`.  |


**Note**: It's possible to join together table expressions as inner joins without using this clause whatsoever, e.g. `SELECT cols... FROM t1, t2 WHERE t1.x = t2.x GROUP BY cols...`

## Details

Unlike most other streaming platforms, `JOIN`s in Materialize have very few, if
any, restrictions. For example, Materialize:

- Does not require time windows when joining streams.
- Does not require any kind of partitioning.

Instead, `JOIN`s work over the available history of both streams, which
ultimately provides an experience more similar to an RDBMS than other streaming
platforms.

### `LATERAL` subqueries

To permit subqueries on the right-hand side of a `JOIN` to access the columns
defined by the left-hand side, declare the subquery as `LATERAL`. Normally, a
subquery only has access to the columns within its own context.

Table function invocations always have implicit access to the columns defined by
the left-hand side of the join, so declaring them as `LATERAL` is a permitted
no-op.

When a join contains a `LATERAL` cross-reference, the right-hand relation is
recomputed for each row in the left-hand relation, then joined to the
left-hand row according to the usual rules of the selected join type.

> **Warning:** `LATERAL` subqueries can be very expensive to compute. For best results, do not
> materialize a view containing a `LATERAL` subquery without first inspecting the
> plan via the [`EXPLAIN PLAN`](/sql/explain-plan/) statement. In many common patterns
> involving `LATERAL` joins, Materialize can optimize away the join entirely.


As a simple example, the following query uses `LATERAL` to count from 1 to `x`
for all the values of `x` in `xs`.

```mzsql
SELECT * FROM
  (VALUES (1), (3)) xs (x)
  CROSS JOIN LATERAL generate_series(1, x) y;
```
```nofmt
 x | y
---+---
 1 | 1
 3 | 1
 3 | 2
 3 | 3
```

For a real-world example of a `LATERAL` subquery, see the [Top-K by group
idiom](/transform-data/idiomatic-materialize-sql/top-k/).


## Examples

For these examples, we'll use a small data set:

**Employees**

```nofmt
 id |  name
----+--------
  1 | Frank
  2 | Arjun
  3 | Nikhil
  4 | Cuong
```

**Managers**

```nofmt
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

Inner joins return all tuples from both tables where the join condition is
valid.

![inner join diagram](/images/join-inner.png)

```mzsql
SELECT
  employees."name" AS employee,
  managers."name" AS manager
FROM employees
INNER JOIN managers ON employees.id = managers.manages;
```
```nofmt
 employee | manager
----------+---------
 Cuong    | Arjun
 Nikhil   | Cuong
```

### Left outer join

Left outer joins (also known as left joins) return all tuples from the
left-hand-side table, and all tuples from the right-hand-side table that match
the join condition. Tuples on from the left-hand table that are not joined with
a tuple from the right-hand table contain `NULL` wherever the right-hand table
is referenced.

![left outer join diagram](/images/join-left-outer.png)

```mzsql
SELECT
  employees."name" AS employee,
  managers."name" AS manager
FROM employees
LEFT OUTER JOIN managers ON employees.id = managers.manages;
```
```nofmt
 employee | manager
----------+---------
 Cuong    | Arjun
 Nikhil   | Cuong
 Arjun    |
 Frank    |
 ```

### Right outer join

Right outer joins (also known as right joins) are simply the right-hand-side
equivalent of left outer joins.

Right outer joins return all tuples from the right-hand-side table, and all
tuples from the left-hand-side table that match the join condition. Tuples on
from the right-hand table that are not joined with a tuple from the left-hand
table contain `NULL` wherever the left-hand table is referenced.

![right outer join diagram](/images/join-right-outer.png)

```mzsql
SELECT
  employees."name" AS employee,
  managers."name" AS manager
FROM employees
RIGHT OUTER JOIN managers ON employees.id = managers.manages;
```
```nofmt
 employee | manager
----------+---------
 Cuong    | Arjun
 Nikhil   | Cuong
          | Frank
 ```

### Full outer join

Full outer joins perform both a left outer join and a right outer join. They
return all tuples from both tables, and join them together where the join
conditions are met.

Tuples that are not joined with the other table contain `NULL` wherever the
other table is referenced.

![full outer join diagram](/images/join-full-outer.png)

```mzsql
SELECT
  employees."name" AS employee,
  managers."name" AS manager
FROM employees
FULL OUTER JOIN managers ON employees.id = managers.manages;
```
```nofmt
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
product](https://en.wikipedia.org/wiki/Cartesian_product) of the two tables,
i.e. all combinations of tuples from the left-hand table combined with tuples
from the right-hand table.

![cross join diagram](/images/join-cross.png)

Our example dataset doesn't have a meaningful cross-join query, but the above
diagram shows how cross joins form the Cartesian product.

## Related pages

- [`SELECT`](/sql/select)
- [`CREATE VIEW`](/sql/create-view)


---

## Recursive CTEs


Recursive CTEs operate on the recursively-defined structures like trees or graphs implied from queries over your data.

## Syntax



```mzsql
WITH MUTUALLY RECURSIVE
  [((RETURN AT | ERROR AT) RECURSION LIMIT <limit>)]
  <cte_ident> ( <col_ident> <col_type> [, ...] ) AS ( <select_stmt> )
  [, <cte_ident> ( <col_ident> <col_type> [, ...] ) AS ( <select_stmt> ) [, ...]]
<select_stmt>

```

| Syntax element | Description |
| --- | --- |
| **(RETURN AT \| ERROR AT) RECURSION LIMIT** `<limit>` | Optional. Control the recursion behavior:  \| Option \| Description \| \|--------\|-------------\| \| `RETURN AT RECURSION LIMIT <limit>` \| Stop the fixpoint computation after `<limit>` iterations and use the current values computed for each recursive CTE binding in the `select_stmt`. Useful when debugging and validating the correctness of recursive queries. \| \| `ERROR AT RECURSION LIMIT <limit>` \| Stop the fixpoint computation after `<limit>` iterations and fail the query with an error. A good safeguard against accidentally running a non-terminating dataflow in production clusters. \|  |
| `<cte_ident>` ( `<col_ident>` `<col_type>` [, ...] ) | A binding that gives the SQL fragment defined under `select_stmt` a `cte_ident` alias. Unlike regular CTEs, a recursive CTE binding must explicitly state its type as a comma-separated list of (`col_ident` `col_type`) pairs. This alias can be used in the same binding or in all other (preceding and subsequent) bindings in the enclosing recursive CTE block.  |
| **AS** ( `<select_stmt>` ) | The `SELECT` statement that defines the recursive CTE. Any `cte_ident` alias can be referenced in all `recursive_cte_binding` definitions that live under the same block, as well as in the final `select_stmt` for that block.  |


## Details

Within a recursive CTEs block, any `cte_ident` alias can be referenced in all `recursive_cte_binding` definitions that live under the same block, as well as in the final `select_stmt` for that block.

A `WITH MUTUALLY RECURSIVE` block with a general form

```mzsql
WITH MUTUALLY RECURSIVE
  -- A sequence of bindings, all in scope for all definitions.
  $R_1(...) AS ( $sql_cte_1 ),
  ...
  $R_n(...) AS ( $sql_cte_n )
  -- Compute the result from the final values of all bindings.
  $sql_body
```

is evaluated as if it was performing the following steps:

1. Initially, bind `$R_1, ..., $R_n` to the empty collection.
1. Repeat in a loop:
   1. Update `$R_1` using the current values bound to `$R_1, ..., $R_n` in `$sql_cte_1`.
   1. Update `$R_2` using the current values bound to `$R_1, ..., $R_n` in `$sql_cte_2`. Note that this includes the new value of `$R_1` bound above.
   1. ...
   1. Update `$R_n` using the current values bound to `$R_1, ..., $R_n` in `$sql_cte_n`. Note that this includes the new values of `$R_1, ..., $R_{n-1}` bound above.
1. Exit the loop when one of the following conditions is met:
   1. The values bound to all CTEs have stopped changing.
   1. The optional early exit condition to `RETURN` or `ERROR AT ITERATION $i` was set and we have reached iteration `$i`.

Note that Materialize's ability to [efficiently handle incremental changes to your inputs](https://materialize.com/guides/incremental-computation/) extends across loop iterations.
For each iteration, Materialize performs work resulting only from the input changes for this iteration and feeds back the resulting output changes to the next iteration.
When the set of changes for all bindings becomes empty, the recursive computation stops and the final `select_stmt` is evaluated.

> **Warning:** In the absence of recursive CTEs, every `SELECT` query is guaranteed to compute its result or fail with an error within a finite amount of time.
> However, introducing recursive CTEs complicates the situation as follows:
> 1. The query might not converge (and may never terminate).
>    Non-terminating queries never return a result and can consume a lot of your cluster resources. See [an example](#non-terminating-queries) below.
> 2. A small update to a few (or even one) data points in your input might cascade in big updates in your recursive computation.
>    This most likely will manifest in spikes of the cluster resources allocated to your recursive dataflows.
>    See [an example](#queries-with-update-locality) below.


## Examples

Let's consider a very simple schema consisting of `users` that belong to a
hierarchy of geographical `areas` and exchange `transfers` between each other.
Use the [SQL Shell](/console/) to run the sequence of
commands below.


### Example schema

```mzsql
-- A hierarchy of geographical locations with various levels of granularity.
CREATE TABLE areas(id int not null, parent int, name text);
-- A collection of users.
CREATE TABLE users(id char(1) not null, area_id int not null, name text);
-- A collection of transfers between these users.
CREATE TABLE transfers(src_id char(1), tgt_id char(1), amount numeric, ts timestamp);
```

### Example data

```mzsql
DELETE FROM areas;
DELETE FROM users;
DELETE FROM transfers;

INSERT INTO areas VALUES
  (1, 2    , 'Brooklyn'),
  (2, 3    , 'New York'),
  (3, 7    , 'United States of America'),
  (4, 5    , 'Kreuzberg'),
  (5, 6    , 'Berlin'),
  (6, 7    , 'Germany'),
  (7, null , 'Earth');
INSERT INTO users VALUES
  ('A', 1, 'Alice'),
  ('B', 4, 'Bob'),
  ('C', 2, 'Carol'),
  ('D', 5, 'Dan');
INSERT INTO transfers VALUES
  ('B', 'C', 20.0 , now()),
  ('A', 'D', 15.0 , now() + '05 seconds'),
  ('C', 'D', 25.0 , now() + '10 seconds'),
  ('A', 'B', 10.0 , now() + '15 seconds'),
  ('C', 'A', 35.0 , now() + '20 seconds');
```

### Transitive closure

The following view will compute `connected` as the transitive closure of a graph where:
* each `user` is a graph vertex, and
* a graph edge between users `x` and `y` exists only if a transfer from `x` to `y` was made recently (using the rather small `10 seconds` period here for the sake of illustration):

```mzsql
CREATE MATERIALIZED VIEW connected AS
WITH MUTUALLY RECURSIVE
  connected(src_id char(1), dst_id char(1)) AS (
    SELECT DISTINCT src_id, tgt_id FROM transfers WHERE mz_now() <= ts + interval '10s'
    UNION
    SELECT c1.src_id, c2.dst_id FROM connected c1 JOIN connected c2 ON c1.dst_id = c2.src_id
  )
SELECT src_id, dst_id FROM connected;
```

To see results change over time, you can [`SUBSCRIBE`](/sql/subscribe/) to the
materialized view and then use a different SQL Shell session to insert
some sample data into the base tables used in the view:

```mzsql
SUBSCRIBE(SELECT * FROM connected) WITH (SNAPSHOT = FALSE);
```

You'll see results change as new data is inserted. When you’re done, cancel out
of the `SUBSCRIBE` using **Stop streaming**.

> **Note:** Depending on your base data, the number of records in the `connected` result might get close to the square of the number of `users`.


### Strongly connected components

Another thing that you might be interested in is identifying maximal sub-graphs where every pair of `users` are `connected` (the so-called [_strongly connected components (SCCs)_](https://en.wikipedia.org/wiki/Strongly_connected_component)) of the graph defined above.
This information might be useful to identify clusters of closely-tight users in your application.

Since you already have `connected` defined as a `MATERIALIZED VIEW`, you can piggy-back on that information to derive the SCCs of your graph.
Two `users` will be in the same SCC only if they are `connected` in both directions.
Consequently, given the `connected` contents, we can:

1. Restrict `connected` to the subset of `symmetric` connections that go in both directions.
2. Identify the `scc` of each `users` entry with the lowest `dst_id` of all `symmetric` neighbors and its own `id`.

```mzsql
CREATE MATERIALIZED VIEW strongly_connected_components AS
  WITH
    symmetric(src_id, dst_id) AS (
      SELECT src_id, dst_id FROM connected
      INTERSECT ALL
      SELECT dst_id, src_id FROM connected
    )
  SELECT u.id, least(min(c.dst_id), u.id)
  FROM users u
  LEFT JOIN symmetric c ON(u.id = c.src_id)
  GROUP BY u.id;
```

Again, you can insert some sample data into the base tables and observe how the
materialized view contents change over time using `SUBSCRIBE`:

```mzsql
SUBSCRIBE(SELECT * FROM strongly_connected_components) WITH (SNAPSHOT = FALSE);
```

When you’re done, cancel out of the `SUBSCRIBE` using **Stop streaming**.

> **Note:** The `strongly_connected_components` definition given above is not recursive, but relies on the recursive CTEs from the `connected` definition.
> If you don't need to keep track of the `connected` contents for other reasons, you can use [this alternative SCC definition](https://twitter.com/frankmcsherry/status/1628519795971727366) which computes SCCs directly using repeated forward and backward label propagation.


### Aggregations over a hierarchy

You might want to keep track of the aggregated net balance per area for a recent period of time.
This can be achieved in three steps:

1. Sum up the aggregated net balance for the set period for each user in an `user_balances` CTE.
2. Sum up the `user_balances` of users directly associated with an area in a `direct_balances` CTE.
3. Recursively add to `direct_balances` the `indirect_balances` sum of all child areas.

A materialized view that does the above three steps in three CTEs (of which the last one is recursive) can be defined as follows:

```mzsql
CREATE MATERIALIZED VIEW area_balances AS
  WITH MUTUALLY RECURSIVE
    user_balances(id char(1), balance numeric) AS (
      WITH
        credits AS (
          SELECT src_id as id, sum(amount) credit
          FROM transfers
          WHERE mz_now() <= ts + interval '10s'
          GROUP BY src_id
        ),
        debits AS (
          SELECT tgt_id as id, sum(amount) debit
          FROM transfers
          WHERE mz_now() <= ts + interval '10s'
          GROUP BY tgt_id
        )
      SELECT
        id,
        coalesce(debit, 0) - coalesce(credit, 0) as balance
      FROM
        users
        LEFT JOIN credits USING(id)
        LEFT JOIN debits USING(id)
    ),
    direct_balances(id int, parent int, balance numeric) AS (
      SELECT
        a.id as id,
        a.parent as parent,
        coalesce(sum(ub.balance), 0) as balance
      FROM
        areas a
        LEFT JOIN users u ON (a.id = u.area_id)
        LEFT JOIN user_balances ub ON (u.id = ub.id)
      GROUP BY
        a.id, a.parent
    ),
    indirect_balances(id int, parent int, balance numeric) AS (
      SELECT
        db.id as id,
        db.parent as parent,
        db.balance + coalesce(sum(ib.balance), 0) as balance
      FROM
        direct_balances db
        LEFT JOIN indirect_balances ib ON (db.id = ib.parent)
      GROUP BY
        db.id, db.parent, db.balance
    )
  SELECT
    id, balance
  FROM
    indirect_balances;
```

As before, you can insert [the example data](#example-data) and observe how the materialized view contents change over time from the `psql` with the `\watch` command:

```mzsql
SELECT id, name, balance FROM area_balances JOIN areas USING(id) ORDER BY id;
\watch 1
```

### Non-terminating queries

Let's look at a slight variation of the [transitive closure example](#transitive-closure) from above with the following changes:

1. All `UNION` operators are replaced with `UNION ALL`.
2. The `mz_now() < ts + $period` predicate from the first `UNION` branch is omitted.
3. The `WITH MUTUALLY RECURSIVE` clause has an optional `ERROR AT RECURSION LIMIT 100`.
4. The final result in this example is ordered by `src_id, dst_id`.

```mzsql
WITH MUTUALLY RECURSIVE (ERROR AT RECURSION LIMIT 100)
  connected(src_id char(1), dst_id char(1)) AS (
    SELECT DISTINCT src_id, tgt_id FROM transfers
    UNION ALL
    SELECT src_id, dst_id FROM connected
    UNION ALL
    SELECT c1.src_id, c2.dst_id FROM connected c1 JOIN connected c2 ON c1.dst_id = c2.src_id
  )
SELECT src_id, dst_id FROM connected ORDER BY src_id, dst_id;
```

After inserting [the example data](#example-data) you can observe that executing the above statement returns the following error:

```text
ERROR:  Evaluation error: Recursive query exceeded the recursion limit 100. (Use RETURN AT RECURSION LIMIT to not error, but return the current state as the final result when reaching the limit.)
```

The recursive CTE `connected` has not converged to a fixpoint within the first 100 iterations!
To see why, you can run variants of the same query where the

```mzsql
ERROR AT RECURSION LIMIT 100
```

clause is replaced by

```mzsql
RETURN AT RECURSION LIMIT $n -- where $n = 1, 2, 3, ...
```

and observe how the result changes after `$n` iterations.


**After 1 iteration:**
```text
 src_id | dst_id
--------+--------
 A      | B
 ...
```

**After 2 iterations:**
```text
 src_id | dst_id
--------+--------
 A      | B
 A      | B
 ...
```

**After 3 iterations:**
```text
 src_id | dst_id
--------+--------
 A      | B
 A      | B
 A      | B
 ...
```



Changing the `UNION` to `UNION ALL` in the `connected` definition caused a full copy of `transfer` to be added to the current value of `connected` in each iteration!
Consequently, `connected` never stops growing and the recursive CTE computation never reaches a fixpoint.

### Queries with "update locality"

The examples presented so far have the following "update locality" property:

> **Note:** A change in a source collection will usually cause a _bounded amount_ of changes to the contents of the recursive CTE bindings derived after each iteration.


For example:

- Most of the time, inserting or removing `transfers` will not change the contents of the `connected` collection. This is true because:
  - An alternative path from `x` to `y` already existed before the insertion, or
  - An alternative path from `x` to `y` will exist after the removal.
- Inserting or removing `transfers` will not change most of the contents of the `area_balances` collection. This is true because:
  - Areas are organized in a hierarchy with a maximum height `h`.
  - A single transfer contributes directly only to the `area_balances` of the areas where the `src_id` and `tgt_id` belong.
  - A single transfer contributes indirectly only to the `area_balances` of the areas ancestor areas of the above two areas.
  - Consequently, each `transfer` change will affect at most `2` areas in each iteration and at most `2*h` areas in total.

However, note that not all iterative algorithms have this property. For example:

- In a naive PageRank implementation, the introduction of a link between two pages `x` and `y` will change the PageRank of these two pages and every page `z` transitively connected to either `x` or `y`.
  Since most graphs exhibit [small-world properties](https://en.wikipedia.org/wiki/Small-world_network), this might represent most of the existing pages.
- In a [naive k-means clustering algorithm](https://en.wikipedia.org/wiki/K-means_clustering), inserting or removing a new data point will affect the positions of the `k` mean points after each iteration.

Depending on the size and update frequency of your input collections, expressing algorithms that violate the "update locality" property (such as the examples above) using recursive CTEs in Materialize might lead to excessive CPU and memory consumption in the clusters that compute these recursive queries.

## Related pages

- [Regular CTEs](/sql/select/#regular-ctes)
- [`SELECT`](/sql/select)
