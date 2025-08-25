---
title: "SELECT"
description: "`SELECT` binds SQL queries to named views or materialized views, and allows to interactively query data maintained in Materialize ."
menu:
  main:
    parent: commands
---

[//]: # "TODO(morsapaes) More than adapting this to the new architecture,
rewrite the page entirely at some point."

The `SELECT` statement is the root of a SQL query, and is used both to bind SQL
queries to named [views](../create-view) or [materialized views](../create-materialized-view),
 and to interactively query data maintained in Materialize. For interactive queries, you should consider creating [indexes](../create-index)
on the underlying relations based on common query patterns.

## Syntax

### select_stmt

{{< diagram "select-stmt.svg" >}}

### simple_select_stmt

{{< diagram "simple-select-stmt.svg" >}}

Field | Use
------|-----
_select&lowbar;with&lowbar;ctes_, _select&lowbar;with&lowbar;recursive&lowbar;ctes_ | [Common table expressions](#common-table-expressions-ctes) (CTEs) for this query.
**(** _col&lowbar;ident_... **)** | Rename the CTE's columns to the list of identifiers, both of which must be the same length.
**ALL** | Return all rows from query _(Default)_.
**DISTINCT** | <a name="select-distinct"></a>Return only distinct values.
**DISTINCT ON (** _col&lowbar;ref_... **)**  | <a name="select-distinct-on"></a>Return only the first row with a distinct value for _col&lowbar;ref_. If an `ORDER BY` clause is also present, then `DISTINCT ON` will respect that ordering when choosing which row to return for each distinct value of `col_ref...`. Please note that in this case, you should start the `ORDER BY` clause with the same `col_ref...` as the `DISTINCT ON` clause. For an example, see [Top K](/transform-data/idiomatic-materialize-sql/top-k/#select-top-1-item).
_target&lowbar;elem_ | Return identified columns or functions.
**FROM** _table&lowbar;expr_ | The tables you want to read from; note that these can also be other `SELECT` statements, [Common Table Expressions](#common-table-expressions-ctes) (CTEs), or [table function calls](/sql/functions/table-functions).
_join&lowbar;expr_ | A join expression; for more details, see the [`JOIN` documentation](/sql/select/join/).
**WHERE** _expression_ | Filter tuples by _expression_.
**GROUP BY** _col&lowbar;ref_ | Group aggregations by _col&lowbar;ref_.
**OPTIONS (** _hint&lowbar;list_ **)** | Specify one or more [query hints](#query-hints).
**HAVING** _expression_ | Filter aggregations by _expression_.
**ORDER BY** _col&lowbar;ref_... | Sort results in either **ASC** or **DESC** order (_default: **ASC**_).<br/><br/>Use the **NULLS FIRST** and **NULLS LAST** options to determine whether nulls appear before or after non-null values in the sort ordering _(default: **NULLS LAST** for **ASC**, **NULLS FIRST** for **DESC**)_.<br/><br>
**LIMIT** _expression_ | Limit the number of returned results to _expression_.
**OFFSET** _integer_ | Skip the first _integer_ number of rows.
**UNION** | Records present in `select_stmt` or `another_select_stmt`.<br/><br/>**DISTINCT** returns only unique rows from these results _(implied default)_.<br/><br/>With **ALL** specified, each record occurs a number of times equal to the sum of the times it occurs in each input statement.
**INTERSECT** | Records present in both `select_stmt` and `another_select_stmt`.<br/><br/>**DISTINCT** returns only unique rows from these results _(implied default)_.<br/><br/>With **ALL** specified, each record occurs a number of times equal to the lesser of the times it occurs in each input statement.
**EXCEPT** | Records present in `select_stmt` but not in `another_select_stmt`.<br/><br/>**DISTINCT** returns only unique rows from these results _(implied default)_.<br/><br/>With **ALL** specified, each record occurs a number of times equal to the times it occurs in `select_stmt` less the times it occurs in `another_select_stmt`, or not at all if the former is greater than latter.

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

### Common table expressions (CTEs)

Common table expressions, also known as CTEs or `WITH` queries, create aliases for statements.

#### Regular CTEs

{{< diagram "with-ctes.svg" >}}

##### cte_binding

{{< diagram "cte-binding.svg" >}}

With _regular CTEs_, any `cte_ident` alias can be referenced in subsequent `cte_binding` definitions and in the final `select_stmt`.
Regular CTEs can enhance legibility of complex queries, but doesn't alter the queries' semantics.
For an example, see [Using regular CTEs](#using-regular-ctes).

#### Recursive CTEs


In addition, Materialize also provides support for _recursive CTEs_ that can mutually reference each other.
Recursive CTEs can be used to define computations on recursively defined structures (such as trees or graphs) implied by your data.
For details and examples, see the [Recursive CTEs](/sql/select/recursive-ctes)
page.

#### Known limitations

CTEs have the following limitations, which we are working to improve:

- `INSERT`/`UPDATE`/`DELETE` (with `RETURNING`) is not supported inside a CTE.
- SQL99-compliant `WITH RECURSIVE` CTEs are not supported (use the [non-standard flavor](/sql/select/recursive-ctes) instead).

### Query hints

Users can specify query hints to help Materialize optimize queries.

The following query hints are valid within the `OPTION` clause.

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

{{< include-md file="shared-content/sql-command-privileges/select.md" >}}

## Related pages

- [`CREATE VIEW`](../create-view)
- [`CREATE MATERIALIZED VIEW`](../create-materialized-view)
- [`SHOW FULL VIEWS`](../show-views)
