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

This is covered in much greater detail in our [architecture
overview](../../overview/architecture), but here's a quick summary of how
Materialize handles `SELECT` in different circumstances.

Scenario | `SELECT` behavior
---------|------------------
**Creating view** | The query description is bound to the name given to it by `CREATE VIEW`.
**Reading from a source or materialized view** | Reads directly from the maintained data.
**Querying non-materialized views** | Creates a dataflow, which is torn down after returning results to the client.

## Syntax

{{< diagram "select-stmt.svg" >}}

Field | Use
------|-----
**WITH** ... **AS** ... | [Common table expressions](#common-table-expressions-ctes) (CTEs) for this query.
**(** _col&lowbar;ident_... **)** | Rename the CTE's columns to the list of identifiers, both of which must be the same length.
**ALL** | Return all rows from query _(Default)_.
**DISTINCT** | Return only distinct values.
**DISTINCT ON (** _col&lowbar;ref_... **)**  | Return only the first row with a distinct value for _col&lowbar;ref_.
_target&lowbar;elem_ | Return identified columns or functions.
**FROM** _table&lowbar;ref_ | The tables you want to read from; note that these can also be other `SELECT` statements or [Common Table Expressions](#common-table-expressions-ctes) (CTEs).
_join&lowbar;expr_ | A join expression; for more details, see the [`JOIN` documentation](../join).
**WHERE** _expression_ | Filter tuples by _expression_.
**GROUP BY** _col&lowbar;ref_ | Group aggregations by _col&lowbar;ref_.
**OPTION (** _hint&lowbar;list_ **)** | Specify one or more [query hints](#query-hints).
**HAVING** _expression_ | Filter aggregations by _expression_.
**ORDER BY** _col&lowbar;ref_... | Sort results in either **ASC** or **DESC** order (_default: **ASC**_).<br/><br/>Use the **NULLS FIRST** and **NULLS LAST** options to determine whether nulls appear before or after non-null values in the sort ordering _(default: **NULLS LAST** for **ASC**, **NULLS FIRST** for **DESC**)_.<br/><br>
**LIMIT** | Limit the number of returned results to _integer_.
**OFFSET** | Skip the first _integer_ number of rows.
**UNION** | Records present in `select_stmt` or `another_select_stmt`.<br/><br/>**DISTINCT** returns only unique rows from these results _(implied default)_.<br/><br/>With **ALL** specified, each record occurs a number of times equal to the sum of the times it occurs in each input statement.
**INTERSECT** | Records present in both `select_stmt` and `another_select_stmt`.<br/><br/>**DISTINCT** returns only unique rows from these results _(implied default)_.<br/><br/>With **ALL** specified, each record occurs a number of times equal to the lesser of the times it occurs in each input statement.
**EXCEPT** | Records present in `select_stmt` but not in `another_select_stmt`.<br/><br/>**DISTINCT** returns only unique rows from these results _(implied default)_.<br/><br/>With **ALL** specified, each record occurs a number of times equal to the times it occurs in `select_stmt` less the times it occurs in `another_select_stmt`, or not at all if the former is greater than latter.

## Details

Because Materialize works very differently from a traditional RDBMS, it's
important to understand the implications that certain features of `SELECT` will
have on Materialize.

### Creating materialized views

Creating a materialized view generates a persistent dataflow, which has a
different performance profile from performing a `SELECT` in an RDBMS.

A materialized view has resource and latency costs that should
be carefully considered depending on its main usage. Materialize must maintain
the results of the query in durable storage, but often it must also maintain
additional intermediate state. To learn more about the costs of
materialization, check out our [architecture overview](../../overview/architecture).

### Reading from indexed relations

Performing a `SELECT` on an **indexed** source, view or materialized view is
Materialize's ideal operation. When Materialize receives such a `SELECT` query,
it quickly returns the maintained results from memory.

Materialize also quickly returns results for queries that only filter, project,
and re-order results.

### Ad hoc queries

Queries over non-materialized views will create an ephemeral dataflow to compute
the results. These dataflows are bound to the active [cluster](/overview/key-concepts#clusters),
 which you can change using:

```sql
SET cluster = <cluster name>;
```

Performing a `SELECT` query that does not directly read out of a dataflow
requires Materialize to evaluate your query. Materialize will construct a
temporary dataflow to materialize your query, and remove the dataflow as soon as
it returns the query results to you.

### Common table expressions (CTEs)

Common table expressions, also known as CTEs or `WITH` queries, create aliases
for statements that subsequent expressions can refer to (including subsequent
CTEs). This can enhance legibility of complex queries, but doesn't alter the
queries' semantics.

For an example, see [Using CTEs](#using-ctes).

#### Known limitations

CTEs have the following limitations, which we are working to improve:

- CTEs only support `SELECT` queries. {{% gh 4867 %}}
- Materialize inlines the CTE where it's referenced, which could cause
  unexpected performance characteristics for especially complex expressions. {{%
  gh 4867 %}}
- `WITH RECURSIVE` CTEs are not available yet. {{% gh 2516 %}}

### Query hints

Users can specify any query hints to help Materialize optimize
query planning more efficiently.

The following query hints are valid within the `OPTION` clause.

Hint | Value type | Description
------|------------|------------
`expected_group_size` | `int` | How many rows will have the same group key. Materialize can render `min` and `max` expressions more efficiently with this information.

For an example, see [Using query hints](#using-query-hints).

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

### Creating a view

This assumes you've already [created a source](../create-source).

The following query creates a materialized view representing the total of all
purchases made by users per region.

``` sql
CREATE MATERIALIZED VIEW mat_view AS
    SELECT region.id, sum(purchase.total)
    FROM mysql_simple_purchase AS purchase
    JOIN mysql_simple_user AS user ON purchase.user_id = user.id
    JOIN mysql_simple_region AS region ON user.region_id = region.id
    GROUP BY region.id;
```

In this case, Materialized will create a dataflow to maintain the results of
this query, and that dataflow will live on until the view it's maintaining is
dropped.

### Reading from a view

Assuming you create the view listed above, named `mat_view`:

```sql
SELECT * FROM mat_view
```

In this case, Materialized simply returns the results of the dataflow you
created to maintain the view.

### Querying views

```sql
SELECT region.id, sum(purchase.total)
FROM mysql_simple_purchase AS purchase
JOIN mysql_simple_user AS user ON purchase.user_id = user.id
JOIN mysql_simple_region AS region ON user.region_id = region.id
GROUP BY region.id;
```

In this case, Materialized will spin up the same dataflow as it did for creating
a materialized view, but it will tear down the dataflow once it's returned its
results to the client. If you regularly want to view the results of this query,
you may want to [create a view](../create-view) for it.

### Using CTEs

```sql
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

With regard to dataflows, this is similar to [Querying views](#querying-views)
above: Materialize tears down the created dataflow after returning the results.

### Using query hints

```sql
SELECT a,
       min(b) AS min
FROM example
GROUP BY a
OPTION (expected_group_size = 100)
```

Here the hint indicates that there may be up to a hundred distinct `(a, b)` pairs
for each `a` value, and Materialize can optimize its dataflow rendering with that
knowledge.

## Related pages

- [`CREATE VIEW`](../create-view)
- [`CREATE MATERIALIZED VIEW`](../create-materialized-view)
- [`SHOW FULL VIEWS`](../show-views)
