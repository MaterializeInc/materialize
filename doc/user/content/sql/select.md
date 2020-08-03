---
title: "SELECT"
description: "`SELECT` reads from or queries your materialized views."
menu:
  main:
    parent: 'sql'
---

`SELECT` is used in a few ways within Materialize. You can use it to:

- Query materialized views and materialized sources, e.g. `SELECT * FROM some_view;`
- Describe a view you want to materialize. e.g. `CREATE MATERIALIZED VIEW some_view AS SELECT...`
- Describe a view without materializing it, e.g. `CREATE VIEW some_nonmaterialized_view AS...`

To better understand the distinction between these uses, you should check out
our [architecture overview](../../overview/architecture).

## Conceptual framework

The `SELECT` statement is the root of a SQL query, and is used both to interactively query data Materialize manages, and to bind SQL queries to named views which can be queried later.

When used to interactively query data, your query must depend only on materialized sources and views. There are exceptions, discussed in more detail below. When used to construct views, this restriction is lifted.

This is covered in much greater detail in our [architecture
overview](../../overview/architecture), but here's a quick summary of how
Materialize handles `SELECT` in different circumstances.

Scenario | `SELECT` behavior
---------|------------------
**Creating view** | The query description is bound to the name given to it by `CREATE VIEW`.
**Reading from a materialized source or view** | Reads directly from the maintained data.
**Querying materialized sources and views** | Constructs a dataflow, which is torn down after returning results to the client.

## Syntax

{{< diagram "select-stmt.svg" >}}

Field | Use
------|-----
**ALL** | Return all rows from query _(implied default)_.
**DISTINCT** | Return only distinct values from query.
**DISTINCT ON (** _col&lowbar;ref..._ **)**  | Return only the first row with a distinct value for _col&lowbar;ref_.
_target&lowbar;elem_ | Return identified columns or functions.
**FROM** _table&lowbar;ref_ | The tables you want to read from; note that these can also be other `SELECT` statements.
_join&lowbar;expr_ | A join expression; for more details, see our [`JOIN` documentation](../join).
**WHERE** _expression_ | Filter tuples by _expression_.
**GROUP BY** _col&lowbar;ref_ | Group aggregations by _col&lowbar;ref_.
**HAVING** _expression_ | Filter aggregations by _expression_.
**ORDER BY** _col&lowbar;ref_ ... | Order results in either **ASC** or **DESC** order (_**ASC** is implied default_).<br/><br>
**LIMIT** | Limit the number of returned results to _expr_.
**OFFSET** | Skip the first _expr_ number of rows.
**UNION** | Records present in `select_stmt` or `another_select_stmt`.<br/><br/>**DISTINCT** returns only unique rows from these results _(implied default)_.<br/><br/>With **ALL** specified, each record occurs a number of times equal to the sum of the times it occurs in each input statement.
**INTERSECT** | Records present in both `select_stmt` and `another_select_stmt`.<br/><br/>**DISTINCT** returns only unique rows from these results _(implied default)_.<br/><br/>With **ALL** specified, each record occurs a number of times equal to the lesser of the times it occurs in each input statement.
**EXCEPT** | Records present in `select_stmt` but not in `another_select_stmt`.<br/><br/>**DISTINCT** returns only unique rows from these results _(implied default)_.<br/><br/>With **ALL** specified, each record occurs a number of times equal to the times it occurs in `select_stmt` less the times it occurs in `another_select_stmt`, or not at all if the former is greater than latter.
**AS OF** | If provided, `SELECT` will report the results at the supplied timestamp, meaning it reflects exactly those input updates at or before this timestamp.
## Details

Because Materialize works very differently from a traditional RDBMS, it's important to understand the implications that certain features of `SELECT` will impact your Materialize instances.

### Creating materialized views

Creating a materialized view generates a persistent dataflow, which has a different performance profile from performing a `SELECT` in an RDBMS.

A materialized view costs Materialize both CPU and RAM to maintain. Materialize must maintain the results of the query, but often it must also maintain additional intermediate state. To learn more about the costs of materialization, check out our [architecture overview](../../overview/architecture).

### Reading from sources and views

Performing a `SELECT * FROM` a materialized source or materialized view is Materialize's ideal operation. When Materialize receives such a `SELECT` query, it quickly returns the maintained results from memory, with no re-execution.

Materialize also quickly returns results for queries that only filter, project, and re-order results of materialized sources or materialized views.

### Querying sources and views

Performing a `SELECT` query that does not directly read out of a materialization requires Materialize to evaluate your query. Materialize will construct a temporary dataflow to materialize your query, and remove the dataflow as soon as it returns the query results to you.

When you perform a `SELECT` query, all of your inputs must be *queryable*. An input is queryable if it is a constant collection, materialized, or it depends only on queryable inputs itself. The main reason an input might not be queryable is it if relies on an unmaterialized source for its definition. You can use the [`SHOW FULL VIEWS`](../show-views) command to report which sources and views are queryable.

You can create a materialized view out of any query using [`CREATE MATERIALIZED VIEW`](../create-materialized-view), and it will always then be queryable.

If you supply an `AS OF <time>` argument to your `SELECT` query the queryable requirement is lifted.

## Examples

### Creating a view

This assumes you've already [created a source](../create-source).

The following query creates a materialized view representing the total of all purchases made by users per region.

``` sql
CREATE MATERIALIZED VIEW mat_view AS
    SELECT region.id, sum(purchase.total)
    FROM mysql_simple_purchase AS purchase
    JOIN mysql_simple_user AS user ON purchase.user_id = user.id
    JOIN mysql_simple_region AS region ON user.region_id = region.id
    GROUP BY region.id;
```

In this case, Materialized will create a dataflow to maintain the results of this query, and that dataflow will live on until the view it's maintaining is dropped.

### Reading from a view

Assuming you create the view listed above, named `mat_view`:

```sql
SELECT * FROM mat_view
```

In this case, Materialized simply returns the results of the dataflow you created to maintain the view.

### Querying views

```sql
SELECT region.id, sum(purchase.total)
FROM mysql_simple_purchase AS purchase
JOIN mysql_simple_user AS user ON purchase.user_id = user.id
JOIN mysql_simple_region AS region ON user.region_id = region.id
GROUP BY region.id;
```

In this case, Materialized will spin up the same dataflow as it did for creating a materialized view, but it will tear down the dataflow once it's returned its results to the client. If you regularly want to view the results of this query, you may want to [create a view](../create-view) for it.

## Related pages

- [`CREATE VIEW`](../create-view)
- [`CREATE MATERIALIZED VIEW`](../create-materialized-view)
- [`SHOW FULL VIEWS`](../show-views)