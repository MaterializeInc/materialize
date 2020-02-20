---
title: "SELECT"
description: "`SELECT` reads data from your Kafka streams or materialized views."
menu:
  main:
    parent: 'sql'
---

`SELECT` is used in a few ways within Materialize, letting you:

- Express a view you want to materialize. e.g. `CREATE MATERIALIZED VIEW some_view AS
  SELECT...`
- Read from materialized views or sources, e.g. `SELECT * FROM some_view;`
- Describe a view without materializing it, e.g. `CREATE VIEW some_nonmaterialized_view AS...`

To better understand the distinction between these uses, you should check out
our [architecture overview](../../overview/architecture).

## Conceptual framework

To perform reads, Materialize simply returns the result set of a dataflow.
(Naturally, if the dataflow doesn't exist, it must be created.)

This is covered in much greater detail in our [architecture
overview](../../overview/architecture), but here's a quick summary of how
Materialize handles `SELECT` in different circumstances.

Scenario | `SELECT` dataflow behavior
---------|------------------
**Creating view** | The created dataflow is persisted using the name given to it by `CREATE VIEW`.
**Reading from view** | Returns the view's dataflow's current result set.
**Reading from source** | Generates a dataflow, which is torn down after returning results to client.

## Syntax

{{< diagram "select_stmt.html" >}}

Field | Use
------|-----
**ALL** | Return all rows from query _(implied default)_.
**DISTINCT** | Return only distinct values from query.
**DISTINCT ON (** _col&lowbar;ref..._ **)**  | Return only the first row with a distinct value for _col&lowbar;ref_.
_target&lowbar;elem_ | Return identified columns or functions.
**FROM** _table&lowbar;ref_ | The table you want to read from; note that this can also be other `SELECT` statements.
_join&lowbar;expr_ | A join expression; for more details, see our [`JOIN` documentation](../join).
**WHERE** _expression_ | Filter tuples by _expression_.
**GROUP BY** _col&lowbar;ref_ | Group aggregations by _col&lowbar;ref_.
**HAVING** _expression_ | Filter aggregations by _expression_.
**ORDER BY** _col&lowbar;ref_ ... | Order results in either **ASC** or **DESC** order (_**ASC** is implied default_).<br/><br>**ORDER BY** does not work when creating views, but does work when reading from views.
**LIMIT** | Limit the number of returned results to _expr_.
**OFFSET** | Skip the first _expr_ number of rows.
**UNION** | Records present in `select_stmt` or `another_select_stmt`.<br/><br/>**DISTINCT** returns only unique rows from these results _(implied default)_.<br/><br/>With **ALL** specified, each record occurs a number of times equal to the sum of the times it occurs in each input statement.
**INTERSECT** | Records present in both `select_stmt` and `another_select_stmt`.<br/><br/>**DISTINCT** returns only unique rows from these results _(implied default)_.<br/><br/>With **ALL** specified, each record occurs a number of times equal to the lesser of the times it occurs in each input statement.
**EXCEPT** | Records present in `select_stmt` but not in `another_select_stmt`.<br/><br/>**DISTINCT** returns only unique rows from these results _(implied default)_.<br/><br/>With **ALL** specified, each record occurs a number of times equal to the times it occurs in `select_stmt` less the times it occurs in `another_select_stmt`, or not at all if the former is greater than latter.

## Details

Because Materialize works very differently from a traditional RDBMS, it's important to understand the implications that certain features of `SELECT` will impact your Materialize instances.

### Creating materialized views

Creating materialized views generates a persistent dataflow, which has a different performance profile from performing a `SELECT` in an RDBMS.

When creating views, using the following features of `SELECT` have certain side effects:

Field | Impact
------|-------
**DISTINCT** | Materialize must keep a copy of every unique view it has ever seen for the column, which grows memory usage linearly with the number of distinct values in the column.
**ORDER BY** | Materialize does not support creating views with an **ORDER BY** clause. Instead, you can order the results when reading from a view.

### Reading from views

Performing a `SELECT` on an existing view is Materialize's ideal operation. When it receives the `SELECT` targeting a view, it returns the view's underlying dataflow's result set from memory.

### Reading from sources

While this is covered more thoroughly in our [architecture overview](../../overview/architecture), it's important to understand what Materialize does to ensure its behavior matches your expectations.

Whenever Materialize reads directly from a source, it must create a dataflow, and that dataflow is torn down as soon as Materialize reads the results and returns to the client.

This means if you repeatedly send the same `SELECT` statement to Materialize which required reading from a source, it must calculate the results for the query every time it's received, i.e. it cannot incrementally maintain the results of the query in a view. To make these kinds of statements more efficient, you should instead [create a view](../create-view).

## Examples

### Creating a view

This assumes you've already [created a source](../create-source).

The following query creates a materialized view representing the total of all purchases made by users per region.

``` sql
CREATE VIEW mat_view AS
    SELECT region.id, sum(purchase.total)
    FROM mysql_simple_purchase AS purchase
    JOIN mysql_simple_user AS user
        ON purchase.user_id = user.id
    JOIN mysql_simple_region AS region
        ON user.region_id = region.id
    GROUP BY region.id;
```

In this case, Materialized will create a dataflow to maintain the results of this query, and that dataflow will live on until the view it's maintaining is dropped.

### Reading from a view

Assuming you create the view listed above, named `mat_view`:

```sql
SELECT * FROM mat_view
```

In this case, Materialized simply returns the results of the dataflow you created to maintain the view.

### Reading from sources

```sql
SELECT region.id, sum(purchase.total)
FROM mysql_simple_purchase AS purchase
JOIN mysql_simple_user AS user
    ON purchase.user_id = user.id
JOIN mysql_simple_region AS region
    ON user.region_id = region.id
GROUP BY region.id;
```

In this case, Materialized will spin up the same dataflow as it did for creating a view, but it will tear down the dataflow once it's returned its results to the client. If you regularly wanted to view the results of this query, you would want to [create a view](../create-view) for it.

## Related pages

- [`CREATE VIEW`](../create-view)
- [`CREATE SOURCE`](../create-source)
