---
title: "CREATE VIEW"
description: "`CREATE VIEW` creates an alias for a `SELECT` statement."
menu:
  main:
    parent: 'sql'
---

`CREATE VIEW` creates a _non-materialized_ view, which only provides an alias
for the `SELECT` statement it includes.

Note that this is very different from Materialize's main type of view,
materialized views, which you can create with [`CREATE MATERIALIZED
VIEW`](../create-materialized-view).

## Conceptual framework

`CREATE VIEW` simply stores the verbatim `SELECT` query, and provides a
shorthand for performing the query.

Note that you can only `SELECT` from non-materialized views if all of the views
in their `FROM` clauses are themselves materialized or have transitive access to
materialized views. For more information on this restriction, see [Selecting
from non-materialized views](#selecting-from-non-materialized-views).

If you plan on repeatedly reading from a view, we recommend using [materialized
views](../create-materialized-view) instead.

## Syntax

{{< diagram "create-view.html" >}}

Field | Use
------|-----
**OR REPLACE** | If a view exists with the same name, replace it with the view defined in this statement. You cannot replace views that other views or sinks depend on, nor can you replace a non-view object with a view.
_view&lowbar;name_ | A name for the view.
_select&lowbar;stmt_ | The [`SELECT` statement](../select) whose output you want to materialize and maintain.

## Details

### Querying non-materialized views

You can only directly `SELECT` from a non-materialized view if all of the
sources it depends on (i.e. views and sources in its `FROM` clause) have access
to materialized data (i.e. indexes). That is to say that all of a
non-materialized view's data must exist somewhere in memory for it to process
`SELECT` statements. For those inclined toward mathematics, it's possible to
think of this as "transitive materialization."

If non-materialized views can process `SELECT` statements, we call them
"queryable."

However, this limitation does not apply to creating materialized views.
Materialized view definitions can `SELECT` from non-materialized view,
irrespective of the non-materialized view's dependencies. This is done by
essentially inlining the definition of the non-materialized view into the
materialized view's definition.

The diagram below demonstrates this restriction using a number of views
(`a`-`h`) with a complex set of interdependencies.

{{<
    figure src="/images/transitive-materialization.png"
    alt="transitive materialization diagram"
    width="500"
>}}

A few things to note from this example:

- **c** can be materialized despite the dependency on a non-materialized view.
- If **g** were materialized, all views could process `SELECT`.

### Memory

Non-materialized views do not store the results of the query. Instead, they
simply store the verbatim of the included `SELECT`. This means they take up very
little memory, but also provide very little benefit in terms of reducing the
latency and computation needed to answer queries.

### Converting to materialized view

You can convert a non-materialized view into a materialized view by [adding an
index](../create-index/#converting-views-to-materialized-views).

## Examples

```sql
CREATE VIEW purchase_sum_by_region
AS
    SELECT sum(purchase.amount) AS region_sum,
           region.id AS region_id
    FROM region
    INNER JOIN user
        ON region.id = user.region_id
    INNER JOIN purchase
        ON purchase.user_id = user.id
    GROUP BY region.id;
```

This view is useful only in as much as it is easier to type
`purchase_sum_by_region` than the entire `SELECT` statement.

However, it's important to note that you could only `SELECT` from this view:

- In the definition of [`CREATE MATERIALIZED VIEW`](../create-materialized-view) or [`CREATE VIEW`](../create-view) statements.
- If `region`, `user`, and `purchase` had access to materialized data (i.e.
  indexes) directly or transitively.

## Related pages

- [`CREATE MATERIALIZED VIEW`](../create-materialized-view)
