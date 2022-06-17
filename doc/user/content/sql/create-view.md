---
title: "CREATE VIEW"
description: "`CREATE VIEW` creates an alias for a `SELECT` statement."
menu:
  # This should also have a "non-content entry" under Reference, which is
  # configured in doc/user/config.toml
  main:
    parent: 'commands'
---

`CREATE VIEW` creates a _non-materialized_ view, which only provides an alias
for the `SELECT` statement it includes.

Note that this is very different from Materialize's main type of view,
materialized views, which you can create with [`CREATE MATERIALIZED
VIEW`](../create-materialized-view).

## Conceptual framework

`CREATE VIEW` simply stores the verbatim `SELECT` query, and provides a
shorthand for performing the query. For more information, see [Key Concepts: Non-materialized views](/overview/key-concepts/#non-materialized-views).

## Syntax

{{< diagram "create-view.svg" >}}

Field | Use
------|-----
**TEMP** / **TEMPORARY** | Mark the view as [temporary](#temporary-views).
**OR REPLACE** | If a view exists with the same name, replace it with the view defined in this statement. You cannot replace views that other views depend on, nor can you replace a non-view object with a view.
**IF NOT EXISTS** | If specified, _do not_ generate an error if a view of the same name already exists. <br/><br/>If _not_ specified, throw an error if a view of the same name already exists. _(Default)_
_view&lowbar;name_ | A name for the view.
**(** _col_ident_... **)** | Rename the `SELECT` statement's columns to the list of identifiers, both of which must be the same length. Note that this is required for statements that return multiple columns with the same identifier.
_select&lowbar;stmt_ | The [`SELECT` statement](../select) whose output you want to materialize and maintain.

## Details

### Querying non-materialized views

You can only directly `SELECT` from a non-materialized view if all of the
objects it depends on (i.e. views and sources in its `FROM` clause) have access
to materialized data (i.e. indexes or constants). That is to say that all of a
non-materialized view's data must exist somewhere in memory for it to process
`SELECT` statements. For those inclined toward mathematics, it's possible to
think of this as "transitive materialization."

If views can process `SELECT` statements, we call them "queryable."

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
- If **g** were materialized, all views would be queryable.

### Memory

Non-materialized views do not store the results of the query. Instead, they
simply store the verbatim of the included `SELECT`. This means they take up very
little memory, but also provide very little benefit in terms of reducing the
latency and computation needed to answer queries.

### Converting to materialized view

You can convert a non-materialized view into a materialized view by [adding an
index](../create-index/#materializing-views).

### Temporary views

The `TEMP`/`TEMPORARY` keyword creates a temporary view. Temporary views are
automatically dropped at the end of the SQL session and are not visible to other
connections. They are always created in the special `mz_temp` schema.

Temporary views may depend upon other temporary database objects, but non-temporary
views may not depend on temporary objects.

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
- [`CREATE INDEX`](../create-index)
