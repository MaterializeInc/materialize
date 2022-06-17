---
title: "CREATE MATERIALIZED VIEW"
description: "`CREATE MATERIALIZED VIEW` creates a materialized view, which Materialize will incrementally maintain as updates occur to the underlying data."
menu:
  main:
    parent: 'commands'
---

`CREATE MATERIALIZED VIEW` creates a materialized view, which lets you retrieve
incrementally updated results of a `SELECT` query very quickly. Despite the
simplicity of creating a materialized view, it's Materialize's most powerful
feature.

## Conceptual framework

`CREATE MATERIALIZED VIEW` computes and maintains the results of a `SELECT`
query in memory. For more information, see [Key Concepts: Materialized views](/overview/key-concepts/#materialized-views).

## Syntax

{{< diagram "create-materialized-view.svg" >}}

Field | Use
------|-----
**TEMP** / **TEMPORARY** | Mark the materialized view as [temporary](#temporary-materialized-views).
**OR REPLACE** | If a view exists with the same name, replace it with the view defined in this statement. You cannot replace views that other views depend on, nor can you replace a non-view object with a view.
**IF NOT EXISTS** | If specified, _do not_ generate an error if a view of the same name already exists. <br/><br/>If _not_ specified, throw an error if a view of the same name already exists. _(Default)_
_view&lowbar;name_ | A name for the view.
**(** _col_ident_... **)** | Rename the `SELECT` statement's columns to the list of identifiers, both of which must be the same length. Note that this is required for statements that return multiple columns with the same identifier.
_select&lowbar;stmt_ | The [`SELECT` statement](../select) whose output you want to materialize and maintain.

## Details

### Memory

Views are maintained in memory. Because of this, one must be sure that all
intermediate stages of the query, as well as its result set can fit in the
memory of a single machine, while also understanding the rate at which the
query's result set will grow.

For more detail about how different clauses impact memory usage, check out our
[`SELECT`](../select) documentation.

### Indexes

A brief mention on indexes: Materialize automatically creates an in-memory index
which stores all columns in the `SELECT` query's result set; this is the crucial
structure that the view maintains to provide low-latency access to your query's
results.

Some things you might want to do with indexes...

- View the details of a view's indexes through [`SHOW INDEX`](../show-index).
- If you find that your queries would benefit from other indexes, e.g. you want
  to join two relations on some foreign key, you can [create
  indexes](../create-index).

### Temporary materialized views

The `TEMP`/`TEMPORARY` keyword creates a temporary materialized view. Temporary
materialized views are automatically dropped at the end of the SQL session and
are not visible to other connections. They are always created in the special `mz_temp`
schema.

Temporary materialized views may depend upon other temporary database objects,
but non-temporary materialized views may not depend on temporary objects.

## Examples

```sql
CREATE MATERIALIZED VIEW purchase_sum_by_region
AS
    SELECT sum(purchase.amount) AS region_sum, region.id AS region_id
    FROM mysql_simple_region AS region
    INNER JOIN mysql_simple_user AS user ON region.id = user.region_id
    INNER JOIN mysql_simple_purchase AS purchase ON purchase.user_id = user.id
    GROUP BY region.id;
```

In this example, as new users or purchases come in, the results of the view are
incrementally updated. For example, if a new purchase comes in for a specific
user, the underlying dataflow will determine which region that user belongs to,
and then increment the `region_sum` field with those results.

## Related pages

- [`SELECT`](../select)
- [`CREATE SOURCE`](../create-source)
