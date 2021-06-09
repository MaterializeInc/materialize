---
title: "CREATE VIEWS"
description: "`CREATE VIEWS` creates a view for each table included in the replication stream of a Postgres source."
menu:
  main:
    parent: 'sql'
---

`CREATE VIEWS` creates a view for each table included in the replication stream of a [Postgres source](/sql/create-source/postgres/). It is distinct from both the more general [`CREATE VIEW`](/sql/create-view/) command, which provides an alias for `SELECT` statements, and from [materialized views](/sql/create-materialized-view).

## Conceptual framework


## Syntax

{{< diagram "create-views.svg" >}}

Field | Use
------|-----
**TEMP** / **TEMPORARY** | Mark the view as [temporary](#temporary-views).
**OR REPLACE** | If a view exists with the same name, replace it with the view defined in this statement. You cannot replace views that other views or sinks depend on, nor can you replace a non-view object with a view.
**IF NOT EXISTS** | If specified, _do not_ generate an error if a view of the same name already exists. <br/><br/>If _not_ specified, throw an error if a view of the same name already exists. _(Default)_
_view&lowbar;name_ | A name for the view.
_select&lowbar;stmt_ | The [`SELECT` statement](../select) whose output you want to materialize and maintain.

## Details


### Memory



### Temporary views

The `TEMP`/`TEMPORARY` keyword creates a temporary view. Temporary views are
automatically dropped at the end of the SQL session and are not visible to other
connections. They are always created in the special `mz_temp` schema.

Temporary views may depend upon other temporary database objects, but non-temporary
views may not depend on temporary objects.

## Examples

```sql

```



## Related pages

- [`CREATE MATERIALIZED VIEW`](../create-materialized-view)
