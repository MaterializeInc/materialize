---
title: "CREATE VIEWS"
description: "`CREATE VIEWS` creates a view for each table included in the replication stream of a Postgres source."
menu:
  main:
    parent: 'sql'
---

`CREATE VIEWS` creates a view for each table included in the replication stream of a [Postgres source](/sql/create-source/postgres/). It is distinct from both the more general [`CREATE VIEW`](/sql/create-view/) command, which provides an alias for `SELECT` statements, and from [materialized views](/sql/create-materialized-view).

## Syntax

{{< diagram "create-views.svg" >}}

Field | Use
------|-----
**TEMP** / **TEMPORARY** | Mark the view as [temporary](#temporary-views).
**IF NOT EXISTS** | If specified, _do not_ generate an error if a view of the same name already exists. <br/><br/>If _not_ specified, throw an error if a view of the same name already exists. _(Default)_
_src_name_ | The name of the [Postgres source](/sql/create-source/postgres) for which you are creating table views.

## Details

### Temporary views

The `TEMP`/`TEMPORARY` keyword creates a temporary view. Temporary views are
automatically dropped at the end of the SQL session and are not visible to other
connections. They are always created in the special `mz_temp` schema.

Temporary views may depend upon other temporary database objects, but non-temporary
views may not depend on temporary objects.

## Examples

### Creating views for all tables included in the publication

```sql
CREATE VIEWS FROM SOURCE "mz_source";
SHOW FULL VIEWS;
```

### Creating views for specific tables included in the publication

This command creates a view only from `sample_table` and renames it `renamed_sample_table`.

```sql
CREATE VIEWS FROM SOURCE "mz_source" ("sample_table" AS "renamed_sample_table");
SHOW FULL VIEWS;
```

## Related pages

- [`CREATE SOURCE FROM POSTGRES`](/sql/create-source/postgres/)
