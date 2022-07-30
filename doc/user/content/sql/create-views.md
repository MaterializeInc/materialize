---
title: "CREATE VIEWS"
description: "`CREATE VIEWS` creates views for the distinct components of a multiplexed stream."
menu:
  main:
    parent: 'commands'
---

`CREATE VIEWS` creates views for the distinct components of a multiplexed stream. For example, it separates the replication stream of a [Postgres source](/sql/create-source/postgres/) out into views that reproduce the upstream tables that populate the stream.

`CREATE VIEWS` is distinct from both the more general [`CREATE VIEW`](/sql/create-view/) command, which provides an alias for `SELECT` statements, and from [materialized views](/sql/create-materialized-view).

## Syntax

{{< diagram "create-views.svg" >}}

Field | Use
------|-----
**TEMP** / **TEMPORARY** | Mark the view as [temporary](#temporary-views).  |
**IF NOT EXISTS** | If specified, _do not_ generate an error if a view of the same name already exists. <br/><br/>If _not_ specified, throw an error if a view of the same name already exists. _(Default)_
_src_name_ | The name of the [Postgres source](/sql/create-source/postgres) for which you are creating table views.
_upstream_table_  | Optional. The names of the upstream table for which to create a view. You can include multiple tables as a comma-separated list. If unspecified, views will be created for all tables in the publication.
_new_view_name_  | Optional. The name for the new view. If unspecified, the name of the upstream table will be used.

## Details

### Postgres schemas

For Postgres sources, `CREATE VIEWS` will attempt to create each upstream table in the same schema as Postgres. If the publication contains tables` "public"."foo"` and `"otherschema"."foo"`, `CREATE VIEWS` is the equivalent of

```
CREATE VIEW "public"."foo";
CREATE VIEW "otherschema"."foo";
```

Therefore, in order for `CREATE VIEWS` to succeed, all upstream schemas included in the publication must exist in Materialize as well, or you must explicitly specify the downstream schemas and rename the resulting tables.

For example:

```sql
CREATE VIEWS FROM "mz_source"
("public"."foo" AS "foo", "otherschema"."foo" AS "foo2");
```

### Temporary views

The `TEMP`/`TEMPORARY` keyword creates temporary views. Temporary views are
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

### Creating views for specific tables

This command creates views from `table1` and `table2`.

```sql
CREATE VIEWS FROM SOURCE "mz_source" ("table1", "table2");
SHOW FULL VIEWS;
```
### Creating views and renaming tables

```sql
CREATE VIEWS FROM "mz_source"
("public"."table1" AS "table1", "otherschema"."table1" AS "table2");
```

## Related pages

- [`CREATE SOURCE FROM POSTGRES`](/sql/create-source/postgres/)
