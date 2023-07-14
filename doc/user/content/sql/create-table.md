---
title: "CREATE TABLE"
description: "`CREATE TABLE` creates a table that is persisted in durable storage."
pagerank: 40
menu:
  # This should also have a "non-content entry" under Reference, which is
  # configured in doc/user/config.toml
  main:
    parent: 'commands'
---

`CREATE TABLE` defines a table that is persisted in durable storage and can be
written to, updated and seamlessly joined with other tables, views or sources.

Tables in Materialize are similar to tables in standard relational databases:
they consist of rows and columns where the columns are fixed when the table is
created but rows can be added to at will via [`INSERT`](../insert) statements.

{{< warning >}}
At the moment, tables have many [known limitations](#known-limitations). In most
situations, you should use [sources](/sql/create-source) instead.
{{< /warning >}}

[//]: # "TODO(morsapaes) Bring back When to use a table? once there's more
clarity around best practices."

## Syntax

{{< diagram "create-table.svg" >}}

### `col_option`

{{< diagram "col-option.svg" >}}

Field | Use
------|-----
**TEMP** / **TEMPORARY** | Mark the table as [temporary](#temporary-tables).
_table&lowbar;name_ | A name for the table.
_col&lowbar;name_ | The name of the column to be created in the table.
_col&lowbar;type_ | The data type of the column indicated by _col&lowbar;name_.
**NOT NULL** | Do not allow the column to contain _NULL_ values. Columns without this constraint can contain _NULL_ values.
*default_expr* | A default value to use for the column in an [`INSERT`](/sql/insert) statement if an explicit value is not provided. If not specified, `NULL` is assumed.

## Details

### Known limitations

Tables do not currently support:

- Primary keys
- Unique constraints
- Check constraints

See also the known limitations for [`INSERT`](../insert#known-limitations),
[`UPDATE`](../update#known-limitations), and [`DELETE`](../delete#known-limitations).

### Temporary tables

The `TEMP`/`TEMPORARY` keyword creates a temporary table. Temporary tables are
automatically dropped at the end of the SQL session and are not visible to other
connections. They are always created in the special `mz_temp` schema.

Temporary tables may depend upon other temporary database objects, but non-temporary
tables may not depend on temporary objects.

## Examples

### Creating a table

You can create a table `t` with the following statement:

```sql
CREATE TABLE t (a int, b text NOT NULL);
```

Once a table is created, you can inspect the table with various `SHOW` commands.

```sql
SHOW TABLES;
TABLES
------
t

SHOW COLUMNS IN t;
name       nullable  type
-------------------------
a          true      int4
b          false     text
```

## Privileges

{{< private-preview />}}

The privileges required to execute this statement are:

- `CREATE` privileges on the containing schema.
- `USAGE` privileges on all types used in the table definition.

## Related pages

- [`INSERT`](../insert)
- [`DROP TABLE`](../drop-table)
