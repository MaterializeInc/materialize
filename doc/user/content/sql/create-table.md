---
title: "CREATE TABLE"
description: "`CREATE TABLE` creates a non-streaming, in-memory data source."
menu:
  main:
    parent: 'sql'
---

`CREATE TABLE` creates a non-streaming, in-memory data source.

## Conceptual framework

Tables store non-streaming data that is inserted via [INSERT](insert.md) statements.

### When to create a table

You might want to create a table when...

- You want to manually insert rows of data into Materialize.
- You want to test Materialize's features without setting up a data stream.

## Syntax

{{< diagram "create-table.svg" >}}

Field | Use
------|-----
_table&lowbar;name_ | A name for the table.
_col&lowbar;name_ | The name of the column to be created in the table.
_col&lowbar;type_ | The data type of the column indicated by _col_name_.
_NOT NULL_ | Provide a `NOT NULL` constraint if a column is not nullable. All columns without this constraint will be nullable.

## Details

### Restrictions

Tables will not persist any data that is inserted. This means that restarting a
Materialize instance will lose any data that was previously stored in a table.

Additionally, tables do not currently support:
    - Primary keys
    - Unique constraints
    - Check constraints
    - Default column values
    - Insert statements that use anything other than a `VALUES` clause, such as
      `INSERT INTO ... SELECT`
    - `UPDATE ...` and `DELETE` statements

## Examples

### Creating a table

We can create a table `t` with the following statement:

```sql
CREATE TABLE t (a int, b text NOT NULL);
```

Once a table is created, we can inspect the table with various `SHOW` commands.

```sql
SHOW TABLES;
TABLES
------
t

SHOW COLUMNS IN t;
Field      Nullable  Type
-------------------------
a          YES       int4
b          NO        text
```

## Related pages

- [`INSERT`](insert.md)
- [`DROP TABLE`](drop-table.md)
