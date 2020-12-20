---
title: "CREATE TABLE"
description: "`CREATE TABLE` creates a non-streaming, in-memory data source."
menu:
  main:
    parent: 'sql'
---

{{< version-added v0.5.0 >}}

`CREATE TABLE` creates a non-streaming, in-memory data source.

{{< warning >}}
At the moment, tables do not persist any data that is inserted. This means that restarting a
Materialize instance will lose any data that was previously stored in a table.
{{< /warning >}}


## Conceptual framework

Tables store non-streaming data that is inserted via [INSERT](../insert) statements.

### When to create a table

You might want to create a table when...

- Manually inserting rows of data into Materialize.
- Testing Materialize's features without setting up a data stream.

## Syntax

{{< diagram "create-table.svg" >}}

### `col_option`

{{< diagram "col-option.svg" >}}

Field | Use
------|-----
_table&lowbar;name_ | A name for the table.
_col&lowbar;name_ | The name of the column to be created in the table.
_col&lowbar;type_ | The data type of the column indicated by _col&lowbar;name_.
**NOT NULL** | Do not allow the column to contain _NULL_ values. Columns without this constraint can contain _NULL_ values.
*default_expr* | A default value to use for the column in an [`INSERT`](/sql/insert) statement if an explicit value is not provided. If not specified, `NULL` is assumed.

## Details

### Restrictions

Tables do not persist any data that is inserted. This means that restarting a
Materialize instance will lose any data that was previously stored in a table.

Additionally, tables do not currently support:
- Primary keys
- Unique constraints
- Check constraints
- Insert statements that refer to data in other relations, e.g.:
  ```sql
  INSERT INTO t1 SELECT * FROM t2
  ```
- `UPDATE ...` and `DELETE` statements

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

## Related pages

- [`INSERT`](../insert)
- [`DROP TABLE`](../drop-table)
