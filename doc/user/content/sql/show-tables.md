---
title: "SHOW TABLES"
description: "`SHOW TABLES` returns a list of all tables available to your Materialize instances."
menu:
  main:
    parent: commands
---

`SHOW TABLES` returns a list of all tables available to your Materialize instances.

## Syntax

{{< diagram "show-tables.svg" >}}

Field | Use
------|-----
_schema&lowbar;name_ | The schema to show tables from. Defaults to `public` in the current database. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).
**EXTENDED** | Returns system tables as well as user-created tables. By default, only user-created tables are returned.
**FULL**  | Returns a column that lists table type (`system`, `user`, or `temp`).

## Details

### Output format

`SHOW TABLES`'s output is a table with one column, `name`.

## Examples

### Show user-created tables
```sql
SHOW TABLES;
```
```nofmt
my_table
my_other_table
```

### Show tables from specified schema
```sql
SHOW SCHEMAS;
```
```nofmt
public
```
```sql
SHOW TABLES FROM public;
```
```nofmt
my_table
my_other_table
```

### Show all tables and include table type

`EXTENDED` lists system tables as well as user-created tables; `FULL` adds the `type` column that specifies whether the table was created by a user or a temp process or is a system table.

```sql
SHOW EXTENDED FULL TABLES;
```
```nofmt
name                  |  type
----------------------+--------
 mz_array_types       | system
 mz_base_types        | system
 ...
 my_table             | user
 my_other_table       | user
(23 rows)
```

## Related pages

- [`SHOW CREATE TABLE`](../show-create-table)
- [`CREATE TABLE`](../create-table)
