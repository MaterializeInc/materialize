---
title: "SHOW TABLES"
description: "`SHOW TABLES` returns a list of all tables available in Materialize."
menu:
  main:
    parent: commands
---

`SHOW TABLES` returns a list of all tables available in Materialize.

## Syntax

{{< diagram "show-tables.svg" >}}

Field | Use
------|-----
_schema&lowbar;name_ | The schema to show tables from. Defaults to first resolvable schema in the search path. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).

## Details

### Output format

`SHOW TABLES`'s output is a table with one column, `name`.

## Examples

### Show user-created tables
```sql
SHOW TABLES;
```
```nofmt
 name
----------------
 my_table
 my_other_table
```

### Show tables from specified schema
```sql
SHOW SCHEMAS;
```
```nofmt
  name
--------
 public
```
```sql
SHOW TABLES FROM public;
```
```nofmt
 name
----------------
 my_table
 my_other_table
```

## Related pages

- [`SHOW CREATE TABLE`](../show-create-table)
- [`CREATE TABLE`](../create-table)
