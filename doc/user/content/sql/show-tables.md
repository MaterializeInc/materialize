---
title: "SHOW TABLES"
description: "`SHOW TABLES` returns a list of all tables available to your Materialize instances."
menu:
  main:
    parent: 'sql'
---

`SHOW TABLES` returns a list of all tables available to your Materialize
instances.

## Syntax

{{< diagram "show-tables.svg" >}}

Field | Use
------|-----
_schema&lowbar;name_ | The schema to show tables from. Defaults to `public` in the current database. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).

## Examples

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
```sql
SHOW TABLES;
```
```nofmt
my_table
my_other_table
```

## Related pages

- [`SHOW CREATE TABLE`](../show-create-table)
- [`CREATE TABLE`](../create-table)
