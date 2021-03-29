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
`EXTENDED` | Returns system tables as well as user-created tables. By default, only user-created tables are returned.

## Details

### Output format

`SHOW TABLES`'s output is a table with one column, `name`.

{{< version-changed v0.5.0 >}}
The output column is renamed from `TABLES` to `name`.
{{< /version-changed >}}

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
