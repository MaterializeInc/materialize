---
title: "SHOW TABLES"
description: "`SHOW TABLES` returns a list of all tables available in Materialize."
menu:
  main:
    parent: commands
---

`SHOW TABLES` returns a list of tables (including [tables created from sources
and webhooks](/sql/create-table/)) from a schema.

## Syntax

{{% include-example file="examples/show_tables" example="syntax" %}}

{{% include-example file="examples/show_tables" example="syntax-options"
 %}}

## Details

### Output format

`SHOW TABLES`'s output is a table with one column, `name`.

## Examples

### Show tables from the current schema

```mzsql
SHOW TABLES;
```
```nofmt
 name
----------------
 my_table
 my_other_table
```

### Show tables from specified schema
```mzsql
SHOW SCHEMAS;
```
```nofmt
  name
--------
 public
```
```mzsql
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
