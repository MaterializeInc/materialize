---
audience: developer
canonical_url: https://materialize.com/docs/sql/show-tables/
complexity: intermediate
description: '`SHOW TABLES` returns a list of all tables available in Materialize.'
doc_type: reference
keywords:
- SHOW TABLES
- FROM
product_area: Indexes
status: stable
title: SHOW TABLES
---

# SHOW TABLES

## Purpose
`SHOW TABLES` returns a list of all tables available in Materialize.

If you need to understand the syntax and options for this command, you're in the right place.


`SHOW TABLES` returns a list of all tables available in Materialize.



`SHOW TABLES` returns a list of all tables available in Materialize.

## Syntax

This section covers syntax.

```mzsql
SHOW TABLES [FROM <schema_name>];
```text

Syntax element                | Description
------------------------------|------------
**FROM** <schema_name>        | If specified, only show tables from the specified schema. Defaults to first resolvable schema in the search path. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).

## Details

This section covers details.

### Output format

`SHOW TABLES`'s output is a table with one column, `name`.

## Examples

This section covers examples.

### Show user-created tables
```mzsql
SHOW TABLES;
```text
```nofmt
 name
----------------
 my_table
 my_other_table
```bash

### Show tables from specified schema
```mzsql
SHOW SCHEMAS;
```text
```nofmt
  name
--------
 public
```text
```mzsql
SHOW TABLES FROM public;
```text
```nofmt
 name
----------------
 my_table
 my_other_table
```

## Related pages

- [`SHOW CREATE TABLE`](../show-create-table)
- [`CREATE TABLE`](../create-table)

