---
audience: developer
canonical_url: https://materialize.com/docs/sql/show-schemas/
complexity: intermediate
description: '`SHOW SCHEMAS` returns a list of all schemas available in Materialize.'
doc_type: reference
keywords:
- FROM
- SHOW SCHEMAS
product_area: Indexes
status: stable
title: SHOW SCHEMAS
---

# SHOW SCHEMAS

## Purpose
`SHOW SCHEMAS` returns a list of all schemas available in Materialize.

If you need to understand the syntax and options for this command, you're in the right place.


`SHOW SCHEMAS` returns a list of all schemas available in Materialize.



`SHOW SCHEMAS` returns a list of all schemas available in Materialize.

## Syntax

This section covers syntax.

```mzsql
SHOW SCHEMAS [ FROM <database_name> ];
```text

Syntax element                | Description
------------------------------|------------
**FROM** <database_name>      | If specified, only show schemas from the specified database. Defaults to the current database. For available databases, see [`SHOW DATABASES`](../show-databases).

## Details

This section covers details.

### Output format

`SHOW SCHEMAS`'s output is a table with one column, `name`.

## Examples

This section covers examples.

```mzsql
SHOW DATABASES;
```text
```nofmt
   name
-----------
materialize
my_db
```text
```mzsql
SHOW SCHEMAS FROM my_db
```text
```nofmt
  name
--------
 public
```

## Related pages

- [`CREATE SCHEMA`](../create-schema)
- [`DROP SCHEMA`](../drop-schema)

