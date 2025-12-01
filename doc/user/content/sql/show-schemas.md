---
title: "SHOW SCHEMAS"
description: "`SHOW SCHEMAS` returns a list of all schemas available in Materialize."
menu:
  main:
    parent: commands
---

`SHOW SCHEMAS` returns a list of all schemas available in Materialize.

## Syntax

```mzsql
SHOW SCHEMAS [ FROM <database_name> ];
```

Syntax element                | Description
------------------------------|------------
**FROM** <database_name>      | If specified, only show schemas from the specified database. Defaults to the current database. For available databases, see [`SHOW DATABASES`](../show-databases).

## Details

### Output format

`SHOW SCHEMAS`'s output is a table with one column, `name`.

## Examples

```mzsql
SHOW DATABASES;
```
```nofmt
   name
-----------
materialize
my_db
```
```mzsql
SHOW SCHEMAS FROM my_db
```
```nofmt
  name
--------
 public
```

## Related pages

- [`CREATE SCHEMA`](../create-schema)
- [`DROP SCHEMA`](../drop-schema)
