---
title: "SHOW SCHEMAS"
description: "`SHOW SCHEMAS` returns a list of all schemas available to your Materialize instances."
menu:
  main:
    parent: commands
---

`SHOW SCHEMAS` returns a list of all schemas available to your Materialize
instances.

## Syntax

{{< diagram "show-schemas.svg" >}}

Field | Use
------|-----
_database&lowbar;name_ | The database to show schemas from. Defaults to the current database. For available databases, see [`SHOW DATABASES`](../show-databases).

## Details

### Output format

`SHOW SCHEMAS`'s output is a table with one column, `name`.

## Examples

```sql
SHOW DATABASES;
```
```nofmt
   name
-----------
materialize
my_db
```
```sql
SHOW SCHEMAS FROM my_db
```
```nofmt
 name
------
public
```

## Related pages

- [`CREATE SCHEMA`](../create-schema)
- [`DROP SCHEMA`](../drop-schema)
