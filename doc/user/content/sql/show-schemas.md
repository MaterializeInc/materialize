---
title: "SHOW SCHEMAS"
description: "`SHOW SCHEMAS` returns a list of all schemas available to your Materialize instances."
menu:
  main:
    parent: 'sql'
---

`SHOW SCHEMAS` returns a list of all schemas available to your Materialize
instances.

## Syntax

{{< diagram "show-schemas.svg" >}}

Field | Use
------|-----
_database&lowbar;name_ | The database to show schemas from. Defaults to the current database. For available databases, see [`SHOW DATABASES`](../show-databases).

## Examples

```sql
SHOW DATABASES;
```
```nofmt
materialize
my_db
```
```sql
SHOW SCHEMAS FROM my_db
```
```nofmt
public
```

## Related pages

- [`CREATE SCHEMA`](../create-schema)
- [`DROP SCHEMA`](../drop-schema)
