---
title: "SHOW TYPES"
description: "`SHOW TYPES` returns a list of the data types in your Materialize instance."
menu:
  main:
    parent: commands
---

`SHOW TYPES` returns a list of the data types in your Materialize instance. By default, only custom types are returned.

## Syntax

{{< diagram "show-types.svg" >}}

Field | Use
------|-----
_schema&lowbar;name_ | The schema to show types from. Defaults to `public` in the current database. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).


## Examples

### Show custom data types

```sql
SHOW TYPES;
```
```
     name       |  type
----------------+--------
 int4_list      | user
```

## Related pages

* [`CREATE TYPE`](../create-type)
* [`DROP TYPE`](../drop-type)
