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
`EXTENDED` |  Returns system types as well as user-created types. By default, only user-created types are returned.
`FULL`| Returns the creator of the data type (user or system).

## Examples

### Show custom data types

```sql
SHOW TYPES;
```
```
      name
----------------
  int4_list
```

### Show all data types

```sql
SHOW EXTENDED FULL TYPES;
```
```
     name       |  type
----------------+--------
 _bool          | system
 _bytea         | system
 _date          | system
 _float4        | system
 _float8        | system
 int4_list      | user
...
```

## Related pages

* [`CREATE TYPE`](../create-type)
* [`DROP TYPE`](../drop-type)
