---
title: "SHOW TYPES"
description: "`SHOW TYPES` returns a list of the data types in your Materialize instance."
menu:
  main:
    parent: 'sql'
---

`SHOW TYPES` returns a list of the data types in your Materialize instance. By default, only user-created columns are returned.

## Syntax

{{< diagram "show-types.svg" >}}

Field | Use
------|-----
`EXTENDED` |  Returns system columns as well as user-created columns. By default, only user-created columns are returned.
`FULL`| Returns the type of column (user or system).

## Examples

### Show custom data types

```sql
SHOW TYPES;
```
```
      name
----------------
  int4_list
  int4_list_list
  int4_map
  int5_map
(4 rows)
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
 int4_list_list | user
 int4_map       | user
 int5_map       | user
...
```

## Related pages

* [`CREATE TYPE`](../create-type)
* [`DROP TYPE`](../drop-type)
