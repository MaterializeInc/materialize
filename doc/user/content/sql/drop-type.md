---
title: "DROP TYPE"
description: "`DROP TYPE` removes a user-defined data type."
menu:
  main:
    parent: 'sql'
---

{{< version-added v0.7.1 />}}

`DROP TYPE` removes a [custom data type](../create-type). You cannot use it on default data types.

## Syntax

{{< diagram "drop-type.svg" >}}

Field | Use
------|-----
_data_type_name_ | The name of the type to remove.
`CASCADE` | Automatically removes any objects that depend on the type, such as tables or other types.
`IF EXISTS`  |  Do not issue an error if the named type doesn't exist.
`RESTRICT`  |  Do not remove the type if any other objects depend on it. _(Default.)_

## Examples

### Remove a data type with no dependencies
```sql
CREATE TYPE int4_map AS MAP (key_type=text, value_type=int4);

SHOW TYPES;
```
```
    name
--------------
  int4_map
(1 row)
```

```sql
DROP TYPE int4_map;

SHOW TYPES;
```
```
  name
--------------
(0 rows)
```

### Remove a data type with dependent objects

By default, `DROP TYPE` will not remove a type with dependent objects. The `CASCADE` switch will remove both the specified type and *all its dependent objects*.

In the example below, the `CASCADE` switch removes `int4_list`, `int4_list_list` (which depends on `int4_list`), and the table *t*, which has a column of data type `int4_list`.

```sql
CREATE TYPE int4_list AS LIST (element_type = int4);

CREATE TYPE int4_list_list AS LIST (element_type = int4_list);

CREATE TABLE t (a int4_list);

SHOW TYPES;
```
```
      name
----------------
 int4_list
 int4_list_list
(2 rows)
```

```sql
DROP TYPE int4_list CASCADE;

SHOW TYPES;

SELECT * FROM t;
```
```
 name
------
(0 rows)
ERROR:  unknown catalog item 't'
```

## Related pages

* [`CREATE TYPE`](../create-type)
* [`SHOW TYPES`](../show-types)
