---
title: "DROP TYPE"
description: "`DROP TYPE` removes a user-defined data type."
menu:
  main:
    parent: 'sql'
---

{{< version-added v0.7.1 />}}

`DROP TYPE` removes a user-defined [custom data type](../create-type). It cannot be used on default data types.

## Syntax

{{< diagram "drop-type.svg" >}}

Field | Use
------|-----
_data_type_name_ | The name of the type to remove.
`CASCADE` | Automatically drop objects that depend on the type (such as table columns, functions, operators, or other types)
`IF EXISTS`  |  Issue a notice, but not an error, if the type doesn't exist.
`RESTRICT`  |  Refuse to drop the type if any objects depend on it. This is the default.


## Examples

These examples use the custom drop types from the [CREATE TYPE examples](../create-type#examples).

### DROP TYPE (no dependencies)

```sql
DROP TYPE int4_list_list;
```
```
DROP TYPE
```


### DROP TYPE (dependencies)

```sql
DROP TYPE _int4_map_ CASCADE;
```
```
DROP TYPE
```
