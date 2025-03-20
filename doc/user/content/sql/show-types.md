---
title: "SHOW TYPES"
description: "`SHOW TYPES` returns a list of the data types in Materialize."
menu:
  main:
    parent: commands
---

`SHOW TYPES` returns a list of the data types in Materialize. Only custom types
are returned.

## Syntax

```mzsql
SHOW TYPES [FROM <schema_name>]
```

Option                 | Description
-----------------------|------------
**FROM** <schema_name> | If specified, only show types from the specified schema. Defaults to first resolvable schema in the search path. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).

## Examples

### Show custom data types

```mzsql
SHOW TYPES;
```
```
   name
-----------
 int4_list
```

## Related pages

* [`CREATE TYPE`](../create-type)
* [`DROP TYPE`](../drop-type)
