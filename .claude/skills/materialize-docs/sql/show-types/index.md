---
audience: developer
canonical_url: https://materialize.com/docs/sql/show-types/
complexity: intermediate
description: '`SHOW TYPES` returns a list of the data types in Materialize.'
doc_type: reference
keywords:
- SHOW TYPES
- FROM
product_area: Indexes
status: stable
title: SHOW TYPES
---

# SHOW TYPES

## Purpose
`SHOW TYPES` returns a list of the data types in Materialize.

If you need to understand the syntax and options for this command, you're in the right place.


`SHOW TYPES` returns a list of the data types in Materialize.



`SHOW TYPES` returns a list of the data types in Materialize. Only custom types
are returned.

## Syntax

This section covers syntax.

```mzsql
SHOW TYPES [FROM <schema_name>];
```text

Syntax element                | Description
------------------------------|------------
**FROM** <schema_name>        | If specified, only show types from the specified schema. Defaults to first resolvable schema in the search path. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).

## Examples

This section covers examples.

### Show custom data types

```mzsql
SHOW TYPES;
```text
```
   name
-----------
 int4_list
```

## Related pages

* [`CREATE TYPE`](../create-type)
* [`DROP TYPE`](../drop-type)

