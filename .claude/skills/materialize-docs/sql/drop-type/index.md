---
audience: developer
canonical_url: https://materialize.com/docs/sql/drop-type/
complexity: intermediate
description: '`DROP TYPE` removes a user-defined data type.'
doc_type: reference
keywords:
- DROP TYPE
- IF EXISTS
- CREATE TYPE
- RESTRICT
- CASCADE
product_area: Indexes
status: stable
title: DROP TYPE
---

# DROP TYPE

## Purpose
`DROP TYPE` removes a user-defined data type.

If you need to understand the syntax and options for this command, you're in the right place.


`DROP TYPE` removes a user-defined data type.



`DROP TYPE` removes a [custom data type](../create-type). You cannot use it on default data types.

## Syntax

This section covers syntax.

```mzsql
DROP TYPE [IF EXISTS] <data_type_name> [RESTRICT|CASCADE];
```text

Syntax element | Description
---------------|------------
**IF EXISTS**  | Optional. If specified, do not return an error if the named type doesn't exist.
`<data_type_name>` | The name of the type to remove.
**CASCADE** | Optional. If specified, remove the type and its dependent objects, such as tables or other types.
**RESTRICT** | Optional. Don't remove the type if any objects depend on it. _(Default.)_

## Examples

This section covers examples.

### Remove a type with no dependent objects
```mzsql
CREATE TYPE int4_map AS MAP (KEY TYPE = text, VALUE TYPE = int4);

SHOW TYPES;
```text
```
    name
--------------
  int4_map
(1 row)
```text

```mzsql
DROP TYPE int4_map;

SHOW TYPES;
```text
```
  name
--------------
(0 rows)
```bash

### Remove a type with dependent objects

By default, `DROP TYPE` will not remove a type with dependent objects. The **CASCADE** switch will remove both the specified type and *all its dependent objects*.

In the example below, the **CASCADE** switch removes `int4_list`, `int4_list_list` (which depends on `int4_list`), and the table *t*, which has a column of data type `int4_list`.

```mzsql
CREATE TYPE int4_list AS LIST (ELEMENT TYPE = int4);

CREATE TYPE int4_list_list AS LIST (ELEMENT TYPE = int4_list);

CREATE TABLE t (a int4_list);

SHOW TYPES;
```text
```
      name
----------------
 int4_list
 int4_list_list
(2 rows)
```text

```mzsql
DROP TYPE int4_list CASCADE;

SHOW TYPES;

SELECT * FROM t;
```text
```
 name
------
(0 rows)
ERROR:  unknown catalog item 't'
```bash

### Remove a type only if it has no dependent objects

You can use either of the following commands:

- ```mzsql
  DROP TYPE int4_list;
  ```text
- ```mzsql
  DROP TYPE int4_list RESTRICT;
  ```bash

### Do not issue an error if attempting to remove a nonexistent type

```mzsql
DROP TYPE IF EXISTS int4_list;
```

## Privileges

The privileges required to execute this statement are:

- Ownership of the dropped type.
- `USAGE` privileges on the containing schema.


## Related pages

* [`CREATE TYPE`](../create-type)
* [`SHOW TYPES`](../show-types)
* [`DROP OWNED`](../drop-owned)

