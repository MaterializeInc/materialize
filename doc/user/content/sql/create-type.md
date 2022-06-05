---
title: "CREATE TYPE"
description: "`CREATE TYPE` defines a new data type."
menu:
  main:
    parent: 'commands'
---

`CREATE TYPE` defines a new data type.

## Conceptual framework

`CREATE TYPE` creates custom types, which let you create named versions of
anonymous types. For more information, see [SQL Data Types: Custom
types](../types/#custom-types).

### Use

Currently, custom types provide a shorthand for referring to
otherwise-annoying-to-type names, but in the future will provide [binary
encoding and decoding][binary] for these types, as well.

[binary]:https://github.com/MaterializeInc/materialize/issues/4628

## Syntax

{{< diagram "create-type.svg" >}}

 Field               | Use
---------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------
 _type&lowbar;name_  | A name for the type.
 **MAP / LIST**      | The data type. If not specified, a row type is assumed.
 _property_ **=** _val_ | A property of the new type. This is required when specifying a `LIST` or `MAP` type. Note that type properties can only refer to data types within the catalog, i.e. they cannot refer to anonymous `list` or `map` types.

### `row` properties

Field               | Use
--------------------|----------------------------------------------------
_field_name_        | The name of a field in a row type.
_field_type_        | The data type of a field indicated by _field_name_.

### `list` properties

Field | Use
-----|-----
`element_type` | Creates a custom [`list`](../types/list) whose elements are of `element_type`.

### `map` properties

Field | Use
-----|-----
`key_type` | Creates a custom [`map`](../types/map) whose keys are of `key_type`. `key_type` must resolve to [`text`](../types/text).
`value_type` | Creates a custom [`map`](../types/map) whose values are of `value_type`.

## Details

For details about the custom types `CREATE TYPE` creates, see [SQL Data Types:
Custom types](../types/#custom-types).

### Properties

All custom type properties' values must refer to [named types](/sql/types), e.g.
`integer`.

To create a custom nested `list` or `map`, you must first create a custom `list`
or `map`. This creates a named type, which can then be referred to in another
custom type's properties.

## Examples

### Custom `list`

```sql
CREATE TYPE int4_list AS LIST (element_type = int4);

SELECT '{1,2}'::int4_list::text AS custom_list;
```
```
 custom_list
-------------
 {1,2}
```

### Nested custom `list`

```sql
CREATE TYPE int4_list_list AS LIST (element_type = int4_list);

SELECT '{{1,2}}'::int4_list_list::text AS custom_nested_list;
```
```
 custom_nested_list
--------------------
 {{1,2}}
```

### Custom `map`

```sql
CREATE TYPE int4_map AS MAP (key_type=text, value_type=int4);

SELECT '{a=>1}'::int4_map::text AS custom_map;
```
```
 custom_map
------------
 {a=>1}
```

### Nested custom `map`

```sql
CREATE TYPE int4_map_map AS MAP (key_type=text, value_type=int4_map);

SELECT '{a=>{a=>1}}'::int4_map_map::text AS custom_nested_map;
```
```
 custom_nested_map
-------------------
{a=>{a=>1}}
```

### Custom `row` type
```sql
CREATE TYPE row_type AS (a int, b text);
SELECT ROW(1, 'a')::row_type as custom_row_type;
```
```
custom_row_type
-----------------
(1,a)
```

### Nested `row` type
```sql
CREATE TYPE nested_row_type AS (a row_type, b float8);
SELECT ROW(ROW(1, 'a'), 2.3)::nested_row_type AS custom_nested_row_type;
```
```
custom_nested_row_type
------------------------
("(1,a)",2.3)
```

## Related pages

* [`DROP TYPE`](../drop-type)
* [`SHOW TYPES`](../show-types)
