---
title: "CREATE TYPE"
description: "`CREATE TYPE` defines a new data type."
menu:
  main:
    parent: 'sql'
---

{{< version-added v0.6.1 />}}

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

Field | Use
------|-----
_type&lowbar;name_ | A name for the type.
_field_ **=** _val_ | A property of the new type. Note that type properties can only refer to data types within the catalog, i.e. they cannot refer to anonymous `list` or `map` types.

### `list` properties

Name | Use
-----|-----
`element_type` | Creates a custom [`list`](../types/list) whose elements are are of `element_type`.

### `map` properties

Name | Use
-----|-----
`key_type` | Creates a custom [`map`](../types/map) whose keys are are of `key_type`. `key_type` must resolve to [`text`](../types/text).
`value_type` | Creates a custom [`map`](../types/map) whose values are are of `value_type`.

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

### Custom `list`s

```sql
CREATE TYPE int4_list AS LIST (element_type = int4);

SELECT '{1,2}'::int4_list::text AS custom_list;
```
```
 custom_list
-------------
 {1,2}
```

### Nested custom `list`s

```sql
CREATE TYPE int4_list_list AS LIST (element_type = int4_list);

SELECT '{{1,2}}'::int4_list_list::text AS custom_nested_list;
```
```
 custom_nested_list
--------------------
 {{1,2}}
```

### Custom `map`s

```sql
CREATE TYPE int4_map AS MAP (key_type=text, value_type=int4);

SELECT '{a=>1}'::int4_map::text AS custom_map;
```
```
 custom_map
------------
 {a=>1}
```

### Nested custom `map`s

```sql
CREATE TYPE int4_map_map AS MAP (key_type=text, value_type=int4_map);

SELECT '{a=>{a=>1}}'::int4_map_map::text AS custom_nested_map;
```
```
 custom_nested_map
-------------------
{a=>{a=>1}}
```

## Related pages

* [`DROP TYPE`](../drop-type)
* [`SHOW TYPES`](../show-types)
