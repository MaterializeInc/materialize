---
title: "map Data Type"
description: "Expresses a map"
menu:
  main:
    parent: 'sql-types'
aliases:
  - /sql/types/map
---

`map` data expresses an unordered map with [`text`](../text) keys and an
arbitrary uniform value type.

Detail | Info
-------|------
**Quick Syntax** | `'{a=>123.4, b=>111.1}'::map[text=>double]'`
**Size** | Variable
**Catalog name** | Anonymous, but [nameable](../../create-type)

## Syntax

{{< diagram "type-map.svg" >}}

Field | Use
------|-----
_map&lowbar;string_ | A well-formed map object.
_value&lowbar;type_ | The [type](../../types) of the map's values.

## Map functions + operators

### Operators

{{% map-operators %}}

### Functions

{{< fnlist "Map" >}}

## Details

### Construction

A well-formed `map` is a collection of `key => value` mappings separated by
commas. Each individual `map` must be correctly contained by a set of curly
braces (`{}`).

You can construct maps from strings using the following syntax:
```sql
SELECT '{a=>123.4, b=>111.1}'::map[text=>double] as m;
```
```nofmt
  m
------------------
 {a=>123.4,b=>111.1}
```

You can create nested maps the same way:
```sql
SELECT '{a=>{b=>{c=>d}}}'::map[text=>map[text=>map[text=>text]]] as nested_map;
```
```nofmt
  nested_map
------------------
 {a=>{b=>{c=>d}}}
```

### Constraints

- Keys must be of type [`text`](../text).
- Values can be of any [type](../../types) as long as the type is uniform.
- Keys must be unique. If duplicate keys are present in a map, only one of the
  (`key`, `value`) pairs will be retained. There is no guarantee which will be
  retained.

### Custom types

You can create [custom `map` types](/sql/types/#custom-types), which lets you
create a named entry in the catalog for a specific type of `map`.

Currently, custom types only provides a shorthand for referring to
otherwise-annoying-to-type names, but in the future will provide [binary
encoding and decoding][binary] for these types, as well.

[binary]:https://github.com/MaterializeInc/materialize/issues/4628

### Valid casts

#### Between `map`s

Two `map` types can only be cast to and from one another if they are
structurally equivalent, e.g. one is a [custom map
type](/sql/types#custom-types) and the other is a [built-in
map](/sql/types#built-in-types) and their key-value types are structurally
equivalent.

#### From `map`

You can [cast](../../functions/cast) `map` to and from the following types:

- [`text`](../text) (by assignment)
- Other `map`s as noted above.

#### To `map`

- [`text`](../text) (explicitly)
- Other `map`s as noted above.

## Examples

### Operators

#### Retrieve value with key (`->`)

Retrieves and returns the target value or `NULL`.

```sql
SELECT '{a=>1, b=>2}'::map[text=>int] -> 'a' as field_map;
```
```nofmt
 field_map
-----------
 1
```

```sql
SELECT '{a=>1, b=>2}'::map[text=>int] -> 'c' as field_map;
```
```nofmt
 field_map
----------
 NULL
```

Field accessors can also be chained together.

```sql
SELECT '{a=>{b=>1}}, {c=>{d=>2}}'::map[text=>map[text=>int]] -> 'a' -> 'b' as field_map;
```
```nofmt
 field_map
-------------
 1
```

Note that all returned values are of the map's value type.

<hr/>

#### LHS contains RHS (`@>`)

```sql
SELECT '{a=>1, b=>2}'::map[text=>int] @>
       '{a=>1}'::map[text=>int] AS lhs_contains_rhs;
```
```nofmt
 lhs_contains_rhs
------------------
 t
```

<hr/>

#### RHS contains LHS (`<@`)

```sql
SELECT '{a=>1, b=>2}'::map[text=>int] <@
       '{a=>1}'::map[text=>int] as rhs_contains_lhs;
```
```nofmt
 rhs_contains_lhs
------------------
 f
```

<hr/>

#### Search top-level keys (`?`)

```sql
SELECT '{a=>1.9, b=>2.0}'::map[text=>double] ? 'a' AS search_for_key;
```
```nofmt
 search_for_key
----------------
 t
```

```sql
SELECT '{a=>{aa=>1.9}}, {b=>{bb=>2.0}}'::map[text=>map[text=>double]]
        ? 'aa' AS search_for_key;
```
```nofmt
 search_for_key
----------------
 f
```

#### Search for all top-level keys (`?&`)

Returns `true` if all keys provided on the RHS are present in the top-level of
the map, `false` otherwise.

```sql
SELECT '{a=>1, b=>2}'::map[text=>int] ?& ARRAY['b', 'a'] as search_for_all_keys;
```
```nofmt
 search_for_all_keys
---------------------
 t
```

```sql
SELECT '{a=>1, b=>2}'::map[text=>int] ?& ARRAY['c', 'b'] as search_for_all_keys;
```
```nofmt
 search_for_all_keys
---------------------
 f
```

#### Search for any top-level keys (`?|`)

Returns `true` if any keys provided on the RHS are present in the top-level of
the map, `false` otherwise.

```sql
SELECT '{a=>1, b=>2}'::map[text=>int] ?| ARRAY['c', 'b'] as search_for_any_keys;
```
```nofmt
 search_for_any_keys
---------------------
 t
```

```sql
SELECT '{a=>1, b=>2}'::map[text=>int] ?| ARRAY['c', 'd', '1'] as search_for_any_keys;
```
```nofmt
 search_for_any_keys
---------------------
 f
```

#### Count entries in map (`map_length`)

Returns the number of entries in the map.

```sql
SELECT map_length('{a=>1, b=>2}'::map[text=>int]) as count;
```
```nofmt
 count
---------------------
 2
```
