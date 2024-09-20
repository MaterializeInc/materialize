---
title: "map type"
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

You can construct maps using the `MAP` expression:

```mzsql
SELECT MAP['a' => 1, 'b' => 2];
```
```nofmt
     map
-------------
 {a=>1,b=>2}
```

You can nest `MAP` constructors:

```mzsql
SELECT MAP['a' => MAP['b' => 'c']];
```
```nofmt
     map
-------------
 {a=>{b=>c}}
```

You can also elide the `MAP` keyword from the interior map expressions:

```mzsql
SELECT MAP['a' => ['b' => 'c']];
```
```nofmt
     map
-------------
 {a=>{b=>c}}
```

`MAP` expressions evalute expressions for both keys and values:

```mzsql
SELECT MAP['a' || 'b' => 1 + 2];
```
```nofmt
     map
-------------
 {ab=>3}
```

Alternatively, you can construct a map from the results of a subquery. The
subquery must return two columns: a key column of type `text` and a value column
of any type, in that order. Note that, in this form of the `MAP` expression,
parentheses are used rather than square brackets.

```mzsql
SELECT MAP(SELECT key, value FROM test0 ORDER BY x DESC LIMIT 3);
```
```nofmt
       map
------------------
 {a=>1,b=>2,c=>3}
```

With all constructors, if the same key appears multiple times, the last value
for the key wins.

Note that you can also construct maps using the available [`text`
cast](#text-to-map-casts).

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
otherwise-annoying-to-type names.

### `text` to `map` casts

The textual format for a `map` is a sequence of `key => value` mappings
separated by commas and surrounded by curly braces (`{}`). For example:

```mzsql
SELECT '{a=>123.4, b=>111.1}'::map[text=>double] as m;
```
```nofmt
  m
------------------
 {a=>123.4,b=>111.1}
```

You can create nested maps the same way:
```mzsql
SELECT '{a=>{b=>{c=>d}}}'::map[text=>map[text=>map[text=>text]]] as nested_map;
```
```nofmt
  nested_map
------------------
 {a=>{b=>{c=>d}}}
```

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

```mzsql
SELECT MAP['a' => 1, 'b' => 2] -> 'a' as field_map;
```
```nofmt
 field_map
-----------
 1
```

```mzsql
SELECT MAP['a' => 1, 'b' => 2] -> 'c' as field_map;
```
```nofmt
 field_map
----------
 NULL
```

Field accessors can also be chained together.

```mzsql
SELECT MAP['a' => ['b' => 1], 'c' => ['d' => 2]] -> 'a' -> 'b' as field_map;
```
```nofmt
 field_map
-------------
 1
```

Note that all returned values are of the map's value type.

<hr/>

#### LHS contains RHS (`@>`)

```mzsql
SELECT MAP['a' => 1, 'b' => 2] @> MAP['a' => 1] AS lhs_contains_rhs;
```
```nofmt
 lhs_contains_rhs
------------------
 t
```

<hr/>

#### RHS contains LHS (`<@`)

```mzsql
SELECT MAP['a' => 1, 'b' => 2] <@ MAP['a' => 1] as rhs_contains_lhs;
```
```nofmt
 rhs_contains_lhs
------------------
 f
```

<hr/>

#### Search top-level keys (`?`)

```mzsql
SELECT MAP['a' => 1.9, 'b' => 2.0] ? 'a' AS search_for_key;
```
```nofmt
 search_for_key
----------------
 t
```

```mzsql
SELECT MAP['a' => ['aa' => 1.9], 'b' => ['bb' => 2.0]] ? 'aa' AS search_for_key;
```
```nofmt
 search_for_key
----------------
 f
```

#### Search for all top-level keys (`?&`)

Returns `true` if all keys provided on the RHS are present in the top-level of
the map, `false` otherwise.

```mzsql
SELECT MAP['a' => 1, 'b' => 2] ?& ARRAY['b', 'a'] as search_for_all_keys;
```
```nofmt
 search_for_all_keys
---------------------
 t
```

```mzsql
SELECT MAP['a' => 1, 'b' => 2] ?& ARRAY['c', 'b'] as search_for_all_keys;
```
```nofmt
 search_for_all_keys
---------------------
 f
```

#### Search for any top-level keys (`?|`)

Returns `true` if any keys provided on the RHS are present in the top-level of
the map, `false` otherwise.

```mzsql
SELECT MAP['a' => 1, 'b' => 2] ?| ARRAY['c', 'b'] as search_for_any_keys;
```
```nofmt
 search_for_any_keys
---------------------
 t
```

```mzsql
SELECT MAP['a' => 1, 'b' => 2] ?| ARRAY['c', 'd', '1'] as search_for_any_keys;
```
```nofmt
 search_for_any_keys
---------------------
 f
```

#### Count entries in map (`map_length`)

Returns the number of entries in the map.

```mzsql
SELECT map_length(MAP['a' => 1, 'b' => 2]);
```
```nofmt
 map_length
------------
 2
```
