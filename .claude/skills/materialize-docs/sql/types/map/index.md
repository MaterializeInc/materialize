---
audience: developer
canonical_url: https://materialize.com/docs/sql/types/map/
complexity: intermediate
description: Expresses a map
doc_type: reference
keywords:
- SELECT MAP
- Catalog name
- map type
- Quick Syntax
- Size
product_area: Indexes
status: stable
title: map type
---

# map type

## Purpose
Expresses a map

If you need to understand the syntax and options for this command, you're in the right place.


Expresses a map


`map` data expresses an unordered map with [`text`](../text) keys and an
arbitrary uniform value type.

Detail | Info
-------|------
**Quick Syntax** | `'{a=>123.4, b=>111.1}'::map[text=>double]'`
**Size** | Variable
**Catalog name** | Anonymous, but [nameable](../../create-type)

## Syntax

[See diagram: type-map.svg]

Field | Use
------|-----
_map&lowbar;string_ | A well-formed map object.
_value&lowbar;type_ | The [type](../../types) of the map's values.

## Map functions + operators

This section covers map functions + operators.

### Operators

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: map-operators --> --> -->

### Functions


## Details

This section covers details.

### Construction

You can construct maps using the `MAP` expression:

```mzsql
SELECT MAP['a' => 1, 'b' => 2];
```text
```nofmt
     map
-------------
 {a=>1,b=>2}
```text

You can nest `MAP` constructors:

```mzsql
SELECT MAP['a' => MAP['b' => 'c']];
```text
```nofmt
     map
-------------
 {a=>{b=>c}}
```text

You can also elide the `MAP` keyword from the interior map expressions:

```mzsql
SELECT MAP['a' => ['b' => 'c']];
```text
```nofmt
     map
-------------
 {a=>{b=>c}}
```text

`MAP` expressions evalute expressions for both keys and values:

```mzsql
SELECT MAP['a' || 'b' => 1 + 2];
```text
```nofmt
     map
-------------
 {ab=>3}
```text

Alternatively, you can construct a map from the results of a subquery. The
subquery must return two columns: a key column of type `text` and a value column
of any type, in that order. Note that, in this form of the `MAP` expression,
parentheses are used rather than square brackets.

```mzsql
SELECT MAP(SELECT key, value FROM test0 ORDER BY x DESC LIMIT 3);
```text
```nofmt
       map
------------------
 {a=>1,b=>2,c=>3}
```text

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
```text
```nofmt
  m
------------------
 {a=>123.4,b=>111.1}
```text

You can create nested maps the same way:
```mzsql
SELECT '{a=>{b=>{c=>d}}}'::map[text=>map[text=>map[text=>text]]] as nested_map;
```text
```nofmt
  nested_map
------------------
 {a=>{b=>{c=>d}}}
```bash

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

This section covers examples.

### Operators

#### Retrieve value with key (`->`)

Retrieves and returns the target value or `NULL`.

```mzsql
SELECT MAP['a' => 1, 'b' => 2] -> 'a' as field_map;
```text
```nofmt
 field_map
-----------
 1
```text

```mzsql
SELECT MAP['a' => 1, 'b' => 2] -> 'c' as field_map;
```text
```nofmt
 field_map
----------
 NULL
```text

Field accessors can also be chained together.

```mzsql
SELECT MAP['a' => ['b' => 1], 'c' => ['d' => 2]] -> 'a' -> 'b' as field_map;
```text
```nofmt
 field_map
-------------
 1
```text

Note that all returned values are of the map's value type.

<hr/>

#### LHS contains RHS (`@>`)

```mzsql
SELECT MAP['a' => 1, 'b' => 2] @> MAP['a' => 1] AS lhs_contains_rhs;
```text
```nofmt
 lhs_contains_rhs
------------------
 t
```text

<hr/>

#### RHS contains LHS (`<@`)

```mzsql
SELECT MAP['a' => 1, 'b' => 2] <@ MAP['a' => 1] as rhs_contains_lhs;
```text
```nofmt
 rhs_contains_lhs
------------------
 f
```text

<hr/>

#### Search top-level keys (`?`)

```mzsql
SELECT MAP['a' => 1.9, 'b' => 2.0] ? 'a' AS search_for_key;
```text
```nofmt
 search_for_key
----------------
 t
```text

```mzsql
SELECT MAP['a' => ['aa' => 1.9], 'b' => ['bb' => 2.0]] ? 'aa' AS search_for_key;
```text
```nofmt
 search_for_key
----------------
 f
```bash

#### Search for all top-level keys (`?&`)

Returns `true` if all keys provided on the RHS are present in the top-level of
the map, `false` otherwise.

```mzsql
SELECT MAP['a' => 1, 'b' => 2] ?& ARRAY['b', 'a'] as search_for_all_keys;
```text
```nofmt
 search_for_all_keys
---------------------
 t
```text

```mzsql
SELECT MAP['a' => 1, 'b' => 2] ?& ARRAY['c', 'b'] as search_for_all_keys;
```text
```nofmt
 search_for_all_keys
---------------------
 f
```bash

#### Search for any top-level keys (`?|`)

Returns `true` if any keys provided on the RHS are present in the top-level of
the map, `false` otherwise.

```mzsql
SELECT MAP['a' => 1, 'b' => 2] ?| ARRAY['c', 'b'] as search_for_any_keys;
```text
```nofmt
 search_for_any_keys
---------------------
 t
```text

```mzsql
SELECT MAP['a' => 1, 'b' => 2] ?| ARRAY['c', 'd', '1'] as search_for_any_keys;
```text
```nofmt
 search_for_any_keys
---------------------
 f
```bash

#### Count entries in map (`map_length`)

Returns the number of entries in the map.

```mzsql
SELECT map_length(MAP['a' => 1, 'b' => 2]);
```text
```nofmt
 map_length
------------
 2
```