---
title: "SQL Data Types"
description: "Learn more about the SQL data types you love...."
menu:
  main:
    identifier: sql-types
    name: Data Types
    parent: sql
    weight: 1
disable_list: true
---

Materialize's type system consists of two classes of types:

- [Built-in types](#built-in-types)
- [Custom types](#custom-types) created through [`CREATE TYPE`][create-type]

## Built-in types

Type | Aliases | Use | Size (bytes) | Catalog name | Syntax
-----|---------|-----|--------------|----------------|--------
[`bool`](bool) | `boolean` | State of `TRUE` or `FALSE` | 1 | Named | `TRUE`, `FALSE`
[`date`](date) | | Date without a specified time | 4 | Named | `DATE '2007-02-01'`
[`float4`](float) | `real` | Single precision floating-point number | 4 | Named | `1.23`
[`float8`](float) | `float`, `double`, `double precision` | Double precision floating-point number | 8 | Named | `1.23`
[`int4`](integer) | `int`, `integer` | Signed integer | 4 | Named | `123`
[`int8`](integer) | `bigint` | Large signed integer | 8 | Named | `123`
[`interval`](interval) | | Duration of time | 32 | Named | `INTERVAL '1-2 3 4:5:6.7'`
[`jsonb`](jsonb) | `json` | JSON | Variable | Named | `'{"1":2,"3":4}'::jsonb`
[`map`](map) | | Map with [`text`](text) keys and a uniform value type | Variable | Anonymous | `'{a: 1, b: 2}'::map[text=>int]`
[`list`](list) | | Multidimensional list | Variable | Anonymous | `LIST[[1,2],[3]]`
[`record`](record) | | Tuple with arbitrary contents | Variable | Anonymous | `ROW($expr, ...)`
[`numeric`](numeric) | `decimal` | Signed exact number with user-defined precision and scale | 16 | Unnameable | `1.23`
[`oid`](oid) | | PostgreSQL object identifier | 4 | Named | `123`
[`text`](text) | `string` | Unicode string | Variable | Named | `'foo'`
[`time`](time) | | Time without date | 4 | Named | `TIME '01:23:45'`
[`timestamp`](timestamp) | | Date and time | 8 | Named | `TIMESTAMP '2007-02-01 15:04:05'`
[`timestamp with time zone`](timestamp) | `timestamptz` | Date and time with timezone | 8 | Named | `TIMESTAMPTZ '2007-02-01 15:04:05+06'`
[Arrays](array) (`[]`) | | Multidimensional array | Variable | Named | `ARRAY[...]`

#### Catalog name

Value | Description
------|------------
**Named** | Named types can be referred to using a qualified object name, i.e. they are objects within the `pg_catalog` schema. Each named type a unique OID.
**Anonymous** | Anonymous types cannot be referred to using a qualified object name, i.e. they do not exist as objects anywhere.<br/><br/>Anonymous types do not have unique OIDs for all of their possible permutations, e.g. `int4 list`, `float8 list`, and `date list list` share the same OID.<br/><br/>You can create named versions of some anonymous types using [custom types](#custom-types).
**Unnameable** | Unnameable types are anonymous and do not yet support being custom types.

## Custom types

Custom types, in general, provide a mechanism to create names for specific
instances of  anonymous types to suite users' needs.

However, types are considered custom if the type:
- Was created through [`CREATE TYPE`][create-type].
- Contains a reference to a custom type.

To create custom types, see [`CREATE TYPE`][create-type].

### Use

Currently, custom types only provides a shorthand for referring to
otherwise-annoying-to-type names, but in the future will provide [binary
encoding and decoding][binary] for these types, as well.

[binary]:https://github.com/MaterializeInc/materialize/issues/4628

### Casts

Structurally equivalent types can be cast to and from one another; the required
context depends on the types themselves, though.

From | To | Cast permitted
-----|----|-----------------
Custom type | Built-in type | Implicitly
Built-in type | Custom type | Implicitly
Custom type 1 | Custom type 2 | [For explicit casts](../functions/cast/)

### Equality

Values in custom types are never considered equal to:

- Other custom types, irrespective of their structure or value.
- Built-in types, but can be coerced to and from structurally equivalent custom
  types.

### Polymorphism

If custom types and built-in types are both used in [polymorphic
functions](list/#polymorphism), the resultant type is the "least custom type"
that can be derived.

This is a little easier to understand if we make it concrete, so we'll focus on
concatenting two lists and appending an element to list.

For these operations, Materialize uses the following polymorphc parameters:

- `ListAny`, which accepts any `list`, and constrains all lists to being of the
  same structurally equivalent type.
- `ListElementAny`, which accepts any type, but must be equal to the element
  type of the `list` type used with `ListAny`. For instance, if `ListAny` is
  constrained to being `int4 list`, `ListElementAny` must be `int4`.

When concatenating two lists, we'll use `list_cat` whose signature is `list_cat(l: ListAny, r: ListAny)`.

If we concatenate a custom `list` (in this example, `custom_list`) and a
structurally equivalent built-in `list` (`int4 list`), the result is of the same
type as the custom `list` (`custom_list`).

```sql
CREATE TYPE custom_list AS LIST (element_type=int4);

SELECT pg_typeof(
  list_cat('{1}'::custom_list, '{2}'::int4 list)
) AS custom_list_built_in_list_cat;

```
```
 custom_list_built_in_list_cat
-------------------------------
 custom_list
```

When appending an element to a list, we'll use `list_append` whose signature is
`list_append(l: ListAny, e: ListElementAny)`.

If we append a structurally appropriate elment (`int4`) to a custom `list`
(`custom_list`), the result is of the same type as the custom `list`
(`custom_list`).

```sql
SELECT pg_typeof(
  list_append('{1}'::custom_list, 2)
) AS custom_list_built_in_element_cat;

```
```
 custom_list_built_in_element_cat
----------------------------------
 custom_list
```

If we append a concatenate a structurally appropriate custom element
(`custom_list`) to a built-in `list` (`int4 list list`), the result is a `list`
of custom elements.

```sql
SELECT pg_typeof(
  list_append('{{1}}'::int4 list list, '{2}'::custom_list)
) AS built_in_list_custom_element_append;

```
```
 built_in_list_custom_element_append
-------------------------------------
 custom_list list
```

This is the "least custom type" we could support for these values (i.e. we don't
invent a new custom type whose element is `custom_list`, and we don't coerce
`custom_list` into an anonymous built-in list).

Note that in this example, `custom_list list` is considered a custom type
because it contains a reference to a custom type.

[create-type]: ../create-type
