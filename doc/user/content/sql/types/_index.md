---
title: "SQL data types"
description: "Learn more about the SQL data types supported in Materialize"
menu:
  main:
    identifier: sql-types
    name: SQL data types
    parent: reference
    weight: 110
disable_list: true
---

Materialize's type system consists of two classes of types:

- [Built-in types](#built-in-types)
- [Custom types](#custom-types) created through [`CREATE TYPE`][create-type]

## Built-in types

Type | Aliases | Use | Size (bytes) | Catalog name | Syntax
-----|-------|-----|--------------|----------------|-----
[`bigint`](integer) | `int8` | Large signed integer | 8 | Named | `123`
[`boolean`](boolean) | `bool` | State of `TRUE` or `FALSE` | 1 | Named | `TRUE`, `FALSE`
[`bytea`](bytea) | `bytea` | Unicode string | Variable | Named | `'\xDEADBEEF'` or `'\\000'`
[`date`](date) | | Date without a specified time | 4 | Named | `DATE '2007-02-01'`
[`double precision`](float) | `float`, `float8`, `double` | Double precision floating-point number | 8 | Named | `1.23`
[`integer`](integer) | `int`, `int4` | Signed integer | 4 | Named | `123`
[`interval`](interval) | | Duration of time | 32 | Named | `INTERVAL '1-2 3 4:5:6.7'`
[`jsonb`](jsonb) | `json` | JSON | Variable | Named | `'{"1":2,"3":4}'::jsonb`
[`map`](map) | | Map with [`text`](text) keys and a uniform value type | Variable | Anonymous | `'{a => 1, b => 2}'::map[text=>int]`
[`list`](list) | | Multidimensional list | Variable | Anonymous | `LIST[[1,2],[3]]`
[`numeric`](numeric) | `decimal` | Signed exact number with user-defined precision and scale | 16 | Named | `1.23`
[`oid`](oid) | | PostgreSQL object identifier | 4 | Named | `123`
[`real`](float) | `float4` | Single precision floating-point number | 4 | Named | `1.23`
[`record`](record) | | Tuple with arbitrary contents | Variable | Unnameable | `ROW($expr, ...)`
[`smallint`](integer) | `int2` | Small signed integer | 2 | Named | `123`
[`text`](text) | `string` | Unicode string | Variable | Named | `'foo'`
[`time`](time) | | Time without date | 4 | Named | `TIME '01:23:45'`
[`uint2`](uint) | | Small unsigned integer | 2 | Named | `123`
[`uint4`](uint) | `uint` | Unsigned integer | 4 | Named | `123`
[`uint8`](uint) | | Large unsigned integer | 8 | Named | `123`
[`timestamp`](timestamp) | | Date and time | 8 | Named | `TIMESTAMP '2007-02-01 15:04:05'`
[`timestamp with time zone`](timestamp) | `timestamp with time zone` | Date and time with timezone | 8 | Named | `TIMESTAMPTZ '2007-02-01 15:04:05+06'`
[Arrays](array) (`[]`) | | Multidimensional array | Variable | Named | `ARRAY[...]`

#### Catalog name

Value | Description
------|------------
**Named** | Named types can be referred to using a qualified object name, i.e. they are objects within the `pg_catalog` schema. Each named type a unique OID.
**Anonymous** | Anonymous types cannot be referred to using a qualified object name, i.e. they do not exist as objects anywhere.<br/><br/>Anonymous types do not have unique OIDs for all of their possible permutations, e.g. `int4 list`, `float8 list`, and `date list list` share the same OID.<br/><br/>You can create named versions of some anonymous types using [custom types](#custom-types).
**Unnameable** | Unnameable types are anonymous and do not yet support being custom types.

## Custom types

Custom types, in general, provide a mechanism to create names for specific
instances of anonymous types to suit users' needs.

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

Values in custom types are _never_ considered equal to:

- Other custom types, irrespective of their structure or value.
- Built-in types, but built-in types can be coerced to and from structurally
  equivalent custom types.

### Polymorphism

When using custom types as values for [polymorphic
functions](list/#polymorphism), the following additional constraints apply:

- If any value passed to a polymorphic parameter is a custom type, the resultant
  type must use the custom type in the appropriate location.

  For example, if a custom type is used as:
  - `listany`, the resultant `list` must be of exactly the same type.
  - `listelementany`, the resultant `list`'s element must be of the custom type.
- If custom types and built-in types are both used, the resultant type is the
  "least custom type" that can be derived––i.e. the resultant type will have the
  fewest possible layers of custom types that still fulfill all constraints.
  Materialize will neither create nor discover a custom type that fills the
  constraints, nor will it coerce a custom type to a built-in type.

  For example, if appending a custom `list` to a built-in `list list`, the
  resultant type will be a `list` of custom `list`s.

#### Examples

This is a little easier to understand if we make it concrete, so we'll focus on
concatenating two lists and appending an element to list.

For these operations, Materialize uses the following polymorphic parameters:

- `listany`, which accepts any `list`, and constrains all lists to being of the
  same structurally equivalent type.
- `listelementany`, which accepts any type, but must be equal to the element
  type of the `list` type used with `listany`. For instance, if `listany` is
  constrained to being `int4 list`, `listelementany` must be `int4`.

When concatenating two lists, we'll use `list_cat` whose signature is
`list_cat(l: listany, r: listany)`.

If we concatenate a custom `list` (in this example, `custom_list`) and a
structurally equivalent built-in `list` (`int4 list`), the result is of the same
type as the custom `list` (`custom_list`).

```sql
CREATE TYPE custom_list AS LIST (element_type=int4);

SELECT pg_typeof(
  list_cat('{1}'::custom_list, '{2}'::int4 list)
) AS custom_list_built_in_list_cat;

```
```nofmt
 custom_list_built_in_list_cat
-------------------------------
 custom_list
```

When appending an element to a list, we'll use `list_append` whose signature is
`list_append(l: listany, e: listelementany)`.

If we append a structurally appropriate element (`int4`) to a custom `list`
(`custom_list`), the result is of the same type as the custom `list`
(`custom_list`).

```sql
SELECT pg_typeof(
  list_append('{1}'::custom_list, 2)
) AS custom_list_built_in_element_cat;

```
```nofmt
 custom_list_built_in_element_cat
----------------------------------
 custom_list
```

If we append a structurally appropriate custom element (`custom_list`) to a
built-in `list` (`int4 list list`), the result is a `list` of custom elements.

```sql
SELECT pg_typeof(
  list_append('{{1}}'::int4 list list, '{2}'::custom_list)
) AS built_in_list_custom_element_append;

```
```nofmt
 built_in_list_custom_element_append
-------------------------------------
 custom_list list
```

This is the "least custom type" we could support for these values––i.e.
Materialize will not create or discover a custom type whose elements are
`custom_list`, nor will it coerce `custom_list` into an anonymous built-in
list.

Note that `custom_list list` is considered a custom type because it contains a
reference to a custom type. Because it's a custom type, it enforces custom
types' polymorphic constraints.

For example, values of type `custom_list list` and `custom_nested_list` cannot
both be used as `listany` values for the same function:

```sql
CREATE TYPE custom_nested_list AS LIST (element_type=custom_list);

SELECT list_cat(
  -- result is "custom_list list"
  list_append('{{1}}'::int4 list list, '{2}'::custom_list),
  -- result is custom_nested_list
  '{{3}}'::custom_nested_list
);
```
```nofmt
ERROR: Cannot call function list_cat(custom_list list, custom_nested_list)...
```

As another example, when using `custom_list list` values for `listany`
parameters, you can only use `custom_list` or `int4 list` values for
`listelementany` parameters––using any other custom type will fail:

```sql
CREATE TYPE second_custom_list AS LIST (element_type=int4);

SELECT list_append(
  -- elements are custom_list
  '{{1}}'::custom_nested_list,
  -- second_custom_list is not interoperable with custom_list because both
  -- are custom
  '{2}'::second_custom_list
);
```
```nofmt
ERROR:  Cannot call function list_append(custom_nested_list, second_custom_list)...
```

To make custom types interoperable, you must cast them to the same type. For
example, casting `custom_nested_list` to `custom_list list` (or vice versa)
makes the values passed to `listany` parameters of the same custom type:

```sql
SELECT pg_typeof(
  list_cat(
    -- result is "custom_list list"
    list_append(
      '{{1}}'::int4 list list,
      '{2}'::custom_list
    ),
    -- result is "custom_list list"
    '{{3}}'::custom_nested_list::custom_list list
  )
) AS complex_list_cat;
```
```nofmt
 complex_list_cat
------------------
 custom_list list
```

[create-type]: ../create-type
