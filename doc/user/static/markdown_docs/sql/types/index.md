<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/self-managed/v25.2/)
 /  [Reference](/docs/self-managed/v25.2/sql/)

</div>

# SQL data types

Materialize’s type system consists of two classes of types:

- [Built-in types](#built-in-types)
- [Custom types](#custom-types) created through
  [`CREATE TYPE`](../create-type)

## Built-in types

| Type | Aliases | Use | Size (bytes) | Catalog name | Syntax |
|----|----|----|----|----|----|
| [`bigint`](integer) | `int8` | Large signed integer | 8 | Named | `123` |
| [`boolean`](boolean) | `bool` | State of `TRUE` or `FALSE` | 1 | Named | `TRUE`, `FALSE` |
| [`bytea`](bytea) | `bytea` | Unicode string | Variable | Named | `'\xDEADBEEF'` or `'\\000'` |
| [`date`](date) |  | Date without a specified time | 4 | Named | `DATE '2007-02-01'` |
| [`double precision`](float) | `float`, `float8`, `double` | Double precision floating-point number | 8 | Named | `1.23` |
| [`integer`](integer) | `int`, `int4` | Signed integer | 4 | Named | `123` |
| [`interval`](interval) |  | Duration of time | 32 | Named | `INTERVAL '1-2 3 4:5:6.7'` |
| [`jsonb`](jsonb) | `json` | JSON | Variable | Named | `'{"1":2,"3":4}'::jsonb` |
| [`map`](map) |  | Map with [`text`](text) keys and a uniform value type | Variable | Anonymous | `'{a => 1, b => 2}'::map[text=>int]` |
| [`list`](list) |  | Multidimensional list | Variable | Anonymous | `LIST[[1,2],[3]]` |
| [`numeric`](numeric) | `decimal` | Signed exact number with user-defined precision and scale | 16 | Named | `1.23` |
| [`oid`](oid) |  | PostgreSQL object identifier | 4 | Named | `123` |
| [`real`](float) | `float4` | Single precision floating-point number | 4 | Named | `1.23` |
| [`record`](record) |  | Tuple with arbitrary contents | Variable | Unnameable | `ROW($expr, ...)` |
| [`smallint`](integer) | `int2` | Small signed integer | 2 | Named | `123` |
| [`text`](text) | `string` | Unicode string | Variable | Named | `'foo'` |
| [`time`](time) |  | Time without date | 4 | Named | `TIME '01:23:45'` |
| [`uint2`](uint) |  | Small unsigned integer | 2 | Named | `123` |
| [`uint4`](uint) |  | Unsigned integer | 4 | Named | `123` |
| [`uint8`](uint) |  | Large unsigned integer | 8 | Named | `123` |
| [`timestamp`](timestamp) |  | Date and time | 8 | Named | `TIMESTAMP '2007-02-01 15:04:05'` |
| [`timestamp with time zone`](timestamp) | `timestamp with time zone` | Date and time with timezone | 8 | Named | `TIMESTAMPTZ '2007-02-01 15:04:05+06'` |
| [Arrays](array) (`[]`) |  | Multidimensional array | Variable | Named | `ARRAY[...]` |
| [`uuid`](uuid) |  | UUID | 16 | Named | `UUID 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'` |

#### Catalog name

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th>Value</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><strong>Named</strong></td>
<td>Named types can be referred to using a qualified object name, i.e.
they are objects within the <code>pg_catalog</code> schema. Each named
type a unique OID.</td>
</tr>
<tr>
<td><strong>Anonymous</strong></td>
<td>Anonymous types cannot be referred to using a qualified object name,
i.e. they do not exist as objects anywhere.<br />
<br />
Anonymous types do not have unique OIDs for all of their possible
permutations, e.g. <code>int4 list</code>, <code>float8 list</code>, and
<code>date list list</code> share the same OID.<br />
<br />
You can create named versions of some anonymous types using <a
href="#custom-types">custom types</a>.</td>
</tr>
<tr>
<td><strong>Unnameable</strong></td>
<td>Unnameable types are anonymous and do not yet support being custom
types.</td>
</tr>
</tbody>
</table>

## Custom types

Custom types, in general, provide a mechanism to create names for
specific instances of anonymous types to suit users’ needs.

However, types are considered custom if the type:

- Was created through [`CREATE TYPE`](../create-type).
- Contains a reference to a custom type.

To create custom types, see [`CREATE TYPE`](../create-type).

### Use

Currently, custom types only provides a shorthand for referring to
otherwise-annoying-to-type names.

### Casts

Structurally equivalent types can be cast to and from one another; the
required context depends on the types themselves, though.

| From          | To            | Cast permitted                           |
|---------------|---------------|------------------------------------------|
| Custom type   | Built-in type | Implicitly                               |
| Built-in type | Custom type   | Implicitly                               |
| Custom type 1 | Custom type 2 | [For explicit casts](../functions/cast/) |

### Equality

Values in custom types are *never* considered equal to:

- Other custom types, irrespective of their structure or value.
- Built-in types, but built-in types can be coerced to and from
  structurally equivalent custom types.

### Polymorphism

When using custom types as values for [polymorphic
functions](list/#polymorphism), the following additional constraints
apply:

- If any value passed to a polymorphic parameter is a custom type, the
  resultant type must use the custom type in the appropriate location.

  For example, if a custom type is used as:

  - `listany`, the resultant `list` must be of exactly the same type.
  - `listelementany`, the resultant `list`’s element must be of the
    custom type.

- If custom types and built-in types are both used, the resultant type
  is the “least custom type” that can be derived––i.e. the resultant
  type will have the fewest possible layers of custom types that still
  fulfill all constraints. Materialize will neither create nor discover
  a custom type that fills the constraints, nor will it coerce a custom
  type to a built-in type.

  For example, if appending a custom `list` to a built-in `list list`,
  the resultant type will be a `list` of custom `list`s.

#### Examples

This is a little easier to understand if we make it concrete, so we’ll
focus on concatenating two lists and appending an element to list.

For these operations, Materialize uses the following polymorphic
parameters:

- `listany`, which accepts any `list`, and constrains all lists to being
  of the same structurally equivalent type.
- `listelementany`, which accepts any type, but must be equal to the
  element type of the `list` type used with `listany`. For instance, if
  `listany` is constrained to being `int4 list`, `listelementany` must
  be `int4`.

When concatenating two lists, we’ll use `list_cat` whose signature is
`list_cat(l: listany, r: listany)`.

If we concatenate a custom `list` (in this example, `custom_list`) and a
structurally equivalent built-in `list` (`int4 list`), the result is of
the same type as the custom `list` (`custom_list`).

<div class="highlight">

``` chroma
CREATE TYPE custom_list AS LIST (ELEMENT TYPE int4);

SELECT pg_typeof(
  list_cat('{1}'::custom_list, '{2}'::int4 list)
) AS custom_list_built_in_list_cat;
```

</div>

```
 custom_list_built_in_list_cat
-------------------------------
 custom_list
```

When appending an element to a list, we’ll use `list_append` whose
signature is `list_append(l: listany, e: listelementany)`.

If we append a structurally appropriate element (`int4`) to a custom
`list` (`custom_list`), the result is of the same type as the custom
`list` (`custom_list`).

<div class="highlight">

``` chroma
SELECT pg_typeof(
  list_append('{1}'::custom_list, 2)
) AS custom_list_built_in_element_cat;
```

</div>

```
 custom_list_built_in_element_cat
----------------------------------
 custom_list
```

If we append a structurally appropriate custom element (`custom_list`)
to a built-in `list` (`int4 list list`), the result is a `list` of
custom elements.

<div class="highlight">

``` chroma
SELECT pg_typeof(
  list_append('{{1}}'::int4 list list, '{2}'::custom_list)
) AS built_in_list_custom_element_append;
```

</div>

```
 built_in_list_custom_element_append
-------------------------------------
 custom_list list
```

This is the “least custom type” we could support for these values––i.e.
Materialize will not create or discover a custom type whose elements are
`custom_list`, nor will it coerce `custom_list` into an anonymous
built-in list.

Note that `custom_list list` is considered a custom type because it
contains a reference to a custom type. Because it’s a custom type, it
enforces custom types’ polymorphic constraints.

For example, values of type `custom_list list` and `custom_nested_list`
cannot both be used as `listany` values for the same function:

<div class="highlight">

``` chroma
CREATE TYPE custom_nested_list AS LIST (element_type=custom_list);

SELECT list_cat(
  -- result is "custom_list list"
  list_append('{{1}}'::int4 list list, '{2}'::custom_list),
  -- result is custom_nested_list
  '{{3}}'::custom_nested_list
);
```

</div>

```
ERROR: Cannot call function list_cat(custom_list list, custom_nested_list)...
```

As another example, when using `custom_list list` values for `listany`
parameters, you can only use `custom_list` or `int4 list` values for
`listelementany` parameters––using any other custom type will fail:

<div class="highlight">

``` chroma
CREATE TYPE second_custom_list AS LIST (element_type=int4);

SELECT list_append(
  -- elements are custom_list
  '{{1}}'::custom_nested_list,
  -- second_custom_list is not interoperable with custom_list because both
  -- are custom
  '{2}'::second_custom_list
);
```

</div>

```
ERROR:  Cannot call function list_append(custom_nested_list, second_custom_list)...
```

To make custom types interoperable, you must cast them to the same type.
For example, casting `custom_nested_list` to `custom_list list` (or vice
versa) makes the values passed to `listany` parameters of the same
custom type:

<div class="highlight">

``` chroma
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

</div>

```
 complex_list_cat
------------------
 custom_list list
```

</div>

<a href="#top" class="back-to-top">Back to top ↑</a>

<div class="theme-switcher">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzeXN0ZW0iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+U3lzdGVtIFRoZW1lPC90aXRsZT4KICAgICAgICA8cGF0aCBkPSJNMjU2IDE3NmE4MCA4MCAwIDEwODAgODAgODAuMjQgODAuMjQgMCAwMC04MC04MHptMTcyLjcyIDgwYTE2NS41MyAxNjUuNTMgMCAwMS0xLjY0IDIyLjM0bDQ4LjY5IDM4LjEyYTExLjU5IDExLjU5IDAgMDEyLjYzIDE0Ljc4bC00Ni4wNiA3OS41MmExMS42NCAxMS42NCAwIDAxLTE0LjE0IDQuOTNsLTU3LjI1LTIzYTE3Ni41NiAxNzYuNTYgMCAwMS0zOC44MiAyMi42N2wtOC41NiA2MC43OGExMS45MyAxMS45MyAwIDAxLTExLjUxIDkuODZoLTkyLjEyYTEyIDEyIDAgMDEtMTEuNTEtOS41M2wtOC41Ni02MC43OEExNjkuMyAxNjkuMyAwIDAxMTUxLjA1IDM5M0w5My44IDQxNmExMS42NCAxMS42NCAwIDAxLTE0LjE0LTQuOTJMMzMuNiAzMzEuNTdhMTEuNTkgMTEuNTkgMCAwMTIuNjMtMTQuNzhsNDguNjktMzguMTJBMTc0LjU4IDE3NC41OCAwIDAxODMuMjggMjU2YTE2NS41MyAxNjUuNTMgMCAwMTEuNjQtMjIuMzRsLTQ4LjY5LTM4LjEyYTExLjU5IDExLjU5IDAgMDEtMi42My0xNC43OGw0Ni4wNi03OS41MmExMS42NCAxMS42NCAwIDAxMTQuMTQtNC45M2w1Ny4yNSAyM2ExNzYuNTYgMTc2LjU2IDAgMDEzOC44Mi0yMi42N2w4LjU2LTYwLjc4QTExLjkzIDExLjkzIDAgMDEyMDkuOTQgMjZoOTIuMTJhMTIgMTIgMCAwMTExLjUxIDkuNTNsOC41NiA2MC43OEExNjkuMyAxNjkuMyAwIDAxMzYxIDExOWw1Ny4yLTIzYTExLjY0IDExLjY0IDAgMDExNC4xNCA0LjkybDQ2LjA2IDc5LjUyYTExLjU5IDExLjU5IDAgMDEtMi42MyAxNC43OGwtNDguNjkgMzguMTJhMTc0LjU4IDE3NC41OCAwIDAxMS42NCAyMi42NnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="system" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzdW4iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+TGlnaHQgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0yMzQgMjZoNDR2OTJoLTQ0ek0yMzQgMzk0aDQ0djkyaC00NHpNMzM4LjAyNSAxNDIuODU3bDY1LjA1NC02NS4wNTQgMzEuMTEzIDMxLjExMy02NS4wNTQgNjUuMDU0ek03Ny44MTUgNDAzLjA3NGw2NS4wNTQtNjUuMDU0IDMxLjExMyAzMS4xMTMtNjUuMDU0IDY1LjA1NHpNMzk0IDIzNGg5MnY0NGgtOTJ6TTI2IDIzNGg5MnY0NEgyNnpNMzM4LjAyOSAzNjkuMTRsMzEuMTEyLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMiAzMS4xMTJ6TTc3LjgwMiAxMDguOTJsMzEuMTEzLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMyAzMS4xMTJ6TTI1NiAzNThhMTAyIDEwMiAwIDExMTAyLTEwMiAxMDIuMTIgMTAyLjEyIDAgMDEtMTAyIDEwMnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="sun" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJtb29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgPHRpdGxlPkRhcmsgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0xNTIuNjIgMTI2Ljc3YzAtMzMgNC44NS02Ni4zNSAxNy4yMy05NC43N0M4Ny41NCA2Ny44MyAzMiAxNTEuODkgMzIgMjQ3LjM4IDMyIDM3NS44NSAxMzYuMTUgNDgwIDI2NC42MiA0ODBjOTUuNDkgMCAxNzkuNTUtNTUuNTQgMjE1LjM4LTEzNy44NS0yOC40MiAxMi4zOC02MS44IDE3LjIzLTk0Ljc3IDE3LjIzLTEyOC40NyAwLTIzMi42MS0xMDQuMTQtMjMyLjYxLTIzMi42MXoiIC8+CiAgICAgIDwvc3ZnPg=="
class="moon" />

</div>

<div>

<a
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/types/_index.md"
class="btn-ghost"><img
src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTgiIGhlaWdodD0iMTgiIHZpZXdib3g9IjAgMCAyMyAyMyIgZmlsbD0iY3VycmVudENvbG9yIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPgogICAgICAgIDxwYXRoIGQ9Ik0yMC44OTQ1IDExLjQ5NjhDMjAuODk0NSAxMC4yMzk0IDIwLjYxNTEgOS4wNTE5IDIwLjEyNjEgNy44NjQzN0MxOS42MzcxIDYuNzQ2NjkgMTguOTM4NSA1LjY5ODg4IDE4LjE3MDEgNC45MzA0N0MxNy40MDE3IDQuMTYyMDcgMTYuMzUzOSAzLjQ2MzUgMTUuMjM2MiAyLjk3NDUyQzE0LjExODUgMi40ODU1MyAxMi44NjExIDIuMjA2MTMgMTEuNjAzOCAyLjIwNjEzQzEwLjM0NjQgMi4yMDYxMyA5LjE1ODg0IDIuNDg1NTMgNy45NzEzIDIuOTc0NTJDNi44NTM2MiAzLjQ2MzUgNS44MDU3OSA0LjE2MjA3IDUuMDM3MzggNC45MzA0N0M0LjI2ODk4IDUuNjk4ODggMy41NzA0NCA2Ljc0NjY5IDMuMDgxNDUgNy44NjQzN0MyLjU5MjQ3IDguOTgyMDUgMi4zMTMwNCAxMC4yMzk0IDIuMzEzMDQgMTEuNDk2OEMyLjMxMzA0IDEzLjUyMjYgMi45NDE3NCAxNS4zMzg5IDQuMTI5MjggMTcuMDE1NEM1LjMxNjgxIDE4LjY5MTkgNi45MjM0NyAxOS44MDk2IDguODA5NTYgMjAuMzY4NFYxNy45MjM1QzguMjUwNzIgMTcuOTkzNCA3Ljk3MTI5IDE3Ljk5MzMgNy44MzE1OCAxNy45OTMzQzYuNzgzNzYgMTcuOTkzMyA2LjAxNTM1IDE3LjUwNDQgNS41OTYyMiAxNi41MjY0QzUuNDU2NTEgMTYuMTc3MSA1LjI0Njk1IDE1LjgyNzggNS4wMzczOCAxNS42MTgzQzQuOTY3NTMgMTUuNTQ4NCA0Ljg5NzY4IDE1LjQ3ODYgNC43NTc5NyAxNS4zMzg5QzQuNjE4MjYgMTUuMTk5MiA0LjQ3ODU0IDE1LjEyOTMgNC4zMzg4MyAxNC45ODk2QzQuMTk5MTIgMTQuODQ5OSA0LjEyOTI4IDE0Ljc4IDQuMTI5MjggMTQuNzhDNC4xMjkyOCAxNC42NDAzIDQuMjY4OTggMTQuNjQwMyA0LjU0ODQgMTQuNjQwM0M0LjgyNzgyIDE0LjY0MDMgNS4xMDcyNCAxNC43MTAyIDUuMzE2ODEgMTQuODQ5OUM1LjUyNjM3IDE0Ljk4OTYgNS43MzU5NCAxNS4xMjkzIDUuODc1NjUgMTUuMzM4OUM2LjAxNTM2IDE1LjU0ODQgNi4xNTUwNyAxNS43NTggNi4zNjQ2MyAxNS45Njc2QzYuNTA0MzQgMTYuMTc3MSA2LjcxMzkxIDE2LjMxNjggNi45MjM0OCAxNi40NTY1QzcuMTMzMDQgMTYuNTk2MyA3LjQxMjQ2IDE2LjY2NjEgNy43NjE3MyAxNi42NjYxQzguMTgwODYgMTYuNjY2MSA4LjUzMDE0IDE2LjU5NjMgOC45NDkyNyAxNi40NTY1QzkuMDg4OTggMTUuODk3NyA5LjQzODI1IDE1LjQ3ODYgOS44NTczOCAxNS4xMjkzQzguMjUwNzIgMTQuOTg5NiA3LjA2MzE4IDE0LjU3MDUgNi4yOTQ3NyAxMy45NDE4QzUuNTI2MzcgMTMuMzEzMSA1LjEwNzI0IDEyLjE5NTQgNS4xMDcyNCAxMC42NTg2QzUuMTA3MjQgOS41NDA4OSA1LjQ1NjUyIDguNTYyOTQgNi4xNTUwNyA3Ljc5NDUzQzYuMDE1MzYgNy4zNzU0IDUuOTQ1NSA2Ljk1NjI2IDUuOTQ1NSA2LjUzNzEzQzUuOTQ1NSA1Ljk3ODI5IDYuMDg1MjEgNS40MTk0NiA2LjM2NDYzIDQuOTMwNDdDNi45MjM0NyA0LjkzMDQ3IDcuNDEyNDUgNS4wMDAzMiA3LjgzMTU4IDUuMjA5ODlDOC4yNTA3MSA1LjQxOTQ1IDguNzM5NyA1LjY5ODg2IDkuMjk4NTQgNi4xMTc5OUMxMC4wNjY5IDUuOTc4MjggMTAuODM1NCA1LjgzODU4IDExLjc0MzUgNS44Mzg1OEMxMi41MTE5IDUuODM4NTggMTMuMjgwMyA1LjkwODQ1IDEzLjk3ODggNi4wNDgxNkMxNC41Mzc3IDUuNjI5MDMgMTUuMDI2NyA1LjM0OTYgMTUuNDQ1OCA1LjIwOTg5QzE1Ljg2NDkgNS4wMDAzMiAxNi4zNTM5IDQuOTMwNDcgMTYuOTEyNyA0LjkzMDQ3QzE3LjE5MjIgNS40MTk0NiAxNy4zMzE5IDUuOTc4MjkgMTcuMzMxOSA2LjUzNzEzQzE3LjMzMTkgNi45NTYyNiAxNy4yNjIgNy4zNzU0IDE3LjEyMjMgNy43MjQ2N0MxNy44MjA5IDguNDkzMDggMTguMTcwMSA5LjQ3MTA1IDE4LjE3MDEgMTAuNTg4N0MxOC4xNzAxIDEyLjEyNTUgMTcuNzUxIDEzLjE3MzQgMTYuOTgyNiAxMy44NzE5QzE2LjIxNDIgMTQuNTcwNSAxNS4wMjY2IDE0LjkxOTcgMTMuNDIgMTUuMDU5NEMxNC4xMTg1IDE1LjU0ODQgMTQuMzk4IDE2LjE3NzEgMTQuMzk4IDE2Ljk0NTVWMjAuMjI4N0MxNi4zNTM5IDE5LjYgMTcuODkwNyAxOC40ODIzIDE5LjA3ODIgMTYuODc1N0MyMC4yNjU4IDE1LjMzODkgMjAuODk0NSAxMy41MjI2IDIwLjg5NDUgMTEuNDk2OFpNMjIuNzEwNyAxMS40OTY4QzIyLjcxMDcgMTMuNTIyNiAyMi4yMjE3IDE1LjQwODcgMjEuMjQzOCAxNy4wODUyQzIwLjI2NTggMTguODMxNiAxOC44Njg3IDIwLjE1ODggMTcuMTkyMiAyMS4xMzY4QzE1LjQ0NTggMjIuMTE0OCAxMy42Mjk2IDIyLjYwMzggMTEuNjAzOCAyMi42MDM4QzkuNTc3OTYgMjIuNjAzOCA3LjY5MTg4IDIyLjExNDggNi4wMTUzNiAyMS4xMzY4QzQuMjY4OTggMjAuMTU4OCAyLjk0MTc0IDE4Ljc2MTggMS45NjM3NyAxNy4wODUyQzAuOTg1Nzk2IDE1LjMzODkgMC40OTY4MDcgMTMuNTIyNiAwLjQ5NjgwNyAxMS40OTY4QzAuNDk2ODA3IDkuNDcxMDQgMC45ODU3OTYgNy41ODQ5NiAxLjk2Mzc3IDUuOTA4NDRDMi45NDE3NCA0LjE2MjA2IDQuMzM4ODQgMi44MzQ4MyA2LjAxNTM2IDEuODU2ODZDNy43NjE3MyAwLjg3ODg4NiA5LjU3Nzk2IDAuMzg5ODk3IDExLjYwMzggMC4zODk4OTdDMTMuNjI5NiAwLjM4OTg5NyAxNS41MTU2IDAuODc4ODg2IDE3LjE5MjIgMS44NTY4NkMxOC45Mzg1IDIuODM0ODMgMjAuMjY1OCA0LjIzMTkyIDIxLjI0MzggNS45MDg0NEMyMi4yMjE3IDcuNTg0OTYgMjIuNzEwNyA5LjQ3MTA0IDIyLjcxMDcgMTEuNDk2OFoiIC8+CiAgICAgIDwvc3ZnPg==" />
Edit this page</a>

</div>

<div class="footer-links">

[Home](https://materialize.com) [Status](https://status.materialize.com)
[GitHub](https://github.com/MaterializeInc/materialize)
[Blog](https://materialize.com/blog)
[Contact](https://materialize.com/contact)

Cookie Preferences

[Privacy Policy](https://materialize.com/privacy-policy/)

</div>

© 2025 Materialize Inc.

</div>
