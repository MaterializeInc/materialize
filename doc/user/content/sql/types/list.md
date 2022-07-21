---
title: "List types"
description: "Lists are ordered sequences of homogenously typed elements"
menu:
  main:
    parent: sql-types
---

Lists are ordered sequences of homogenously typed elements. Lists' elements can
be other lists, known as "layered lists."

Detail | Info
-------|------
**Quick Syntax** | `LIST[[1,2],[3]]`
**Size** | Variable
**Catalog name** | Anonymous, but [nameable](../../create-type)

## Syntax

{{< diagram "type-list.svg" >}}

Field | Use
------|-----
_element_ | An element of any [data type](../) to place in the list. Note that all elements must be of the same type.

## List functions + operators

#### Polymorphism

<!--
  If any type other than list supports fully polymorphic functions, this
  should be moved to doc/user/content/sql/types/_index.md
-->

List functions and operators are polymorphic, which applies the following
constraints to their arguments:

- All instances of `listany` must be lists of the same type.
- All instances of `listelementany` must be of the same type.
- If a function uses both `listany` and `listelementany` parameters, all
  instances of `listany` must be a list of the type used in `listelementany`.
- If any value passed to a polymorphic parameter is a [custom type](/sql/types/#custom-types), additional constraints apply.

### Operators

{{% list-operators %}}

### Functions

{{< fnlist "List" >}}

## Details

### Type name

The name of a list type is the name of its element type followed by `list`, e.g.
`int list`. This rule can be applied recursively, e.g. `int list list` for a
`list` of `int list`s which is a two-layer list.

### Construction

You can construct lists using the `LIST` expression:

```sql
SELECT LIST[1, 2, 3];
```
```nofmt
  list
---------
 {1,2,3}
```

You can nest `LIST` constructors to create layered lists:

```sql
SELECT LIST[LIST['a', 'b'], LIST['c']];
```
```nofmt
    list
-------------
 {{a,b},{c}}
```

You can also elide the `LIST` keyword from the interior list expressions:

```sql
SELECT LIST[['a', 'b'], ['c']];
```
```nofmt
    list
-------------
 {{a,b},{c}}
```

Alternatively, you can construct a list from the results of a subquery. The
subquery must return a single column. Note that, in this form of the `LIST`
expression, parentheses are used rather than square brackets.

```sql
SELECT LIST(SELECT x FROM test0 WHERE x > 0 ORDER BY x DESC LIMIT 3);
```
```nofmt
    x
---------
 {4,3,2}
```

Layered lists can be “ragged”, i.e. the length of lists in each layer can differ
from one another. This differs from `array`, which requires that each dimension
of a multidimensional array only contain arrays of the same length.

Note that you can also construct lists using the available [`text`
cast](#text-to-list-casts).

### Accessing lists

You can access elements of lists through:

  - [Indexing](#indexing-elements) for individual elements
  - [Slicing](#slicing-ranges) for ranges of elements

#### Indexing elements

To access an individual element of list, you can “index” into it using brackets
(`[]`) and 1-index element positions:

```sql
SELECT LIST[['a', 'b'], ['c']][1];
```
```nofmt
 ?column?
----------
 {a,b}
```

Indexing operations can be chained together to descend the list’s layers:

```sql
SELECT LIST[['a', 'b'], ['c']][1][2];
```
```nofmt
 ?column?
----------
 b
```

If the index is invalid (either less than 1 or greater than the maximum index),
lists return _NULL_.

```sql
SELECT LIST[['a', 'b'], ['c']][1][5] AS exceed_index;
```
```nofmt
 exceed_index
--------------

```

Lists have types based on their layers (unlike arrays' dimension), and error if
you attempt to index a non-list element (i.e. indexing past the list’s last
layer):

```sql
SELECT LIST[['a', 'b'], ['c']][1][2][3];
```
```nofmt
ERROR:  cannot subscript type string
```

#### Slicing ranges

To access contiguous ranges of a list, you can slice it using `[first index :
last index]`, using 1-indexed positions:

```sql
SELECT LIST[1,2,3,4,5][2:4] AS two_to_four;
```
```nofmt
  slice
---------
 {2,3,4}
```

You can omit the first index to use the first value in the list, and omit the
last index to use all elements remaining in the list.

```sql
SELECT LIST[1,2,3,4,5][:3] AS one_to_three;
```
```nofmt
 one_to_three
--------------
 {1,2,3}
```

```sql
SELECT LIST[1,2,3,4,5][3:] AS three_to_five;
```
```nofmt
 three_to_five
---------------
 {3,4,5}
```

If the first index exceeds the list's maximum index, the operation returns an
empty list:

```sql
SELECT LIST[1,2,3,4,5][10:] AS exceed_index;
```
```nofmt
 exceed_index
--------------
 {}
```

If the last index exceeds the list’s maximum index, the operation returns all
remaining elements up to its final element.

```sql
SELECT LIST[1,2,3,4,5][2:10] AS two_to_end;
```
```nofmt
 two_to_end
------------
 {2,3,4,5}
```

Performing successive slices behaves more like a traditional programming
language taking slices of an array, rather than PostgreSQL's slicing, which
descends into each layer.

```sql
SELECT LIST[1,2,3,4,5][2:][2:3] AS successive;
```
```nofmt
 successive
------------
 {3,4}
```

### Output format

We represent lists textually using an opening curly brace (`{`), followed by the
textual representation of each element separated by commas (`,`), terminated by
a closing curly brace (`}`). For layered lists, this format is applied
recursively to each list layer. No additional whitespace is added.

We render _NULL_ elements as the literal string `NULL`. Non-null elements are
rendered as if that element had been cast to `text`.

Elements whose textual representations contain curly braces, commas, whitespace,
double quotes, backslashes, or which are exactly the string `NULL` (in any case)
get wrapped in double quotes in order to distinguish the representation of the
element from the representation of the containing list. Within double quotes,
backslashes and double quotes are backslash-escaped.

The following example demonstrates the output format and includes many of the
aforementioned special cases.

```sql
SELECT LIST[['a', 'white space'], [NULL, ''], ['escape"m\e', 'nUlL']];
```
```nofmt
                        list
-----------------------------------------------------
 {{a,"white space"},{NULL,""},{"escape\"m\\e",nUlL}}
```

### `text` to `list` casts

To cast `text` to a `list`, you must format the text similar to list's [output
format](#output-format).

The text you cast must:

- Begin with an opening curly brace (`{`) and end with a closing curly brace
  (`}`)
- Separate each element with a comma (`,`)
- Use a representation for elements that can be cast from text to the list's
  element type.

    For example, to cast `text` to a `date list`, you use `date`'s `text`
    representation:

    ```sql
    SELECT '{2001-02-03, 2004-05-06}'::date list as date_list;
    ```

    ```nofmt
            date_list
    -------------------------
     {2001-02-03,2004-05-06}
    ```

    You cannot include the `DATE` keyword.
- Escape any special representations (`{`, `}`, `"`, `\`, whitespace, or the
  literal string `NULL`) you want parsed as text using...
    - Double quotes (`"`) to escape an entire contiguous string
    - Backslashes (`\`) to escape an individual character in any context, e.g.
      `\"` to escape double quotes within an escaped string.

      Note that escaping any character in the string "null" (case-insensitive)
      generates a `text` value equal to "NULL" and not a _NULL_ value.

    For example:

    ```sql
    SELECT '{
        "{brackets}",
        "\"quotes\"",
        \\slashes\\,
        \ leading space,
        trailing space\ ,
        \NULL
    }'::text list as escape_examples;
    ```

    ```nofmt
                                       escape_examples
    -------------------------------------------------------------------------------------
     {"{brackets}","\"quotes\"","\\slashes\\"," leading space","trailing space ","NULL"}
    ```

    Note that all unescaped whitespace is trimmed.

### List vs. array

`list` is a Materialize-specific type and is designed to provide:

- Similar semantics to Avro and JSON arrays
- A more ergonomic experience than PostgreSQL-style arrays

This section focuses on the distinctions between Materialize’s `list` and
`array` types, but with some knowledge of the PostgreSQL array type, you should
also be able to infer how list differs from it, as well.

#### Terminology

Feature | Array term | List term
--------|------------|-----------
**Nested structure** | Multidimensional array | Layered list
**Accessing single element** | Subscripting | Indexing<sup>1</sup>
**Accessing range of elements** | Subscripting | Slicing<sup>1</sup>

<sup>1</sup>In places some places, such as error messages, Materialize refers to
both list indexing and list slicing as subscripting.

#### Type definitions

**Lists** require explicitly declared layers, and each possible layer is treated
as a distinct type. For example, a list of `int`s with two layers is `int list
list` and one with three is `int list list list`. Because their number of layers
differ, they cannot be used interchangeably.

**Arrays** only have one type for each non-array type, and all arrays share that
type irrespective of their dimensions. This means that arrays of the same
element type can be used interchangeably in most situations, without regard to
their dimension. For example, arrays of `text` are all of type `text[]` and 1D,
2D, and 3D `text[]` can all be used in the same columns.

#### Nested structures

**Lists** allow each element of a layer to be of a different length. For
example, in a two-layer list, each of the first layer’s lists can be of a
different length:

```sql
SELECT LIST[[1,2], [3]] AS ragged_list;
```
```
ragged_list
-------------
{{1,2},{3}}
```

This is known as a "ragged list."

**Arrays** require each element of a dimension to have the same length. For
example, if the first element in a 2D list has a length of 2, all subsequent
members must also have a length of 2.

```sql
SELECT ARRAY[[1,2], [3]] AS ragged_array;
```
```
ERROR:  number of array elements (3) does not match declared cardinality (4)
```
#### Accessing single elements

**Lists** support accessing single elements via [indexing](#indexing-elements).
When indexed, lists return a value with one less layer than the indexed list.
For example, indexing a two-layer list returns a one-layer list.

```sql
SELECT LIST[['foo'],['bar']][1] AS indexing;
```
```
 indexing
--------------
 {foo}
```

Attempting to index twice into a `text list` (i.e. a one-layer list), fails
because you cannot index `text`.

```sql
SELECT LIST['foo'][1][2];
```
```
ERROR:  cannot subscript type text
```

##### Accessing ranges of elements

**Lists** support accessing ranges of elements via [slicing](#slicing-ranges).
However, lists do not currently support PostgreSQL-style slicing, which
descends into layers in each slice.

**Arrays** require each element of a dimension to have the same length. For
example, if the first element in a 2D list has a length of 2, all subsequent
members must also have a length of 2.

### Custom types

You can create [custom `list` types](/sql/types/#custom-types), which lets you
create a named entry in the catalog for a specific type of list.

Currently, custom types only provides a shorthand for referring to
otherwise-annoying-to-type names, but in the future will provide [binary
encoding and decoding][binary] for these types, as well.

Note that custom `list` types have special rules regarding [polymorphism](/sql/types/#polymorphism).

[binary]:https://github.com/MaterializeInc/materialize/issues/4628

### Valid casts

#### Between `list`s

You can cast one list type to another if the type of the source list’s elements
can be cast to the target list’s elements’ type. For example, `float list` can
be cast to `int list`, but `float list` cannot be cast to `timestamp list`.

Note that this rule also applies to casting between custom list types.

#### From `list`

You can [cast](../../functions/cast) `list` to:

- [`text`](../text) (implicitly)
- Other `lists` as noted above.

#### To `list`

You can [cast](../../functions/cast) the following types to `list`:

- [arrays](../array) (explicitly). See [details](../array#array-to-list-casts).
- [`text`](../text) (explicitly). See [details](#text-to-list-casts).
- Other `lists` as noted above.

### Known limitations

- `list` data can only be sent to PostgreSQL as `text` {{% gh 4628 %}}

## Examples

### Literals

```sql
SELECT LIST[[1.5, NULL],[2.25]];
```
```nofmt
         list
----------------------
 {{1.50,NULL},{2.25}}
```

### Casting between lists

```sql
SELECT LIST[[1.5, NULL],[2.25]]::int list list;
```
```nofmt
      list
----------------
 {{2,NULL},{2}}
```

### Casting to text

```sql
SELECT LIST[[1.5, NULL],[2.25]]::text;
```
```nofmt
      list
------------------
 {{1,NULL},{2}}
```

Despite the fact that the output looks the same as the above examples, it is, in
fact, `text`.

```sql
SELECT length(LIST[[1.5, NULL],[2.25]]::text);
```
```nofmt
 length
--------
     20
```

### Casting from text

```sql
SELECT '{{1.5,NULL},{2.25}}'::numeric(38,2) list list AS text_to_list;
```
```nofmt
     text_to_list
----------------------
 {{1.50,NULL},{2.25}}
```
