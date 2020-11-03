---
title: "List Data Types"
description: "Lists are an ordered, multidimensional sequence of any type, including other lists"
menu:
  main:
    parent: sql-types
---

{{< version-added v0.5.1 >}}

Lists are an ordered, multidimensional sequence of any type, including other lists.

Detail | Info
-------|------
**Quick Syntax** | `LIST[[1,2],[3]]`
**Size** | Variable

## Syntax

{{< diagram "type-list.svg" >}}

Field | Use
------|-----
_element_ | An element of any [data type](../) to place in the list.

## List functions + operators

#### Polymorphism

List functions and operators are polymorphic, which applies the following constraints to their
arguments:

- All instances of `listany` must be lists of the same type.
- All instances of `listelementany` must be of the same type.
  - If a function uses both `listany` and `listelementany`, all instances of
    `listany` must be a list of the type used in `listelementany`.

### Operators

{{% list-operators %}}

### Functions

{{< fnlist "List" >}}

## Details

### Type name

The name of a list type is the name of its element type followed by `list`, e.g.
`int list`. This rule can be applied recursively, e.g. `int list list` for a
`list` of `int list`s, i.e. a 2D list.

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

You can nest `LIST` constructors to create multidimensional lists:

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

Multidimensional lists can be "ragged", i.e. the length of each nested list
can differ from others. This differs from `array`, which does not
support ragged multideimensional arrays, i.e. the length of each nested array
must be the same.

Note that you can also construct lists using the available [`text`
cast](#text-to-list-casts).

### Accessing lists

#### Elements (subscripting)

Accessing elements of `list`s (known as subscripting) uses brackets (`[]`)
and 1-index element positions:

```sql
SELECT LIST[['a', 'b'], ['c']][1];
```
```nofmt
 ?column?
----------
 {a,b}
```

Subscripts can be chained together to descend the list's dimensions:

```sql
SELECT LIST[['a', 'b'], ['c']][1][2];
```
```nofmt
 ?column?
----------
 b
```

If the index is invalid (either less than 1, or greater the maximum index),
lists return _null_.

```sql
SELECT LIST[['a', 'b'], ['c']][1][5] AS exceed_index;
```
```nofmt
 exceed_index
--------------

```

Lists have types based on their dimension (unlike arrays), and error if you
attmept to subscript a non-list element (i.e. the depth of subscripting exceeds
the list'd maximum dimension):

```sql
SELECT LIST[['a', 'b'], ['c']][1][2][3];
```
```nofmt
ERROR:  cannot subscript type string
```

#### Ranges/slices (subscripting)

Slicing `lists` (confusingly _also_ known as subscripting) uses the format
`[first index : last index]`, using 1-indexed positions:

```sql
SELECT LIST[1,2,3,4,5][2:4] AS two_to_four;
```
```nofmt
  slice
---------
 {2,3,4}
```

You can omit the first index to use the first value in the `list`, and omit the
last index to use all elements remaining in the `list`.

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

If the first index exceeds the list's maximum index, the operation returns _null_:

```sql
SELECT LIST[1,2,3,4,5][10:] AS exceed_index;
```
```nofmt
 exceed_index
--------------

```

If the last index exceeds the list's maximum index, the operation returns all
remaining elements up to its final element.

```sql
SELECT LIST[1,2,3,4,5][2:10] AS two_to_end;
```
```nofmt
 two_to_end
------------
 {3,4,5}
```

#### Multidimensional slices

{{< experimental v0.5.1 >}}
Multidimensional slices
{{< /experimental >}}

To perform a slice on dimensions beyond the first, indicate the range you want
to slice along each dimension, separating the ranges with commas:

```sql
SELECT LIST[[1,2], [3,4]][1:2, 2:2] AS slice_second_dim;
```
```nofmt
 slice_second_dim
------------------
 {{2},{4}}
````

You can only slice along as many dimensions as the list contains:

```sql
SELECT LIST[[1,2], [3,4]][1:2, 2:2, 1:1] AS failed_third_dim_slice;
```
```nofmt
ERROR:  cannot slice on 3 dimensions; list only has 2 dimensions
```

### Output format

We represent lists textually using an opening curly brace (`{`), followed by the
textual representation of each element separated by commas (`,`), terminated by
a closing curly brace (`}`). For multidimensional lists, this format is applied
recursively to each list dimension. No additional whitespace is added.

We render _null_ elements as the literal string `NULL`. Non-null elements are
rendered as if that element had been cast to `text`.

An element whose textual representation contains curly braces, commas,
whitespace, double quotes, backslashes, or is exactly the string `NULL` (in any
case) is wrapped in double quotes in order to distinguish the representation of
the element from the representation of the containing list. Within double
quotes, backslashes and double quotes are backslash-escaped.

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

{{< version-added v0.5.2 >}}

To cast `text` to a `list`, you must format the text similar to list's
[output format](#output-format).

The text you cast must:

- Begin with an opening curly brace (`{`) and end with a closing curly brace (`}`)
- Separate each element with a comma (`,`)
- Use a representation for elements form that can be cast from text to the
  list's element type.

    For example, to cast `text` to a `date list`, you use `date`'s `text` representation:

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
      generates a `text` value equal to "NULL" and not a _null_ value.

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

`list` is a Materialize-specific type and is designed to provide...

- Similar semantics to Avro and JSON arrays
- A more ergonomic experience than PostgreSQL-style arrays

This section focuses on the distinctions between Materialize's `list` and
`array` types, but with some knowledge of the PostgreSQL array type, you should
also be able to infer how `list` differs from them, as well.

#### Number of dimensions

- **Lists** require explicitly declared dimensions, and each possible dimension is
  treated as a distinct type. This means lists of the same element type cannot
  be used interchangeably if their dimensions differ.

  For example, a 2D list of `int`s is `int list list` and a 3D one is `int list
  list list`. Because their dimensions differ, they cannot be used
  interchangeably.

  ##### Element subscripting

  Subscripting into a list returns a value with a dimension one less than the
  subscripted list. For example, subscripting into a 2D list returns a 1D list.

  ```sql
  SELECT LIST[['foo'],['bar']][1] AS subscripting;
  ```
  ```
   subscripting
  --------------
   {foo}
  ```

  Attempting to subscript twice into an `text list` (i.e. a 1D list), fails
  because you cannot subscript `text`.

  ```sql
  SELECT LIST['foo'][1][2];
  ```
  ```
  ERROR:  cannot subscript type text
  ```

  ##### Range subscripting/slicing

  [Multidimensional slicing](#multidimensional-slices) is expressed as a list of
  comma-separated ranges inside a single square bracket.

- **Arrays** only have one type for each non-array type, and all arrays share
  that type irrespective of their dimensions. This means that arrays of the same
  element type can be used interchangeably in most situations, without regard to
  their dimension.

  For example, arrays of `text` are all of type `text[]` and 1D, 2D, and 3D
  `text[]` can all be used in the same columns.

  ##### Subscripting

  Arrays currently only provide limited subscripting support and are not
  recommended.

#### Sizes of dimensions

- **Lists** allow each element of a dimension to be of a different length. For
  example, in a 2D list, each of the first dimension's lists can be of a
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

- **Arrays** require each element of a dimension to have the same length. For
  example, if the first element in a 2D list has a length of 2, all subsequent
  members must also have a length of 2.

  ```sql
  SELECT ARRAY[[1,2], [3]] AS ragged_array;
  ```
  ```
  ERROR:  number of array elements (3) does not match declared cardinality (4)
  ```

### Valid casts

#### Between `list`s

You can cast one `list` type to another if the source list's elements' type can
be cast to the target list's elements' type. For example, `float list` can be
cast to `int list`, but `float list` cannot be cast to `timestamp list`.

#### From `list`

You can [cast](../../functions/cast) `list` to:

- [`text`](../text)
- Other `lists` as noted above.

#### To `list`

You can [cast](../../functions/cast) the following types to `list`:

- [`text`](../text)&mdash;see [details](#text-to-list-casts)
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
