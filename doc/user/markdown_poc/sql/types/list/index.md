<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [SQL commands](/docs/sql/)  /  [SQL data
types](/docs/sql/types/)

</div>

# List types

Lists are ordered sequences of homogenously typed elements. Lists’
elements can be other lists, known as “layered lists.”

| Detail           | Info                                         |
|------------------|----------------------------------------------|
| **Quick Syntax** | `LIST[[1,2],[3]]`                            |
| **Size**         | Variable                                     |
| **Catalog name** | Anonymous, but [nameable](../../create-type) |

## Syntax

<div class="rr-diagram">

![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSI0MTMiIGhlaWdodD0iMTQ3Ij4KICAgPHBvbHlnb24gcG9pbnRzPSI5IDYxIDEgNTcgMSA2NSI+PC9wb2x5Z29uPgogICA8cG9seWdvbiBwb2ludHM9IjE3IDYxIDkgNTcgOSA2NSI+PC9wb2x5Z29uPgogICA8cmVjdCB4PSIzMSIgeT0iNDciIHdpZHRoPSI1MCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIyOSIgeT0iNDUiIHdpZHRoPSI1MCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMzkiIHk9IjY1Ij5MSVNUPC90ZXh0PgogICA8cmVjdCB4PSIxMjEiIHk9IjQ3IiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMTE5IiB5PSI0NSIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIxMjkiIHk9IjY1Ij5bPC90ZXh0PgogICA8cmVjdCB4PSIyMDciIHk9IjQ3IiB3aWR0aD0iNzIiIGhlaWdodD0iMzIiIC8+CiAgIDxyZWN0IHg9IjIwNSIgeT0iNDUiIHdpZHRoPSI3MiIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjIxNSIgeT0iNjUiPmVsZW1lbnQ8L3RleHQ+CiAgIDxyZWN0IHg9IjIwNyIgeT0iMyIgd2lkdGg9IjI0IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjIwNSIgeT0iMSIgd2lkdGg9IjI0IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIyMTUiIHk9IjIxIj4sPC90ZXh0PgogICA8cmVjdCB4PSIzMzkiIHk9IjQ3IiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iMzM3IiB5PSI0NSIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIzNDciIHk9IjY1Ij5dPC90ZXh0PgogICA8cmVjdCB4PSIxMjEiIHk9IjExMyIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjExOSIgeT0iMTExIiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjEyOSIgeT0iMTMxIj4oPC90ZXh0PgogICA8cmVjdCB4PSIxNjciIHk9IjExMyIgd2lkdGg9IjU4IiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSIxNjUiIHk9IjExMSIgd2lkdGg9IjU4IiBoZWlnaHQ9IjMyIiBjbGFzcz0ibm9udGVybWluYWwiIC8+CiAgIDx0ZXh0IGNsYXNzPSJub250ZXJtaW5hbCIgeD0iMTc1IiB5PSIxMzEiPnF1ZXJ5PC90ZXh0PgogICA8cmVjdCB4PSIyNDUiIHk9IjExMyIgd2lkdGg9IjI2IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjI0MyIgeT0iMTExIiB3aWR0aD0iMjYiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjI1MyIgeT0iMTMxIj4pPC90ZXh0PgogICA8cGF0aCBjbGFzcz0ibGluZSIgZD0ibTE3IDYxIGgyIG0wIDAgaDEwIG01MCAwIGgxMCBtMjAgMCBoMTAgbTI2IDAgaDEwIG00MCAwIGgxMCBtNzIgMCBoMTAgbS0xMTIgMCBsMjAgMCBtLTEgMCBxLTkgMCAtOSAtMTAgbDAgLTI0IHEwIC0xMCAxMCAtMTAgbTkyIDQ0IGwyMCAwIG0tMjAgMCBxMTAgMCAxMCAtMTAgbDAgLTI0IHEwIC0xMCAtMTAgLTEwIG0tOTIgMCBoMTAgbTI0IDAgaDEwIG0wIDAgaDQ4IG0tMTMyIDQ0IGgyMCBtMTMyIDAgaDIwIG0tMTcyIDAgcTEwIDAgMTAgMTAgbTE1MiAwIHEwIC0xMCAxMCAtMTAgbS0xNjIgMTAgdjE0IG0xNTIgMCB2LTE0IG0tMTUyIDE0IHEwIDEwIDEwIDEwIG0xMzIgMCBxMTAgMCAxMCAtMTAgbS0xNDIgMTAgaDEwIG0wIDAgaDEyMiBtMjAgLTM0IGgxMCBtMjYgMCBoMTAgbS0yODQgMCBoMjAgbTI2NCAwIGgyMCBtLTMwNCAwIHExMCAwIDEwIDEwIG0yODQgMCBxMCAtMTAgMTAgLTEwIG0tMjk0IDEwIHY0NiBtMjg0IDAgdi00NiBtLTI4NCA0NiBxMCAxMCAxMCAxMCBtMjY0IDAgcTEwIDAgMTAgLTEwIG0tMjc0IDEwIGgxMCBtMjYgMCBoMTAgbTAgMCBoMTAgbTU4IDAgaDEwIG0wIDAgaDEwIG0yNiAwIGgxMCBtMCAwIGg5NCBtMjMgLTY2IGgtMyIgLz4KICAgPHBvbHlnb24gcG9pbnRzPSI0MDMgNjEgNDExIDU3IDQxMSA2NSI+PC9wb2x5Z29uPgogICA8cG9seWdvbiBwb2ludHM9IjQwMyA2MSAzOTUgNTcgMzk1IDY1Ij48L3BvbHlnb24+Cjwvc3ZnPg==)

</div>

| Field | Use |
|----|----|
| *element* | An element of any [data type](../) to place in the list. Note that all elements must be of the same type. |

## List functions + operators

#### Polymorphism

List functions and operators are polymorphic, which applies the
following constraints to their arguments:

- All instances of `listany` must be lists of the same type.
- All instances of `listelementany` must be of the same type.
- If a function uses both `listany` and `listelementany` parameters, all
  instances of `listany` must be a list of the type used in
  `listelementany`.
- If any value passed to a polymorphic parameter is a [custom
  type](/docs/sql/types/#custom-types), additional constraints apply.

### Operators

| Operator | Description |
|----|----|
| `listany || listany` | Concatenate the two lists. |
| `listany || listelementany` | Append the element to the list. |
| `listelementany || listany` | Prepend the element to the list. |
| `listany @> listany` | Check if the first list contains all elements of the second list. |
| `listany <@ listany` | Check if all elements of the first list are contained in the second list. |

### Functions

<table class="inline-headings">
<colgroup>
<col style="width: 100%" />
</colgroup>
<thead>
<tr>
<th><p>Function</p></th>
</tr>
</thead>
<tbody>
<tr>
<td><div id="list_agg" class="heading">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>list_agg(x: any) -&gt; L</code></pre>
</div>
</div>
<p>Aggregate values (including nulls) as a list <a
href="/docs/sql/functions/list_agg">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="list_append" class="heading">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>list_append(l: listany, e: listelementany) -&gt; L</code></pre>
</div>
</div>
<p>Appends <code>e</code> to <code>l</code>.</p></td>
</tr>
<tr>
<td><div id="list_cat" class="heading">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>list_cat(l1: listany, l2: listany) -&gt; L</code></pre>
</div>
</div>
<p>Concatenates <code>l1</code> and <code>l2</code>.</p></td>
</tr>
<tr>
<td><div id="list_length" class="heading">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>list_length(l: listany) -&gt; int</code></pre>
</div>
</div>
<p>Return the number of elements in <code>l</code>.</p></td>
</tr>
<tr>
<td><div id="list_prepend" class="heading">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>list_prepend(e: listelementany, l: listany) -&gt; listany</code></pre>
</div>
</div>
<p>Prepends <code>e</code> to <code>l</code>.</p></td>
</tr>
</tbody>
</table>

## Details

### Type name

The name of a list type is the name of its element type followed by
`list`, e.g. `int list`. This rule can be applied recursively, e.g.
`int list list` for a `list` of `int list`s which is a two-layer list.

### Construction

You can construct lists using the `LIST` expression:

<div class="highlight">

``` chroma
SELECT LIST[1, 2, 3];
```

</div>

```
  list
---------
 {1,2,3}
```

You can nest `LIST` constructors to create layered lists:

<div class="highlight">

``` chroma
SELECT LIST[LIST['a', 'b'], LIST['c']];
```

</div>

```
    list
-------------
 {{a,b},{c}}
```

You can also elide the `LIST` keyword from the interior list
expressions:

<div class="highlight">

``` chroma
SELECT LIST[['a', 'b'], ['c']];
```

</div>

```
    list
-------------
 {{a,b},{c}}
```

Alternatively, you can construct a list from the results of a subquery.
The subquery must return a single column. Note that, in this form of the
`LIST` expression, parentheses are used rather than square brackets.

<div class="highlight">

``` chroma
SELECT LIST(SELECT x FROM test0 WHERE x > 0 ORDER BY x DESC LIMIT 3);
```

</div>

```
    x
---------
 {4,3,2}
```

Layered lists can be “ragged”, i.e. the length of lists in each layer
can differ from one another. This differs from `array`, which requires
that each dimension of a multidimensional array only contain arrays of
the same length.

Note that you can also construct lists using the available [`text`
cast](#text-to-list-casts).

### Accessing lists

You can access elements of lists through:

- [Indexing](#indexing-elements) for individual elements
- [Slicing](#slicing-ranges) for ranges of elements

#### Indexing elements

To access an individual element of list, you can “index” into it using
brackets (`[]`) and 1-index element positions:

<div class="highlight">

``` chroma
SELECT LIST[['a', 'b'], ['c']][1];
```

</div>

```
 ?column?
----------
 {a,b}
```

Indexing operations can be chained together to descend the list’s
layers:

<div class="highlight">

``` chroma
SELECT LIST[['a', 'b'], ['c']][1][2];
```

</div>

```
 ?column?
----------
 b
```

If the index is invalid (either less than 1 or greater than the maximum
index), lists return *NULL*.

<div class="highlight">

``` chroma
SELECT LIST[['a', 'b'], ['c']][1][5] AS exceed_index;
```

</div>

```
 exceed_index
--------------
```

Lists have types based on their layers (unlike arrays’ dimension), and
error if you attempt to index a non-list element (i.e. indexing past the
list’s last layer):

<div class="highlight">

``` chroma
SELECT LIST[['a', 'b'], ['c']][1][2][3];
```

</div>

```
ERROR:  cannot subscript type string
```

#### Slicing ranges

To access contiguous ranges of a list, you can slice it using
`[first index : last index]`, using 1-indexed positions:

<div class="highlight">

``` chroma
SELECT LIST[1,2,3,4,5][2:4] AS two_to_four;
```

</div>

```
  slice
---------
 {2,3,4}
```

You can omit the first index to use the first value in the list, and
omit the last index to use all elements remaining in the list.

<div class="highlight">

``` chroma
SELECT LIST[1,2,3,4,5][:3] AS one_to_three;
```

</div>

```
 one_to_three
--------------
 {1,2,3}
```

<div class="highlight">

``` chroma
SELECT LIST[1,2,3,4,5][3:] AS three_to_five;
```

</div>

```
 three_to_five
---------------
 {3,4,5}
```

If the first index exceeds the list’s maximum index, the operation
returns an empty list:

<div class="highlight">

``` chroma
SELECT LIST[1,2,3,4,5][10:] AS exceed_index;
```

</div>

```
 exceed_index
--------------
 {}
```

If the last index exceeds the list’s maximum index, the operation
returns all remaining elements up to its final element.

<div class="highlight">

``` chroma
SELECT LIST[1,2,3,4,5][2:10] AS two_to_end;
```

</div>

```
 two_to_end
------------
 {2,3,4,5}
```

Performing successive slices behaves more like a traditional programming
language taking slices of an array, rather than PostgreSQL’s slicing,
which descends into each layer.

<div class="highlight">

``` chroma
SELECT LIST[1,2,3,4,5][2:][2:3] AS successive;
```

</div>

```
 successive
------------
 {3,4}
```

### Output format

We represent lists textually using an opening curly brace (`{`),
followed by the textual representation of each element separated by
commas (`,`), terminated by a closing curly brace (`}`). For layered
lists, this format is applied recursively to each list layer. No
additional whitespace is added.

We render *NULL* elements as the literal string `NULL`. Non-null
elements are rendered as if that element had been cast to `text`.

Elements whose textual representations contain curly braces, commas,
whitespace, double quotes, backslashes, or which are exactly the string
`NULL` (in any case) get wrapped in double quotes in order to
distinguish the representation of the element from the representation of
the containing list. Within double quotes, backslashes and double quotes
are backslash-escaped.

The following example demonstrates the output format and includes many
of the aforementioned special cases.

<div class="highlight">

``` chroma
SELECT LIST[['a', 'white space'], [NULL, ''], ['escape"m\e', 'nUlL']];
```

</div>

```
                        list
-----------------------------------------------------
 {{a,"white space"},{NULL,""},{"escape\"m\\e",nUlL}}
```

### `text` to `list` casts

To cast `text` to a `list`, you must format the text similar to list’s
[output format](#output-format).

The text you cast must:

- Begin with an opening curly brace (`{`) and end with a closing curly
  brace (`}`)

- Separate each element with a comma (`,`)

- Use a representation for elements that can be cast from text to the
  list’s element type.

  For example, to cast `text` to a `date list`, you use `date`’s `text`
  representation:

  <div class="highlight">

  ``` chroma
  SELECT '{2001-02-03, 2004-05-06}'::date list as date_list;
  ```

  </div>

  ```
          date_list
  -------------------------
   {2001-02-03,2004-05-06}
  ```

  You cannot include the `DATE` keyword.

- Escape any special representations (`{`, `}`, `"`, `\`, whitespace, or
  the literal string `NULL`) you want parsed as text using…

  - Double quotes (`"`) to escape an entire contiguous string

  - Backslashes (`\`) to escape an individual character in any context,
    e.g. `\"` to escape double quotes within an escaped string.

    Note that escaping any character in the string “null”
    (case-insensitive) generates a `text` value equal to “NULL” and not
    a *NULL* value.

  For example:

  <div class="highlight">

  ``` chroma
  SELECT '{
      "{brackets}",
      "\"quotes\"",
      \\slashes\\,
      \ leading space,
      trailing space\ ,
      \NULL
  }'::text list as escape_examples;
  ```

  </div>

  ```
                                     escape_examples
  -------------------------------------------------------------------------------------
   {"{brackets}","\"quotes\"","\\slashes\\"," leading space","trailing space ","NULL"}
  ```

  Note that all unescaped whitespace is trimmed.

### List vs. array

`list` is a Materialize-specific type and is designed to provide:

- Similar semantics to Avro and JSON arrays
- A more ergonomic experience than PostgreSQL-style arrays

This section focuses on the distinctions between Materialize’s `list`
and `array` types, but with some knowledge of the PostgreSQL array type,
you should also be able to infer how list differs from it, as well.

#### Terminology

| Feature | Array term | List term |
|----|----|----|
| **Nested structure** | Multidimensional array | Layered list |
| **Accessing single element** | Subscripting | Indexing<sup>1</sup> |
| **Accessing range of elements** | Subscripting | Slicing<sup>1</sup> |

<sup>1</sup>In some places, such as error messages, Materialize refers
to both list indexing and list slicing as subscripting.

#### Type definitions

**Lists** require explicitly declared layers, and each possible layer is
treated as a distinct type. For example, a list of `int`s with two
layers is `int list list` and one with three is `int list list list`.
Because their number of layers differ, they cannot be used
interchangeably.

**Arrays** only have one type for each non-array type, and all arrays
share that type irrespective of their dimensions. This means that arrays
of the same element type can be used interchangeably in most situations,
without regard to their dimension. For example, arrays of `text` are all
of type `text[]` and 1D, 2D, and 3D `text[]` can all be used in the same
columns.

#### Nested structures

**Lists** allow each element of a layer to be of a different length. For
example, in a two-layer list, each of the first layer’s lists can be of
a different length:

<div class="highlight">

``` chroma
SELECT LIST[[1,2], [3]] AS ragged_list;
```

</div>

```
ragged_list
-------------
{{1,2},{3}}
```

This is known as a “ragged list.”

**Arrays** require each element of a dimension to have the same length.
For example, if the first element in a 2D list has a length of 2, all
subsequent members must also have a length of 2.

<div class="highlight">

``` chroma
SELECT ARRAY[[1,2], [3]] AS ragged_array;
```

</div>

```
ERROR:  number of array elements (3) does not match declared cardinality (4)
```

#### Accessing single elements

**Lists** support accessing single elements via
[indexing](#indexing-elements). When indexed, lists return a value with
one less layer than the indexed list. For example, indexing a two-layer
list returns a one-layer list.

<div class="highlight">

``` chroma
SELECT LIST[['foo'],['bar']][1] AS indexing;
```

</div>

```
 indexing
--------------
 {foo}
```

Attempting to index twice into a `text list` (i.e. a one-layer list),
fails because you cannot index `text`.

<div class="highlight">

``` chroma
SELECT LIST['foo'][1][2];
```

</div>

```
ERROR:  cannot subscript type text
```

##### Accessing ranges of elements

**Lists** support accessing ranges of elements via
[slicing](#slicing-ranges). However, lists do not currently support
PostgreSQL-style slicing, which descends into layers in each slice.

**Arrays** require each element of a dimension to have the same length.
For example, if the first element in a 2D list has a length of 2, all
subsequent members must also have a length of 2.

### Custom types

You can create [custom `list` types](/docs/sql/types/#custom-types),
which lets you create a named entry in the catalog for a specific type
of list.

Currently, custom types only provides a shorthand for referring to
otherwise-annoying-to-type names.

Note that custom `list` types have special rules regarding
[polymorphism](/docs/sql/types/#polymorphism).

### Valid casts

#### Between `list`s

You can cast one list type to another if the type of the source list’s
elements can be cast to the target list’s elements’ type. For example,
`float list` can be cast to `int list`, but `float list` cannot be cast
to `timestamp list`.

Note that this rule also applies to casting between custom list types.

#### From `list`

You can [cast](../../functions/cast) `list` to:

- [`text`](../text) (implicitly)
- Other `lists` as noted above.

#### To `list`

You can [cast](../../functions/cast) the following types to `list`:

- [arrays](../array) (explicitly). See
  [details](../array#array-to-list-casts).
- [`text`](../text) (explicitly). See [details](#text-to-list-casts).
- Other `lists` as noted above.

## Examples

### Literals

<div class="highlight">

``` chroma
SELECT LIST[[1.5, NULL],[2.25]];
```

</div>

```
         list
----------------------
 {{1.50,NULL},{2.25}}
```

### Casting between lists

<div class="highlight">

``` chroma
SELECT LIST[[1.5, NULL],[2.25]]::int list list;
```

</div>

```
      list
----------------
 {{2,NULL},{2}}
```

### Casting to text

<div class="highlight">

``` chroma
SELECT LIST[[1.5, NULL],[2.25]]::text;
```

</div>

```
      list
------------------
 {{1,NULL},{2}}
```

Despite the fact that the output looks the same as the above examples,
it is, in fact, `text`.

<div class="highlight">

``` chroma
SELECT length(LIST[[1.5, NULL],[2.25]]::text);
```

</div>

```
 length
--------
     20
```

### Casting from text

<div class="highlight">

``` chroma
SELECT '{{1.5,NULL},{2.25}}'::numeric(38,2) list list AS text_to_list;
```

</div>

```
     text_to_list
----------------------
 {{1.50,NULL},{2.25}}
```

### List containment

<div class="note">

**NOTE:** Like [array containment operators in
PostgreSQL](https://www.postgresql.org/docs/current/functions-array.html#FUNCTIONS-ARRAY),
list containment operators in Materialize **do not** account for
duplicates.

</div>

<div class="highlight">

``` chroma
SELECT LIST[1,4,3] @> LIST[3,1] AS contains;
```

</div>

```
 contains
----------
 t
```

<div class="highlight">

``` chroma
SELECT LIST[2,7] <@ LIST[1,7,4,2,6] AS is_contained_by;
```

</div>

```
 is_contained_by
-----------------
 t
```

<div class="highlight">

``` chroma
SELECT LIST[7,3,1] @> LIST[1,3,3,3,3,7] AS contains;
```

</div>

```
 contains
----------
 t
```

<div class="highlight">

``` chroma
SELECT LIST[1,3,7,NULL] @> LIST[1,3,7,NULL] AS contains;
```

</div>

```
 contains
----------
 f
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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/types/list.md"
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
