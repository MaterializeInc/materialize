---
title: "Array Data Types"
description: "Express sequences of other types"
menu:
  main:
    parent: sql-types
---

{{< version-added v0.5.0 >}}

Arrays are a multidimensional sequence of any non-array type.

{{< warning >}}
Use of arrays is not recommended. Arrays in Materialize exist to facilitate
compatibility with PostgreSQL. Specifically, many of the PostgreSQL
compatibility views in the [system catalog](/sql/system-tables/) must expose
array types. Unfortunately, PostgreSQL arrays have odd semantics and do not
interoperate well with modern data formats like JSON and Avro.

Use the [`list` type](/sql/types/list) instead.
{{< /warning >}}

## Details

### Type name

The name of an array type is the name of an element type followed by square
brackets (`[]`) . For example, the type `int[]` specifies an integer array.

For compatibility with PostgreSQL, array types may optionally indicate
additional dimensions, as in `int[][][]`, or the sizes of dimensions, as in
`int[3][4]`. However, as in PostgreSQL, these additional annotations are
ignored. Arrays of the same element type are considered to be of the same type
regardless of their dimensions. For example, the type `int[3][4]` is exactly
equivalent to the type `int[]`.

To reduce confusion, we recommend that you use the simpler form of the type name
whenever possible.

### Construction

{{< experimental v0.5.0 >}}
The `ARRAY` expression syntax
{{< /experimental >}}

You can construct arrays using the special `ARRAY` expression:

```sql
SELECT ARRAY[1, 2, 3]
```
```nofmt
  array
---------
 {1,2,3}
```

You can nest `ARRAY` constructors to create multidimensional arrays:

```sql
SELECT ARRAY[ARRAY['a', 'b'], ARRAY['c', 'd']]
```
```nofmt
     array
---------------
 {{a,b},{c,d}}
```

Arrays cannot be "ragged." The length of each array expression must equal the
length of all other array constructors in the same dimension. For example, the
following ragged array is rejected:

```sql
SELECT ARRAY[ARRAY[1, 2], ARRAY[3]]
```
```nofmt
ERROR:  number of array elements (3) does not match declared cardinality (4)
```

### Output format

The textual representation of an array consists of an opening curly brace (`{`),
followed by the textual representation of each element separated by commas
(`,`), followed by a closing curly brace (`}`). For multidimensional arrays,
this format is applied recursively to each array dimension. No additional
whitespace is added.

Null elements are rendered as the literal string `NULL`. Non-null elements
are rendered as if that element had been cast to `text`.

An element whose textual representation contains curly braces, commas,
whitespace, double quotes, backslashes, or is exactly the string `NULL` (in any
case) is wrapped in double quotes in order to distinguish the representation of
the element from the representation of the containing array. Within double
quotes, backslashes and double quotes are backslash-escaped.

The following example demonstrates the output format and includes many of the
aforementioned special cases.

```sql
SELECT ARRAY[ARRAY['a', 'white space'], ARRAY[NULL, ''], ARRAY['escape"m\e', 'nUlL']]
```
```nofmt
                         array
-------------------------------------------------------
 {{a,"white space"},{NULL,""},{"escape\"m\\e","nUlL"}}
```


### Valid casts

You can [cast](/sql/functions/cast) all array types to [`text`](/sql/types/text).

You cannot presently cast any other type to an array type.

## Examples

```sql
SELECT ARRAY[ARRAY[1, 2], ARRAY[NULL, 4]]::text
```
```nofmt
      array
------------------
 {{1,2},{NULL,4}}
```
