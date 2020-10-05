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
compatibility views in the [system catalog](/sql/system-tables) must expose
array types. Unfortunately, PostgreSQL arrays have odd semantics and do not
interoperate well with modern data formats like JSON and Avro.

A forthcoming release will introduce a new [list type](https://github.com/MaterializeInc/materialize/issues/2997) that interoperates better with JSON and Avro.
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

### Valid casts

You can [cast](/sql/functions/cast) all array types to [`text`](/sql/types/text).

You cannot presently cast any other type to an array type.
