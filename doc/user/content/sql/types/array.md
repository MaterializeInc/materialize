---
title: "Array types"
description: "Express sequences of other types"
menu:
  main:
    parent: sql-types
---

Arrays are a multidimensional sequence of any non-array type.

{{< warning >}}
We do not recommend using arrays, which exist in Materialize primarily to
facilitate compatibility with PostgreSQL. Specifically, many of the PostgreSQL
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

Alternatively, you can construct an array from the results subquery.  These subqueries must return a single column. Note
that, in this form of the `ARRAY` expression, parentheses are used rather than square brackets.

```sql
SELECT ARRAY(SELECT x FROM test0 WHERE x > 0 ORDER BY x DESC LIMIT 3);
```
```nofmt
    x
---------
 {4,3,2}
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

### Textual format

The textual representation of an array consists of an opening curly brace (`{`),
followed by the textual representation of each element separated by commas
(`,`), followed by a closing curly brace (`}`). For multidimensional arrays,
this format is applied recursively to each array dimension. No additional
whitespace is added.

Null elements are rendered as the literal string `NULL`. Non-null elements are
rendered as if that element had been cast to `text`.

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

### Catalog names

Builtin types (e.g. `integer`) have a builtin array type that can be referred to
by prefixing the type catalog name name with an underscore. For example,
`integer`'s catalog name is `pg_catalog.int4`, so its array type's catalog name
is `pg_catalog_int4`.

Array element | Catalog name | OID
--------------|--------------|-----
[`bigint`](../bigint) | `pg_catalog._int8` | 1016
[`boolean`](../boolean) | `pg_catalog._bool` | 1000
[`date`](../date) | `pg_catalog._date` | 1182
[`double precision`](../float) | `pg_catalog._float8` | 1022
[`integer`](../integer) | `pg_catalog._bool` | 1007
[`interval`](../interval) | `pg_catalog._interval` | 1187
[`jsonb`](../jsonb) | `pg_catalog.3807` | 1000
[`numeric`](../numeric) | `pg_catalog._numeric` | 1231
[`oid`](../oid) | `pg_catalog._oid` | 1028
[`real`](../float) | `pg_catalog._float4` | 1021
[`text`](../text) | `pg_catalog._bool` | 1009
[`time`](../time) | `pg_catalog._time` | 1183
[`timestamp`](../timestamp) | `pg_catalog._timestamp` | 1115
[`timestamp with time zone`](../timestamp) | `pg_catalog._timestamptz` | 1185
[`uuid`](../uuid) | `pg_catalog._uuid` | 2951

### Valid casts

You can [cast](/sql/functions/cast) all array types to:
- [`text`](../text) (by assignment)
- [`list`](../list) (explicit)

You can cast `text` to any array type. The input must conform to the [textual
format](#textual-format) described above, with the additional restriction that
you cannot yet use a cast to construct a multidimensional array.

### Array to `list` casts

You can cast any type of array to a list of the same element type, as long as
the array has only 0 or 1 dimensions, i.e. you can cast `integer[]` to `integer
list`, as long as the array is empty or does not contain any arrays itself.

```sql
SELECT pg_typeof('{1,2,3}`::integer[]::integer list);
```
```
integer list
```

## Examples

```sql
SELECT '{1,2,3}'::int[]
```
```nofmt
  int4
---------
 {1,2,3}
```

```sql
SELECT ARRAY[ARRAY[1, 2], ARRAY[NULL, 4]]::text
```
```nofmt
      array
------------------
 {{1,2},{NULL,4}}
```
