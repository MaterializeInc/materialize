---
title: "Integer Data Types"
description: "Express signed integers"
aliases:
- /docs/sql/types/bigint
- /docs/sql/types/int
- /docs/sql/types/int4
- /docs/sql/types/int8
menu:
  main:
    parent: 'sql-types'
---

`integer` and `bigint` data express signed integers.

Type      | Aliases       | Size          | Minimum value              | Maximum value
----------|---------------|---------------|----------------------------|--------------------------
`integer` | `int`, `int4` | 4 bytes       | -2,147,483,648             | 2,147,483,647
`bigint`  | `int8`        | 8 bytes       | -9,223,372,036,854,775,808 | 9,223,372,036,854,775,807

The SQL standard specifies only the `integer`, `int` and `bigint` type names.
Materialize additionally permits the `int4` and `int8` aliases, for
compatibility with other SQL database systems.

## Details

### Valid casts

In addition to the casts listed below, all integer types can be cast to and from
all other integer types.

#### From `int`

You can [cast](../../functions/cast) `integer` or `bigint` to:

- [`bool`](../boolean)
- [`decimal`](../decimal)
- [`float`](../float)
- [`string`](../string)

#### To `int`

You can [cast](../../functions/cast) the following types to `integer` or `bigint`:

- [`decimal`](../decimal)
- [`float`](../float)

## Examples

```sql
SELECT 123::integer AS int_v;
```
```nofmt
 int_v
-------
   123
```

<hr/>

```sql
SELECT 1.23::integer AS int_v;
```
```nofmt
 int_v
-------
     1
```
