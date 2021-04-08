---
title: "Integer Data Types"
description: "Express signed integers"
menu:
  main:
    parent: 'sql-types'
aliases:
  - /sql/types/bigint
  - /sql/types/int
  - /sql/types/int4
  - /sql/types/int8
---

## `integer` info

Detail | Info
-------|------
**Size** | 4 bytes
**Aliases** | `int`, `int4`
**Catalog name** | `pg_catalog.int4`
**OID** | 23
**Range** | [-2,147,483,648, 2,147,483,647]

## `bigint` info

Detail | Info
-------|------
**Size** | 8 bytes
**Aliases** | `int8`
**Catalog name** | `pg_catalog.int8`
**OID** | 20
**Range** | [-9,223,372,036,854,775,808, 9,223,372,036,854,775,807]

## Details

### Valid casts

In addition to the casts listed below, all integer types can be cast to and from
all other integer types. `bigint`/`int8` to `int`/`int4` is a cast by assignment and
`int`/`int4` to `bigint`/`int8` is an implicit cast.

#### From `integer` or `bigint`

You can [cast](../../functions/cast) `integer` or `bigint` to:

- [`boolean`](../boolean) (explicitly)
- [`numeric`](../numeric) (implicitly)
- [`real`/`double precision`](../float) (implicitly)
- [`text`](../text) (by assignment)

#### To `integer` or `bigint`

You can [cast](../../functions/cast) the following types to `integer` or `bigint`:

- [`bool`](../boolean) (implicitly)
- [`jsonb`](../jsonb)(explicitly)
- [`oid`](../oid) (by assignment)
- [`numeric`](../numeric) (by assignment)
- [`real`/`double precision`](../float) (by assignment)
- [`text`](../text) (explicitly)

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
