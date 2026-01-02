---
audience: developer
canonical_url: https://materialize.com/docs/sql/types/boolean/
complexity: intermediate
description: Expresses TRUE or FALSE
doc_type: reference
keywords:
- SELECT TRUE
- Quick Syntax
- Catalog name
- OID
- Aliases
- SELECT FALSE
- Size
- boolean type
product_area: Indexes
status: stable
title: boolean type
---

# boolean type

## Purpose
Expresses TRUE or FALSE

If you need to understand the syntax and options for this command, you're in the right place.


Expresses TRUE or FALSE



`boolean` data expresses a binary value of either `TRUE` or `FALSE`.

Detail | Info
-------|------
**Quick Syntax** | `TRUE` or `FALSE`
**Size** | 1 byte
**Aliases** | `bool`
**Catalog name** | `pg_catalog.bool`
**OID** | 16

## Syntax

[See diagram: type-bool.svg]

## Details

This section covers details.

### Valid casts

#### From `boolean`

You can [cast](../../functions/cast) from `boolean` to:

- [`int`](../int) (explicitly)
- [`text`](../text) (by assignment)

#### To `boolean`

You can [cast](../../functions/cast) the following types to `boolean`:

- [`int`](../int) (explicitly)
- [`jsonb`](../jsonb) (explicitly)
- [`text`](../text) (explicitly)

## Examples

This section covers examples.

```mzsql
SELECT TRUE AS t_val;
```text
```nofmt
 t_val
-------
 t
```text

```mzsql
SELECT FALSE AS f_val;
 f_val
-------
 f
```

