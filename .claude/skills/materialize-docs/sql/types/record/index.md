---
audience: developer
canonical_url: https://materialize.com/docs/sql/types/record/
complexity: intermediate
description: A tuple with arbitrary contents
doc_type: reference
keywords:
- Quick Syntax
- Catalog name
- SELECT RECORD
- Size
- record type
- SELECT ROW
product_area: Indexes
status: stable
title: record type
---

# record type

## Purpose
A tuple with arbitrary contents

If you need to understand the syntax and options for this command, you're in the right place.


A tuple with arbitrary contents



A `record` is a tuple that can contain an arbitrary number of elements of any
type.

Detail | Info
-------|------
**Quick Syntax** | `ROW($expr, ...)`
**Size** | Variable
**Catalog name** | Unnameable

## Syntax

[See diagram: type-record.svg]

## Details

Record types can be used to represent nested data.

The fields of a record are named `f1`, `f2`, and so on. To access a
field of a record, use the `.` operator. Note that you need to parenthesize the
record expression to ensure that the `.` is interpreted as the field selection
operator, rather than part of a database- or schema-qualified table name.

### Catalog name

`record` is a named type in PostgreSQL (`pg_catalog.record`), but is
currently an [unnameable](../#catalog-name) type in Materialize.

### Valid casts

You can [cast](../../functions/cast) `record` to [`text`](../../types/text/) by assignment.

You cannot cast from any other types to `record`.

## Examples

This section covers examples.

```mzsql
SELECT ROW(1, 2) AS record;
```text
```nofmt
 record
--------
 (1,2)
```text

<hr>

```mzsql
SELECT record, (record).f2 FROM (SELECT ROW(1, 2) AS record);
```text
```nofmt
record | f2
--------+----
 (1,2)  |  2
```text

<hr>

Forgetting to parenthesize the record expression in a field selection operation
will result in errors like the following

```mzsql
SELECT record.f2 FROM (SELECT ROW(1, 2) AS record);
```text
```nofmt
ERROR:  column "record.f2" does not exist
```

as the expression `record.f2` specifies a column named `f2` from a table named
`record`, rather than the field `f2` from the record-typed column named
`record`.

