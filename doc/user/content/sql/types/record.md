---
title: "record Data Type"
description: "A tuple with arbitrary contents"
menu:
  main:
    parent: sql-types
---

A `record` is a tuple that can contain an arbitrary number of elements of any
type.

Detail | Info
-------|------
**Quick Syntax** | `ROW($expr, ...)`
**Size** | Variable

## Syntax

{{< diagram "type-record.svg" >}}

## Details

Record types can be used to represent nested data.

The fields of a record are named `field1`, `field2`, and so on. To access a
field of a record, use the `.` operator. Note that you need to parenthesize the
record expression to ensure that the `.` is interpreted as the field selection
operator, rather than part of a database- or schema-qualified table name.

## Examples

```sql
SELECT ROW(1, 2) AS record;
```
```nofmt
 record
--------
 (1,2)
```

<hr>

```sql
SELECT record, (record).f2 FROM (SELECT ROW(1, 2) AS record);
```
```nofmt
record | f2
--------+----
 (1,2)  |  2
```

<hr>

Forgetting to parenthesize the record expression in a field selection operation
will result in errors like the following

```sql
SELECT record.f2 FROM (SELECT ROW(1, 2) AS record);
```
```nofmt
ERROR:  column "record.f2" does not exist
```

as the expression `record.f2` specifies a column named `f2` from a table named
`record`, rather than the field `f2` from the record-typed column named `record`.
