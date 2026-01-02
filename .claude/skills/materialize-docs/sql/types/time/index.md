---
audience: developer
canonical_url: https://materialize.com/docs/sql/types/time/
complexity: intermediate
description: Expresses a time without a specific date
doc_type: reference
keywords:
- time type
- Quick Syntax
- Catalog name
- OID
- SELECT TIME
- Size
- Min value
- SELECT DATE
product_area: Indexes
status: stable
title: time type
---

# time type

## Purpose
Expresses a time without a specific date

If you need to understand the syntax and options for this command, you're in the right place.


Expresses a time without a specific date



`time` data expresses a time without a specific date.

Detail | Info
-------|------
**Quick Syntax** | `TIME '01:23:45'`
**Size** | 4 bytes
**Catalog name** | `pg_catalog.time`
**OID** | 1083
**Min value** | `TIME '00:00:00'`
**Max value** | `TIME '23:59:59.999999'`

## Syntax

[See diagram: type-time.svg]

Field | Use
------|------------
_time&lowbar;str_ | A string representing a time of day in `H:M:S.NS` format.

## Details

This section covers details.

### Valid casts

#### From `time`

You can [cast](../../functions/cast) `time` to:

- [`interval`](../interval) (implicitly)
- [`text`](../text) (by assignment)

#### To `time`

You can [cast](../../functions/cast) from the following types to `time`:

- [`interval`](../interval) (by assignment)
- [`text`](../text) (explicitly)
- [`timestamp`](../timestamp) (by assignment)
- [`timestamptz`](../timestamp) (by assignment)

### Valid operations

`time` data supports the following operations with other types.

Operation | Computes
----------|------------
[`date`](../date) `+` [`time`](../time) | [`timestamp`](../timestamp)
[`time`](../time) `+` [`interval`](../interval) | `time`
[`time`](../time) `-` [`interval`](../interval) | `time`
[`time`](../time) `-` [`time`](../time) | [`interval`](../interval)

## Examples

This section covers examples.

```mzsql
SELECT TIME '01:23:45' AS t_v;
```text
```nofmt
   t_v
----------
 01:23:45
```text

<hr/>

```mzsql
SELECT DATE '2001-02-03' + TIME '12:34:56' AS d_t;
```text
```nofmt
         d_t
---------------------
 2001-02-03 12:34:56
```

