---
audience: developer
canonical_url: https://materialize.com/docs/sql/types/date/
complexity: intermediate
description: Expresses a date without a specified time
doc_type: reference
keywords:
- Quick Syntax
- Catalog name
- OID
- Size
- date type
- Min value
- SELECT DATE
product_area: Indexes
status: stable
title: date type
---

# date type

## Purpose
Expresses a date without a specified time

If you need to understand the syntax and options for this command, you're in the right place.


Expresses a date without a specified time



`date` data expresses a date without a specified time.

Detail | Info
-------|------
**Quick Syntax** | `DATE '2007-02-01'`
**Size** | 1 byte
**Catalog name** | `pg_catalog.date`
**OID** | 1082
**Min value** | 4714-11-24 BC
**Max value** | 262143-12-31 AD
**Resolution** | 1 day

## Syntax

[See diagram: type-date.svg]

Field | Use
------|----
_date&lowbar;str_ | A string representing a date in `Y-M-D`, `Y M-D`, `Y M D` or `YMD` format.
_time&lowbar;str_ | _(NOP)_ A string representing a time of day in `H:M:S.NS` format.
_tz&lowbar;offset_ | _(NOP)_ The timezone's distance, in hours, from UTC.

## Details

This section covers details.

### Valid casts

#### From `date`

You can [cast](../../functions/cast) `date` to:

- [`text`](../text) (by assignment)
- [`timestamp`](../timestamp) (implicitly)
- [`timestamptz`](../timestamp) (implicitly)

#### To `date`

You can [cast](../../functions/cast) from the following types to `date`:

- [`text`](../text) (explicitly)
- [`timestamp`](../timestamp) (by assignment)
- [`timestamptz`](../timestamp) (by assignment)

### Valid operations

`time` data supports the following operations with other types.

Operation | Computes
----------|------------
[`date`](../date) `+` [`interval`](../interval) | [`timestamp`](../timestamp)
[`date`](../date) `-` [`interval`](../interval) | [`timestamp`](../timestamp)
[`date`](../date) `+` [`time`](../time) | [`timestamp`](../timestamp)
[`date`](../date) `-` [`date`](../date) | [`interval`](../interval)

## Examples

This section covers examples.

```mzsql
SELECT DATE '2007-02-01' AS date_v;
```text
```nofmt
   date_v
------------
 2007-02-01
```

