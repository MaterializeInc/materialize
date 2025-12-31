---
audience: developer
canonical_url: https://materialize.com/docs/sql/functions/timezone-and-at-time-zone/
complexity: intermediate
description: Converts timestamp to a different time zone.
doc_type: reference
keywords:
- SELECT TIMESTAMPTZ
- TIMEZONE and AT TIME ZONE functions
- SELECT TIMEZONE
- 'Known limitation:'
- SELECT TIMESTAMP
- 'Note:'
product_area: Indexes
status: stable
title: TIMEZONE and AT TIME ZONE functions
---

# TIMEZONE and AT TIME ZONE functions

## Purpose
Converts timestamp to a different time zone.

If you need to understand the syntax and options for this command, you're in the right place.


Converts timestamp to a different time zone.



`TIMEZONE` and `AT TIME ZONE` convert a [`timestamp`](../../types/timestamp/#timestamp-info) or a [`timestamptz`](../../types/timestamp/#timestamp-with-time-zone-info) to a different time zone.

**Known limitation:** You must explicitly cast the type for the time zone.

## Signatures

[See diagram: func-timezone.svg]

[See diagram: func-at-time-zone.svg]

Parameter | Type | Description
----------|------|------------
_zone_ | [`text`](../../types/text) | The target time zone.
_type_  |[`text`](../../types/text) or [`numeric`](../../types/numeric) |  The datatype in which the time zone is expressed
_timestamp_ | [`timestamp`](../../types/timestamp/#timestamp-info) | The timestamp without time zone.  |   |
_timestamptz_ | [`timestamptz`](../../types/timestamp/#timestamp-with-time-zone-info) | The timestamp with time zone.

## Return values

`TIMEZONE` and  `AT TIME ZONE` return [`timestamp`](../../types/timestamp/#timestamp-info) if the input is [`timestamptz`](../../types/timestamp/#timestamp-with-time-zone-info), and [`timestamptz`](../../types/timestamp/#timestamp-with-time-zone-info) if the input is [`timestamp`](../../types/timestamp/#timestamp-info).

**Note:** `timestamp` and `timestamptz` always store data in UTC, even if the date is returned as the local time.

## Examples

This section covers examples.

### Convert timestamp to another time zone, returned as UTC with offset

```mzsql
SELECT TIMESTAMP '2020-12-21 18:53:49' AT TIME ZONE 'America/New_York'::text;
```text
```
        timezone
------------------------
2020-12-21 23:53:49+00
(1 row)
```text

```mzsql
SELECT TIMEZONE('America/New_York'::text,'2020-12-21 18:53:49');
```text
```
        timezone
------------------------
2020-12-21 23:53:49+00
(1 row)
```bash

### Convert timestamp to another time zone, returned as specified local time

```mzsql
SELECT TIMESTAMPTZ '2020-12-21 18:53:49+08' AT TIME ZONE 'America/New_York'::text;
```text
```
        timezone
------------------------
2020-12-21 05:53:49
(1 row)
```text

```mzsql
SELECT TIMEZONE ('America/New_York'::text,'2020-12-21 18:53:49+08');
```text
```
        timezone
------------------------
2020-12-21 05:53:49
(1 row)
```

## Related topics
* [`timestamp` and `timestamp with time zone` data types](../../types/timestamp)

