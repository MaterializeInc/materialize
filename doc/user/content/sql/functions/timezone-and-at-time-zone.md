---
title: "TIMEZONE and AT TIME ZONE functions"
description: "Converts timestamp to a different time zone."
menu:
  main:
    parent: 'sql-functions'
---

`TIMEZONE` and `AT TIME ZONE` convert a [`timestamp`](../../types/timestamp/#timestamp-info) or a [`timestamptz`](../../types/timestamp/#timestamp-with-time-zone-info) to a different time zone.

**Known limitation:** You must explicitly cast the type for the time zone.

## Signatures

{{< diagram "func-timezone.svg" >}}

{{< diagram "func-at-time-zone.svg" >}}

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

### Convert timestamp to another time zone, returned as UTC with offset

```sql
SELECT TIMESTAMP '2020-12-21 18:53:49' AT TIME ZONE 'America/New_York'::text;
```
```
        timezone
------------------------
2020-12-21 23:53:49+00
(1 row)
```

```sql
SELECT TIMEZONE('America/New_York'::text,'2020-12-21 18:53:49');
```
```
        timezone
------------------------
2020-12-21 23:53:49+00
(1 row)
```

### Convert timestamp to another time zone, returned as specified local time

```sql
SELECT TIMESTAMPTZ '2020-12-21 18:53:49+08' AT TIME ZONE 'America/New_York'::text;
```
```
        timezone
------------------------
2020-12-21 05:53:49
(1 row)
```

```sql
SELECT TIMEZONE ('America/New_York'::text,'2020-12-21 18:53:49+08');
```
```
        timezone
------------------------
2020-12-21 05:53:49
(1 row)
```

## Related topics
* [`timestamp` and `timestamp with time zone` data types](../../types/timestamp)
