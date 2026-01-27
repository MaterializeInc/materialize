# TIMEZONE and AT TIME ZONE functions
Converts timestamp to a different time zone.
`TIMEZONE` and `AT TIME ZONE` convert a [`timestamp`](../../types/timestamp/#timestamp-info) or a [`timestamptz`](../../types/timestamp/#timestamp-with-time-zone-info) to a different time zone.

**Known limitation:** You must explicitly cast the type for the time zone.

## Signatures



```mzsql
TIMEZONE ( <zone>::<type>, <timestamp> | <timestamptz> )

```

| Syntax element | Description |
| --- | --- |
| `<zone>` | A [`text`](/sql/types/text/) value specifying the target time zone.  |
| `<type>` | A [`text`](/sql/types/text/) or [`numeric`](/sql/types/numeric/) type specifying the datatype in which the time zone is expressed. **Known limitation:** You must explicitly cast the type for the time zone.  |
| `<timestamp>` | A [`timestamp`](/sql/types/timestamp/) value (timestamp without time zone).  |
| `<timestamptz>` | A [`timestamptz`](/sql/types/timestamp/) value (timestamp with time zone).  |




```mzsql
<timestamp> | <timestamptz> AT TIME ZONE <zone>::<type>

```

| Syntax element | Description |
| --- | --- |
| `<timestamp>` | A [`timestamp`](/sql/types/timestamp/) value (timestamp without time zone).  |
| `<timestamptz>` | A [`timestamptz`](/sql/types/timestamp/) value (timestamp with time zone).  |
| **AT TIME ZONE** `<zone>::<type>` | The target time zone. `<zone>` is a [`text`](/sql/types/text/) value. `<type>` is a [`text`](/sql/types/text/) or [`numeric`](/sql/types/numeric/) type. **Known limitation:** You must explicitly cast the type for the time zone.  |


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

```mzsql
SELECT TIMESTAMP '2020-12-21 18:53:49' AT TIME ZONE 'America/New_York'::text;
```
```
        timezone
------------------------
2020-12-21 23:53:49+00
(1 row)
```

```mzsql
SELECT TIMEZONE('America/New_York'::text,'2020-12-21 18:53:49');
```
```
        timezone
------------------------
2020-12-21 23:53:49+00
(1 row)
```

### Convert timestamp to another time zone, returned as specified local time

```mzsql
SELECT TIMESTAMPTZ '2020-12-21 18:53:49+08' AT TIME ZONE 'America/New_York'::text;
```
```
        timezone
------------------------
2020-12-21 05:53:49
(1 row)
```

```mzsql
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
