# Timestamp types

Expresses a date and time



`timestamp` and `timestamp with time zone` data expresses a date and time in
UTC.

## `timestamp` info

Detail | Info
-------|------
**Quick Syntax** | `TIMESTAMP WITH TIME ZONE '2007-02-01 15:04:05+06'`
**Size** | 8 bytes
**Catalog name** | `pg_catalog.timestamp`
**OID** | 1083
**Min value** | 4713 BC
**Max value** | 294276 AD
**Max resolution** | 1 microsecond

## `timestamp with time zone` info

Detail | Info
-------|------
**Quick Syntax** | `TIMESTAMPTZ '2007-02-01 15:04:05+06'`
**Aliases** | `timestamp with time zone`
**Size** | 8 bytes
**Catalog name** | `pg_catalog.timestamptz`
**OID** | 1184
**Min value** | 4713 BC
**Max value** | 294276 AD
**Max resolution** | 1 microsecond

## Syntax



```mzsql
TIMESTAMP [(<precision>)] [WITH [OUT] TIME ZONE] '<date_str>' [<time_str>] [<tz_offset>]
TIMESTAMPTZ [(<precision>)] '<date_str>' [<time_str>] [<tz_offset>]

```

| Syntax element | Description |
| --- | --- |
| **TIMESTAMP** [(`<precision>`)] | A timestamp without time zone. `<precision>` is optional and specifies the number of digits of precision to use to represent fractional seconds. If unspecified, timestamps use six digits of precision (microsecond resolution).  |
| **WITH TIME ZONE** \| **WITHOUT TIME ZONE** | Optional. `WITH TIME ZONE` applies the `<tz_offset>` field. `WITHOUT TIME ZONE` ignores the `<tz_offset>` field. This is the default if neither is specified.  |
| **TIMESTAMPTZ** [(`<precision>`)] | A shorter alias for `TIMESTAMP WITH TIME ZONE`.  |
| `'<date_str>'` | A string representing a date in `Y-M-D`, `Y M-D`, `Y M D` or `YMD` format.  |
| `<time_str>` | Optional. A string representing a time of day in `H:M:S.NS` format. Can be separated from `<date_str>` by a space or `T`.  |
| `<tz_offset>` | Optional. The timezone's distance from UTC, in hours, specified as `+HH` or `-HH`.  |


## Details

- `timestamp` and `timestamp with time zone` store data in
  [UTC](https://en.wikipedia.org/wiki/Coordinated_Universal_Time).
- The difference between the two types is that `timestamp with time zone` can read or write
  timestamps with the offset specified by the timezone. Importantly,
  `timestamp with time zone` itself doesn't store any timezone data; Materialize simply
  performs the conversion from the time provided and UTC.
- Materialize assumes all clients expect UTC time, and does not currently
  support any other timezones.

### Valid casts

In addition to the casts listed below, `timestamp` and `timestamptz` can be cast to and from each other implicitly.

#### From `timestamp` or `timestamptz`

You can [cast](../../functions/cast) `timestamp` or `timestamptz` to:

- [`date`](../date) (by assignment)
- [`text`](../text) (by assignment)
- [`time`](../time) (by assignment)

#### To `timestamp` or `timestamptz`

You can [cast](../../functions/cast) the following types to `timestamp` or `timestamptz`:

- [`date`](../date) (implicitly)
- [`text`](../text) (explicitly)

### Valid operations

`timestamp` and `timestamp with time zone` data (collectively referred to as
`timestamp/tz`) supports the following operations with other types.

Operation | Computes
----------|------------
[`date`](../date) `+` [`interval`](../interval) | [`timestamp/tz`](../timestamp)
[`date`](../date) `-` [`interval`](../interval) | [`timestamp/tz`](../timestamp)
[`date`](../date) `+` [`time`](../time) | [`timestamp/tz`](../timestamp)
[`timestamp/tz`](../timestamp) `+` [`interval`](../interval) | [`timestamp/tz`](../timestamp)
[`timestamp/tz`](../timestamp) `-` [`interval`](../interval) | [`timestamp/tz`](../timestamp)
[`timestamp/tz`](../timestamp) `-` [`timestamp/tz`](../timestamp) | [`interval`](../interval)

## Examples

### Return timestamp

```mzsql
SELECT TIMESTAMP '2007-02-01 15:04:05' AS ts_v;
```
```nofmt
        ts_v
---------------------
 2007-02-01 15:04:05
```

### Return timestamp with time zone

```mzsql
SELECT TIMESTAMPTZ '2007-02-01 15:04:05+06' AS tstz_v;
```
```nofmt
         tstz_v
-------------------------
 2007-02-01 09:04:05 UTC
```

## Related topics
* [`TIMEZONE` and `AT TIME ZONE` functions](../../functions/timezone-and-at-time-zone)
