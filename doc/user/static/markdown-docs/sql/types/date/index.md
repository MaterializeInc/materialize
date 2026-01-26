# date type

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



```mzsql
DATE '<date_str>' [<time_str>] [<tz_offset>]

```

| Syntax element | Description |
| --- | --- |
| `'<date_str>'` | A string representing a date in `Y-M-D`, `Y M-D`, `Y M D` or `YMD` format.  |
| `<time_str>` | _(NOP)_ A string representing a time of day in `H:M:S.NS` format. Can be separated from `<date_str>` by `T`.  |
| `<tz_offset>` | _(NOP)_ The timezone's distance from UTC, in hours, specified as `+HH` or `-HH`.  |


## Details

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

```mzsql
SELECT DATE '2007-02-01' AS date_v;
```
```nofmt
   date_v
------------
 2007-02-01
```
