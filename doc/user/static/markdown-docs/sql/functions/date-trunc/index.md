# date_trunc function

Truncates a timestamp at the specified time component



`date_trunc` computes _ts_val_'s "floor value" of the specified time component,
i.e. the largest time component less than or equal to the provided value.

To align values along arbitrary values, see [`date_bin`].

## Signatures



```mzsql
date_trunc ( '<time_unit>', <val> )

```

| Syntax element | Description |
| --- | --- |
| `'<time_unit>'` | The time unit to truncate to. Valid values: `microseconds`, `milliseconds`, `second`, `minute`, `hour`, `day`, `week`, `month`, `quarter`, `year`, `decade`, `century`, `millenium`.  |
| `<val>` | A [`timestamp`](/sql/types/timestamp/), [`timestamp with time zone`](/sql/types/timestamp/), or [`interval`](/sql/types/interval/) value to truncate.  |


Parameter | Type | Description
----------|------|------------
_val_ | [`timestamp`], [`timestamp with time zone`], [`interval`] | The value you want to truncate.

### Return value

`date_trunc` returns the same type as _val_.

## Examples

```mzsql
SELECT date_trunc('hour', TIMESTAMP '2019-11-26 15:56:46.241150') AS hour_trunc;
```
```nofmt
          hour_trunc
-------------------------------
 2019-11-26 15:00:00.000000000
```

```mzsql
SELECT date_trunc('year', TIMESTAMP '2019-11-26 15:56:46.241150') AS year_trunc;
```
```nofmt
          year_trunc
-------------------------------
 2019-01-01 00:00:00.000000000
```

```mzsql
SELECT date_trunc('millennium', INTERVAL '1234 years 11 months 23 days 23:59:12.123456789') AS millennium_trunc;
```
```nofmt
          millennium_trunc
-------------------------------
 1000 years
```

[`date_bin`]: ../date-bin
[`interval`]: ../../types/interval/
[`timestamp`]: ../../types/timestamp
[`timestamp with time zone`]: ../../types/timestamptz
