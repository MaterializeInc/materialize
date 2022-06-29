---
title: "date_bin_hopping function"
description: "Builds a set of 'hopping' timestamps from a source timestamp"
draft: true
#menu:
  #main:
    #parent: 'sql-functions'
---

`date_bin_hopping` returns every "[binned](../date-bin)" value:
- Greater than or equal to the source timestamp minus `width`
- Less than or equal to the source timestamp
- Where the bin's stride is equal to `hop`

`date_bin_hopping` provides a primitive operation to express what are referred
to as "hopping windows" in other systems.

## Signatures

{{< diagram "func-date-bin-hopping.svg" >}}

Parameter | Type | Description
----------|------|------------
_hop_ | [`interval`] | Define bins of this width.
_width_ | [`interval`] | Produce an "oldest" bin at the equivalent to `date_bin(hop, source + hop - width)`.
_source_ | [`timestamp`], [`timestamp with time zone`] | Determine this value's bins; produce a "newest" bin at the equivalent to `date_bin(hop, source)`.
_origin_ | Must be the same as _source_ | Align bins to this value. If not provided, defaults to the Unix epoch.

### Return value

`date_bin_hopping` returns the same type as _source_.

## Details

- `origin` and `source` cannot be more than 2^63 nanoseconds apart.
- `hop` and `width` cannot contain any years or months, but e.g. can exceed 30
  days.
- `hop` and `width` only support values between 1 and 9,223,372,036 seconds.

## Examples

```sql
SELECT * FROM date_bin_hopping('45s', '1m', TIMESTAMP '2001-01-01 00:01:20');
```
```nofmt
  date_bin_hopping
---------------------
 2001-01-01 00:00:30
 2001-01-01 00:01:15
```

```sql
SELECT date_bin_hopping AS timeframe_start, sum(v)
  FROM ( VALUES
    (TIMESTAMP '2021-01-01 01:05', 41),
    (TIMESTAMP '2021-01-01 01:07', 21),
    (TIMESTAMP '2021-01-01 01:09', 51),
    (TIMESTAMP '2021-01-01 01:11', 31),
    (TIMESTAMP '2021-01-01 01:13', 11),
    (TIMESTAMP '2021-01-01 01:17', 61)
  ) t (ts, v),
  date_bin_hopping(INTERVAL '5m', INTERVAL '10m', t.ts)
GROUP BY timeframe_start
ORDER BY 1;
```
```nofmt
   timeframe_start   | sum
---------------------+-----
 2021-01-01 01:00:00 | 113
 2021-01-01 01:05:00 | 155
 2021-01-01 01:10:00 | 103
 2021-01-01 01:15:00 |  61
```

[`interval`]: ../../types/interval
[`timestamp`]: ../../types/timestamp
[`timestamp with time zone`]: ../../types/timestamptz
