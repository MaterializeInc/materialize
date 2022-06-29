---
title: "date_bin function"
description: "Bins a timestamp into a specified interval"
menu:
  main:
    parent: 'sql-functions'
---

`date_bin` returns the largest value less than or equal to `source` that is a
multiple of `stride` starting at `origin`––for shorthand, we call this
"binning."

For example, on this number line of abstract units:

```nofmt
          x
...|---|---|---|...
   7   8   9   10
```

With a `stride` of 1, we would have bins (...7, 8, 9, 10...).

Here are some example results:

`source` | `origin` | `stride`  | Result
---------|----------|-----------|-------
8.75     | 1        | 1 unit    | 8
8.75     | 1        | 2 units   | 7
8.75     | 1.75     | 1.5 units | 7.75

`date_bin` is similar to [`date_trunc`], but supports arbitrary
strides, rather than only unit times.

## Signatures

{{< diagram "func-date-bin.svg" >}}

Parameter | Type | Description
----------|------|------------
_stride_ | [`interval`] | Define bins of this width.
_source_ | [`timestamp`], [`timestamp with time zone`] | Determine this value's bin.
_origin_ | Must be the same as _source_ | Align bins to this value.

### Return value

`date_bin` returns the same type as _source_.

## Details

- `origin` and `source` cannot be more than 2^63 nanoseconds apart.
- `stride` cannot contain any years or months, but e.g. can exceed 30 days.
- `stride` only supports values between 1 and 9,223,372,036 seconds.

## Examples

```sql
SELECT
  date_bin(
    '15 minutes',
    timestamp '2001-02-16 20:38:40',
    timestamp '2001-02-16 20:05:00'
  );
```
```nofmt
      date_bin
---------------------
 2001-02-16 20:35:00
```

```sql
SELECT
  str,
  "interval",
  date_trunc(str, ts)
    = date_bin("interval"::interval, ts, timestamp '2001-01-01') AS equal
FROM (
  VALUES
  ('week', '7 d'),
  ('day', '1 d'),
  ('hour', '1 h'),
  ('minute', '1 m'),
  ('second', '1 s')
) intervals (str, interval),
(VALUES (timestamp '2020-02-29 15:44:17.71393')) ts (ts);
```
```nofmt
  str   | interval | equal
--------+----------+-------
 day    | 1 d      | t
 hour   | 1 h      | t
 week   | 7 d      | t
 minute | 1 m      | t
 second | 1 s      | t
```

[`date_trunc`]: ../date-trunc
[`interval`]: ../../types/interval
[`timestamp`]: ../../types/timestamp
[`timestamp with time zone`]: ../../types/timestamptz
