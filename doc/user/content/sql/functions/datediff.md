---
title: "datediff function"
description: "Determines the number of date part boundaries that are crossed between two expressions."
menu:
  main:
    parent: 'sql-functions'
---

The `datediff(datepart, start, end)`
function returns the number of `datepart` boundaries that are crossed between the two provided expressions.

## Signatures

Parameter | Type | Description
----------|------|------------
_datepart_ | [text](../../types) | The specific part of date or time that we operate on.
_start_ | [date](../../types), [time](../../types), [timestamp](../../types), [timestamptz](../../types) | Where we start measuring from.
_end_ | [date](../../types), [time](../../types), [timestamp](../../types), [timestamptz](../../types) | Where we measure until.

## Examples

This function returns the number of `datepart` _boundaries_ that are crossed.

```
SELECT datediff('millennia', '2000-12-31', '2001-01-01') as d;
  d
-----
  1
```

Even though the difference between `2000-12-31` and `2001-01-01` is only a single day, we cross
the millenium boundary from one date to the other, hence the above statement returns 1.

This function also considers leap years.

```
SELECT datediff('day', '2004-02-28', '2004-03-01') as leap;
    leap
------------
     2

SELECT datediff('day', '2005-02-28', '2005-03-01') as non_leap;
  non_leap
------------
     1
```

In the first statement when using the year `2004` (a leap year), the number of day boundaries we
cross is 2. Whereas in the second statement when we use the year `2005`, we only cross 1 day boundary.

## `datepart` specifiers

| Specifier                                                        | Description      |
|------------------------------------------------------------------|------------------|
| `millenniums`, `millennium`, `millennia`, `mil`                  | Millennia        |
| `centuries`, `century`, `cent`, `c`                              | Centuries        |
| `decades`, `decade`, `decs`, `dec`                               | Decades          |
| `years`, `year`, `yrs`, `yr`, `y`                                | Years            |
| `quarter`, `qtr`                                                 | Quarters         |
| `months`, `month`, `mons`, `mon`                                 | Months           |
| `weeks`, `week`, `w`                                             | Weeks            |
| `days`, `day`, `d`                                               | Days             |
| `hours`, `hour`, `hrs`, `hr`, `h`                                | Hours            |
| `minutes`, `minute`, `mins`, `min`, `m`                          | Minutes          |
| `seconds`, `second`, `secs`, `sec`, `s`                          | Seconds          |
| `milliseconds`, `millisecond`, `mseconds`, `msecs`, `msec`, `ms` | Milliseconds     |
| `microseconds`, `microsecond`, `useconds`, `usecs`, `usec`, `us` | Microseconds     |
