---
title: "datediff function"
description: "Returns the difference between two date, time or timestamp expressions based on the specified date or time part."
menu:
  main:
    parent: 'sql-functions'
---

The `datediff(datepart, start, end)` function returns the difference between two date, time or timestamp expressions based on the specified date or time part.

## Signatures

Parameter | Type | Description
----------|------|------------
_datepart_ | [text](../../types) | The date or time part to return. Must be one of [`datepart` specifiers](#datepart-specifiers).
_start_ | [date](../../types), [time](../../types), [timestamp](../../types), [timestamptz](../../types) | The date, time, or timestamp expression to start measuring from.
_end_ | [date](../../types), [time](../../types), [timestamp](../../types), [timestamptz](../../types) | The date, time, or timestamp expression to measuring until.

### `datepart` specifiers

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

## Examples

To calculate the difference between two dates in millennia:

```
SELECT datediff('millennia', '2000-12-31', '2001-01-01') as d;
  d
-----
  1
```

Even though the difference between `2000-12-31` and `2001-01-01` is a single day, a millennium boundary is crossed from one date to the other, so the result is `1`.

To see how this function handles leap years:
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

In the statement that uses a leap year (`2004`), the number of day boundaries crossed is `2`. When using a non-leap year (`2005`), only `1` day boundary is crossed.
