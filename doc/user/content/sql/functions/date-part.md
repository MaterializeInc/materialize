---
title: "date_part function"
description: "Returns a specified time component from a time-based value"
menu:
  main:
    parent: 'sql-functions'
---

`date_part` returns some time component from a time-based value, such as the year from a Timestamp.
It is mostly functionally equivalent to the function [`EXTRACT`](../extract), except to maintain
PostgreSQL compatibility, `date_part` returns values of type [`float`](../../types/float). This can
result in a loss of precision in certain uses. Using [`EXTRACT`](../extract) is recommended instead.

## Signatures

{{< diagram "func-date-part.svg" >}}

Parameter | Type                                                                                                                                                          | Description
----------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|------------
_val_ | [`time`](../../types/time), [`timestamp`](../../types/timestamp), [`timestamp with time zone`](../../types/timestamptz), [`interval`](../../types/interval), [`date`](../../types/date) | The value from which you want to extract a component. vals of type [`date`](../../types/date) are first cast to type [`timestamp`](../../types/timestamp).

### Arguments

`date_part` supports multiple synonyms for most time periods.

Time period | Synonyms
------------|---------
epoch | `EPOCH`
millennium   | `MIL`, `MILLENNIUM`, `MILLENNIA`
century | `C`, `CENT`, `CENTURY`, `CENTURIES`
decade  |  `DEC`, `DECS`, `DECADE`, `DECADES`
year | `Y`, `YEAR`, `YEARS`, `YR`, `YRS`
quarter  | `QTR`, `QUARTER`
month | `MON`, `MONS`, `MONTH`, `MONTHS`
week | `W`, `WEEK`, `WEEKS`
day  | `D`, `DAY`, `DAYS`
hour   |`H`, `HR`, `HRS`, `HOUR`, `HOURS`
minute | `M`, `MIN`, `MINS`, `MINUTE`, `MINUTES`
second | `S`, `SEC`, `SECS`, `SECOND`, `SECONDS`
microsecond  | `US`, `USEC`, `USECS`, `USECONDS`, `MICROSECOND`, `MICROSECONDS`
millisecond | `MS`, `MSEC`, `MSECS`, `MSECONDS`, `MILLISECOND`, `MILLISECONDS`
day of week |`DOW`
ISO day of week | `ISODOW`
day of year | `DOY`

### Return value

`date_part` returns a [`float`](../../types/float) value.

## Examples

### Extract second from timestamptz

```sql
SELECT date_part('S', TIMESTAMP '2006-01-02 15:04:05.06');
```
```nofmt
 date_part
-----------
      5.06
```

### Extract century from date

```sql
SELECT date_part('CENTURIES', DATE '2006-01-02');
```
```nofmt
 date_part
-----------
        21
```
