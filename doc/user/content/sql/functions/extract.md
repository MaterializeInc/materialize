---
title: "EXTRACT Function"
description: "Returns a specified time component from a time-based value"
menu:
  main:
    parent: 'sql-functions'
---

`EXTRACT` returns some time component from a time-based value, such as the year from a Timestamp.

## Signatures

{{< diagram "func-extract.svg" >}}

Parameter | Type                                                                                                                                                | Description
----------|-----------------------------------------------------------------------------------------------------------------------------------------------------|------------
_val_ | [`date`](../../types/date), [`time`](../../types/time), [`timestamp`](../../types/timestamp), [`timestamp with time zone`](../../types/timestamptz) | The value from which you want to extract a component.

### Arguments

`EXTRACT` supports multiple synonyms for most time periods.

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

`EXTRACT` returns a [`float`](../../types/float) value.

## Examples

### Extract second from timestamptz

```sql
SELECT EXTRACT(S FROM TIMESTAMP '2006-01-02 15:04:05.06')
AS sec_extr;
```
```nofmt
 sec_extr
----------
     5.06
```

### Extract century from date

```sql
SELECT EXTRACT(CENTURIES FROM DATE '2006-01-02')
AS sec_extr;
```
```nofmt
 sec_extr
----------
      21
```
