---
audience: developer
canonical_url: https://materialize.com/docs/sql/functions/extract/
complexity: intermediate
description: Returns a specified time component from a time-based value
doc_type: reference
keywords:
- EXTRACT function
- SELECT EXTRACT
product_area: Indexes
status: stable
title: EXTRACT function
---

# EXTRACT function

## Purpose
Returns a specified time component from a time-based value

If you need to understand the syntax and options for this command, you're in the right place.


Returns a specified time component from a time-based value



`EXTRACT` returns some time component from a time-based value, such as the year from a Timestamp.

## Signatures

[See diagram: func-extract.svg]

Parameter | Type                                                                                                                                                                                    | Description
----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------
_val_ | [`date`](../../types/date), [`time`](../../types/time), [`timestamp`](../../types/timestamp), [`timestamp with time zone`](../../types/timestamptz), [`interval`](../../types/interval) | The value from which you want to extract a component.

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

`EXTRACT` returns a [`numeric`](../../types/numeric) value.

## Examples

This section covers examples.

### Extract second from timestamptz

```mzsql
SELECT EXTRACT(S FROM TIMESTAMP '2006-01-02 15:04:05.06');
```text
```nofmt
 extract
---------
    5.06
```bash

### Extract century from date

```mzsql
SELECT EXTRACT(CENTURIES FROM DATE '2006-01-02');
```text
```nofmt
 extract
---------
      21
```

