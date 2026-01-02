---
audience: developer
canonical_url: https://materialize.com/docs/sql/functions/justify-interval/
complexity: intermediate
description: Adjust interval using justify_days and justify_hours, with additional
  sign adjustments
doc_type: reference
keywords:
- justify_interval function
- SELECT JUSTIFY_INTERVAL
product_area: Indexes
status: stable
title: justify_interval function
---

# justify_interval function

## Purpose
Adjust interval using justify_days and justify_hours, with additional sign adjustments

If you need to understand the syntax and options for this command, you're in the right place.


Adjust interval using justify_days and justify_hours, with additional sign adjustments



`justify_interval` returns a new [`interval`](../../types/interval) such that 30-day time periods are
converted to months, 24-hour time periods are represented as days, and all fields have the same sign. It is a
combination of ['justify_days'](../justify-days) and ['justify_hours'](../justify-hours) with additional sign
adjustment.

## Signatures

[See diagram: func-justify-interval.svg]

Parameter | Type                                                                                                                                                                                            | Description
----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------
_interval_ | [`interval`](../../types/interval) | The interval value to justify.


### Return value

`justify_interval` returns an [`interval`](../../types/interval) value.

## Example

This section covers example.

```mzsql
SELECT justify_interval(interval '1 mon -1 hour');
```text
```nofmt
 justify_interval
------------------
 29 days 23:00:00
```

