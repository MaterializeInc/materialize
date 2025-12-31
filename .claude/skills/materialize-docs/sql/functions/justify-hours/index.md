---
audience: developer
canonical_url: https://materialize.com/docs/sql/functions/justify-hours/
complexity: intermediate
description: Adjust interval so 24-hour time periods are represented as days
doc_type: reference
keywords:
- SELECT JUSTIFY_HOURS
- justify_hours function
product_area: Indexes
status: stable
title: justify_hours function
---

# justify_hours function

## Purpose
Adjust interval so 24-hour time periods are represented as days

If you need to understand the syntax and options for this command, you're in the right place.


Adjust interval so 24-hour time periods are represented as days



`justify_hours` returns a new [`interval`](../../types/interval) such that 24-hour time periods are
converted to days.

## Signatures

[See diagram: func-justify-hours.svg]

Parameter | Type                                                                                                                                                                                            | Description
----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------
_interval_ | [`interval`](../../types/interval) | The interval value to justify.


### Return value

`justify_hours` returns an [`interval`](../../types/interval) value.

## Example

This section covers example.

```mzsql
SELECT justify_hours(interval '27 hours');
```text
```nofmt
 justify_hours
----------------
 1 day 03:00:00
```

