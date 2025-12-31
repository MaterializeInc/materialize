---
audience: developer
canonical_url: https://materialize.com/docs/sql/functions/justify-days/
complexity: intermediate
description: Adjust interval so 30-day time periods are represented as months
doc_type: reference
keywords:
- justify_days function
- SELECT JUSTIFY_DAYS
product_area: Indexes
status: stable
title: justify_days function
---

# justify_days function

## Purpose
Adjust interval so 30-day time periods are represented as months

If you need to understand the syntax and options for this command, you're in the right place.


Adjust interval so 30-day time periods are represented as months



`justify_days` returns a new [`interval`](../../types/interval) such that 30-day time periods are
converted to months.

## Signatures

[See diagram: func-justify-days.svg]

Parameter | Type                                                                                                                                                                                            | Description
----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------
_interval_ | [`interval`](../../types/interval) | The interval value to justify.


### Return value

`justify_days` returns an [`interval`](../../types/interval) value.

## Example

This section covers example.

```mzsql
SELECT justify_days(interval '35 days');
```text
```nofmt
  justify_days
----------------
 1 month 5 days
```

