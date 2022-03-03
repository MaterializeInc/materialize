---
title: "justify_hours Function"
description: "Adjust interval so 24-hour time periods are represented as days"
menu:
  main:
    parent: 'sql-functions'
---

`justify_hours` returns a new [`interval`](../../types/interval) such that 24-hour time periods are
converted to days.

## Signatures

{{< diagram "func-justify-hours.svg" >}}

Parameter | Type                                                                                                                                                                                            | Description
----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------
_interval_ | [`interval`](../../types/interval) | The interval value to justify.


### Return value

`justify_hours` returns an [`interval`](../../types/interval) value.

## Example

```sql
SELECT justify_hours(interval '27 hours');
```
```nofmt
 justify_hours
----------------
 1 day 03:00:00
```
