---
title: "justify_days function"
description: "Adjust interval so 30-day time periods are represented as months"
menu:
  main:
    parent: 'sql-functions'
---

`justify_days` returns a new [`interval`](../../types/interval) such that 30-day time periods are
converted to months.

## Signatures

{{< diagram "func-justify-days.svg" >}}

Parameter | Type                                                                                                                                                                                            | Description
----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------
_interval_ | [`interval`](../../types/interval) | The interval value to justify.


### Return value

`justify_days` returns an [`interval`](../../types/interval) value.

## Example

```sql
SELECT justify_days(interval '35 days');
```
```nofmt
  justify_days
----------------
 1 month 5 days
```
