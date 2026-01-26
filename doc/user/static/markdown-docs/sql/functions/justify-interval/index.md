# justify_interval function

Adjust interval using justify_days and justify_hours, with additional sign adjustments



`justify_interval` returns a new [`interval`](../../types/interval) such that 30-day time periods are
converted to months, 24-hour time periods are represented as days, and all fields have the same sign. It is a
combination of ['justify_days'](../justify-days) and ['justify_hours'](../justify-hours) with additional sign
adjustment.

## Signatures



```mzsql
justify_interval ( <interval> )

```

| Syntax element | Description |
| --- | --- |
| `<interval>` | An [`interval`](/sql/types/interval/) value to justify. Returns a new interval such that 30-day time periods are converted to months, 24-hour time periods are represented as days, and all fields have the same sign. It is a combination of [`justify_days`](/sql/functions/justify-days) and [`justify_hours`](/sql/functions/justify-hours) with additional sign adjustment.  |


Parameter | Type                                                                                                                                                                                            | Description
----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------
_interval_ | [`interval`](../../types/interval) | The interval value to justify.


### Return value

`justify_interval` returns an [`interval`](../../types/interval) value.

## Example

```mzsql
SELECT justify_interval(interval '1 mon -1 hour');
```
```nofmt
 justify_interval
------------------
 29 days 23:00:00
```
