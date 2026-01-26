# justify_hours function

Adjust interval so 24-hour time periods are represented as days



`justify_hours` returns a new [`interval`](../../types/interval) such that 24-hour time periods are
converted to days.

## Signatures



```mzsql
justify_hours ( <interval> )

```

| Syntax element | Description |
| --- | --- |
| `<interval>` | An [`interval`](/sql/types/interval/) value to justify. Returns a new interval such that 24-hour time periods are converted to days.  |


Parameter | Type                                                                                                                                                                                            | Description
----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------
_interval_ | [`interval`](../../types/interval) | The interval value to justify.


### Return value

`justify_hours` returns an [`interval`](../../types/interval) value.

## Example

```mzsql
SELECT justify_hours(interval '27 hours');
```
```nofmt
 justify_hours
----------------
 1 day 03:00:00
```
