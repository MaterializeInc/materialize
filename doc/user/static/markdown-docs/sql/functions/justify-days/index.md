# justify_days function

Adjust interval so 30-day time periods are represented as months



`justify_days` returns a new [`interval`](../../types/interval) such that 30-day time periods are
converted to months.

## Signatures



```mzsql
justify_days ( <interval> )

```

| Syntax element | Description |
| --- | --- |
| `<interval>` | An [`interval`](/sql/types/interval/) value to justify. Returns a new interval such that 30-day time periods are converted to months.  |


Parameter | Type                                                                                                                                                                                            | Description
----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------
_interval_ | [`interval`](../../types/interval) | The interval value to justify.


### Return value

`justify_days` returns an [`interval`](../../types/interval) value.

## Example

```mzsql
SELECT justify_days(interval '35 days');
```
```nofmt
  justify_days
----------------
 1 month 5 days
```
