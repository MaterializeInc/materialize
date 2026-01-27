# Aggregate function filters
Use FILTER to specify which rows are sent to an aggregate function
You can use a `FILTER` clause on an aggregate function to specify which rows are sent to an [aggregate function](/sql/functions/#aggregate-functions). Rows for which the `filter_clause` evaluates to true contribute to the aggregation.

Temporal filters cannot be used in aggregate function filters.

## Syntax



```mzsql
<aggregate_name> ( <expression> )
FILTER (WHERE <filter_clause>)

```

| Syntax element | Description |
| --- | --- |
| `<aggregate_name>` | The name of the aggregate function.  |
| `<expression>` | The expression to aggregate.  |
| **FILTER** (WHERE `<filter_clause>`) | Specifies which rows are sent to the aggregate function. Rows for which the `<filter_clause>` evaluates to true contribute to the aggregation. Temporal filters cannot be used in aggregate function filters.  |


## Examples

```mzsql
SELECT
    COUNT(*) AS unfiltered,
    -- The FILTER guards the evaluation which might otherwise error.
    COUNT(1 / (5 - i)) FILTER (WHERE i < 5) AS filtered
FROM generate_series(1,10) AS s(i)
```
