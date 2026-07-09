---
title: "array_agg function"
description: "Aggregate values (including nulls) as an array."
menu:
  main:
    parent: 'sql-functions'
---

The `array_agg(value)` function aggregates values (including nulls) as an array.
The input values to the aggregate can be [filtered](../filters).

## Syntax

{{% include-syntax file="examples/sql_functions/array_agg" example="syntax" %}}

## Signatures

Parameter | Type | Description
----------|------|------------
_value_ | [any](../../types) | The values you want aggregated.

### Return value

`array_agg` returns the aggregated values as an [array](../../types/array/).

Any `ORDER BY` applied to the input rows, for example in a feeding subquery, is
ignored. The order in which values are aggregated is otherwise unspecified. To
aggregate in a specific order, specify `ORDER BY` within the aggregate function
call itself.

## Details

### Usage in dataflows

While `array_agg` is available in Materialize, materializing `array_agg(values)`
is considered an incremental view maintenance anti-pattern. Any change to the data
underlying the function call will require the function to be recomputed entirely,
discarding the benefits of maintaining incremental updates.

Instead, we recommend that you materialize all components required for the
`array_agg` function call and create a non-materialized view using `array_agg`
on top of that. That pattern is illustrated in the following statements:

```mzsql
CREATE MATERIALIZED VIEW foo_view AS SELECT * FROM foo;
CREATE VIEW bar AS SELECT array_agg(foo_view.bar) FROM foo_view;
```

## Examples

```mzsql
SELECT
    title,
    ARRAY_AGG (
        first_name || ' ' || last_name
        ORDER BY
            last_name
    ) actors
FROM
    film
GROUP BY
    title;
```
