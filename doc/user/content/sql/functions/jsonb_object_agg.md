---
title: "jsonb_object_agg function"
description: "Aggregate keys and values (including nulls) into a jsonb object"
menu:
  main:
    parent: 'sql-functions'
---

The `jsonb_object_agg(keys, values)` aggregate function zips together `keys`
and `values` into a [`jsonb`](/sql/types/jsonb) object.
The input values to the aggregate can be [filtered](../filters).

## Syntax

{{< diagram "jsonb-object-agg.svg" >}}

## Signatures

Parameter | Type | Description
----------|------|------------
_keys_    | any  | The keys to aggregate.
_values_  | any  | The values to aggregate.

### Return value

`jsonb_object_agg` returns the aggregated key–value pairs as a jsonb object.
Each row in the input corresponds to one key–value pair in the output.

If there are duplicate keys in the input, it is unspecified which key–value
pair is retained in the output.

If `keys` is null for any input row, that entry pair will be dropped.

This function always executes on the data from `value` as if it were sorted in ascending order before the function call. Any specified ordering is
ignored. If you need to perform aggregation in a specific order, you must specify `ORDER BY` within the aggregate function call itself. Otherwise incoming rows are not guaranteed any order.

### Usage in dataflows

While `jsonb_object_agg` is available in Materialize, materializing
`jsonb_object_agg(expression)` is considered an incremental view maintenance
anti-pattern. Any change to the data underlying the function call will require
the function to be recomputed entirely, discarding the benefits of maintaining
incremental updates.

Instead, we recommend that you materialize all components required for the
`jsonb_object_agg` function call and create a non-materialized view using
`jsonb_object_agg` on top of that. That pattern is illustrated in the following
statements:

```sql
CREATE MATERIALIZED VIEW foo_view AS SELECT key_col, val_col FROM foo;
CREATE VIEW bar AS SELECT jsonb_object_agg(key_col, val_col) FROM foo_view;
```

## Examples

Consider this query:
```sql
SELECT
  jsonb_object_agg(
    t.col1,
    t.col2
    ORDER BY t.ts ASC
  ) FILTER (WHERE t.col2 IS NOT NULL) AS my_agg
FROM (
  VALUES
  ('k1', 1, now()),
  ('k2', 2, now() - INTERVAL '1s'),
  ('k2', -1, now()),
  ('k2', NULL, now() + INTERVAL '1s')
  ) AS t(col1, col2, ts);
```
```nofmt
      my_agg
------------------
 {"k1": 1, "k2": -1}
```
In this example, there are multiple values associated with the `k2` key.

The `FILTER` clause in the statement above returns values that are not `NULL` and orders them by the timestamp column to return the most recent associated value.

## See also

* [`jsonb_agg`](/sql/functions/jsonb_agg)
