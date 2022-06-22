---
title: "jsonb_agg function"
description: "Aggregates values (including nulls) as a jsonb array."
menu:
  main:
    parent: 'sql-functions'
---

The `jsonb_agg(expression)` function aggregates all values indicated by its expression,
returning the values (including nulls) as a [`jsonb`](/sql/types/jsonb) array.
The input values to the aggregate can be [filtered](../filters).

## Syntax

{{< diagram "jsonb-agg.svg" >}}

## Signatures

Parameter | Type | Description
----------|------|------------
_expression_ | [jsonb](../../types) | The values you want aggregated.

### Return value

`jsonb_agg` returns the aggregated values as a `jsonb` array.

This function always executes on the data from `value` as if it were sorted in ascending order before the function call. Any specified ordering is
ignored. If you need to perform aggregation in a specific order, you must specify `ORDER BY` within the aggregate function call itself. Otherwise incoming rows are not guaranteed any order.

## Details

### Usage in dataflows

While `jsonb_agg` is available in Materialize, materializing `jsonb_agg(expression)`
is considered an incremental view maintenance anti-pattern. Any change to the data
underlying the function call will require the function to be recomputed entirely,
discarding the benefits of maintaining incremental updates.

Instead, we recommend that you materialize all components required for the
`jsonb_agg` function call and create a non-materialized view using `jsonb_agg`
on top of that. That pattern is illustrated in the following statements:

```sql
CREATE MATERIALIZED VIEW foo_view AS SELECT * FROM foo;
CREATE VIEW bar AS SELECT jsonb_agg(foo_view.bar) FROM foo_view;
```

## Examples

```sql
SELECT jsonb_agg(1);
```
```nofmt
 jsonb_agg
-----------
 [1]
```
<hr/>

```sql
SELECT jsonb_agg('example'::text);
```
```nofmt
  jsonb_agg
-------------
 ["example"]
```

## See also

* [`jsonb_object_agg`](/sql/functions/jsonb_object_agg)
