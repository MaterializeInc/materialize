---
title: "jsonb_agg Function"
description: "Aggregates values (including nulls) as a jsonb array."
menu:
  main:
    parent: 'sql-functions'
---

The `jsonb_agg(expression)` function aggregates all values indicated by its expression,
returning the values (including nulls) as a jsonb array.

## Signatures

Parameter | Type | Description
----------|------|------------
_expression_ | [jsonb](../../types) | The values you want aggregated.

### Return value

`jsonb_agg` returns the aggregated values as a jsonb array.

## Details

### Usage in dataflows

While `jsonb_agg` is available in Materialize, materializing `jsonb_agg(expression)`
is considered an incremental view maintenance anti-pattern. Any change to the data
underlying the function call will require the function to be recomputed entirely,
discarding the benefits of maintaining incremental updates.

Instead, it is recommended to materialize all components required for the `jsonb_agg`
function call and create a non-materialized view using `jsonb_agg` on top of
that. That pattern is illustrated in the following statements:

```sql
CREATE MATERIALIZED VIEW foo_view AS SELECT * FROM foo;
CREATE VIEW bar AS jsonb_agg(foo_view.bar);
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
