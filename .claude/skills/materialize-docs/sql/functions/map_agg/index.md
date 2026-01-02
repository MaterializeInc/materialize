---
audience: developer
canonical_url: https://materialize.com/docs/sql/functions/map_agg/
complexity: advanced
description: Aggregate keys and values (excluding null keys) into a map
doc_type: reference
keywords:
- CREATE A
- CREATE VIEW
- SELECT KEY_COL
- SELECT MAP_AGG
- CREATE MATERIALIZED
- map_agg function
product_area: Indexes
status: stable
title: map_agg function
---

# map_agg function

## Purpose
Aggregate keys and values (excluding null keys) into a map

If you need to understand the syntax and options for this command, you're in the right place.


Aggregate keys and values (excluding null keys) into a map



The `map_agg(keys, values)` aggregate function zips together `keys`
and `values` into a [`map`](/sql/types/map).

The input values to the aggregate can be [filtered](../filters).

## Syntax

[See diagram: func-map-agg.svg]

## Signatures

This section covers signatures.

| Parameter | Type                       | Description              |
| --------- | -------------------------- | ------------------------ |
| _keys_    | [`text`](/sql/types/text/) | The keys to aggregate.   |
| _values_  | any                        | The values to aggregate. |

### Return value

`map_agg` returns the aggregated key–value pairs as a map.

-   Each row in the input corresponds to one key–value pair in the output,
    unless the `key` is null, in which case the pair is ignored. (`map` keys
    must be non-`NULL` strings.)
-   If multiple rows have the same key, we retain only the value sorted in the
    greatest/last position. You can determine this order using `ORDER BY` within
    the aggregate function itself; otherwise, incoming rows are not guaranteed
    to be handled in any order.

### Usage in dataflows

While `map_agg` is available in Materialize, materializing
`map_agg(expression)` is considered an incremental view maintenance
anti-pattern. Any change to the data underlying the function call will require
the function to be recomputed entirely, discarding the benefits of maintaining
incremental updates.

Instead, we recommend that you materialize all components required for the
`map_agg` function call and create a non-materialized view using
`map_agg` on top of that. That pattern is illustrated in the following
statements:

```mzsql
CREATE MATERIALIZED VIEW foo_view AS SELECT key_col, val_col FROM foo;
CREATE VIEW bar AS SELECT map_agg(key_col, val_col) FROM foo_view;
```bash

## Examples

Consider this query:

```mzsql
SELECT
  map_agg(
    t.k,
    t.v
    ORDER BY t.ts ASC, t.v DESC
  ) FILTER (WHERE t.v != -8) AS my_agg
FROM (
  VALUES
    -- k1
    ('k1', 3, now()),
    ('k1', 2, now() + INTERVAL '1s'),
    ('k1', 1, now() + INTERVAL '1s'),
    -- k2
    ('k2', -9, now() - INTERVAL '1s'),
    ('k2', -8, now()),
    ('k2', NULL, now() + INTERVAL '1s'),
    -- null
    (NULL, 99, now()),
    (NULL, 100, now())
  ) AS t(k, v, ts);
```text

```nofmt
      my_agg
------------------
 {k1=>1,k2=>-9}
```

In this example:

-   We order values by their timestamp (`t.ts ASC`) and then break ties using the
    smallest values (`t.v DESC`).
-   We filter out any values equal to exactly `-8`.
-   All keys with a `NULL` value get excluded automatically.
-   `k1` has two values tied with the same `t.ts` value; because we've also
    ordered `t.v DESC`, the last value we see will be `1`.
-   `k2` has its value for `-8` filtered out `FILTER (WHERE t.v != -8)`;
    however, this `FILTER` also removes the `NULL` value at `now() + INTERVAL '1s'` because `WHERE null != -8` evaluates to `false`.

