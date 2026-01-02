---
audience: developer
canonical_url: https://materialize.com/docs/sql/functions/list_agg/
complexity: advanced
description: Concatenates input values (including nulls) into a string.
doc_type: reference
keywords:
- CREATE A
- SELECT TITLE
- SELECT LIST_AGG
- list_agg function
- CREATE VIEW
- CREATE MATERIALIZED
product_area: Indexes
status: stable
title: list_agg function
---

# list_agg function

## Purpose
Concatenates input values (including nulls) into a string.

If you need to understand the syntax and options for this command, you're in the right place.


Concatenates input values (including nulls) into a string.



The `list_agg(value)` aggregate function concatenates
input values (including nulls) into a [`list`](/sql/types/list).
The input values to the aggregate can be [filtered](../filters).

## Syntax

[See diagram: list-agg.svg]

## Signatures

Parameter | Type | Description
----------|------|------------
_value_    | `text`  | The values to concatenate.

### Return value

`list_agg` returns a [`list`](/sql/types/list) value.

This function always executes on the data from `value` as if it were sorted in ascending order before the function call. Any specified ordering is
ignored. If you need to perform aggregation in a specific order, you must specify `ORDER BY` within the aggregate function call itself. Otherwise incoming rows are not guaranteed any order.

## Details

This section covers details.

### Usage in dataflows

While `list_agg` is available in Materialize, materializing `list_agg(values)`
is considered an incremental view maintenance anti-pattern. Any change to the data
underlying the function call will require the function to be recomputed entirely,
discarding the benefits of maintaining incremental updates.

Instead, we recommend that you materialize all components required for the
`list_agg` function call and create a non-materialized view using `list_agg`
on top of that. That pattern is illustrated in the following statements:

```mzsql
CREATE MATERIALIZED VIEW foo_view AS SELECT * FROM foo;
CREATE VIEW bar AS SELECT list_agg(foo_view.bar) FROM foo_view;
```bash

## Examples

This section covers examples.

```mzsql
SELECT
    title,
    LIST_AGG (
        first_name || ' ' || last_name
        ORDER BY
            last_name
    ) actors
FROM
    film
GROUP BY
    title;
```

