---
title: "string_agg function"
description: "Concatenates the non-null input values into a string. Each value after the first is preceded by the corresponding delimiter (if it's not null)."
menu:
  main:
    parent: 'sql-functions'
---

The `string_agg(value, delimiter)` aggregate function concatenates the non-null
input values (i.e. `value`) into [`text`](/sql/types/text). Each value after the
first is preceded by its corresponding `delimiter`, where _null_ values are
equivalent to an empty string.
The input values to the aggregate can be [filtered](../filters).

## Syntax

{{% include-syntax file="examples/sql_functions/string_agg" example="syntax" %}}

## Signatures

Parameter | Type | Description
----------|------|------------
_value_    | `text`  | The values to concatenate.
_delimiter_  | `text`  | The value to precede each concatenated value.

### Return value

`string_agg` returns a [`text`](/sql/types/text) value.

{{% include-headless "/headless/aggregate-input-order-ignored" %}}

### Usage in dataflows

While `string_agg` is available in Materialize, materializing views using it is
considered an incremental view maintenance anti-pattern. Any change to the data
underlying the function call will require the function to be recomputed
entirely, discarding the benefits of maintaining incremental updates.

Instead, we recommend that you materialize all components required for the
`string_agg` function call and create a non-materialized view using
`string_agg` on top of that. That pattern is illustrated in the following
statements:

```mzsql
CREATE MATERIALIZED VIEW foo_view AS SELECT * FROM foo;
CREATE VIEW bar AS SELECT string_agg(foo_view.bar, ',');
```

## Examples

```mzsql
SELECT string_agg(b, ',' ORDER BY a DESC) FROM table;
```
