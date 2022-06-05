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

{{< diagram "string-agg.svg" >}}

## Signatures

Parameter | Type | Description
----------|------|------------
_value_    | `text`  | The values to concatenate.
_delimiter_  | `text`  | The value to precede each concatenated value.

### Return value

`string_agg` returns a [`text`](/sql/types/text) value.

This function always executes on the data from `value` as if it were sorted in ascending order before the function call. Any specified ordering is
ignored. If you need to perform aggregation in a specific order, you must specify `ORDER BY` within the aggregate function call itself. Otherwise incoming rows are not guaranteed any order.

### Usage in dataflows

While `string_agg` is available in Materialize, materializing views using it is
considered an incremental view maintenance anti-pattern. Any change to the data
underlying the function call will require the function to be recomputed
entirely, discarding the benefits of maintaining incremental updates.

Instead, we recommend that you materialize all components required for the
`string_agg` function call and create a non-materialized view using
`string_agg` on top of that. That pattern is illustrated in the following
statements:

```sql
CREATE MATERIALIZED VIEW foo_view AS SELECT * FROM foo;
CREATE VIEW bar AS SELECT string_agg(foo_view.bar, ',');
```

## Examples

```sql
SELECT string_agg(column1, column2)
FROM (
    VALUES ('z', ' !'), ('a', ' @'), ('m', ' #')
);
```
```nofmt
 string_agg
------------
 a #m !z
```

Note that in the following example, the `ORDER BY` of the subquery feeding into `string_agg` gets ignored.

```sql
SELECT column1, column2
FROM (
    VALUES ('z', ' !'), ('a', ' @'), ('m', ' #')
) ORDER BY column1 DESC;
```
```nofmt
 column1 | column2
---------+---------
 z       |  !
 m       |  #
 a       |  @
```

```sql
SELECT string_agg(column1, column2)
FROM (
    SELECT column1, column2
    FROM (
        VALUES ('z', ' !'), ('a', ' @'), ('m', ' #')
    ) f ORDER BY column1 DESC
) g;
```
```nofmt
 string_agg
------------
 a #m !z
```

```sql
SELECT string_agg(b, ',' ORDER BY a DESC) FROM table;
```
