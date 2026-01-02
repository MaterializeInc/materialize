---
audience: developer
canonical_url: https://materialize.com/docs/sql/functions/
complexity: advanced
description: Learn more about the SQL functions and operators supported in Materialize
doc_type: reference
keywords:
- Implicit
- unmaterializable
- 'Warning:'
- CREATE MATERIALIZED
- SELECT COUNT
- SQL functions & operators
- SELECT SIDE_EFFECTING_FUNCTION
- CREATE A
- Assignment
- side-effecting
product_area: Indexes
status: stable
title: SQL functions & operators
---

# SQL functions & operators

## Purpose
Learn more about the SQL functions and operators supported in Materialize

If you need to understand the syntax and options for this command, you're in the right place.


Learn more about the SQL functions and operators supported in Materialize


This page details Materialize's supported SQL [functions](#functions) and [operators](#operators).

## Functions

This section covers functions.

### Unmaterializable functions

Several functions in Materialize are **unmaterializable** because their output
depends upon state besides their input parameters, like the value of a session
parameter or the timestamp of the current transaction. You cannot create an
[index](/sql/create-index) or materialized view that depends on an
unmaterializable function, but you can use them in non-materialized views and
one-off [`SELECT`](/sql/select) statements.

Unmaterializable functions are marked as such in the table below.

### Side-effecting functions

Several functions in Materialize are **side-effecting** because their evaluation
changes system state. For example, the `pg_cancel_backend` function allows
canceling a query running on another connection.

Materialize offers only limited support for these functions. They may be called
only at the top level of a `SELECT` statement, like so:

```mzsql
SELECT side_effecting_function(arg, ...);
```text

You cannot manipulate or alias the function call expression, call multiple
side-effecting functions in the same `SELECT` statement, nor add any additional
clauses to the `SELECT` statement (e.g., `FROM`, `WHERE`).

Side-effecting functions are marked as such in the table below.

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: fnlist --> --> -->

## Operators

This section covers operators.

### Generic operators

Operator | Computes
---------|---------
`val::type` | Cast of `val` as `type` ([docs](cast))

### Boolean operators

Operator | Computes
---------|---------
`AND` | Boolean "and"
`OR` | Boolean "or"
`=` | Equality
`<>` | Inequality
`!=` | Inequality
`<` | Less than
`>` | Greater than
`<=` | Less than or equal to
`>=` | Greater than or equal to
`a BETWEEN x AND y` | `a >= x AND a <= y`
`a NOT BETWEEN x AND y` | `a < x OR a > y`
`a IS NULL` | `a = NULL`
`a ISNULL` | `a = NULL`
`a IS NOT NULL` | `a != NULL`
`a IS TRUE` | `a` is true, requiring `a` to be a boolean
`a IS NOT TRUE` | `a` is not true, requiring `a` to be a boolean
`a IS FALSE` | `a` is false, requiring `a` to be a boolean
`a IS NOT FALSE` | `a` is not false, requiring `a` to be a boolean
`a IS UNKNOWN` | `a = NULL`, requiring `a` to be a boolean
`a IS NOT UNKNOWN` | `a != NULL`, requiring `a` to be a boolean
`a LIKE match_expr [ ESCAPE escape_char ]` | `a` matches `match_expr`, using [SQL LIKE matching](https://www.postgresql.org/docs/13/functions-matching.html#FUNCTIONS-LIKE)
`a ILIKE match_expr [ ESCAPE escape_char ]` | `a` matches `match_expr`, using case-insensitive [SQL LIKE matching](https://www.postgresql.org/docs/13/functions-matching.html#FUNCTIONS-LIKE)

### Numbers operators

Operator | Computes
---------|---------
`+` | Addition
`-` | Subtraction
`*` | Multiplication
`/` | Division
`%` | Modulo
`&` | Bitwise AND
<code>&vert;</code> | Bitwise OR
`#` | Bitwise XOR
`~` | Bitwise NOT
`<<`| Bitwise left shift
`>>`| Bitwise right shift

### String operators

Operator | Computes
---------|---------
<code>&vert;&vert;</code> | Concatenation
`~~` | Matches LIKE pattern case sensitively, see [SQL LIKE matching](https://www.postgresql.org/docs/13/functions-matching.html#FUNCTIONS-LIKE)
`~~*` | Matches LIKE pattern case insensitively (ILIKE), see [SQL LIKE matching](https://www.postgresql.org/docs/13/functions-matching.html#FUNCTIONS-LIKE)
`!~~` | Matches NOT LIKE pattern (case sensitive), see [SQL LIKE matching](https://www.postgresql.org/docs/13/functions-matching.html#FUNCTIONS-LIKE)
`!~~*` | Matches NOT ILIKE pattern (case insensitive), see [SQL LIKE matching](https://www.postgresql.org/docs/13/functions-matching.html#FUNCTIONS-LIKE)
`~` | Matches regular expression, case sensitive
`~*` | Matches regular expression, case insensitive
`!~` | Matches regular expression case sensitively, and inverts the match
`!~*` | Match regular expression case insensitively, and inverts the match

The regular expression syntax supported by Materialize is documented by the
[Rust `regex` crate](https://docs.rs/regex/*/#syntax).
The maximum length of a regular expression is 1 MiB in its raw form, and 10 MiB
after compiling it.

> **Warning:** 
Materialize regular expressions are similar to, but not identical to, PostgreSQL
regular expressions.


### Time-like operators

Operation | Computes
----------|------------
[`date`](../types/date) `+` [`interval`](../types/interval) | [`timestamp`](../types/timestamp)
[`date`](../types/date) `-` [`interval`](../types/interval) | [`timestamp`](../types/timestamp)
[`date`](../types/date) `+` [`time`](../types/time) | [`timestamp`](../types/timestamp)
[`date`](../types/date) `-` [`date`](../types/date) | [`integer`](../types/integer)
[`timestamp`](../types/timestamp) `+` [`interval`](../types/interval) | [`timestamp`](../types/timestamp)
[`timestamp`](../types/timestamp) `-` [`interval`](../types/interval) | [`timestamp`](../types/timestamp)
[`timestamp`](../types/timestamp) `-` [`timestamp`](../types/timestamp) | [`interval`](../types/interval)
[`time`](../types/time) `+` [`interval`](../types/interval) | `time`
[`time`](../types/time) `-` [`interval`](../types/interval) | `time`
[`time`](../types/time) `-` [`time`](../types/time) | [`interval`](../types/interval)

### JSON operators

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: json-operators --> --> -->

### Map operators

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: map-operators --> --> -->

### List operators

List operators are [polymorphic](../types/list/#polymorphism).

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: list-operators --> --> -->


---

## Aggregate function filters


You can use a `FILTER` clause on an aggregate function to specify which rows are sent to an [aggregate function](/sql/functions/#aggregate-functions). Rows for which the `filter_clause` evaluates to true contribute to the aggregation.

Temporal filters cannot be used in aggregate function filters.

## Syntax

[See diagram: aggregate-with-filter.svg]

## Examples

This section covers examples.

```mzsql
SELECT
    COUNT(*) AS unfiltered,
    -- The FILTER guards the evaluation which might otherwise error.
    COUNT(1 / (5 - i)) FILTER (WHERE i < 5) AS filtered
FROM generate_series(1,10) AS s(i)
```text


---

## array_agg function


The `array_agg(value)` function aggregates values (including nulls) as an array.
The input values to the aggregate can be [filtered](../filters).

## Syntax

[See diagram: array-agg.svg]

## Signatures

Parameter | Type | Description
----------|------|------------
_value_ | [any](../../types) | The values you want aggregated.

### Return value

`array_agg` returns the aggregated values as an [array](../../types/array/).

This function always executes on the data from `value` as if it were sorted in ascending order before the function call. Any specified ordering is
ignored. If you need to perform aggregation in a specific order, you must specify `ORDER BY` within the aggregate function call itself. Otherwise incoming rows are not guaranteed any order.

## Details

This section covers details.

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
```bash

## Examples

This section covers examples.

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
```text


---

## CAST function and operator


The `cast` function and operator return a value converted to the specified [type](../../types/).

## Signatures

[See diagram: func-cast.svg]

[See diagram: op-cast.svg]

Parameter | Type | Description
----------|------|------------
_val_ | [Any](../../types) | The value you want to convert.
_type_ | [Typename](../../types) | The return value's type.

The following special syntax is permitted if _val_ is a string literal:

[See diagram: lit-cast.svg]

### Return value

`cast` returns the value with the type specified by the _type_ parameter.

## Details

This section covers details.

### Valid casts

Cast context defines when casts may occur.

Cast context | Definition | Strictness
--------|------------|-----------
**Implicit** | Values are automatically converted. For example, when you add `int4` to `int8`, the `int4` value is automatically converted to `int8`. | Least
**Assignment** | Values of one type are converted automatically when inserted into a column of a different type. | Medium
**Explicit** | You must invoke `CAST` deliberately. | Most

Casts allowed in less strict contexts are also allowed in stricter contexts. That is, implicit casts also occur by assignment, and both implicit casts and casts by assignment can be explicitly invoked.

Source type                                | Return type                                   | Cast context
-------------------------------------------|-----------------------------------------------|----------
[`array`](../../types/array/)<sup>1</sup>  | [`text`](../../types/text/)                   | Assignment
[`bigint`](../../types/integer/)           | [`bool`](../../types/boolean/)                | Explicit
[`bigint`](../../types/integer/)           | [`int`](../../types/integer/)                 | Assignment
[`bigint`](../../types/integer/)           | [`float`](../../types/float/)                 | Implicit
[`bigint`](../../types/integer/)           | [`numeric`](../../types/numeric/)             | Implicit
[`bigint`](../../types/integer/)           | [`real`](../../types/real/)                   | Implicit
[`bigint`](../../types/integer/)           | [`text`](../../types/text/)                   | Assignment
[`bigint`](../../types/integer/)           | [`uint2`](../../types/uint/)                  | Assignment
[`bigint`](../../types/integer/)           | [`uint4`](../../types/uint/)                  | Assignment
[`bigint`](../../types/integer/)           | [`uint8`](../../types/uint/)                  | Assignment
[`bool`](../../types/boolean/)             | [`int`](../../types/integer/)                 | Explicit
[`bool`](../../types/boolean/)             | [`text`](../../types/text/)                   | Assignment
[`bytea`](../../types/bytea/)              | [`text`](../../types/text/)                   | Assignment
[`date`](../../types/date/)                | [`text`](../../types/text/)                   | Assignment
[`date`](../../types/date/)                | [`timestamp`](../../types/timestamp/)         | Implicit
[`date`](../../types/date/)                | [`timestamptz`](../../types/timestamp/)       | Implicit
[`float`](../../types/float/)              | [`bigint`](../../types/integer/)              | Assignment
[`float`](../../types/float/)              | [`int`](../../types/integer/)                 | Assignment
[`float`](../../types/float/)              | [`numeric`](../../types/numeric/)<sup>2</sup> | Assignment
[`float`](../../types/float/)              | [`real`](../../types/real/)                   | Assignment
[`float`](../../types/float/)              | [`text`](../../types/text/)                   | Assignment
[`float`](../../types/float/)              | [`uint2`](../../types/uint/)                  | Assignment
[`float`](../../types/float/)              | [`uint4`](../../types/uint/)                  | Assignment
[`float`](../../types/float/)              | [`uint8`](../../types/uint/)                  | Assignment
[`int`](../../types/integer/)              | [`bigint`](../../types/integer/)              | Implicit
[`int`](../../types/integer/)              | [`bool`](../../types/boolean/)                | Explicit
[`int`](../../types/integer/)              | [`float`](../../types/float/)                 | Implicit
[`int`](../../types/integer/)              | [`numeric`](../../types/numeric/)             | Implicit
[`int`](../../types/integer/)              | [`oid`](../../types/oid/)                     | Implicit
[`int`](../../types/integer/)              | [`real`](../../types/real/)                   | Implicit
[`int`](../../types/integer/)              | [`text`](../../types/text/)                   | Assignment
[`int`](../../types/integer/)              | [`uint2`](../../types/uint/)                  | Assignment
[`int`](../../types/integer/)              | [`uint4`](../../types/uint/)                  | Assignment
[`int`](../../types/integer/)              | [`uint8`](../../types/uint/)                  | Assignment
[`interval`](../../types/interval/)        | [`text`](../../types/text/)                   | Assignment
[`interval`](../../types/interval/)        | [`time`](../../types/time/)                   | Assignment
[`jsonb`](../../types/jsonb/)              | [`bigint`](../../types/integer/)              | Explicit
[`jsonb`](../../types/jsonb/)              | [`float`](../../types/float/)                 | Explicit
[`jsonb`](../../types/jsonb/)              | [`int`](../../types/integer/)                 | Explicit
[`jsonb`](../../types/jsonb/)              | [`real`](../../types/real/)                   | Explicit
[`jsonb`](../../types/jsonb/)              | [`numeric`](../../types/numeric/)             | Explicit
[`jsonb`](../../types/jsonb/)              | [`text`](../../types/text/)                   | Assignment
[`list`](../../types/list/)<sup>1</sup>    | [`list`](../../types/list/)                   | Implicit
[`list`](../../types/list/)<sup>1</sup>    | [`text`](../../types/text/)                   | Assignment
[`map`](../../types/map/)                  | [`text`](../../types/text/)                   | Assignment
[`mz_aclitem`](../../types/mz_aclitem/)    | [`text`](../../types/text/)                   | Explicit
[`numeric`](../../types/numeric/)          | [`bigint`](../../types/integer/)              | Assignment
[`numeric`](../../types/numeric/)          | [`float`](../../types/float/)                 | Implicit
[`numeric`](../../types/numeric/)          | [`int`](../../types/integer/)                 | Assignment
[`numeric`](../../types/numeric/)          | [`real`](../../types/real/)                   | Implicit
[`numeric`](../../types/numeric/)          | [`text`](../../types/text/)                   | Assignment
[`numeric`](../../types/numeric/)          | [`uint2`](../../types/uint/)                  | Assignment
[`numeric`](../../types/numeric/)          | [`uint4`](../../types/uint/)                  | Assignment
[`numeric`](../../types/numeric/)          | [`uint8`](../../types/uint/)                  | Assignment
[`oid`](../../types/oid/)                  | [`int`](../../types/integer/)                 | Assignment
[`oid`](../../types/oid/)                  | [`text`](../../types/text/)                   | Explicit
[`real`](../../types/real/)                | [`bigint`](../../types/integer/)              | Assignment
[`real`](../../types/real/)                | [`float`](../../types/float/)                 | Implicit
[`real`](../../types/real/)                | [`int`](../../types/integer/)                 | Assignment
[`real`](../../types/real/)                | [`numeric`](../../types/numeric/)             | Assignment
[`real`](../../types/real/)                | [`text`](../../types/text/)                   | Assignment
[`real`](../../types/real/)                | [`uint2`](../../types/uint/)                  | Assignment
[`real`](../../types/real/)                | [`uint4`](../../types/uint/)                  | Assignment
[`real`](../../types/real/)                | [`uint8`](../../types/uint/)                  | Assignment
[`record`](../../types/record/)            | [`text`](../../types/text/)                   | Assignment
[`smallint`](../../types/integer/)         | [`bigint`](../../types/integer/)              | Implicit
[`smallint`](../../types/integer/)         | [`float`](../../types/float/)                 | Implicit
[`smallint`](../../types/integer/)         | [`int`](../../types/integer/)                 | Implicit
[`smallint`](../../types/integer/)         | [`numeric`](../../types/numeric/)             | Implicit
[`smallint`](../../types/integer/)         | [`oid`](../../types/oid/)                     | Implicit
[`smallint`](../../types/integer/)         | [`real`](../../types/real/)                   | Implicit
[`smallint`](../../types/integer/)         | [`text`](../../types/text/)                   | Assignment
[`smallint`](../../types/integer/)         | [`uint2`](../../types/uint/)                  | Assignment
[`smallint`](../../types/integer/)         | [`uint4`](../../types/uint/)                  | Assignment
[`smallint`](../../types/integer/)         | [`uint8`](../../types/uint/)                  | Assignment
[`text`](../../types/text/)                | [`bigint`](../../types/integer/)              | Explicit
[`text`](../../types/text/)                | [`bool`](../../types/boolean/)                | Explicit
[`text`](../../types/text/)                | [`bytea`](../../types/bytea/)                 | Explicit
[`text`](../../types/text/)                | [`date`](../../types/date/)                   | Explicit
[`text`](../../types/text/)                | [`float`](../../types/float/)                 | Explicit
[`text`](../../types/text/)                | [`int`](../../types/integer/)                 | Explicit
[`text`](../../types/text/)                | [`interval`](../../types/interval/)           | Explicit
[`text`](../../types/text/)                | [`jsonb`](../../types/jsonb/)                 | Explicit
[`text`](../../types/text/)                | [`list`](../../types/list/)                   | Explicit
[`text`](../../types/text/)                | [`map`](../../types/map/)                     | Explicit
[`text`](../../types/text/)                | [`numeric`](../../types/numeric/)             | Explicit
[`text`](../../types/text/)                | [`oid`](../../types/oid/)                     | Explicit
[`text`](../../types/text/)                | [`real`](../../types/real/)                   | Explicit
[`text`](../../types/text/)                | [`time`](../../types/time/)                   | Explicit
[`text`](../../types/text/)                | [`timestamp`](../../types/timestamp/)         | Explicit
[`text`](../../types/text/)                | [`timestamptz`](../../types/timestamp/)       | Explicit
[`text`](../../types/text/)                | [`uint2`](../../types/uint/)                  | Explicit
[`text`](../../types/text/)                | [`uint4`](../../types/uint/)                  | Assignment
[`text`](../../types/text/)                | [`uint8`](../../types/uint/)                  | Assignment
[`text`](../../types/text/)                | [`uuid`](../../types/uuid/)                   | Explicit
[`time`](../../types/time/)                | [`interval`](../../types/interval/)           | Implicit
[`time`](../../types/time/)                | [`text`](../../types/text/)                   | Assignment
[`timestamp`](../../types/timestamp/)      | [`date`](../../types/date/)                   | Assignment
[`timestamp`](../../types/timestamp/)      | [`text`](../../types/text/)                   | Assignment
[`timestamp`](../../types/timestamp/)      | [`timestamptz`](../../types/timestamp/)       | Implicit
[`timestamptz`](../../types/timestamp/)    | [`date`](../../types/date/)                   | Assignment
[`timestamptz`](../../types/timestamp/)    | [`text`](../../types/text/)                   | Assignment
[`timestamptz`](../../types/timestamp/)    | [`timestamp`](../../types/timestamp/)         | Assignment
[`uint2`](../../types/uint/)               | [`bigint`](../../types/integer/)              | Implicit
[`uint2`](../../types/uint/)               | [`float`](../../types/float/)                 | Implicit
[`uint2`](../../types/uint/)               | [`int`](../../types/integer/)                 | Implicit
[`uint2`](../../types/uint/)               | [`numeric`](../../types/numeric/)             | Implicit
[`uint2`](../../types/uint/)               | [`real`](../../types/real/)                   | Implicit
[`uint2`](../../types/uint/)               | [`text`](../../types/text/)                   | Assignment
[`uint2`](../../types/uint/)               | [`uint4`](../../types/uint/)                  | Implicit
[`uint2`](../../types/uint/)               | [`uint8`](../../types/uint/)                  | Implicit
[`uint4`](../../types/uint)                | [`bigint`](../../types/integer/)              | Implicit
[`uint4`](../../types/uint)                | [`float`](../../types/float/)                 | Implicit
[`uint4`](../../types/uint)                | [`int`](../../types/integer/)                 | Assignment
[`uint4`](../../types/uint)                | [`numeric`](../../types/numeric/)             | Implicit
[`uint4`](../../types/uint)                | [`real`](../../types/real/)                   | Implicit
[`uint4`](../../types/uint)                | [`text`](../../types/text/)                   | Assignment
[`uint4`](../../types/uint)                | [`uint2`](../../types/uint/)                  | Assignment
[`uint4`](../../types/uint)                | [`uint8`](../../types/uint/)                  | Implicit
[`uint8`](../../types/uint/)               | [`bigint`](../../types/integer/)              | Assignment
[`uint8`](../../types/uint/)               | [`float`](../../types/float/)                 | Implicit
[`uint8`](../../types/uint/)               | [`int`](../../types/integer/)                 | Assignment
[`uint8`](../../types/uint/)               | [`real`](../../types/real/)                   | Implicit
[`uint8`](../../types/uint/)               | [`uint2`](../../types/uint/)                  | Assignment
[`uint8`](../../types/uint/)               | [`uint4`](../../types/uint/)                  | Assignment
[`uuid`](../../types/uuid/)                | [`text`](../../types/text/)                   | Assignment

<sup>1</sup> [`Arrays`](../../types/array/) and [`lists`](../../types/list) are composite types subject to special constraints. See their respective type documentation for details.

<sup>2</sup> Casting a [`float`](../../types/float/) to a [`numeric`](../../types/numeric/) can yield an imprecise result due to the floating point arithmetic involved in the conversion.

## Examples

This section covers examples.

```mzsql
SELECT INT '4';
```text
```nofmt
 ?column?
----------
         4
```text

<hr>

```mzsql
SELECT CAST (CAST (100.21 AS numeric(10, 2)) AS float) AS dec_to_float;
```text
```nofmt
 dec_to_float
--------------
       100.21
```text

<hr/>

```mzsql
SELECT 100.21::numeric(10, 2)::float AS dec_to_float;
```text
```nofmt
 dec_to_float
--------------
       100.21
```bash

## Related topics
* [Data Types](../../types/)


---

## COALESCE function


`COALESCE` returns the first non-`NULL` element provided.

## Signatures

Parameter | Type | Description
----------|------|------------
val | [Any](../../types) | The values you want to check.

### Return value

All elements of the parameters for `coalesce` must be of the same type; `coalesce` returns that type, or _NULL_.

## Examples

This section covers examples.

```mzsql
SELECT coalesce(NULL, 3, 2, 1) AS coalesce_res;
```text
```nofmt
 res
-----
   3
```text


---

## csv_extract function


`csv_extract` returns individual component columns from a column containing a CSV file formatted as a string.

## Signatures

[See diagram: func-csv-extract.svg]

Parameter | Type | Description
----------|------|------------
_num_csv_col_ | [`int`](../../types/integer/) | The number of columns in the CSV string.
_col_name_  | [`string`](../../types/text/)  | The name of the column containing the CSV string.

### Return value

`EXTRACT` returns [`string`](../../types/text/) columns.

## Example

Create a table where one column is in CSV format and insert some rows:

```mzsql
CREATE TABLE t (id int, data string);
INSERT INTO t
  VALUES (1, 'some,data'), (2, 'more,data'), (3, 'also,data');
```text

Extract the component columns from the table column which is a CSV string, sorted by column `id`:

```mzsql
SELECT csv.* FROM t, csv_extract(2, data) csv
  ORDER BY t.id;
```text
```nofmt
 column1 | column2
---------+---------
 also    | data
 more    | data
 some    | data
(3 rows)
```text


---

## date_bin function


`date_bin` returns the largest value less than or equal to `source` that is a
multiple of `stride` starting at `origin`––for shorthand, we call this
"binning."

For example, on this number line of abstract units:

```nofmt
          x
...|---|---|---|...
   7   8   9   10
```text

With a `stride` of 1, we would have bins (...7, 8, 9, 10...).

Here are some example results:

`source` | `origin` | `stride`  | Result
---------|----------|-----------|-------
8.75     | 1        | 1 unit    | 8
8.75     | 1        | 2 units   | 7
8.75     | 1.75     | 1.5 units | 7.75

`date_bin` is similar to [`date_trunc`], but supports arbitrary
strides, rather than only unit times.

## Signatures

[See diagram: func-date-bin.svg]

Parameter | Type | Description
----------|------|------------
_stride_ | [`interval`] | Define bins of this width.
_source_ | [`timestamp`], [`timestamp with time zone`] | Determine this value's bin.
_origin_ | Must be the same as _source_ | Align bins to this value.

### Return value

`date_bin` returns the same type as _source_.

## Details

- `origin` and `source` cannot be more than 2^63 nanoseconds apart.
- `stride` cannot contain any years or months, but e.g. can exceed 30 days.
- `stride` only supports values between 1 and 9,223,372,036 seconds.

## Examples

This section covers examples.

```mzsql
SELECT
  date_bin(
    '15 minutes',
    timestamp '2001-02-16 20:38:40',
    timestamp '2001-02-16 20:05:00'
  );
```text
```nofmt
      date_bin
---------------------
 2001-02-16 20:35:00
```text

```mzsql
SELECT
  str,
  "interval",
  date_trunc(str, ts)
    = date_bin("interval"::interval, ts, timestamp '2001-01-01') AS equal
FROM (
  VALUES
  ('week', '7 d'),
  ('day', '1 d'),
  ('hour', '1 h'),
  ('minute', '1 m'),
  ('second', '1 s')
) intervals (str, interval),
(VALUES (timestamp '2020-02-29 15:44:17.71393')) ts (ts);
```text
```nofmt
  str   | interval | equal
--------+----------+-------
 day    | 1 d      | t
 hour   | 1 h      | t
 week   | 7 d      | t
 minute | 1 m      | t
 second | 1 s      | t
```json

[`date_trunc`]: ../date-trunc
[`interval`]: ../../types/interval
[`timestamp`]: ../../types/timestamp
[`timestamp with time zone`]: ../../types/timestamptz


---

## date_part function


`date_part` returns some time component from a time-based value, such as the year from a Timestamp.
It is mostly functionally equivalent to the function [`EXTRACT`](../extract), except to maintain
PostgreSQL compatibility, `date_part` returns values of type [`float`](../../types/float). This can
result in a loss of precision in certain uses. Using [`EXTRACT`](../extract) is recommended instead.

## Signatures

[See diagram: func-date-part.svg]

Parameter | Type                                                                                                                                                          | Description
----------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|------------
_val_ | [`time`](../../types/time), [`timestamp`](../../types/timestamp), [`timestamp with time zone`](../../types/timestamptz), [`interval`](../../types/interval), [`date`](../../types/date) | The value from which you want to extract a component. vals of type [`date`](../../types/date) are first cast to type [`timestamp`](../../types/timestamp).

### Arguments

`date_part` supports multiple synonyms for most time periods.

Time period | Synonyms
------------|---------
epoch | `EPOCH`
millennium   | `MIL`, `MILLENNIUM`, `MILLENNIA`
century | `C`, `CENT`, `CENTURY`, `CENTURIES`
decade  |  `DEC`, `DECS`, `DECADE`, `DECADES`
year | `Y`, `YEAR`, `YEARS`, `YR`, `YRS`
quarter  | `QTR`, `QUARTER`
month | `MON`, `MONS`, `MONTH`, `MONTHS`
week | `W`, `WEEK`, `WEEKS`
day  | `D`, `DAY`, `DAYS`
hour   |`H`, `HR`, `HRS`, `HOUR`, `HOURS`
minute | `M`, `MIN`, `MINS`, `MINUTE`, `MINUTES`
second | `S`, `SEC`, `SECS`, `SECOND`, `SECONDS`
microsecond  | `US`, `USEC`, `USECS`, `USECONDS`, `MICROSECOND`, `MICROSECONDS`
millisecond | `MS`, `MSEC`, `MSECS`, `MSECONDS`, `MILLISECOND`, `MILLISECONDS`
day of week |`DOW`
ISO day of week | `ISODOW`
day of year | `DOY`

### Return value

`date_part` returns a [`float`](../../types/float) value.

## Examples

This section covers examples.

### Extract second from timestamptz

```mzsql
SELECT date_part('S', TIMESTAMP '2006-01-02 15:04:05.06');
```text
```nofmt
 date_part
-----------
      5.06
```bash

### Extract century from date

```mzsql
SELECT date_part('CENTURIES', DATE '2006-01-02');
```text
```nofmt
 date_part
-----------
        21
```text


---

## date_trunc function


`date_trunc` computes _ts_val_'s "floor value" of the specified time component,
i.e. the largest time component less than or equal to the provided value.

To align values along arbitrary values, see [`date_bin`].

## Signatures

[See diagram: func-date-trunc.svg]

Parameter | Type | Description
----------|------|------------
_val_ | [`timestamp`], [`timestamp with time zone`], [`interval`] | The value you want to truncate.

### Return value

`date_trunc` returns the same type as _val_.

## Examples

This section covers examples.

```mzsql
SELECT date_trunc('hour', TIMESTAMP '2019-11-26 15:56:46.241150') AS hour_trunc;
```text
```nofmt
          hour_trunc
-------------------------------
 2019-11-26 15:00:00.000000000
```text

```mzsql
SELECT date_trunc('year', TIMESTAMP '2019-11-26 15:56:46.241150') AS year_trunc;
```text
```nofmt
          year_trunc
-------------------------------
 2019-01-01 00:00:00.000000000
```text

```mzsql
SELECT date_trunc('millennium', INTERVAL '1234 years 11 months 23 days 23:59:12.123456789') AS millennium_trunc;
```text
```nofmt
          millennium_trunc
-------------------------------
 1000 years
```json

[`date_bin`]: ../date-bin
[`interval`]: ../../types/interval/
[`timestamp`]: ../../types/timestamp
[`timestamp with time zone`]: ../../types/timestamptz


---

## datediff function


The `datediff(datepart, start, end)` function returns the difference between two date, time or timestamp expressions based on the specified date or time part.

## Signatures

Parameter | Type | Description
----------|------|------------
_datepart_ | [text](../../types) | The date or time part to return. Must be one of [`datepart` specifiers](#datepart-specifiers).
_start_ | [date](../../types), [time](../../types), [timestamp](../../types), [timestamptz](../../types) | The date, time, or timestamp expression to start measuring from.
_end_ | [date](../../types), [time](../../types), [timestamp](../../types), [timestamptz](../../types) | The date, time, or timestamp expression to measuring until.

### `datepart` specifiers

- **`millenniums`, `millennium`, `millennia`, `mil`**: Millennia
- **`centuries`, `century`, `cent`, `c`**: Centuries
- **`decades`, `decade`, `decs`, `dec`**: Decades
- **`years`, `year`, `yrs`, `yr`, `y`**: Years
- **`quarter`, `qtr`**: Quarters
- **`months`, `month`, `mons`, `mon`**: Months
- **`weeks`, `week`, `w`**: Weeks
- **`days`, `day`, `d`**: Days
- **`hours`, `hour`, `hrs`, `hr`, `h`**: Hours
- **`minutes`, `minute`, `mins`, `min`, `m`**: Minutes
- **`seconds`, `second`, `secs`, `sec`, `s`**: Seconds
- **`milliseconds`, `millisecond`, `mseconds`, `msecs`, `msec`, `ms`**: Milliseconds
- **`microseconds`, `microsecond`, `useconds`, `usecs`, `usec`, `us`**: Microseconds

## Examples

To calculate the difference between two dates in millennia:

```sql
SELECT datediff('millennia', '2000-12-31', '2001-01-01') as d;
  d
-----
  1
```text

Even though the difference between `2000-12-31` and `2001-01-01` is a single day, a millennium boundary is crossed from one date to the other, so the result is `1`.

To see how this function handles leap years:
```sql
SELECT datediff('day', '2004-02-28', '2004-03-01') as leap;
    leap
------------
     2

SELECT datediff('day', '2005-02-28', '2005-03-01') as non_leap;
  non_leap
------------
     1
```text

In the statement that uses a leap year (`2004`), the number of day boundaries crossed is `2`. When using a non-leap year (`2005`), only `1` day boundary is crossed.


---

## encode and decode functions


The `encode` function encodes binary data into one of several textual
representations. The `decode` function does the reverse.

## Signatures

This section covers signatures.

```text
encode(b: bytea, format: text) -> text
decode(s: text, format: text) -> bytea
```bash

## Details

This section covers details.

### Supported formats

The following formats are supported by both `encode` and `decode`.

#### `base64`

The `base64` format is defined in [Section 6.8 of RFC 2045][rfc2045].

To comply with the RFC, the `encode` function inserts a newline (`\n`) after
every 76 characters. The `decode` function ignores any whitespace in its input.

#### `escape`

The `escape` format renders zero bytes and bytes with the high bit set (`0x80` -
`0xff`) as an octal escape sequence (`\nnn`), renders backslashes as `\\`, and
renders all other characters literally. The `decode` function rejects invalid
escape sequences (e.g., `\9` or `\a`).

#### `hex`

The `hex` format represents each byte of input as two hexadecimal digits, with
the most significant digit first. The `encode` function uses lowercase for the
`a`-`f` digits, with no whitespace between digits. The `decode` function accepts
lowercase or uppercase for the `a` - `f` digits and permits whitespace between
each encoded byte, though not within a byte.

## Examples

Encoding and decoding in the `base64` format:

```mzsql
SELECT encode('\x00404142ff', 'base64');
```text
```nofmt
  encode
----------
 AEBBQv8=
```text

```mzsql
SELECT decode('A   EB BQv8 =', 'base64');
```text
```nofmt
    decode
--------------
 \x00404142ff
```text

```mzsql
SELECT encode('This message is long enough that the output will run to multiple lines.', 'base64');
```text
```nofmt
                                    encode
------------------------------------------------------------------------------
 VGhpcyBtZXNzYWdlIGlzIGxvbmcgZW5vdWdoIHRoYXQgdGhlIG91dHB1dCB3aWxsIHJ1biB0byBt+
 dWx0aXBsZSBsaW5lcy4=
```text

<hr>

Encoding and decoding in the `escape` format:

```mzsql
SELECT encode('\x00404142ff', 'escape');
```text
```nofmt
   encode
-------------
 \000@AB\377
```text

```mzsql
SELECT decode('\000@AB\377', 'escape');
```text
```nofmt
    decode
--------------
 \x00404142ff
```text

<hr>

Encoding and decoding in the `hex` format:

```mzsql
SELECT encode('\x00404142ff', 'hex');
```text
```nofmt
   encode
------------
 00404142ff
```text

```mzsql
SELECT decode('00  40  41  42  ff', 'hex');
```text
```nofmt
    decode
--------------
 \x00404142ff
```json

[rfc2045]: https://tools.ietf.org/html/rfc2045#section-6.8


---

## EXTRACT function


`EXTRACT` returns some time component from a time-based value, such as the year from a Timestamp.

## Signatures

[See diagram: func-extract.svg]

Parameter | Type                                                                                                                                                                                    | Description
----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------
_val_ | [`date`](../../types/date), [`time`](../../types/time), [`timestamp`](../../types/timestamp), [`timestamp with time zone`](../../types/timestamptz), [`interval`](../../types/interval) | The value from which you want to extract a component.

### Arguments

`EXTRACT` supports multiple synonyms for most time periods.

Time period | Synonyms
------------|---------
epoch | `EPOCH`
millennium   | `MIL`, `MILLENNIUM`, `MILLENNIA`
century | `C`, `CENT`, `CENTURY`, `CENTURIES`
decade  |  `DEC`, `DECS`, `DECADE`, `DECADES`
 year | `Y`, `YEAR`, `YEARS`, `YR`, `YRS`
 quarter  | `QTR`, `QUARTER`
 month | `MON`, `MONS`, `MONTH`, `MONTHS`
 week | `W`, `WEEK`, `WEEKS`
 day  | `D`, `DAY`, `DAYS`
 hour   |`H`, `HR`, `HRS`, `HOUR`, `HOURS`
 minute | `M`, `MIN`, `MINS`, `MINUTE`, `MINUTES`
 second | `S`, `SEC`, `SECS`, `SECOND`, `SECONDS`
 microsecond  | `US`, `USEC`, `USECS`, `USECONDS`, `MICROSECOND`, `MICROSECONDS`
 millisecond | `MS`, `MSEC`, `MSECS`, `MSECONDS`, `MILLISECOND`, `MILLISECONDS`
 day of week |`DOW`
 ISO day of week | `ISODOW`
 day of year | `DOY`

### Return value

`EXTRACT` returns a [`numeric`](../../types/numeric) value.

## Examples

This section covers examples.

### Extract second from timestamptz

```mzsql
SELECT EXTRACT(S FROM TIMESTAMP '2006-01-02 15:04:05.06');
```text
```nofmt
 extract
---------
    5.06
```bash

### Extract century from date

```mzsql
SELECT EXTRACT(CENTURIES FROM DATE '2006-01-02');
```text
```nofmt
 extract
---------
      21
```text


---

## jsonb_agg function


The `jsonb_agg(expression)` function aggregates all values indicated by its expression,
returning the values (including nulls) as a [`jsonb`](/sql/types/jsonb) array.
The input values to the aggregate can be [filtered](../filters).

## Syntax

[See diagram: jsonb-agg.svg]

## Signatures

Parameter | Type | Description
----------|------|------------
_expression_ | [jsonb](../../types) | The values you want aggregated.

### Return value

`jsonb_agg` returns the aggregated values as a `jsonb` array.

This function always executes on the data from `value` as if it were sorted in ascending order before the function call. Any specified ordering is
ignored. If you need to perform aggregation in a specific order, you must specify `ORDER BY` within the aggregate function call itself. Otherwise incoming rows are not guaranteed any order.

## Details

This section covers details.

### Usage in dataflows

While `jsonb_agg` is available in Materialize, materializing `jsonb_agg(expression)`
is considered an incremental view maintenance anti-pattern. Any change to the data
underlying the function call will require the function to be recomputed entirely,
discarding the benefits of maintaining incremental updates.

Instead, we recommend that you materialize all components required for the
`jsonb_agg` function call and create a non-materialized view using `jsonb_agg`
on top of that. That pattern is illustrated in the following statements:

```mzsql
CREATE MATERIALIZED VIEW foo_view AS SELECT * FROM foo;
CREATE VIEW bar AS SELECT jsonb_agg(foo_view.bar) FROM foo_view;
```bash

## Examples

This section covers examples.

```mzsql
SELECT
  jsonb_agg(t) FILTER (WHERE t.content LIKE 'h%')
    AS my_agg
FROM (
  VALUES
  (1, 'hey'),
  (2, NULL),
  (3, 'hi'),
  (4, 'salutations')
  ) AS t(id, content);
```text
```nofmt
                       my_agg
----------------------------------------------------
 [{"content":"hi","id":3},{"content":"hey","id":1}]
```bash

## See also

* [`jsonb_object_agg`](/sql/functions/jsonb_object_agg)


---

## jsonb_object_agg function


The `jsonb_object_agg(keys, values)` aggregate function zips together `keys`
and `values` into a [`jsonb`](/sql/types/jsonb) object.
The input values to the aggregate can be [filtered](../filters).

## Syntax

[See diagram: jsonb-object-agg.svg]

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

```mzsql
CREATE MATERIALIZED VIEW foo_view AS SELECT key_col, val_col FROM foo;
CREATE VIEW bar AS SELECT jsonb_object_agg(key_col, val_col) FROM foo_view;
```bash

## Examples

Consider this query:
```mzsql
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
```text
```nofmt
      my_agg
------------------
 {"k1": 1, "k2": -1}
```text
In this example, there are multiple values associated with the `k2` key.

The `FILTER` clause in the statement above returns values that are not `NULL` and orders them by the timestamp column to return the most recent associated value.

## See also

* [`jsonb_agg`](/sql/functions/jsonb_agg)


---

## justify_days function


`justify_days` returns a new [`interval`](../../types/interval) such that 30-day time periods are
converted to months.

## Signatures

[See diagram: func-justify-days.svg]

Parameter | Type                                                                                                                                                                                            | Description
----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------
_interval_ | [`interval`](../../types/interval) | The interval value to justify.


### Return value

`justify_days` returns an [`interval`](../../types/interval) value.

## Example

This section covers example.

```mzsql
SELECT justify_days(interval '35 days');
```text
```nofmt
  justify_days
----------------
 1 month 5 days
```text


---

## justify_hours function


`justify_hours` returns a new [`interval`](../../types/interval) such that 24-hour time periods are
converted to days.

## Signatures

[See diagram: func-justify-hours.svg]

Parameter | Type                                                                                                                                                                                            | Description
----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------
_interval_ | [`interval`](../../types/interval) | The interval value to justify.


### Return value

`justify_hours` returns an [`interval`](../../types/interval) value.

## Example

This section covers example.

```mzsql
SELECT justify_hours(interval '27 hours');
```text
```nofmt
 justify_hours
----------------
 1 day 03:00:00
```text


---

## justify_interval function


`justify_interval` returns a new [`interval`](../../types/interval) such that 30-day time periods are
converted to months, 24-hour time periods are represented as days, and all fields have the same sign. It is a
combination of ['justify_days'](../justify-days) and ['justify_hours'](../justify-hours) with additional sign
adjustment.

## Signatures

[See diagram: func-justify-interval.svg]

Parameter | Type                                                                                                                                                                                            | Description
----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------
_interval_ | [`interval`](../../types/interval) | The interval value to justify.


### Return value

`justify_interval` returns an [`interval`](../../types/interval) value.

## Example

This section covers example.

```mzsql
SELECT justify_interval(interval '1 mon -1 hour');
```text
```nofmt
 justify_interval
------------------
 29 days 23:00:00
```text


---

## LENGTH function


`LENGTH` returns the [code points](https://en.wikipedia.org/wiki/Code_point) in
an encoded string.

## Signatures

[See diagram: func-length.svg]

Parameter | Type | Description
----------|------|------------
_str_ | [`string`](../../types/string) or `bytea` | The string whose length you want.
_encoding&lowbar;name_ | [`string`](../../types/string) | The [encoding](#encoding-details) you want to use for calculating the string's length. _Defaults to UTF-8_.

### Return value

`length` returns an [`int`](../../types/int).

## Details

This section covers details.

### Errors

`length` operations might return `NULL` values indicating errors in the
following cases:

- The _encoding&lowbar;name_ provided is not available in our encoding package.
- Some byte sequence in _str_ was not compatible with the selected encoding.

### Encoding details

- Materialize uses the [`encoding`](https://crates.io/crates/encoding) crate.
  See the [list of supported
  encodings](https://lifthrasiir.github.io/rust-encoding/encoding/index.html#supported-encodings),
  as well as their names [within the
  API](https://github.com/lifthrasiir/rust-encoding/blob/4e79c35ab6a351881a86dbff565c4db0085cc113/src/label.rs).
- Materialize attempts to convert [PostgreSQL-style encoding
  names](https://www.postgresql.org/docs/9.5/multibyte.html) into the
  [WHATWG-style encoding names](https://encoding.spec.whatwg.org/) used by the
  API.

    For example, you can refer to `iso-8859-5` (WHATWG-style) as `ISO_8859_5`
    (PostrgreSQL-style).

    However, there are some differences in the names of the same encodings that
    we do not convert. For example, the
    [windows-874](https://encoding.spec.whatwg.org/#windows-1252) encoding is
    referred to as `WIN874` in PostgreSQL; Materialize does not convert these names.

## Examples

This section covers examples.

```mzsql
SELECT length('你好') AS len;
```text
```nofmt
 len
-----
   2
```text

<hr/>

```mzsql
SELECT length('你好', 'big5') AS len;
```text
```nofmt
 len
-----
   3
```text


---

## list_agg function


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
```text


---

## map_agg function


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
```text

In this example:

-   We order values by their timestamp (`t.ts ASC`) and then break ties using the
    smallest values (`t.v DESC`).
-   We filter out any values equal to exactly `-8`.
-   All keys with a `NULL` value get excluded automatically.
-   `k1` has two values tied with the same `t.ts` value; because we've also
    ordered `t.v DESC`, the last value we see will be `1`.
-   `k2` has its value for `-8` filtered out `FILTER (WHERE t.v != -8)`;
    however, this `FILTER` also removes the `NULL` value at `now() + INTERVAL '1s'` because `WHERE null != -8` evaluates to `false`.


---

## normalize function


`normalize` converts a string to a specified Unicode normalization form.

## Signatures

<!-- Unresolved shortcode: {{% include-example file="examples/normalize" exam... -->

Parameter | Type | Description
----------|------|------------
_str_ | [`string`](../../types/string) | The string to normalize.
_form_ | keyword | The Unicode normalization form: `NFC`, `NFD`, `NFKC`, or `NFKD` (unquoted, case-insensitive keywords). _Defaults to `NFC`_.

### Return value

`normalize` returns a [`string`](../../types/string).

## Details

Unicode normalization is a process that converts different binary representations of characters to a canonical form. This is useful when comparing strings that may have been encoded differently.

The four normalization forms are:

- **NFC** (Normalization Form Canonical Composition): Canonical decomposition, followed by canonical composition. This is the default and most commonly used form.
- **NFD** (Normalization Form Canonical Decomposition): Canonical decomposition only. Characters are decomposed into their constituent parts.
- **NFKC** (Normalization Form Compatibility Composition): Compatibility decomposition, followed by canonical composition. This applies more aggressive transformations, converting compatibility variants to standard forms.
- **NFKD** (Normalization Form Compatibility Decomposition): Compatibility decomposition only.

For more information, see:
- [Unicode Normalization Forms](https://unicode.org/reports/tr15/#Norm_Forms)
- [PostgreSQL normalize function](https://www.postgresql.org/docs/current/functions-string.html)

## Examples

<!-- Unresolved shortcode: {{% include-example file="examples/normalize" exam... -->

<hr/>

<!-- Unresolved shortcode: {{% include-example file="examples/normalize" exam... -->

<hr/>

<!-- Unresolved shortcode: {{% include-example file="examples/normalize" exam... -->

<hr/>

<!-- Unresolved shortcode: {{% include-example file="examples/normalize" exam... -->

<hr/>

<!-- Unresolved shortcode: {{% include-example file="examples/normalize" exam... -->


---

## now and mz_now functions


In Materialize, `now()` returns the value of the system clock when the
transaction began as a [`timestamp with time zone`] value.

By contrast, `mz_now()` returns the logical time at which the query was executed
as a [`mz_timestamp`] value.

## Details

This section covers details.

### `mz_now()` clause

```mzsql
mz_now() <comparison_operator> <numeric_expr | timestamp_expr>
```text

- `mz_now()` must be used with one of the following comparison operators: `=`,
`<`, `<=`, `>`, `>=`, or an operator that desugars to them or to a conjunction
(`AND`) of them (for example, `BETWEEN...AND...`). That is, you cannot use
date/time operations directly on  `mz_now()` to calculate a timestamp in the
past or future. Instead, rewrite the query expression to move the operation to
the other side of the comparison.


- `mz_now()` can only be compared to either a
  [`numeric`](/sql/types/numeric) expression or a
  [`timestamp`](/sql/types/timestamp) expression not containing `mz_now()`.


### Usage patterns

The typical uses of `now()` and `mz_now()` are:

* **Temporal filters**

  You can use `mz_now()` in a `WHERE` or `HAVING` clause to limit the working dataset.
  This is referred to as a **temporal filter**.
  See the [temporal filter](/sql/patterns/temporal-filters) pattern for more details.

* **Query timestamp introspection**

  An ad hoc `SELECT` query with `now()` and `mz_now()` can be useful if you need to understand how up to date the data returned by a query is.
  The data returned by the query reflects the results as of the logical time returned by a call to `mz_now()` in that query.

### Logical timestamp selection

When using the [serializable](/get-started/isolation-level#serializable)
isolation level, the logical timestamp may be arbitrarily ahead of or behind the
system clock. For example, at a wall clock time of 9pm, Materialize may choose
to execute a serializable query as of logical time 8:30pm, perhaps because data
for 8:30–9pm has not yet arrived. In this scenario, `now()` would return 9pm,
while `mz_now()` would return 8:30pm.

When using the [strict serializable](/get-started/isolation-level#strict-serializable)
isolation level, Materialize attempts to keep the logical timestamp reasonably
close to wall clock time. In most cases, the logical timestamp of a query will
be within a few seconds of the wall clock time. For example, when executing
a strict serializable query at a wall clock time of 9pm, Materialize will choose
a logical timestamp within a few seconds of 9pm, even if data for 8:30–9pm has
not yet arrived and the query will need to block until the data for 9pm arrives.
In this scenario, both `now()` and `mz_now()` would return 9pm.

### Limitations

#### Materialization

* Queries that use `now()` cannot be materialized. In other words, you cannot
  create an index or a materialized view on a query that calls `now()`.

* Queries that use `mz_now()` can only be materialized if the call to
  `mz_now()` is used in a [temporal filter](/sql/patterns/temporal-filters).

These limitations are in place because `now()` changes every microsecond and
`mz_now()` changes every millisecond. Allowing these functions to be
materialized would be resource prohibitive.

#### `mz_now()` restrictions

The [`mz_now()`](/sql/functions/now_and_mz_now) clause has the following
restrictions:

- When used in a materialized view definition, a view definition that is being
indexed (i.e., although you can create the view and perform ad-hoc query on
the view, you cannot create an index on that view), or a `SUBSCRIBE`
statement:

- `mz_now()` clauses can only be combined using an `AND`, and

- All top-level `WHERE` or `HAVING` conditions must be combined using an `AND`,
  even if the `mz_now()` clause is nested.


  For example:

  <!-- Dynamic table: mz_now/mz_now_combination - see original docs -->

  For alternatives, see [Disjunction (OR)
  alternatives](http://localhost:1313/docs/transform-data/idiomatic-materialize-sql/mz_now/#disjunctions-or).

- If part of a  `WHERE` clause, the `WHERE` clause cannot be an [aggregate
 `FILTER` expression](/sql/functions/filters).

## Examples

This section covers examples.

### Temporal filters

<!-- This example also appears in temporal-filters -->
It is common for real-time applications to be concerned with only a recent period of time.
In this case, we will filter a table to only include records from the last 30 seconds.

```mzsql
-- Create a table of timestamped events.
CREATE TABLE events (
    content TEXT,
    event_ts TIMESTAMP
);

-- Create a view of events from the last 30 seconds.
CREATE VIEW last_30_sec AS
SELECT event_ts, content
FROM events
WHERE mz_now() <= event_ts + INTERVAL '30s';
```text

Next, subscribe to the results of the view.

```mzsql
COPY (SUBSCRIBE (SELECT event_ts, content FROM last_30_sec)) TO STDOUT;
```text

In a separate session, insert a record.

```mzsql
INSERT INTO events VALUES (
    'hello',
    now()
);
```text

Back in the first session, watch the record expire after 30 seconds. Press `Ctrl+C` to quit the `SUBSCRIBE` when you are ready.

```nofmt
1686868190714   1       2023-06-15 22:29:50.711 hello
1686868220712   -1      2023-06-15 22:29:50.711 hello
```text

You can materialize the `last_30_sec` view by creating an index on it (results stored in memory) or by recreating it as a `MATERIALIZED VIEW` (results persisted to storage). When you do so, Materialize will keep the results up to date with records expiring automatically according to the temporal filter.

### Query timestamp introspection

If you haven't already done so in the previous example, create a table called `events` and add a few records.

```mzsql
-- Create a table of timestamped events.
CREATE TABLE events (
    content TEXT,
    event_ts TIMESTAMP
);
-- Insert records
INSERT INTO events VALUES (
    'hello',
    now()
);
INSERT INTO events VALUES (
    'welcome',
    now()
);
INSERT INTO events VALUES (
    'goodbye',
    now()
);
```text

Execute this ad hoc query that adds the current system timestamp and current logical timestamp to the events in the `events` table.

```mzsql
SELECT now(), mz_now(), * FROM events
```text

```nofmt
            now            |    mz_now     | content |       event_ts
---------------------------+---------------+---------+-------------------------
 2023-06-15 22:38:14.18+00 | 1686868693480 | hello   | 2023-06-15 22:29:50.711
 2023-06-15 22:38:14.18+00 | 1686868693480 | goodbye | 2023-06-15 22:29:51.233
 2023-06-15 22:38:14.18+00 | 1686868693480 | welcome | 2023-06-15 22:29:50.874
(3 rows)
```text

Notice when you try to materialize this query, you get errors:

```mzsql
CREATE MATERIALIZED VIEW cant_materialize
    AS SELECT now(), mz_now(), * FROM events;
```text

```nofmt
ERROR:  cannot materialize call to current_timestamp
ERROR:  cannot materialize call to mz_now
```json

[`mz_timestamp`]: /sql/types/mz_timestamp
[`timestamp with time zone`]: /sql/types/timestamptz


---

## Pushdown functions


`try_parse_monotonic_iso8601_timestamp` parses a subset of [ISO 8601]
timestamps that matches the 24 character length output
of the javascript [Date.toISOString()] function.
Unlike other parsing functions, inputs that fail to parse return `NULL`
instead of error.

This allows `try_parse_monotonic_iso8601_timestamp` to be used with
the [temporal filter pushdown] feature on `text` timestamps.
This is particularly useful when working with
[JSON sources](/sql/create-source/#json),
or other external data sources that store timestamps as strings.

Specifically, the accepted format is `YYYY-MM-DDThh:mm:ss.sssZ`:

- A 4-digit positive year, left-padded with zeros followed by
- A literal `-` followed by
- A 2-digit month, left-padded with zeros followed by
- A literal `-` followed by
- A 2-digit day, left-padded with zeros followed by
- A literal `T` followed by
- A 2-digit hour, left-padded with zeros followed by
- A literal `:` followed by
- A 2-digit minute, left-padded with zeros followed by
- A literal `:` followed by
- A 2-digit second, left-padded with zeros followed by
- A literal `.`
- A 3-digit millisecond, left-padded with zeros followed by
- A literal `Z`, indicating the UTC time zone.

Ordinary `text`-to-`timestamp` casts will prevent a filter from being pushed down.
Replacing those casts with `try_parse_monotonic_iso8601_timestamp` can unblock that
optimization for your query.

## Examples

This section covers examples.

```mzsql
SELECT try_parse_monotonic_iso8601_timestamp('2015-09-18T23:56:04.123Z') AS ts;
```text
```nofmt
 ts
--------
 2015-09-18 23:56:04.123
```text

 <hr/>

```mzsql
SELECT try_parse_monotonic_iso8601_timestamp('nope') AS ts;
```text
```nofmt
 ts
--------
 NULL
```json

[ISO 8601]: https://en.wikipedia.org/wiki/ISO_8601
[Date.toISOString()]: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Date/toISOString
[temporal filter pushdown]: /transform-data/patterns/temporal-filters/#temporal-filter-pushdown
[jsonb]: /sql/types/jsonb/


---

## string_agg function


The `string_agg(value, delimiter)` aggregate function concatenates the non-null
input values (i.e. `value`) into [`text`](/sql/types/text). Each value after the
first is preceded by its corresponding `delimiter`, where _null_ values are
equivalent to an empty string.
The input values to the aggregate can be [filtered](../filters).

## Syntax

[See diagram: string-agg.svg]

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

```mzsql
CREATE MATERIALIZED VIEW foo_view AS SELECT * FROM foo;
CREATE VIEW bar AS SELECT string_agg(foo_view.bar, ',');
```bash

## Examples

This section covers examples.

```mzsql
SELECT string_agg(column1, column2)
FROM (
    VALUES ('z', ' !'), ('a', ' @'), ('m', ' #')
);
```text
```nofmt
 string_agg
------------
 a #m !z
```text

Note that in the following example, the `ORDER BY` of the subquery feeding into `string_agg` gets ignored.

```mzsql
SELECT column1, column2
FROM (
    VALUES ('z', ' !'), ('a', ' @'), ('m', ' #')
) ORDER BY column1 DESC;
```text
```nofmt
 column1 | column2
---------+---------
 z       |  !
 m       |  #
 a       |  @
```text

```mzsql
SELECT string_agg(column1, column2)
FROM (
    SELECT column1, column2
    FROM (
        VALUES ('z', ' !'), ('a', ' @'), ('m', ' #')
    ) f ORDER BY column1 DESC
) g;
```text
```nofmt
 string_agg
------------
 a #m !z
```text

```mzsql
SELECT string_agg(b, ',' ORDER BY a DESC) FROM table;
```text


---

## SUBSTRING function


`SUBSTRING` returns a specified substring of a string.

## Signatures

This section covers signatures.

```mzsql
substring(str, start_pos)
substring(str, start_pos, len)
substring(str FROM start_pos)
substring(str FOR len)
substring(str FROM start_pos FOR len)
```text

Parameter | Type | Description
----------|------|------------
_str_ | [`string`](../../types/string) | The base string.
_start&lowbar;pos_ | [`int`](../../types/int) | The starting position for the substring; counting starts at 1.
_len_ | [`int`](../../types/int) | The length of the substring you want to return.

### Return value

`substring` returns a [`string`](../../types/string).

## Examples

This section covers examples.

```mzsql
SELECT substring('abcdefg', 3) AS substr;
```text
```nofmt
 substr
--------
 cdefg
```text

 <hr/>

```mzsql
SELECT substring('abcdefg', 3, 3) AS substr;
```text
```nofmt
 substr
--------
 cde
```text


---

## Table functions


This section covers table functions.

## Overview

[Table functions](/sql/functions/#table-functions) return multiple rows from one
input row. They are typically used in the `FROM` clause, where their arguments
are allowed to refer to columns of earlier tables in the `FROM` clause.

For example, consider the following table whose rows consist of lists of
integers:

```mzsql
CREATE TABLE quizzes(scores int list);
INSERT INTO quizzes VALUES (LIST[5, 7, 8]), (LIST[3, 3]);
```text

Query the `scores` column from the table:

```mzsql
SELECT scores
FROM quizzes;
```text

The query returns two rows, where each row is a list:

```text
 scores
---------
 {3,3}
 {5,7,8}
(2 rows)
```text

Now, apply the [`unnest`](/sql/functions/#unnest) table function to expand the
`scores` list into a collection of rows, where each row contains one list item:

```mzsql
SELECT scores, score
FROM
  quizzes,
  unnest(scores) AS score; -- In Materialize, shorthand for AS t(score)
```text

The query returns 5 rows, one row for each list item:

```text
 scores  | score
---------+-------
 {3,3}   |     3
 {3,3}   |     3
 {5,7,8} |     5
 {5,7,8} |     7
 {5,7,8} |     8
(5 rows)
```text

> **Tip:** 

For illustrative purposes, the original `scores` column is included in the
results (i.e., query projection). In practice, you generally would omit
including the original list to minimize the return data size.


## `WITH ORDINALITY`

When a table function is used in the `FROM` clause, you can add `WITH
ORDINALITY` after the table function call. `WITH ORDINALITY` adds a column that
includes the **1**-based numbering for each output row, restarting at **1** for
each input row.

The following example uses `unnest(...) WITH ORDINALITY` to include the `ordinality` column containing the **1**-based numbering of the unnested items:
```mzsql
SELECT scores, score, ordinality
FROM
  quizzes,
  unnest(scores) WITH ORDINALITY AS t(score,ordinality);
```text

The results includes the `ordinality` column:
```text
 scores  | score | ordinality
---------+-------+------------
 {3,3}   |     3 |          1
 {3,3}   |     3 |          2
 {5,7,8} |     5 |          1
 {5,7,8} |     7 |          2
 {5,7,8} |     8 |          3
(5 rows)
```bash

## Table- and column aliases

You can use table- and column aliases to name both the result column(s) of a table function as well as the ordinality column, if present. For example:
```mzsql
SELECT scores, t.score, t.listidx
FROM
  quizzes,
  unnest(scores) WITH ORDINALITY AS t(score,listidx);
```text

You can also name fewer columns in the column alias list than the number of
columns in the output of the table function (plus `WITH ORDINALITY`, if
present), in which case the extra columns retain their original names.


## `ROWS FROM`

When you select from multiple relations without specifying a relationship, you
get a cross join. This is also the case when you select from multiple table
functions in `FROM` without specifying a relationship.

For example, consider the following query that selects from two table functions
without a relationship:

```mzsql
SELECT *
FROM
  generate_series(1, 2) AS g1,
  generate_series(6, 7) AS g2;
```text

The query returns every combination of rows from both:

```text

 g1 | g2
----+----
  1 |  6
  1 |  7
  2 |  6
  2 |  7
(4 rows)
```text

Using `ROWS FROM` clause with the multiple table functions, you can zip the
outputs of the table functions (i.e., combine the n-th output row from each
table function into a single row) instead of the cross product.
That is, combine first output rows of all the table functions into the first row, the second output rows of all the table functions are combined into
a second row, and so on.

For example, modify the previous query to use `ROWS FROM` with the table
functions:

```mzsql
SELECT *
FROM
  ROWS FROM (
    generate_series(1, 2),
    generate_series(6, 7)
  ) AS t(g1, g2);
```text

Instead of the cross product, the results are the "zipped" rows:

```text
 g1 | g2
----+----
  1 |  6
  2 |  7
(2 rows)
```text

If the table functions in a `ROWS FROM` clause produce a different number of
rows, nulls are used for padding:
```mzsql
SELECT *
FROM
  ROWS FROM (
    generate_series(1, 3),  -- 3 rows
    generate_series(6, 7)   -- 2 rows
  ) AS t(g1, g2);
```text

The row with the `g1` value of 3 has a null `g2` value (note that if using psql,
psql prints null as an empty string):

```text
| g1 | g2   |
| -- | ---- |
| 3  | null |
| 1  | 6    |
| 2  | 7    |
(3 rows)
```text

For `ROWS FROM` clauses:
- you can use `WITH ORDINALITY` on the entire `ROWS FROM` clause, not on the
individual table functions within the `ROWS FROM` clause.
- you can use table- and column aliases only on the entire `ROWS FROM` clause,
not on the individual table functions within `ROWS FROM` clause.

For example:

```mzsql
SELECT *
FROM
  ROWS FROM (
    generate_series(5, 6),
    generate_series(8, 9)
  ) WITH ORDINALITY AS t(g1, g2, o);
```text

The results contain the ordinality value in the `o` column:

```text

 g1 | g2 | o
----+----+---
  5 |  8 | 1
  6 |  9 | 2
(2 rows)
```bash


## Table functions in the `SELECT` clause

You can call table functions in the `SELECT` clause. These will be executed as if they were at the end of the `FROM` clause, but their output columns will be at the appropriate position specified by their positions in the `SELECT` clause.

However, table functions in a `SELECT` clause have a number of restrictions (similar to Postgres):
- If there are multiple table functions in the `SELECT` clause, they are executed as if in an implicit `ROWS FROM` clause.
- `WITH ORDINALITY` and (explicit) `ROWS FROM` are not allowed.
- You can give a table function call a column alias, but not a table alias.
- If there are multiple output columns of a table function (e.g., `regexp_extract` has an output column per capture group), these will be combined into a single column, with a record type.

## Tabletized scalar functions

You can also call ordinary scalar functions in the `FROM` clause as if they were table functions. In that case, their output will be considered a table with a single row and column.

## See also

See a list of table functions in the [function reference](/sql/functions/#table-functions).


---

## TIMEZONE and AT TIME ZONE functions


`TIMEZONE` and `AT TIME ZONE` convert a [`timestamp`](../../types/timestamp/#timestamp-info) or a [`timestamptz`](../../types/timestamp/#timestamp-with-time-zone-info) to a different time zone.

**Known limitation:** You must explicitly cast the type for the time zone.

## Signatures

[See diagram: func-timezone.svg]

[See diagram: func-at-time-zone.svg]

Parameter | Type | Description
----------|------|------------
_zone_ | [`text`](../../types/text) | The target time zone.
_type_  |[`text`](../../types/text) or [`numeric`](../../types/numeric) |  The datatype in which the time zone is expressed
_timestamp_ | [`timestamp`](../../types/timestamp/#timestamp-info) | The timestamp without time zone.  |   |
_timestamptz_ | [`timestamptz`](../../types/timestamp/#timestamp-with-time-zone-info) | The timestamp with time zone.

## Return values

`TIMEZONE` and  `AT TIME ZONE` return [`timestamp`](../../types/timestamp/#timestamp-info) if the input is [`timestamptz`](../../types/timestamp/#timestamp-with-time-zone-info), and [`timestamptz`](../../types/timestamp/#timestamp-with-time-zone-info) if the input is [`timestamp`](../../types/timestamp/#timestamp-info).

**Note:** `timestamp` and `timestamptz` always store data in UTC, even if the date is returned as the local time.

## Examples

This section covers examples.

### Convert timestamp to another time zone, returned as UTC with offset

```mzsql
SELECT TIMESTAMP '2020-12-21 18:53:49' AT TIME ZONE 'America/New_York'::text;
```text
```text
        timezone
------------------------
2020-12-21 23:53:49+00
(1 row)
```text

```mzsql
SELECT TIMEZONE('America/New_York'::text,'2020-12-21 18:53:49');
```text
```text
        timezone
------------------------
2020-12-21 23:53:49+00
(1 row)
```bash

### Convert timestamp to another time zone, returned as specified local time

```mzsql
SELECT TIMESTAMPTZ '2020-12-21 18:53:49+08' AT TIME ZONE 'America/New_York'::text;
```text
```text
        timezone
------------------------
2020-12-21 05:53:49
(1 row)
```text

```mzsql
SELECT TIMEZONE ('America/New_York'::text,'2020-12-21 18:53:49+08');
```text
```text
        timezone
------------------------
2020-12-21 05:53:49
(1 row)
```bash

## Related topics
* [`timestamp` and `timestamp with time zone` data types](../../types/timestamp)


---

## to_char function


`to_char` converts a timestamp into a string using the specified format.

The format string can be composed of any number of [format
specifiers](#format-specifiers), interspersed with regular text. You can place a
specifier token inside of double-quotes to emit it literally.

## Examples

This section covers examples.

#### RFC 2822 format

```mzsql
SELECT to_char(TIMESTAMPTZ '2019-11-26 15:56:46 +00:00', 'Dy, Mon DD YYYY HH24:MI:SS +0000') AS formatted
```text
```nofmt
             formatted
 ---------------------------------
  Tue, Nov 26 2019 15:56:46 +0000
```bash

#### Additional non-interpreted text

Normally the `W` in "Welcome" would be converted to the week number, so we must quote it.
The "to" doesn't match any format specifiers, so quotes are optional.

```mzsql
SELECT to_char(TIMESTAMPTZ '2019-11-26 15:56:46 +00:00', '"Welcome" to Mon, YYYY') AS formatted
```text
```nofmt
       formatted
 ----------------------
  Welcome to Nov, 2019
```bash

#### Ordinal modifiers

```mzsql
SELECT to_char(TIMESTAMPTZ '2019-11-1 15:56:46 +00:00', 'Dth of Mon') AS formatted
```text
```nofmt
  formatted
 ------------
  6th of Nov
```

## Format specifiers

- **`HH`**: hour of day (01-12)
- **`HH12`**: hour of day (01-12)
- **`HH24`**: hour of day (00-23)
- **`MI`**: minute (00-59)
- **`SS`**: second (00-59)
- **`MS`**: millisecond (000-999)
- **`US`**: microsecond (000000-999999)
- **`SSSS`**: seconds past midnight (0-86399)
- **`AM`/`PM`**: uppercase meridiem indicator (without periods)
- **`am`/`pm`**: lowercase meridiem indicator (without periods)
- **`A.M.`/`P.M.`**: uppercase meridiem indicator (with periods)
- **`a.m.`/`p.m.`**: lowercase meridiem indicator (with periods)
- **`Y,YYY`**: Y,YYY year (4 or more digits) with comma
- **`YYYY`**: year (4 or more digits)
- **`YYY`**: last 3 digits of year
- **`YY`**: last 2 digits of year
- **`Y`**: last digit of year
- **`IYYY`**: ISO 8601 week-numbering year (4 or more digits)
- **`IYY`**: last 3 digits of ISO 8601 week-numbering year
- **`IY`**: last 2 digits of ISO 8601 week-numbering year
- **`I`**: last digit of ISO 8601 week-numbering year
- **`BC`/`AD`**: uppercase era indicator (without periods)
- **`bc`/`ad`**: lowercase era indicator (without periods)
- **`B.C.`/`A.D.`**: uppercase era indicator (with periods)
- **`b.c.`/`a.d.`**: lowercase era indicator (with periods)
- **`MONTH`**: full upper case month name (blank-padded to 9 chars)
- **`Month`**: full capitalized month name (blank-padded to 9 chars)
- **`month`**: full lower case month name (blank-padded to 9 chars)
- **`MON`**: abbreviated upper case month name (3 chars in English, localized lengths vary)
- **`Mon`**: abbreviated capitalized month name (3 chars in English, localized lengths vary)
- **`mon`**: abbreviated lower case month name (3 chars in English, localized lengths vary)
- **`MM`**: month number (01-12)
- **`DAY`**: full upper case day name (blank-padded to 9 chars)
- **`Day`**: full capitalized day name (blank-padded to 9 chars)
- **`day`**: full lower case day name (blank-padded to 9 chars)
- **`DY`**: abbreviated upper case day name (3 chars in English, localized lengths vary)
- **`Dy`**: abbreviated capitalized day name (3 chars in English, localized lengths vary)
- **`dy`**: abbreviated lower case day name (3 chars in English, localized lengths vary)
- **`DDD`**: day of year (001-366)
- **`IDDD`**: day of ISO 8601 week-numbering year (001-371; day 1 of the year is Monday of the first ISO week)
- **`DD`**: day of month (01-31)
- **`D`**: day of the week, Sunday (1) to Saturday (7)
- **`ID`**: ISO 8601 day of the week, Monday (1) to Sunday (7)
- **`W`**: week of month (1-5) (the first week starts on the first day of the month)
- **`WW`**: week number of year (1-53) (the first week starts on the first day of the year)
- **`IW`**: week number of ISO 8601 week-numbering year (01-53; the first Thursday of the year is in week 1)
- **`CC`**: century (2 digits) (the twenty-first century starts on 2001-01-01)
- **`J`**: Julian Day (days since November 24, 4714 BC at midnight)
- **`Q`**: quarter (ignored by to_date and to_timestamp)
- **`RM`**: month in upper case Roman numerals (I-XII; I=January)
- **`rm`**: month in lower case Roman numerals (i-xii; i=January)
- **`TZ`**: upper case time-zone name
- **`tz`**: lower case time-zone name

### Specifier modifiers

| Modifier         | Description                                            | Example | Without Modification | With Modification |
|------------------|--------------------------------------------------------|---------|----------------------|-------------------|
| `FM` prefix      | fill mode (suppress leading zeroes and padding blanks) | FMMonth | `July    `           | `July`            |
| `TH`/`th` suffix | upper/lower case ordinal number suffix                 | Dth     | `1`                  | `1st`             |