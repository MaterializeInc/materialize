---
audience: developer
canonical_url: https://materialize.com/docs/sql/types/jsonb/
complexity: intermediate
description: Expresses a JSON object
doc_type: reference
keywords:
- SELECT JSONB_ARRAY_LENGTH
- SELECT T
- SELECT JSONB_BUILD_OBJECT
- SELECT TO_JSONB
- Catalog name
- OID
- SELECT JSONB_BUILD_ARRAY
- Quick Syntax
- Size
- jsonb type
product_area: Indexes
status: stable
title: jsonb type
---

# jsonb type

## Purpose
Expresses a JSON object

If you need to understand the syntax and options for this command, you're in the right place.


Expresses a JSON object


`jsonb` data expresses a JSON object similar to
[PostgreSQL's implementation](https://www.postgresql.org/docs/current/datatype-json.html).

Detail | Info
-------|------
**Quick Syntax** | `'{"1":2,"3":4}'::jsonb`
**Size** | Variable
**Catalog name** | `pg_catalog.jsonb`
**OID** | 3802

Materialize does not yet support a type more similar to PostgreSQL's
implementation of `json`.

## Syntax

[See diagram: type-jsonb.svg]

Field | Use
------|-----
_json&lowbar;string_ | A well-formed [JSON object](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/JSON).

## `jsonb` functions + operators

Materialize supports the following operators and functions.

### Operators

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: json-operators --> --> -->

### Functions


#### Detail

Functions that return `Col`s are considered table functions and can only be used
as tables, i.e. you cannot use them as scalar values. For example, you can only
use `jsonb_object_keys` in the following way:

```mzsql
SELECT * FROM jsonb_object_keys('{"1":2,"3":4}'::jsonb);
```bash

## Details

- `jsonb` elements can be of the following types:
  - Objects
  - Arrays
  - String
  - Number
  - Boolean
  - Null
- Numbers in `jsonb` elements are all equivalent to
  [`numeric`](/sql/types/numeric) in SQL.

### Valid casts

#### From `jsonb`

You can [cast](../../functions/cast) `jsonb` to:

- [`boolean`](../boolean) (explicitly)
- [`numeric`](../numeric) (explicitly)
- [`int`](../integer) (explicitly)
- [`real`/`double precision`](../float) (explicitly)
- [`text`](../text) (by assignment) (stringifies `jsonb`)

#### To `jsonb`

You can explicitly [cast](../../functions/cast) from [`text`](../text) to `jsonb`.

#### Notes about converting `jsonb` to `text`

`jsonb` can have some odd-feeling corner cases when converting to or from
[`text`](/sql/types/text).

- `jsonb::text` always produces the printed version of the JSON.

    ```mzsql
    SELECT ('"a"'::jsonb)::text AS jsonb_elem;
    ```text
    ```nofmt
     jsonb_elem
    ------------
     "a"
    ```text

- `->>` and the `_text` functions produce the printed version of the inner
  element, unless the output is a single JSON string in which case they print it
  without quotes, i.e. as a SQL `text` value.

    ```mzsql
    SELECT ('"a"'::jsonb)->>0 AS string_elem;
    ```text
    ```nofmt
     jsonb_elem
    ------------
     a
    ```text

- `text` values passed to `to_jsonb` with quotes (`"`) produced `jsonb` strings
  with the quotes escaped.

    ```mzsql
    SELECT to_jsonb('"foo"') AS escaped_quotes;
    ```text
    ```nofmt
     escaped_quotes
    ----------------
     "\"foo\""
    ```bash

### Subscripting

You can use subscript notation (`[]`) to extract an element from a `jsonb` array
or object.

The returned value is always of type `jsonb`. If the requested array element or
object key does not exist, or if either the input value or subscript value is
`NULL`, the subscript operation returns `NULL`.

#### Arrays

To extract an element from an array, supply the 0-indexed position as the
subscript:

```mzsql
SELECT ('[1, 2, 3]'::jsonb)[1]
```text
```nofmt
 jsonb
-------
 2
```text

Negative indexes count backwards from the end of the array. [Slice syntax] is
not supported. Note also that 0-indexed positions are at variance with [`list`]
and [`array`] types, whose subscripting operation uses 1-indexed positions.

#### Objects

To extract a value from an object, supply the key as the subscript:

```mzsql
SELECT ('{"a": 1, "b": 2, "c": 3}'::jsonb)['b'];
```text
```nofmt
 jsonb
-------
 2
```text

You can chain subscript operations to retrieve deeply nested elements:

```mzsql
SELECT ('{"1": 2, "a": ["b", "c"]}'::jsonb)['a'][1];
```text
```nofmt
 jsonb
-------
 "c"
```bash

#### Remarks

Because the output type of the subscript operation is always `jsonb`, when
comparing the output of a subscript to a string, you must supply a JSON string
to compare against:

```mzsql
SELECT ('["a", "b"]::jsonb)[1] = '"b"'
```text

Note the extra double quotes on the right-hand side of the comparison.

### Parsing

<!-- JSON Parser Widget - see original docs -->

## Examples

This section covers examples.

### Operators

#### Field access as `jsonb` (`->`)

The type of JSON element you're accessing dictates the RHS's type.

- Use a `string` to return the value for a specific key:

  ```mzsql
  SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb->'1' AS field_jsonb;
  ```text
  ```nofmt
   field_jsonb
  -------------
   2
  ```text

- Use an `int` to return the value in an array at a specific index:

  ```mzsql
  SELECT '["1", "a", 2]'::jsonb->1 AS field_jsonb;
  ```text
  ```nofmt
   field_jsonb
  -------------
   "a"
  ```text
Field accessors can also be chained together.

```mzsql
SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb->'a'->1 AS field_jsonb;
```text
```nofmt
 field_jsonb
-------------
 "c"
```text

Note that all returned values are `jsonb`.

<hr/>

#### Field access as `text` (`->>`)

The type of JSON element you're accessing dictates the RHS's type.

- Use a `string` to return the value for a specific key:

  ```mzsql
  SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb->>'1' AS field_text;
  ```text
  ```nofmt
   field_text
  -------------
   2
  ```text

- Use an `int` to return the value in an array at a specific index:

  ```mzsql
  SELECT '["1", "a", 2]'::jsonb->>1 AS field_text;
  ```text
  ```nofmt
   field_text
  -------------
   a
  ```text

Field accessors can also be chained together, as long as the LHS remains
`jsonb`.

```mzsql
SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb->'a'->>1 AS field_text;
```text
```nofmt
 field_text
-------------
 c
```text

Note that all returned values are `string`.

#### Path access as `jsonb` (`#>`)

You can access specific elements in a `jsonb` value using a "path", which is a
[text array](/sql/types/array) where each element is either a field key or an
array element:

```mzsql
SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb #> '{a,1}' AS field_jsonb;
```text
```nofmt
 field_jsonb
-------------
 "c"
```text

The operator returns a value of type `jsonb`. If the path is invalid, it returns
`NULL`.

#### Path access as `text` (`#>>`)

The `#>>` operator is equivalent to the [`#>`](#path-access-as-jsonb-) operator,
except that the operator returns a value of type `text`.

```mzsql
SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb #>> '{a,1}' AS field_text;
```text
```nofmt
 field_text
-------------
 c
```text

<hr/>

#### `jsonb` concat (`||`)

```mzsql
SELECT '{"1": 2}'::jsonb ||
       '{"a": ["b", "c"]}'::jsonb AS concat;
```text
```nofmt
             concat
---------------------------------
 {"1":2,"a":["b","c"]}
```text

<hr/>

#### Remove key (`-`)

```mzsql
 SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb - 'a' AS rm_key;
```text
```nofmt
  rm_key
-----------
 {"1":2}
```text

<hr/>

#### LHS contains RHS (`@>`)

Here, the left hand side does contain the right hand side, so the result is `t` for true.

```mzsql
SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb @>
       '{"1": 2}'::jsonb AS lhs_contains_rhs;
```text
```nofmt
 lhs_contains_rhs
------------------
 t
```text

<hr/>

#### RHS contains LHS (`<@`)

Here, the right hand side does contain the left hand side, so the result is `t` for true.

```mzsql
SELECT '{"1": 2}'::jsonb <@
       '{"1": 2, "a": ["b", "c"]}'::jsonb AS lhs_contains_rhs;
```text
```nofmt
 rhs_contains_lhs
------------------
 t
```text

<hr/>

#### Search top-level keys (`?`)

```mzsql
SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb ? 'a' AS search_for_key;
```text
```nofmt
 search_for_key
----------------
 t
```text

```mzsql
SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb ? 'b' AS search_for_key;
```text
```nofmt
 search_for_key
----------------
 f
```bash

### Functions

#### `jsonb_array_elements`

##### Expanding a JSON array

```mzsql
SELECT * FROM jsonb_array_elements('[true, 1, "a", {"b": 2}, null]'::jsonb);
```text
```nofmt
   value
-----------
 true
 1.0
 "a"
 {"b":2.0}
 null
```bash

##### Flattening a JSON array

```mzsql
SELECT t.id,
       obj->>'a' AS a,
       obj->>'b' AS b
FROM (
  VALUES
    (1, '[{"a":1,"b":2},{"a":3,"b":4}]'::jsonb),
    (2, '[{"a":5,"b":6},{"a":7,"b":8}]'::jsonb)
) AS t(id, json_col)
CROSS JOIN jsonb_array_elements(t.json_col) AS obj;
```text

```nofmt
 id | a | b
----+---+---
  1 | 1 | 2
  1 | 3 | 4
  2 | 5 | 6
  2 | 7 | 8
```text

<hr/>


#### `jsonb_array_elements_text`

```mzsql
SELECT * FROM jsonb_array_elements_text('[true, 1, "a", {"b": 2}, null]'::jsonb);
```text
```nofmt
   value
-----------
 true
 1.0
 "a"
 {"b":2.0}
 null
```text

<hr/>

#### `jsonb_array_length`

```mzsql
SELECT jsonb_array_length('[true, 1, "a", {"b": 2}, null]'::jsonb);
```text
```nofmt
 jsonb_array_length
--------------------
                  5
```text

<hr/>

#### `jsonb_build_array`

```mzsql
SELECT jsonb_build_array('a', 1::float, 2.0::float, true);
```text
```nofmt
 jsonb_build_array
--------------------
 ["a",1.0,2.0,true]
```text

<hr/>

#### `jsonb_build_object`

```mzsql
SELECT jsonb_build_object(2.0::float, 'b', 'a', 1.1::float);
```text
```nofmt
 jsonb_build_object
--------------------
 {"2":"b","a":1.1}
```text

<hr/>

#### `jsonb_each`

```mzsql
SELECT * FROM jsonb_each('{"1": 2.1, "a": ["b", "c"]}'::jsonb);
```text
```nofmt
 key |   value
-----+-----------
 1   | 2.1
 a   | ["b","c"]
```text

Note that the `value` column is `jsonb`.

<hr/>

#### `jsonb_each_text`

```mzsql
SELECT * FROM jsonb_each_text('{"1": 2.1, "a": ["b", "c"]}'::jsonb);
```text
```nofmt
 key |   value
-----+-----------
 1   | 2.1
 a   | ["b","c"]
```text

Note that the `value` column is `string`.

<hr/>

#### `jsonb_object_keys`

```mzsql
SELECT * FROM jsonb_object_keys('{"1": 2, "a": ["b", "c"]}'::jsonb);
```text
```nofmt
 jsonb_object_keys
-------------------
 1
 a
```text

<hr/>

#### `jsonb_pretty`

```mzsql
SELECT jsonb_pretty('{"1": 2, "a": ["b", "c"]}'::jsonb);
```text
```nofmt
 jsonb_pretty
--------------
 {           +
   "1": 2,   +
   "a": [    +
     "b",    +
     "c"     +
   ]         +
 }
```text

<hr/>

#### `jsonb_typeof`

```mzsql
SELECT jsonb_typeof('[true, 1, "a", {"b": 2}, null]'::jsonb);
```text
```nofmt
 jsonb_typeof
--------------
 array
```text

```mzsql
SELECT * FROM jsonb_typeof('{"1": 2, "a": ["b", "c"]}'::jsonb);
```text
```nofmt
 jsonb_typeof
--------------
 object
```text

<hr/>

#### `jsonb_strip_nulls`

```mzsql
SELECT jsonb_strip_nulls('[{"1":"a","2":null},"b",null,"c"]'::jsonb);
```text
```nofmt
    jsonb_strip_nulls
--------------------------
 [{"1":"a"},"b",null,"c"]
```text

<hr/>

#### `to_jsonb`

```mzsql
SELECT to_jsonb(t) AS jsonified_row
FROM (
  VALUES
  (1, 'hey'),
  (2, NULL),
  (3, 'hi'),
  (4, 'salutations')
  ) AS t(id, content)
WHERE t.content LIKE 'h%';
```text
```nofmt
      jsonified_row
--------------------------
 {"content":"hi","id":3}
 {"content":"hey","id":1}
```

Note that the output is `jsonb`.

[Slice syntax]: /sql/types/list#slicing-ranges
[`list`]: /sql/types/list
[`array`]: /sql/types/array