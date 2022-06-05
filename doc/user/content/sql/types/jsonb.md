---
title: "jsonb Data Type"
description: "Expresses a JSON object"
menu:
  main:
    parent: 'sql-types'
aliases:
  - /sql/types/json
---

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

{{< diagram "type-jsonb.svg" >}}

Field | Use
------|-----
_json&lowbar;string_ | A well-formed [JSON object](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/JSON).

## `jsonb` functions + operators

Materialize supports the following operators and functions.

### Operators

{{% json-operators %}}

### Functions

{{< fnlist "JSON" >}}

#### Detail

Functions that return `Col`s are considered table functions and can only be used
as tables, i.e. you cannot use them as scalar values. For example, you can only
use `jsonb_object_keys` in the following way:

```sql
SELECT * FROM jsonb_object_keys('{"1":2,"3":4}'::jsonb);
```

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

    ```sql
    SELECT ('"a"'::jsonb)::text AS jsonb_elem;
    ```
    ```nofmt
     jsonb_elem
    ------------
     "a"
    ```

- `->>` and the `_text` functions produce the printed version of the inner
  element, unless the output is a single JSON string in which case they print it
  without quotes, i.e. as a SQL `text` value.

    ```sql
    SELECT ('"a"'::jsonb)->>0 AS string_elem;
    ```
    ```nofmt
     jsonb_elem
    ------------
     a
    ```

- `text` values passed to `to_jsonb` with quotes (`"`) produced `jsonb` strings
  with the quotes escaped.

    ```sql
    SELECT to_jsonb('"foo"') AS escaped_quotes;
    ```
    ```nofmt
     escaped_quotes
    ----------------
     "\"foo\""
    ```



### Subscripting

You can use subscript notation (`[]`) to extract an element from a `jsonb` array
or object.

The returned value is always of type `jsonb`. If the requested array element or
object key does not exist, or if either the input value or subscript value is
`NULL`, the subscript operation returns `NULL`.

#### Arrays

To extract an element from an array, supply the 0-indexed position as the
subscript:

```sql
SELECT ('[1, 2, 3]'::jsonb)[1]
```
```nofmt
 jsonb
-------
 2
```

Negative indexes count backwards from the end of the array. [Slice syntax] is
not supported. Note also that 0-indexed positions are at variance with [`list`]
and [`array`] types, whose subscripting operation uses 1-indexed positions.

#### Objects

To extract a value from an object, supply the key as the subscript:

```sql
SELECT ('{"a": 1, "b": 2, "c": 3}'::jsonb)['b'];
```
```nofmt
 jsonb
-------
 2
```

You can chain subscript operations to retrieve deeply nested elements:

```sql
SELECT ('{"1": 2, "a": ["b", "c"]}'::jsonb)['a'][1];
```
```nofmt
 jsonb
-------
 "c"
```

#### Remarks

Because the output type of the subscript operation is always `jsonb`, when
comparing the output of a subscript to a string, you must supply a JSON string
to compare against:

```sql
SELECT ('["a", "b"]::jsonb)[1] = '"b"'
```

Note the extra double quotes on the right-hand side of the comparison.

## Examples

### Operators

#### Field access as `jsonb` (`->`)

The type of JSON element you're accessing dictates the RHS's type.

- JSON objects require a `string`:

  ```sql
  SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb->'1' AS field_jsonb;
  ```
  ```nofmt
   field_jsonb
  -------------
   2.0
  ```

- JSON arrays require an `int`:

  ```sql
  SELECT '["1", "a", 2]'::jsonb->1 AS field_jsonb;
  ```
  ```nofmt
   field_jsonb
  -------------
   "a"
  ```
Field accessors can also be chained together.

```sql
SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb->'a'->1 AS field_jsonb;
```
```nofmt
 field_jsonb
-------------
 "c"
```

Note that all returned values are `jsonb`.

<hr/>

#### Field access as `text` (`->>`)

The type of JSON element you're accessing dictates the RHS's type.

- JSON objects require `text`:

  ```sql
  SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb->>'1' AS field_text;
  ```
  ```nofmt
   field_text
  -------------
   2.0
  ```

- JSON arrays require an `int`:

  ```sql
  SELECT '["1", "a", 2]'::text->>1 AS field_text;
  ```
  ```nofmt
   field_text
  -------------
   a
  ```

Field accessors can also be chained together, as long as the LHS remains
`jsonb`.

```sql
SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb->'a'->>1 AS field_text;
```
```nofmt
 field_text
-------------
 c
```

Note that all returned values are `string`.

#### Path access as `jsonb` (`#>`)

You can access specific elements in a `jsonb` value using a "path", which is a
[text array](/sql/types/array) where each element is either a field key or an
array element:

```sql
SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb #> '{a,1}' AS field_jsonb;
```
```nofmt
 field_jsonb
-------------
 "c"
```

The operator returns a value of type `jsonb`. If the path is invalid, it returns
`NULL`.

#### Path access as `text` (`#>>`)

The `#>>` operator is equivalent to the [`#>`](#path-access-as-jsonb-) operator,
except that the operator returns a value of type `text`.

```sql
SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb #>> '{a,1}' AS field_text;
```
```nofmt
 field_text
-------------
 c
```

<hr/>

#### `jsonb` concat (`||`)

```sql
SELECT '{"1": 2}'::jsonb ||
       '{"a": ["b", "c"]}'::jsonb AS concat;
```
```nofmt
             concat
---------------------------------
 {"1":2.0,"a":["b","c"]}
```

<hr/>

#### Remove key (`-`)

```sql
 SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb - 'a' AS rm_key;
```
```nofmt
  rm_key
-----------
 {"1":2.0}
```

<hr/>

#### LHS contains RHS (`@>`)

```sql
SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb @>
       '{"1": 2}'::jsonb AS lhs_contains_rhs;
```
```nofmt
 lhs_contains_rhs
------------------
 t
```

<hr/>

#### RHS contains LHS (`<@`)

```sql
SELECT '{"1": 2}'::jsonb <@
       '{"1": 2, "a": ["b", "c"]}'::jsonb AS lhs_contains_rhs;
```
```nofmt
 rhs_contains_lhs
------------------
 t
```

<hr/>

#### Search top-level keys (`?`)

```sql
SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb ? 'a' AS search_for_key;
```
```nofmt
 search_for_key
----------------
 t
```

```sql
SELECT '{"1": 2, "a": ["b", "c"]}'::jsonb ? 'b' AS search_for_key;
```
```nofmt
 search_for_key
----------------
 f
```

### Functions

#### `jsonb_array_elements`

```sql
SELECT * FROM jsonb_array_elements('[true, 1, "a", {"b": 2}, null]'::jsonb);
```
```nofmt
   value
-----------
 true
 1.0
 "a"
 {"b":2.0}
 null
```

Note that the `value` column is `jsonb`.

<hr/>


#### `jsonb_array_elements_text`

```sql
SELECT * FROM jsonb_array_elements_text('[true, 1, "a", {"b": 2}, null]'::jsonb);
```
```nofmt
   value
-----------
 true
 1.0
 "a"
 {"b":2.0}
 null
```

Note that the `value` column is `string`.

<hr/>

#### `jsonb_array_length`

```sql
SELECT jsonb_array_length('[true, 1, "a", {"b": 2}, null]'::jsonb);
```
```nofmt
 jsonb_array_length
--------------------
                  5
```

<hr/>

#### `jsonb_build_array`

```sql
SELECT jsonb_build_array('a', 1::float, 2.0::float, true);
```
```nofmt
 jsonb_build_array
--------------------
 ["a",1.0,2.0,true]
```

<hr/>

#### `jsonb_build_object`

```sql
SELECT jsonb_build_object(2::float, 'b', 'a', 1::float);
```
```nofmt
 jsonb_build_object
--------------------
 {"2":true,"a":1.0}
```

<hr/>

#### `jsonb_each`

```sql
SELECT * FROM jsonb_each('{"1": 2, "a": ["b", "c"]}'::jsonb);
```
```nofmt
 key |   value
-----+-----------
 1   | 2.0
 a   | ["b","c"]
```

Note that the `value` column is `jsonb`.

<hr/>

#### `jsonb_each_text`

```sql
SELECT * FROM jsonb_each_text('{"1": 2, "a": ["b", "c"]}'::jsonb);
```
```nofmt
 key |   value
-----+-----------
 1   | 2.0
 a   | ["b","c"]
```

Note that the `value` column is `string`.

<hr/>

#### `jsonb_object_keys`

```sql
SELECT * FROM jsonb_object_keys('{"1": 2, "a": ["b", "c"]}'::jsonb);
```
```nofmt
 jsonb_object_keys
-------------------
 1
 a
```

<hr/>

#### `jsonb_pretty`

```sql
SELECT jsonb_pretty('{"1": 2, "a": ["b", "c"]}'::jsonb);
```
```nofmt
 jsonb_pretty
--------------
 {           +
   "1": 2.0, +
   "a": [    +
     "b",    +
     "c"     +
   ]         +
 }
```

<hr/>

#### `jsonb_typeof`

```sql
SELECT jsonb_typeof('[true, 1, "a", {"b": 2}, null]'::jsonb);
```
```nofmt
 jsonb_typeof
--------------
 array
```

```sql
SELECT * FROM jsonb_typeof('{"1": 2, "a": ["b", "c"]}'::jsonb);
```
```nofmt
 jsonb_typeof
--------------
 object
```

<hr/>

#### `jsonb_strip_nulls`

```sql
SELECT jsonb_strip_nulls('[{"1":"a","2":null},"b",null,"c"]'::jsonb);
```
```nofmt
    jsonb_strip_nulls
--------------------------
 [{"1":"a"},"b",null,"c"]
```

<hr/>

#### `to_jsonb`

```sql
SELECT to_jsonb('hello');
```
```nofmt
 to_jsonb
----------
 "hello"
```

Note that the output is `jsonb`.

[Slice syntax]: /sql/types/list#slicing-ranges
[`list`]: /sql/types/list
[`array`]: /sql/types/array
