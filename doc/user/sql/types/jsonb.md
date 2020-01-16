---
title: "jsonb Data Type"
description: "Expresses a JSON object"
menu:
  main:
    parent: 'sql-types'
---

`jsonb` data expresses a JavaScript Object Notation (JSON) object.

Detail | Info
-------|------
**Quick Syntax** | `'{"1":2,"3":4}'::JSONB`
**Size** | Variable

## Syntax

{{< diagram "type-jsonb.html" >}}

Field | Use
------|-----
_json&lowbar;string_ | A well-formed [JSON object](https://www.w3schools.com/js/js_json_syntax.asp).

## JSONB functions + operators

Materialize supports the following operators and functions.

### Operators

{{% json-operators %}}

### Functions

{{< fnlist "JSON" >}}

#### Detail

Functions that return `Col`s are considered table function and can only be used as tables, i.e. you cannot use them as scalar values. For example, you can only use `jsonb_object_keys` in the following way:

```sql
SELECT * FROM jsonb_object_keys('{"1":2,"3":4}'::JSONB);
```

## Details

- `jsonb` elements can be of the following types:
  - Objects
  - Arrays
  - String
  - Number
  - Boolean
  - Null
- Numbers in `jsonb` elements are all equivalent to `float` in SQL.
    - To operate on elements as `int`s, you must cast them to `float` and then to, e.g. `::float::int`.

### Valid casts

#### From `jsonb`

You can [cast](../../functions/cast) `jsonb` to:

- [`string`](../string) (stringifies `jsonb`)
- [`float`](../string)
- [`bool`](../bool)

#### To `jsonb`

You can [cast](../../functions/cast) the following types to `jsonb`:

- [`string`](../string) (parses `jsonb`)

#### Notes about converting `jsonb` to `string`/`text`

`jsonb` can have some odd-feeling corner cases when converting to or from `string` (also known as `text`).

- `jsonb::text` always produces the printed version of the JSON.

    ```sql
    SELECT ('"a"'::JSONB)::STRING AS jsonb_elem;
    ```
    ```nofmt
     jsonb_elem
    ------------
     "a"
    ```

- `->>` and the `_text` functions produce the printed version of the inner element, unless the output is a single JSON string in which case they print it without quotes, i.e. as a SQL `string`.

    ```sql
    SELECT ('"a"'::JSONB)->>0 AS string_elem;
    ```
    ```nofmt
     jsonb_elem
    ------------
     a
    ```

- `string` values passed to `to_jsonb` with quotes (`"`) produced JSONB Strings with the quotes escaped.

    ```sql
    SELECT to_jsonb('"foo"') AS escaped_quotes;
    ```
    ```nofmt
     escaped_quotes
    ----------------
     "\"foo\""
    ```

## Examples

### Operators

#### Field access as `jsonb` (`->`)

The type of JSON element you're accessing dictates the RHS's type.

- JSON objects require a `string`:

  ```sql
  SELECT '{"1": 2, "a": ["b", "c"]}'::JSONB->'1' AS field_jsonb;
  ```
  ```nofmt
   field_jsonb
  -------------
   2.0
  ```

- JSON arrays require an `int`:

  ```sql
  SELECT '["1", "a", 2]'::JSONB->1 AS field_jsonb;
  ```
  ```nofmt
   field_jsonb
  -------------
   "a"
  ```
Field accessors can also be chained together.

```sql
SELECT '{"1": 2, "a": ["b", "c"]}'::JSONB->'a'->1 AS field_jsonb;
```
```nofmt
 field_jsonb
-------------
 "c"
```

Note that all returned values are `jsonb`.

<hr/>

#### Field access as `string` (`->>`)

The type of JSON element you're accessing dictates the RHS's type.

- JSON objects require a `string`:

  ```sql
  SELECT '{"1": 2, "a": ["b", "c"]}'::JSONB->>'1' AS field_jsonb;
  ```
  ```nofmt
   field_jsonb
  -------------
   2.0
  ```

- JSON arrays require an `int`:

  ```sql
  SELECT '["1", "a", 2]'::JSONB->>1 AS field_jsonb;
  ```
  ```nofmt
   field_jsonb
  -------------
   a
  ```

Field accessors can also be chained together, as long as the LHS remains `jsonb`.

```sql
SELECT '{"1": 2, "a": ["b", "c"]}'::JSONB->'a'->>1 AS field_jsonb;
```
```nofmt
 field_jsonb
-------------
 c
```

Note that all returned values are `string`.

<hr/>

#### `jsonb` concat (`||`)

```sql
SELECT '{"1": 2}'::JSONB ||
       '{"a": ["b", "c"]}'::JSONB AS concat;
```
```nofmt
             concat
---------------------------------
 {"1":2.0,"a":["b","c"]}
```

<hr/>

#### Remove key (`-`)

```sql
 SELECT '{"1": 2, "a": ["b", "c"]}'::JSONB - 'a' AS rm_key;
```
```nofmt
  rm_key
-----------
 {"1":2.0}
```

<hr/>

#### LHS contains RHS (`@>`)

```sql
SELECT '{"1": 2, "a": ["b", "c"]}'::JSONB @>
       '{"1": 2}'::JSONB AS lhs_contains_rhs;
```
```nofmt
 lhs_contains_rhs
------------------
 t
```

<hr/>

#### RHS contains LHS (`<@`)

```sql
SELECT '{"1": 2}'::JSONB <@
       '{"1": 2, "a": ["b", "c"]}'::JSONB AS lhs_contains_rhs;
```
```nofmt
 rhs_contains_lhs
------------------
 t
```

<hr/>

#### Search top-level keys (`?`)

```sql
SELECT '{"1": 2, "a": ["b", "c"]}'::JSONB ? 'a' AS search_for_key;
```
```nofmt
 search_for_key
----------------
 t
```

```sql
SELECT '{"1": 2, "a": ["b", "c"]}'::JSONB ? 'b' AS search_for_key;
```
```nofmt
 search_for_key
----------------
 f
```

### Functions

#### `jsonb_array_elements`

```sql
SELECT * FROM jsonb_array_elements('[true, 1, "a", {"b": 2}, null]'::JSONB);
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
SELECT * FROM jsonb_array_elements_text('[true, 1, "a", {"b": 2}, null]'::JSONB);
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
SELECT jsonb_array_length('[true, 1, "a", {"b": 2}, null]'::JSONB);
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
SELECT * FROM jsonb_each('{"1": 2, "a": ["b", "c"]}'::JSONB);
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
SELECT * FROM jsonb_each_text('{"1": 2, "a": ["b", "c"]}'::JSONB);
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
SELECT * FROM jsonb_object_keys('{"1": 2, "a": ["b", "c"]}'::JSONB);
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
SELECT jsonb_pretty('{"1": 2, "a": ["b", "c"]}'::JSONB);
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
SELECT jsonb_typeof('[true, 1, "a", {"b": 2}, null]'::JSONB);
```
```nofmt
 jsonb_typeof
--------------
 array
```

```sql
SELECT * FROM jsonb_typeof('{"1": 2, "a": ["b", "c"]}'::JSONB);
```
```nofmt
 jsonb_typeof
--------------
 object
```

<hr/>

#### `jsonb_strip_nulls`

```sql
SELECT jsonb_strip_nulls('[{"1":"a","2":null},"b",null,"c"]'::JSONB);
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
